# Worker to split papers into sections and chunks for RAG.
# Prefers full-paper text when available and falls back to abstract/conclusion.

from typing import List, Dict, Any, Optional
import logging
import re

from utils import connect_to_snowflake
from config import app, image, snowflake_secret, DATABASE, qualify_table

WORDS_PER_CHUNK = 500
WORDS_PER_CHUNK_MAX = 800
CHUNK_OVERLAP_WORDS = 40
MAX_SECTION_WORDS = 25000
MAX_FULL_TEXT_WORDS = 30000
STATEMENT_TIMEOUT_SECONDS = 120

logger = logging.getLogger(__name__)


def _silver_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPERS", database=database)


def _sections_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPER_SECTIONS", database=database)


def _chunks_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPER_CHUNKS", database=database)


def _estimate_word_count(text: Optional[str]) -> int:
    """Rough word count estimate for splitting logic."""
    if not text:
        return 0
    return len(text.split())


def _truncate_words(text: str, max_words: int) -> str:
    words = (text or "").split()
    if len(words) <= max_words:
        return (text or "").strip()
    return " ".join(words[:max_words]).strip()


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _canonical_section_name(name: str) -> str:
    lowered = (name or "").strip().lower()
    if lowered.startswith("abstract"):
        return "abstract"
    if lowered in {"method", "methods", "methodology", "approach", "experimental setup", "experiments"}:
        return "methods"
    if lowered in {"result", "results", "evaluation", "discussion"}:
        return "results"
    if lowered in {"conclusion", "concluding remarks", "summary and discussion"}:
        return "conclusion"
    if lowered in {"introduction", "background", "related work"}:
        return lowered
    if lowered.startswith("limitation"):
        return "limitations"
    return "body"


def _split_full_text_into_sections(full_text: str) -> List[Dict[str, str]]:
    text = _normalize_text(full_text)
    if not text:
        return []

    header_pattern = re.compile(
        r"(?im)^(?:\d+(?:\.\d+)*)?\s*(abstract|introduction|background|related work|method|methods|methodology|approach|experimental setup|experiments|evaluation|results|discussion|limitations|conclusion|concluding remarks|summary and discussion)\s*$"
    )
    matches = list(header_pattern.finditer(text))
    if not matches:
        return [{"section_name": "body", "content": _truncate_words(text, MAX_FULL_TEXT_WORDS)}]

    sections: List[Dict[str, str]] = []
    for idx, match in enumerate(matches):
        section_name = _canonical_section_name(match.group(1))
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        content = _normalize_text(text[start:end])
        if not content:
            continue
        content = _truncate_words(content, MAX_SECTION_WORDS)
        sections.append({"section_name": section_name, "content": content})

    return sections


def _build_sections_for_paper(paper: Dict[str, Any]) -> List[Dict[str, str]]:
    full_text = _truncate_words(_normalize_text(paper.get("full_text") or ""), MAX_FULL_TEXT_WORDS)
    abstract = _normalize_text(paper.get("abstract") or "")
    conclusion = _normalize_text(paper.get("conclusion") or "")

    sections: List[Dict[str, str]] = []
    seen_pairs = set()

    if full_text:
        for section in _split_full_text_into_sections(full_text):
            key = (section["section_name"], section["content"][:200])
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            sections.append(section)

    if abstract:
        key = ("abstract", abstract[:200])
        if key not in seen_pairs:
            sections.insert(0, {"section_name": "abstract", "content": _truncate_words(abstract, MAX_SECTION_WORDS)})
            seen_pairs.add(key)

    if conclusion:
        key = ("conclusion", conclusion[:200])
        if key not in seen_pairs:
            sections.append({"section_name": "conclusion", "content": _truncate_words(conclusion, MAX_SECTION_WORDS)})

    if not sections:
        fallback_parts = []
        if abstract:
            fallback_parts.append(f"Abstract\n{abstract}")
        if conclusion:
            fallback_parts.append(f"Conclusion\n{conclusion}")
        fallback_text = "\n\n".join(fallback_parts).strip()
        if fallback_text:
            sections.append({"section_name": "body", "content": _truncate_words(fallback_text, MAX_SECTION_WORDS)})

    return sections


def _split_into_chunks(
    text: str,
    target_words: int = WORDS_PER_CHUNK,
    max_words: int = WORDS_PER_CHUNK_MAX,
    overlap_words: int = CHUNK_OVERLAP_WORDS,
) -> List[str]:
    """
    Split text into roughly equal chunks by word count.
    Simple sentence-aware splitting with minimal sentence breaking.
    """
    if not text or not text.strip():
        return []

    words = text.split()
    if len(words) <= target_words:
        return [text]

    chunks = []
    i = 0
    overlap_buffer = []

    while i < len(words):
        chunk_words = words[i : i + target_words]
        chunk_text = " ".join(chunk_words)

        if len(chunk_words) < target_words and i + target_words < len(words):
            words_left = len(words) - i
            if words_left > max_words:
                pass
            else:
                chunk_words = words[i:]
                chunk_text = " ".join(chunk_words)

        if chunk_text.strip():
            chunks.append(chunk_text)

        overlap_buffer = chunk_words[-overlap_words:] if len(chunk_words) > overlap_words else chunk_words
        i += len(chunk_words) - len(overlap_buffer)

    return chunks


def _fetch_unchunked_papers(cur, database: str = DATABASE, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch papers that do not yet have sections/chunks.
    """
    silver = _silver_table(database=database)
    sections = _sections_table(database=database)

    cur.execute(
        f"""
        SELECT
          sp.id,
          sp.arxiv_id,
          sp.title,
          sp.abstract,
          sp.conclusion,
          sp.full_text
        FROM {silver} sp
        LEFT JOIN {sections} sec
          ON sec.paper_id = sp.id
        WHERE sec.section_id IS NULL
          AND (
            (sp.full_text IS NOT NULL AND LENGTH(TRIM(sp.full_text)) > 0)
            OR sp.abstract IS NOT NULL
            OR sp.conclusion IS NOT NULL
          )
        LIMIT {int(limit)}
        """
    )
    rows = cur.fetchall()
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _insert_section_and_chunks(
    cur,
    database: str,
    paper_id: int,
    section_name: str,
    section_index: int,
    content: str,
) -> tuple[int, int]:
    """
    Insert a single section and its chunks into the database.
    Returns (section_id, chunks_inserted).
    """
    sections = _sections_table(database=database)
    chunks = _chunks_table(database=database)

    word_count = _estimate_word_count(content)

    cur.execute(
        f"""
        INSERT INTO {sections}
        (paper_id, section_name, section_order, content, token_estimate)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (int(paper_id), section_name, int(section_index), content, int(word_count)),
    )
    cur.execute(
        f"""
        SELECT section_id
        FROM {sections}
        WHERE paper_id = %s AND section_name = %s
        ORDER BY section_id DESC
        LIMIT 1
        """,
        (int(paper_id), section_name),
    )
    section_id = cur.fetchone()[0]

    split_chunks = _split_into_chunks(
        content,
        target_words=WORDS_PER_CHUNK,
        max_words=WORDS_PER_CHUNK_MAX,
        overlap_words=CHUNK_OVERLAP_WORDS,
    )

    if split_chunks:
        chunk_rows = []
        for chunk_idx, chunk_text in enumerate(split_chunks):
            chunk_word_count = _estimate_word_count(chunk_text)
            chunk_rows.append(
                (
                    int(paper_id),
                    int(section_id),
                    int(chunk_idx),
                    chunk_text,
                    int(chunk_word_count),
                    section_name,
                )
            )
        
        # Insert chunks in smaller batches (max 5 at a time) to avoid stalling on large inserts
        batch_size = 5
        for batch_start in range(0, len(chunk_rows), batch_size):
            batch_end = min(batch_start + batch_size, len(chunk_rows))
            batch_to_insert = chunk_rows[batch_start:batch_end]
            logger.debug(
                f"Paper {paper_id}, section '{section_name}': inserting chunks {batch_start}-{batch_end-1}/{len(chunk_rows)}"
            )
            cur.executemany(
                f"""
                INSERT INTO {chunks}
                (paper_id, section_id, chunk_index, chunk_text, token_estimate, chunk_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                batch_to_insert,
            )

    logger.info(
        f"Paper {paper_id}: section '{section_name}' split into {len(split_chunks)} chunks"
    )
    return int(section_id), len(split_chunks)


@app.function(image=image, secrets=[snowflake_secret], timeout=60 * 45)
def chunk_papers(limit: int = 100, database: str = DATABASE) -> Dict[str, Any]:
    """
    Split papers into sections and chunks for RAG retrieval.

    - Reads papers from SILVER_PAPERS that do not yet have chunks
    - Prefers full-paper text when available
    - Falls back to abstract + conclusion if full text is unavailable
    - Splits each section into ~500-word chunks with 40-word overlap
    - Writes to SILVER_PAPER_SECTIONS and SILVER_PAPER_CHUNKS tables
    - Idempotent: reruns only process new papers
    """
    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()

    try:
        # Prevent a single pathological query from hanging the entire run.
        cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {int(STATEMENT_TIMEOUT_SECONDS)}")

        papers_to_chunk = _fetch_unchunked_papers(cur, database=database, limit=limit)
        if not papers_to_chunk:
            return {"status": "ok", "papers_chunked": 0, "note": "No new papers to chunk."}

        total_sections = 0
        total_chunks = 0
        skipped_papers = 0
        skipped_sections = 0

        for idx, paper in enumerate(papers_to_chunk, start=1):
            paper_id = int(paper["id"])
            arxiv_id = paper.get("arxiv_id", "unknown")
            sections_to_insert = _build_sections_for_paper(paper)

            try:
                if not sections_to_insert:
                    skipped_papers += 1
                    logger.warning(f"Skipping paper {arxiv_id} (id={paper_id}) because no usable text was found")
                    continue

                for section_idx, section in enumerate(sections_to_insert):
                    content = section["content"]
                    section_name = section["section_name"]
                    if _estimate_word_count(content) > MAX_SECTION_WORDS:
                        skipped_sections += 1
                        logger.warning(
                            f"Skipping oversized {section_name} section for paper {arxiv_id} (id={paper_id})"
                        )
                        continue

                    _, added_chunks = _insert_section_and_chunks(
                        cur,
                        database=database,
                        paper_id=paper_id,
                        section_name=section_name,
                        section_index=section_idx,
                        content=content,
                    )
                    total_sections += 1
                    total_chunks += added_chunks

                conn.commit()
                logger.info(f"Paper {arxiv_id} (id={paper_id}): chunking complete")
            except Exception as paper_err:
                skipped_papers += 1
                conn.rollback()
                logger.warning(
                    f"Skipping problematic paper {arxiv_id} (id={paper_id}) due to chunking error: {paper_err}"
                )
                continue

        return {
            "status": "ok",
            "papers_chunked": len(papers_to_chunk) - skipped_papers,
            "papers_skipped": skipped_papers,
            "sections_created": total_sections,
            "sections_skipped": skipped_sections,
            "chunks_created": total_chunks,
            "database": database,
        }

    except Exception as e:
        logger.error(f"Error in chunk_papers: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
