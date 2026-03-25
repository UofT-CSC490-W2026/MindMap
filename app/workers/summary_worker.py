"""Worker for generating structured summaries of papers using LLM."""

import json
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

# Allow running this worker from either project root or app directory.
APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from utils import connect_to_snowflake
from config import app, llm_image, openai_secret, snowflake_secret, DATABASE, SCHEMA, qualify_table
from services.llm_client import LLMClient
from services.summary_schema import PaperSummary, SummaryContext

logger = logging.getLogger(__name__)


def _papers_table(database: str = DATABASE, schema: str = SCHEMA) -> str:
    return qualify_table("SILVER_PAPERS", database=database, schema=schema)


def _chunks_table(database: str = DATABASE, schema: str = SCHEMA) -> str:
    return qualify_table("SILVER_PAPER_CHUNKS", database=database, schema=schema)


def _summaries_table(database: str = DATABASE, schema: str = SCHEMA) -> str:
    return qualify_table("GOLD_PAPER_SUMMARIES", database=database, schema=schema)


def _evidence_table(database: str = DATABASE, schema: str = SCHEMA) -> str:
    return qualify_table("GOLD_SUMMARY_EVIDENCE", database=database, schema=schema)


def _fetch_unchunked_papers(
    cur,
    database: str = DATABASE,
    schema: str = SCHEMA,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Fetch papers that do not yet have summaries.
    Returns papers with both id and basic metadata.
    """
    papers = _papers_table(database=database, schema=schema)
    summaries = _summaries_table(database=database, schema=schema)
    
    cur.execute(
        f"""
        SELECT sp.id, sp.arxiv_id, sp.title, sp.abstract
        FROM {papers} sp
        LEFT JOIN {summaries} gs
            ON gs.paper_id = sp.id
        WHERE gs.paper_id IS NULL
        LIMIT {int(limit)}
        """
    )
    rows = cur.fetchall()
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _fetch_paper_chunks(
    cur,
    paper_id: int,
    database: str = DATABASE,
    schema: str = SCHEMA,
    limit: int = 18,
    max_context_chars: int = 32000,
) -> List[Dict[str, Any]]:
    """
    Fetch a bounded, prioritized chunk set for summary generation.
    This keeps LLM context size under control for long papers.
    """
    chunks = _chunks_table(database=database, schema=schema)
    
    cur.execute(
        f"""
        SELECT chunk_id, chunk_text, chunk_type, token_estimate
        FROM {chunks}
        WHERE paper_id = %s
        ORDER BY
            CASE
                WHEN LOWER(chunk_type) = 'abstract' THEN 0
                WHEN LOWER(chunk_type) = 'methods' THEN 1
                WHEN LOWER(chunk_type) = 'results' THEN 2
                WHEN LOWER(chunk_type) = 'conclusion' THEN 3
                WHEN LOWER(chunk_type) = 'discussion' THEN 4
                ELSE 5
            END,
            token_estimate DESC,
            section_id,
            chunk_index
        LIMIT {int(limit * 3)}
        """,
        (int(paper_id),),
    )
    rows = cur.fetchall()
    
    if not rows:
        return []

    selected = []
    current_chars = 0
    for chunk_id, chunk_text, chunk_type, token_estimate in rows:
        text = (chunk_text or "").strip()
        if not text:
            continue
        if selected and (current_chars + len(text)) > max_context_chars:
            break
        selected.append(
            {
                "chunk_id": int(chunk_id),
                "chunk_text": text,
                "chunk_type": (chunk_type or "body").strip(),
                "token_estimate": int(token_estimate or 0),
            }
        )
        current_chars += len(text)
        if len(selected) >= int(limit):
            break

    return selected


def _estimate_token_count(text: str) -> int:
    return max(1, len(text) // 4)


def _insert_summary(
    cur,
    paper_id: int,
    summary: PaperSummary,
    model_name: str = "gpt-4o-mini",
    prompt_version: str = "v1",
    database: str = DATABASE,
    schema: str = SCHEMA,
) -> None:
    """Write summary to GOLD_PAPER_SUMMARIES table."""
    summaries = _summaries_table(database=database, schema=schema)
    summary_json = json.dumps(summary.to_dict())

    cur.execute(
        f"""
        MERGE INTO {summaries} t
        USING (
            SELECT
                %s AS paper_id,
                PARSE_JSON(%s) AS summary_json,
                %s AS model_name,
                %s AS prompt_version
        ) s
        ON t.paper_id = s.paper_id
        WHEN MATCHED THEN UPDATE SET
            t.summary_json = s.summary_json,
            t.model_name = s.model_name,
            t.prompt_version = s.prompt_version,
            t.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (paper_id, summary_json, model_name, prompt_version)
            VALUES (s.paper_id, s.summary_json, s.model_name, s.prompt_version)
        """,
        (int(paper_id), summary_json, model_name, prompt_version),
    )
    logger.info(f"Upserted summary for paper {paper_id}")


def _insert_evidence(
    cur,
    paper_id: int,
    chunk_ids: List[int],
    database: str = DATABASE,
    schema: str = SCHEMA,
) -> None:
    """
    Write evidence mapping from summary fields to chunks.
    
    For v1, we use a simple strategy:
    - All chunks are marked as evidence for all fields
    - Primary evidence is first chunk (evidence_rank = 1)
    - Secondary is next chunks (evidence_rank = 2, 3, ...)
    
    TODO: In future versions, perform field-level attribution using LLM
    """
    if not chunk_ids:
        logger.debug(f"No chunks found for paper {paper_id}, skipping evidence")
        return
    
    evidence = _evidence_table(database=database, schema=schema)

    # Rerunnable behavior: replace old evidence for this paper.
    cur.execute(
        f"""
        DELETE FROM {evidence}
        WHERE paper_id = %s
        """,
        (int(paper_id),),
    )
    
    # For v1: all chunks are equal evidence across all fields
    fields = [
        "research_question",
        "methods",
        "main_claims",
        "key_findings",
        "limitations",
        "conclusion",
    ]
    
    rows_to_insert = []
    for field in fields:
        for rank, chunk_id in enumerate(chunk_ids, start=1):
            rows_to_insert.append(
                (int(paper_id), field, int(chunk_id), int(rank))
            )
    
    if rows_to_insert:
        cur.executemany(
            f"""
            INSERT INTO {evidence}
            (paper_id, summary_field, chunk_id, evidence_rank)
            VALUES (%s, %s, %s, %s)
            """,
            rows_to_insert,
        )
        logger.debug(f"Inserted {len(rows_to_insert)} evidence rows for paper {paper_id}")


@app.function(image=llm_image, secrets=[snowflake_secret, openai_secret], timeout=60 * 15)
def generate_paper_summary(
    paper_id: int,
    model_name: str = "gpt-4o-mini",
    prompt_version: str = "v1",
    force: bool = False,
    database: str = DATABASE,
    schema: str = SCHEMA,
) -> Dict[str, Any]:
    """
    Generate a structured summary for a single paper.
    
    Args:
        paper_id: SILVER_PAPERS.id
        model_name: LLM model to use
        prompt_version: Prompt template version
        force: If True, overwrite existing summary; otherwise skip
        database: Snowflake database
        schema: Snowflake schema
        
    Returns:
        Dictionary with status, summary, and metadata
    """
    conn = connect_to_snowflake(database=database, schema=schema)
    cur = conn.cursor()
    
    try:
        # Check if summary already exists
        if not force:
            summaries = _summaries_table(database=database, schema=schema)
            cur.execute(
                f"""
                SELECT paper_id FROM {summaries} WHERE paper_id = %s LIMIT 1
                """,
                (int(paper_id),),
            )
            if cur.fetchone():
                logger.info(f"Summary already exists for paper {paper_id}, skipping (use force=True to overwrite)")
                return {
                    "status": "skipped",
                    "paper_id": paper_id,
                    "reason": "summary_already_exists",
                }
        
        # Fetch paper metadata
        papers = _papers_table(database=database, schema=schema)
        cur.execute(
            f"""
            SELECT id, title, abstract FROM {papers} WHERE id = %s
            """,
            (int(paper_id),),
        )
        result = cur.fetchone()
        if not result:
            logger.error(f"Paper {paper_id} not found in {papers}")
            return {
                "status": "error",
                "paper_id": paper_id,
                "error": "paper_not_found",
            }
        
        paper_title = result[1]
        
        logger.info(f"Paper {paper_id}: generating summary")

        # Fetch prioritized chunks
        chunk_rows = _fetch_paper_chunks(
            cur,
            paper_id=paper_id,
            database=database,
            schema=schema,
        )
        
        if not chunk_rows:
            logger.warning(f"No chunks found for paper {paper_id}, returning empty summary")
            return {
                "status": "error",
                "paper_id": paper_id,
                "error": "no_chunks_found",
            }

        chunk_texts = [row["chunk_text"] for row in chunk_rows]
        chunk_ids = [row["chunk_id"] for row in chunk_rows]
        chunk_types = [row["chunk_type"] for row in chunk_rows]

        # Build context
        context = SummaryContext(
            paper_title=paper_title,
            chunks=chunk_texts,
            chunk_ids=chunk_ids,
            chunk_types=chunk_types,
        )
        context_string = context.build_context_string()
        estimated_tokens = _estimate_token_count(context_string)

        logger.info(
            f"Paper {paper_id}: summary context ready "
            f"(chunks={len(chunk_rows)}, est_tokens={estimated_tokens})"
        )
        
        # Call LLM
        try:
            llm = LLMClient(model=model_name)
            summary_result = llm.generate_structured_summary(
                context=context_string,
                title=paper_title,
                prompt_version=prompt_version,
                retry_count=2,
            )

            summary = summary_result["result"]
            logger.info(
                f"Paper {paper_id}: LLM summary validation passed "
                f"(attempts={summary_result['attempts']}, usage={summary_result.get('usage', {})})"
            )
        
        except Exception as llm_err:
            logger.error(f"Paper {paper_id}: LLM summary generation failed: {llm_err}")
            return {
                "status": "error",
                "paper_id": paper_id,
                "error": f"llm_generation_failed: {str(llm_err)[:100]}",
            }
        
        # Insert summary
        _insert_summary(
            cur,
            paper_id=paper_id,
            summary=summary,
            model_name=model_name,
            prompt_version=prompt_version,
            database=database,
            schema=schema,
        )
        
        # Insert evidence
        _insert_evidence(
            cur,
            paper_id=paper_id,
            chunk_ids=chunk_ids,
            database=database,
            schema=schema,
        )
        
        conn.commit()
        
        return {
            "status": "ok",
            "paper_id": paper_id,
            "chunks_used": len(chunk_rows),
            "estimated_input_tokens": estimated_tokens,
            "summary_fields_populated": sum(
                1
                for field in [
                    summary.research_question,
                    summary.methods,
                    summary.main_claims,
                    summary.key_findings,
                    summary.limitations,
                    summary.conclusion,
                ]
                if field
            ),
        }
    
    except Exception as e:
        logger.error(f"Error summarizing paper {paper_id}: {e}")
        conn.rollback()
        return {
            "status": "error",
            "paper_id": paper_id,
            "error": str(e)[:200],
        }
    
    finally:
        cur.close()
        conn.close()


@app.function(image=llm_image, secrets=[snowflake_secret, openai_secret], timeout=60 * 60)
def batch_summarize_papers(
    limit: int = 100,
    model_name: str = "gpt-4o-mini",
    prompt_version: str = "v1",
    database: str = DATABASE,
    schema: str = SCHEMA,
) -> Dict[str, Any]:
    """
    Generate summaries for all unsummarized papers.
    
    Args:
        limit: Maximum number of papers to summarize
        model_name: LLM model to use
        prompt_version: Prompt template version
        database: Snowflake database
        schema: Snowflake schema
        
    Returns:
        Summary statistics of the batch run
    """
    conn = connect_to_snowflake(database=database, schema=schema)
    cur = conn.cursor()
    
    try:
        # Fetch papers without summaries
        papers_to_summarize = _fetch_unchunked_papers(
            cur,
            database=database,
            schema=schema,
            limit=limit,
        )
        
        if not papers_to_summarize:
            logger.info("No papers to summarize")
            return {
                "status": "ok",
                "papers_to_summarize": 0,
                "papers_successful": 0,
                "papers_failed": 0,
            }
        
        logger.info(f"Summarizing {len(papers_to_summarize)} papers")
        
        successful = 0
        failed = 0
        errors = []
        
        for idx, paper in enumerate(papers_to_summarize, start=1):
            paper_id = int(paper["id"])
            arxiv_id = paper.get("arxiv_id", "unknown")
            title = paper.get("title", "unknown")
            
            logger.info(f"[{idx}/{len(papers_to_summarize)}] Summarizing paper {arxiv_id} (id={paper_id})")
            
            try:
                result = generate_paper_summary.remote(
                    paper_id=paper_id,
                    model_name=model_name,
                    prompt_version=prompt_version,
                    database=database,
                    schema=schema,
                )
                
                if result["status"] == "ok":
                    successful += 1
                    logger.info(f"✓ Paper {arxiv_id} summarized successfully")
                else:
                    failed += 1
                    errors.append(f"Paper {arxiv_id}: {result.get('error', 'unknown')}")
                    logger.warning(f"✗ Paper {arxiv_id} failed: {result.get('error', 'unknown')}")
            
            except Exception as e:
                failed += 1
                errors.append(f"Paper {arxiv_id}: {str(e)[:100]}")
                logger.error(f"✗ Paper {arxiv_id} error: {e}")
        
        return {
            "status": "ok",
            "papers_to_summarize": len(papers_to_summarize),
            "papers_successful": successful,
            "papers_failed": failed,
            "success_rate": successful / len(papers_to_summarize) if papers_to_summarize else 0,
            "errors": errors[:10],  # First 10 errors
        }
    
    except Exception as e:
        logger.error(f"Batch summarization failed: {e}")
        return {
            "status": "error",
            "error": str(e)[:200],
        }
    
    finally:
        cur.close()
        conn.close()
