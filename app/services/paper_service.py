"""Paper service: fetches normalized paper detail and structured summaries."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from app.config import DATABASE, qualify_table
from app.services.contracts import PaperDetailResponse, PaperSummaryResponse
from app.utils import connect_to_snowflake


GENERIC_SUMMARY_SNIPPETS = [
    "how can ml techniques be applied to improve state-of-the-art results?",
    "the paper presents novel approaches within the ml domain.",
    "key contributions advance the understanding of ml.",
    "this work provides meaningful progress in ml research.",
]


def _quote_ident(identifier: str) -> str:
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _resolve_table_columns(cur, table_name: str) -> Dict[str, str]:
    cur.execute(f"DESC TABLE {table_name}")
    columns = [row[0] for row in cur.fetchall() if row and row[0]]
    return {str(name).lower(): _quote_ident(str(name)) for name in columns}


def _first_sentence(text: str) -> str:
    if not text:
        return ""
    pieces = re.split(r"(?<=[.!?])\s+", text.strip())
    return pieces[0].strip() if pieces else ""


def _last_sentence(text: str) -> str:
    if not text:
        return ""
    pieces = [p.strip() for p in re.split(r"(?<=[.!?])\s+", text.strip()) if p.strip()]
    return pieces[-1] if pieces else ""


def _pick_sentence(text: str, keywords: List[str]) -> str:
    if not text:
        return ""
    pieces = [p.strip() for p in re.split(r"(?<=[.!?])\s+", text.strip()) if p.strip()]
    for sentence in pieces:
        lowered = sentence.lower()
        if any(k in lowered for k in keywords):
            return sentence
    return ""


def _fetch_silver_context_sync(paper_id: int) -> Dict[str, Any]:
    conn = connect_to_snowflake(schema="SILVER", database=DATABASE)
    cur = conn.cursor()
    try:
        silver_table = qualify_table("SILVER_PAPERS", database=DATABASE)
        col_map = _resolve_table_columns(cur, silver_table)
        required = ["id", "title", "abstract", "conclusion"]
        if any(name not in col_map for name in required):
            return {}

        select_cols = [
            f'{col_map["id"]} AS id',
            f'{col_map["title"]} AS title',
            f'{col_map["abstract"]} AS abstract',
            f'{col_map["conclusion"]} AS conclusion',
        ]
        if "tldr" in col_map:
            select_cols.append(f'{col_map["tldr"]} AS tldr')
        else:
            select_cols.append("NULL AS tldr")

        cur.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM {silver_table}
            WHERE {col_map["id"]} = %s
            LIMIT 1
            """,
            (int(paper_id),),
        )
        row = cur.fetchone()
        if not row:
            return {}
        keys = ["id", "title", "abstract", "conclusion", "tldr"]
        return dict(zip(keys, row))
    finally:
        cur.close()
        conn.close()


def _is_low_quality_summary(
    summary: PaperSummaryResponse,
    title: str = "",
    abstract: str = "",
) -> bool:
    fields: List[str] = []
    if summary.research_question:
        fields.append(summary.research_question)
    fields.extend(summary.methods or [])
    fields.extend(summary.main_claims or [])
    fields.extend(summary.key_findings or [])
    fields.extend(summary.limitations or [])
    if summary.conclusion:
        fields.append(summary.conclusion)
    flattened = " ".join(fields).strip().lower()
    if not flattened:
        return True

    if any(snippet in flattened for snippet in GENERIC_SUMMARY_SNIPPETS):
        return True

    # Detect stale generic "ML" boilerplate on clearly non-ML papers.
    if flattened.count(" ml ") + flattened.startswith("ml ") + flattened.endswith(" ml") >= 2:
        domain = f"{title} {abstract}".lower()
        ml_hints = ["machine learning", "deep learning", "transformer", "neural", "language model", "llm", "ml"]
        if not any(hint in domain for hint in ml_hints):
            return True

    return False


def _derive_summary_from_silver(
    paper_id: int,
    title: str,
    abstract: str,
    conclusion: str,
    tldr: str,
) -> PaperSummaryResponse:
    abstract = (abstract or "").strip()
    conclusion = (conclusion or "").strip()
    tldr = (tldr or "").strip()
    context = "\n".join([p for p in [abstract, tldr, conclusion] if p])

    research_question = _pick_sentence(
        context,
        ["investigate", "study", "examine", "analyze", "measure", "what", "whether"],
    ) or _first_sentence(abstract) or ""
    methods_sentence = _pick_sentence(
        context,
        ["method", "approach", "analysis", "experiment", "dataset", "model", "using", "fit", "estimate"],
    )
    findings_sentence = _pick_sentence(
        context,
        ["find", "result", "show", "observe", "improv", "constrain", "evidence", "demonstrate"],
    ) or _first_sentence(tldr)
    conclusion_text = tldr or conclusion or _last_sentence(abstract)

    return PaperSummaryResponse(
        paper_id=int(paper_id),
        research_question=research_question or None,
        methods=[methods_sentence] if methods_sentence else [],
        main_claims=[],
        key_findings=[findings_sentence] if findings_sentence else [],
        limitations=[],
        conclusion=conclusion_text or None,
    )


def _get_paper_detail_sync(paper_id: int) -> PaperDetailResponse:
    conn = connect_to_snowflake(schema="SILVER", database=DATABASE)
    cur = conn.cursor()
    try:
        silver_table = qualify_table("SILVER_PAPERS", database=DATABASE)
        bronze_table = qualify_table("BRONZE_PAPERS", database=DATABASE)
        cur.execute(
            f"""
            SELECT
                p."id",
                p."title",
                p."abstract",
                p."arxiv_id",
                b."raw_payload"
            FROM {silver_table} p
            LEFT JOIN {bronze_table} b
              ON b."raw_payload":entry_id::STRING = CONCAT('https://arxiv.org/abs/', p."arxiv_id")
            WHERE p."id" = %s
            LIMIT 1
            """,
            (int(paper_id),),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Paper {paper_id} not found")

        pid, title, abstract, arxiv_id, raw_payload = row
        payload: dict = {}
        if isinstance(raw_payload, str):
            try:
                payload = json.loads(raw_payload)
            except Exception:
                payload = {}
        elif isinstance(raw_payload, dict):
            payload = raw_payload

        authors_raw = payload.get("authors") if payload else None
        if isinstance(authors_raw, list):
            authors: List[str] = [str(a) for a in authors_raw if a]
        else:
            authors = []

        year = payload.get("year") if payload else None
        if year is None and payload.get("published"):
            try:
                year = int(str(payload["published"])[:4])
            except Exception:
                year = None

        citations = payload.get("citationCount") if payload else None

        return PaperDetailResponse(
            paper_id=int(pid),
            title=title or "Untitled",
            authors=authors,
            year=int(year) if year is not None else None,
            citations=int(citations) if citations is not None else None,
            arxiv_id=arxiv_id,
            abstract=abstract,
        )
    finally:
        cur.close()
        conn.close()


def _get_paper_summary_sync(paper_id: int) -> Optional[PaperSummaryResponse]:
    conn = connect_to_snowflake(schema="GOLD", database=DATABASE)
    cur = conn.cursor()
    try:
        summaries_table = qualify_table("GOLD_PAPER_SUMMARIES", database=DATABASE)
        cur.execute(
            f"""
            SELECT "summary_json"
            FROM {summaries_table}
            WHERE "paper_id" = %s
            LIMIT 1
            """,
            (int(paper_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        summary_json = row[0]
        if isinstance(summary_json, str):
            try:
                summary_json = json.loads(summary_json)
            except Exception:
                summary_json = {}
        if not isinstance(summary_json, dict):
            summary_json = {}
        return PaperSummaryResponse(
            paper_id=int(paper_id),
            research_question=summary_json.get("research_question"),
            methods=summary_json.get("methods") or [],
            main_claims=summary_json.get("main_claims") or [],
            key_findings=summary_json.get("key_findings") or [],
            limitations=summary_json.get("limitations") or [],
            conclusion=summary_json.get("conclusion"),
        )
    finally:
        cur.close()
        conn.close()


async def get_paper_detail(paper_id: int) -> PaperDetailResponse:
    """Fetch normalized paper metadata from Silver + Bronze layers."""
    return _get_paper_detail_sync(paper_id)


async def get_paper_summary(paper_id: int) -> PaperSummaryResponse:
    """Fetch structured summary from Gold layer; generate on demand if missing."""
    silver_context = _fetch_silver_context_sync(paper_id)
    title = (silver_context.get("title") or "") if silver_context else ""
    abstract = (silver_context.get("abstract") or "") if silver_context else ""
    conclusion = (silver_context.get("conclusion") or "") if silver_context else ""
    tldr = (silver_context.get("tldr") or "") if silver_context else ""

    result = _get_paper_summary_sync(paper_id)
    if result is not None and not _is_low_quality_summary(result, title=title, abstract=abstract):
        return result

    # On-demand generation/regeneration via summary worker
    from app.workers.summary_worker import generate_paper_summary
    gen_result = await generate_paper_summary.remote.aio(
        paper_id=paper_id,
        database=DATABASE,
        force=result is not None,
    )
    if isinstance(gen_result, dict) and gen_result.get("status") == "ok":
        refreshed = _get_paper_summary_sync(paper_id)
        if refreshed is not None and not _is_low_quality_summary(refreshed, title=title, abstract=abstract):
            return refreshed

    # Deterministic fallback from Silver for reliability (never return stale generic boilerplate).
    if silver_context:
        fallback = _derive_summary_from_silver(
            paper_id=paper_id,
            title=title,
            abstract=abstract,
            conclusion=conclusion,
            tldr=tldr,
        )
        if not _is_low_quality_summary(fallback, title=title, abstract=abstract):
            return fallback

    raise HTTPException(status_code=404, detail=f"Summary not available for paper {paper_id}")
