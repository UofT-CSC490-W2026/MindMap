"""Paper service: fetches normalized paper detail and structured summaries."""

from __future__ import annotations

import json
from typing import List

from fastapi import HTTPException

from app.config import DATABASE, qualify_table
from app.services.contracts import PaperDetailResponse, PaperSummaryResponse
from app.utils import connect_to_snowflake


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


def _get_paper_summary_sync(paper_id: int) -> PaperSummaryResponse:
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
    result = _get_paper_summary_sync(paper_id)
    if result is not None:
        return result

    # On-demand generation via summary worker
    from app.workers.summary_worker import generate_paper_summary
    gen_result = await generate_paper_summary.remote.aio(paper_id=paper_id, database=DATABASE)
    if not isinstance(gen_result, dict) or gen_result.get("status") not in {"ok"}:
        raise HTTPException(status_code=404, detail=f"Summary not available for paper {paper_id}")

    result = _get_paper_summary_sync(paper_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Summary not available for paper {paper_id}")
    return result
