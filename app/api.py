"""
API endpoints for the MindMap application.

"""

from __future__ import annotations

import json
import os
import modal
import asyncio
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.utils import connect_to_snowflake
from app.config import DATABASE, qualify_table
from app.jobs import run_post_bronze_job
from app.workers.ingestion import ingest_single_paper


api = FastAPI(title="MindMap API", version="0.1.0")

api.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@api.get("/papers")
async def get_papers():
    conn = connect_to_snowflake(schema="SILVER")
    cur = conn.cursor()
    try:
        silver_table = qualify_table("SILVER_PAPERS", database=DATABASE)
        bronze_table = qualify_table("BRONZE_PAPERS", database=DATABASE)
        clusters_table = qualify_table("GOLD_PAPER_CLUSTERS", database=DATABASE)
        cur.execute(f"SHOW TABLES LIKE 'GOLD_PAPER_CLUSTERS' IN SCHEMA {DATABASE}.GOLD")
        has_clusters_table = cur.fetchone() is not None

        if has_clusters_table:
            cur.execute("""
                SELECT 
                    p."id",
                    p."title",
                    p."abstract",
                    p."arxiv_id",
                    b."raw_payload",
                    b."raw_payload":citationCount::INT AS citation_count,
                    b."raw_payload":year::INT AS year,
                    c."cluster_id",
                    c."cluster_name",
                    c."cluster_description"
                FROM {silver_table} p
                LEFT JOIN {bronze_table} b
                  ON b."raw_payload":entry_id::STRING = CONCAT('https://arxiv.org/abs/', p."arxiv_id")
                LEFT JOIN {clusters_table} c
                  ON c."paper_id" = p."id"
                LIMIT 100
            """.format(
                silver_table=silver_table,
                bronze_table=bronze_table,
                clusters_table=clusters_table,
            ))
        else:
            cur.execute("""
                SELECT 
                    p."id",
                    p."title",
                    p."abstract",
                    p."arxiv_id",
                    b."raw_payload",
                    b."raw_payload":citationCount::INT AS citation_count,
                    b."raw_payload":year::INT AS year,
                    NULL AS "cluster_id",
                    NULL AS "cluster_name",
                    NULL AS "cluster_description"
                FROM {silver_table} p
                LEFT JOIN {bronze_table} b
                  ON b."raw_payload":entry_id::STRING = CONCAT('https://arxiv.org/abs/', p."arxiv_id")
                LIMIT 100
            """.format(
                silver_table=silver_table,
                bronze_table=bronze_table,
            ))
        rows = cur.fetchall()
        papers = []
        for r in rows:
            raw_payload = r[4]
            payload = {}
            if isinstance(raw_payload, str):
                try:
                    payload = json.loads(raw_payload)
                except Exception:
                    payload = {}
            elif isinstance(raw_payload, dict):
                payload = raw_payload

            authors_list = payload.get("authors") if isinstance(payload, dict) else None
            if isinstance(authors_list, list):
                authors = ", ".join(str(a) for a in authors_list[:5] if a)
                if len(authors_list) > 5:
                    authors += " et al."
                authors = authors or "Unknown"
            else:
                authors = "Unknown"

            year = int(r[6]) if r[6] is not None else 0
            if year == 0 and isinstance(payload, dict) and payload.get("published"):
                try:
                    year = int(str(payload.get("published"))[:4])
                except Exception:
                    year = 0

            citations = int(r[5]) if r[5] is not None else 0

            papers.append({
                "id": int(r[0]),
                "title": r[1] or "Untitled",
                "shortTitle": (r[1] or "Untitled")[:20],
                "authors": authors,
                "year": year,
                "citations": citations,
                "primaryTopic": "ML",
                "clusterId": int(r[7]) if r[7] is not None else None,
                "clusterName": r[8],
                "clusterDescription": r[9],
                "searchText": r[1] or ""
            })
        return papers
    finally:
        cur.close()
        conn.close()


@api.get("/relationships")
async def get_relationships():
    conn = connect_to_snowflake(schema="GOLD")
    cur = conn.cursor()
    try:
        gold_table = qualify_table("GOLD_PAPER_RELATIONSHIPS", database=DATABASE)
        cur.execute(f"DESC TABLE {gold_table}")
        columns = {str(row[0]).lower() for row in cur.fetchall() if row and row[0]}
        has_reason = "reason" in columns

        reason_select = ', "reason"' if has_reason else ""
        cur.execute("""
            SELECT 
                "source_paper_id",
                "target_paper_id",
                "relationship_type",
                "strength"
                {reason_select}
            FROM {gold_table}
            LIMIT 200
        """.format(gold_table=gold_table, reason_select=reason_select))
        rows = cur.fetchall()

        relationships = []
        for r in rows:
            rel_type = str(r[2]) if r[2] else "SIMILAR"
            reason = r[4] if has_reason else None
            relationships.append({
                "source_paper_id": int(r[0]),
                "target_paper_id": int(r[1]),
                "relationship_type": rel_type,
                "strength": float(r[3]) if r[3] else 0.5,
                "reason": reason,
            })
        return relationships
    finally:
        cur.close()
        conn.close()


@api.get("/papers/search")
async def search_papers(query: str, limit: int = 3):
    url = "https://api.semanticscholar.org/graph/v1/paper/search"
    params = {
        "query": query,
        "limit": limit,
        "fields": "title,authors,year,citationCount,externalIds"
    }
    api_key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
    headers = {"x-api-key": api_key} if api_key else None

    # Retry transient/rate-limit responses and degrade gracefully to avoid frontend 500s.
    max_attempts = 3
    retry_delays = [0.6, 1.2]
    async with httpx.AsyncClient(timeout=30.0) as client:
        for attempt in range(max_attempts):
            try:
                res = await client.get(url, params=params, headers=headers)
            except httpx.HTTPError as exc:
                if attempt < max_attempts - 1:
                    await asyncio.sleep(retry_delays[attempt])
                    continue
                return {
                    "data": [],
                    "rate_limited": False,
                    "upstream_error": str(exc),
                }

            if res.status_code == 429:
                if attempt < max_attempts - 1:
                    retry_after = res.headers.get("Retry-After")
                    try:
                        wait_s = float(retry_after) if retry_after is not None else retry_delays[attempt]
                    except (TypeError, ValueError):
                        wait_s = retry_delays[attempt]
                    await asyncio.sleep(max(0.0, wait_s))
                    continue
                return {
                    "data": [],
                    "rate_limited": True,
                }

            if res.status_code >= 500 and attempt < max_attempts - 1:
                await asyncio.sleep(retry_delays[attempt])
                continue

            if res.is_error:
                return {
                    "data": [],
                    "rate_limited": False,
                    "upstream_status": res.status_code,
                }

            return res.json()

    return {"data": [], "rate_limited": False}


@api.post("/papers/ingest")
async def ingest_paper(body: dict):
    arxiv_id = body.get("arxiv_id")
    if not arxiv_id:
        raise HTTPException(status_code=400, detail="Missing required field: arxiv_id")

    # Run Bronze synchronously so the UI can confirm "Added" as soon as Bronze succeeds.
    try:
        bronze_result = await ingest_single_paper.remote.aio(arxiv_id=arxiv_id, database=DATABASE)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Bronze ingestion failed: {exc}") from exc

    bronze_status = bronze_result.get("status") if isinstance(bronze_result, dict) else "failed"
    if bronze_status not in {"ok", "skipped"}:
        return {
            "status": "failed",
            "stage": "bronze",
            "database": DATABASE,
            "bronze_result": bronze_result,
        }

    # Continue remaining stages asynchronously.
    call = await run_post_bronze_job.spawn.aio(arxiv_id=arxiv_id, database=DATABASE)
    return {
        "job_id": call.object_id,
        "status": "processing",
        "stage": "bronze",
        "bronze_status": bronze_status,
        "bronze_result": bronze_result,
        "database": DATABASE,
    }


@api.get("/papers/{job_id}/status")
async def get_status(job_id: str):
    try:
        call = modal.FunctionCall.from_id(job_id)
    except Exception:
        return {"status": "failed", "error": "Job not found"}

    try:
        result = await call.get.aio(timeout=0)
        if isinstance(result, dict):
            return {"status": "done", **result}
        return {"status": "done", "result": result}
    except TimeoutError:
        return {"status": "processing", "database": DATABASE}
    except Exception as e:
        return {
            "status": "failed",
            "database": DATABASE,
            "error": str(e),
        }

class PipelineRequest(BaseModel):
    query: str = Field(..., min_length=1)
    max_results: int = Field(default=10, ge=1, le=100)
    embed_limit: int = Field(default=200, ge=1, le=2000)
    model_name: str = Field(default="sentence-transformers/all-MiniLM-L6-v2")


def _fetch_papers_by_ids(paper_ids: List[str]) -> List[Dict[str, Any]]:
    if not paper_ids:
        return []

    conn = connect_to_snowflake(schema="SILVER")
    cur = conn.cursor()
    try:
        values_sql = ", ".join(["(%s)"] * len(paper_ids))
        silver_table = qualify_table("SILVER_PAPERS", database=DATABASE)
        cur.execute(
            f"""
            WITH ids(paper_id) AS (SELECT column1 FROM VALUES {values_sql})
            SELECT
              TO_VARCHAR(s."id") AS paper_id,
              s."title",
              s."abstract",
              s."arxiv_id"
            FROM ids i
            LEFT JOIN {silver_table} s
              ON TO_VARCHAR(s."id") = i.paper_id
            """,
            paper_ids,
        )
        rows = cur.fetchall()
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        cur.close()
        conn.close()


@api.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@api.post("/clusters/rebuild")
async def rebuild_clusters(n_clusters: int = Query(default=5, ge=2, le=20)):
    """
    Run topic clustering for the current graph and persist cluster assignments.
    """
    try:
        from app.workers.graph_worker import run_topic_clustering

        result = await run_topic_clustering.remote.aio(
            n_clusters=n_clusters,
            database=DATABASE,
        )
        return {"status": "ok", "database": DATABASE, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Cluster rebuild failed: {exc}") from exc


@api.post("/api/pipeline/run")
def run_pipeline(req: PipelineRequest) -> Dict[str, Any]:
    try:
        from app.workers.ingestion import ingest_from_arxiv
        from app.workers.transformation import main as transform_main
        from app.workers.embedding_worker import run_embedding_batch

        ingest_from_arxiv.remote(query=req.query, max_results=req.max_results)
        transform_main.remote()
        embed_result = run_embedding_batch.remote(limit=req.embed_limit, model_name=req.model_name)
        return {
            "status": "ok",
            "query": req.query,
            "max_results": req.max_results,
            "embed_limit": req.embed_limit,
            "embed_result": embed_result,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {exc}") from exc


@api.get("/api/related")
def related(
    paper_id: str = Query(..., min_length=1),
    k: int = Query(default=10, ge=1, le=100),
) -> Dict[str, Any]:
    try:
        from app.workers.semantic_search import get_related_papers

        neighbors = get_related_papers.remote(paper_id=paper_id, k=k)
        return {
            "paper_id": paper_id,
            "k": k,
            "neighbors": neighbors,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Related lookup failed: {exc}") from exc


@api.get("/api/graph")
def graph(
    paper_id: str = Query(..., min_length=1),
    k: int = Query(default=10, ge=1, le=100),
) -> Dict[str, Any]:
    try:
        from app.workers.semantic_search import get_related_papers

        neighbors = get_related_papers.remote(paper_id=paper_id, k=k)
        related_ids = [str(r.get("paper_id")) for r in neighbors if r.get("paper_id")]

        all_ids = [paper_id] + [pid for pid in related_ids if pid != paper_id]
        papers = _fetch_papers_by_ids(all_ids)
        paper_map = {str(p.get("paper_id")): p for p in papers if p.get("paper_id")}

        nodes: List[Dict[str, Any]] = []
        for pid in all_ids:
            p = paper_map.get(pid, {})
            nodes.append(
                {
                    "id": pid,
                    "kind": "paper",
                    "label": p.get("title") or pid,
                    "title": p.get("title") or "",
                    "authors": "",
                    "year": 0,
                    "citations": 0,
                    "primaryTopic": "Unknown",
                    "searchText": f"{p.get('title') or ''} {p.get('abstract') or ''}".strip(),
                    "arxiv_id": p.get("arxiv_id"),
                }
            )

        links = [
            {
                "source": paper_id,
                "target": str(r.get("paper_id")),
                "kind": "cites",
                "score": float(r.get("score", 0.0)),
            }
            for r in neighbors
            if r.get("paper_id")
        ]

        return {
            "paper_id": paper_id,
            "k": k,
            "nodes": nodes,
            "links": links,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Graph construction failed: {exc}") from exc

class IngestRequest(BaseModel):
    arxiv_id: str = Field(..., min_length=1)

@api.post("/api/papers/ingest")
def ingest_paper(req: IngestRequest) -> Dict[str, Any]:
    try:
        from app.workers.ingestion import ingest_single_paper
        result = ingest_single_paper.remote(arxiv_id=req.arxiv_id)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {exc}") from exc


@api.get("/papers/summary/{paper_id}")
async def get_paper_summary(paper_id: str):
    try:
        pid = int(paper_id)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid paper_id")
    conn = connect_to_snowflake(schema="GOLD")
    cur = conn.cursor()
    try:
        summaries_table = qualify_table("GOLD_PAPER_SUMMARIES", database=DATABASE)
        # Check table exists first
        cur.execute(f"SHOW TABLES LIKE 'GOLD_PAPER_SUMMARIES' IN SCHEMA {DATABASE}.GOLD")
        if cur.fetchone() is None:
            return {"summary": None, "found": False, "reason": "table_missing"}
        cur.execute(
            f'SELECT "summary_json" FROM {summaries_table} WHERE "paper_id" = %s LIMIT 1',
            (pid,),
        )
        row = cur.fetchone()
        if not row:
            return {"summary": None, "found": False, "reason": "no_summary"}
        raw = row[0]
        if isinstance(raw, str):
            try:
                data = json.loads(raw)
            except Exception:
                return {"summary": None, "found": False, "reason": "invalid_json"}
        elif isinstance(raw, dict):
            data = raw
        else:
            return {"summary": None, "found": False, "reason": "invalid_type"}
        return data
    finally:
        cur.close()
        conn.close()


class ChatMessage(BaseModel):
    role: str
    content: str

class PaperChatRequest(BaseModel):
    paper_id: int
    question: str
    session_id: Optional[str] = None

@api.post("/api/paper-chat")
async def paper_chat(req: PaperChatRequest):
    try:
        from app.workers.qa_worker import answer_paper_question
        result = await answer_paper_question.remote.aio(
            paper_id=req.paper_id,
            question=req.question,
            session_id=req.session_id,
            database=DATABASE,
        )
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"QA failed: {exc}") from exc
