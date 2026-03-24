"""
Placeholder for API endpoints (that frontend will use).

"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.utils import connect_to_snowflake


api = FastAPI(title="MindMap API", version="0.1.0")

api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PipelineRequest(BaseModel):
    query: str = Field(..., min_length=1)
    max_results: int = Field(default=10, ge=1, le=100)
    embed_limit: int = Field(default=200, ge=1, le=2000)
    model_name: str = Field(default="sentence-transformers/all-MiniLM-L6-v2")


def _fetch_papers_by_ids(paper_ids: List[str]) -> List[Dict[str, Any]]:
    if not paper_ids:
        return []

    conn = connect_to_snowflake()
    cur = conn.cursor()
    try:
        values_sql = ", ".join(["(%s)"] * len(paper_ids))
        cur.execute(
            f"""
            WITH ids(paper_id) AS (SELECT column1 FROM VALUES {values_sql})
            SELECT
              TO_VARCHAR(s.paper_id) AS paper_id,
              s.title,
              s.abstract,
              s.arxiv_id
            FROM ids i
            LEFT JOIN SILVER_PAPERS s
              ON TO_VARCHAR(s.paper_id) = i.paper_id
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


@api.post("/api/pipeline/run")
def run_pipeline(req: PipelineRequest) -> Dict[str, Any]:
    try:
        from app.workers.ingestion import ingest_from_arxiv
        from app.workers.transformation import transform_to_silver
        from app.workers.embedding_worker import run_embedding_batch

        ingest_from_arxiv.remote(query=req.query, max_results=req.max_results)
        transform_to_silver.remote()
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
