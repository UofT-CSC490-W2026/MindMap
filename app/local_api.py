"""
Local development API server - runs without Modal.
Serves summary and paper data directly from Snowflake.

Usage:
    cd /path/to/MindMap
    PYTHONPATH=app venv/bin/uvicorn app.local_api:api --reload --port 8000
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import httpx
import asyncio
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Load .env manually for local dev
from pathlib import Path
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

from app.utils import connect_to_snowflake
from app.config import DATABASE, qualify_table

api = FastAPI(title="MindMap Local API", version="0.1.0")

api.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@api.get("/health")
def health():
    return {"status": "ok", "mode": "local"}


@api.get("/papers")
async def get_papers():
    conn = connect_to_snowflake(schema="SILVER")
    cur = conn.cursor()
    try:
        silver_table = qualify_table("SILVER_PAPERS", database=DATABASE)
        bronze_table = qualify_table("BRONZE_PAPERS", database=DATABASE)
        clusters_table = qualify_table("GOLD_PAPER_CLUSTERS", database=DATABASE)
        cur.execute(f"SHOW TABLES LIKE 'GOLD_PAPER_CLUSTERS' IN SCHEMA {DATABASE}.GOLD")
        has_clusters = cur.fetchone() is not None

        if has_clusters:
            cur.execute(f"""
                SELECT p."id", p."title", p."abstract", p."arxiv_id",
                       b."raw_payload",
                       b."raw_payload":citationCount::INT,
                       b."raw_payload":year::INT,
                       c."cluster_id", c."cluster_name", c."cluster_description"
                FROM {silver_table} p
                LEFT JOIN {bronze_table} b
                  ON b."raw_payload":entry_id::STRING = CONCAT('https://arxiv.org/abs/', p."arxiv_id")
                LEFT JOIN {clusters_table} c ON c."paper_id" = p."id"
                LIMIT 100
            """)
        else:
            cur.execute(f"""
                SELECT p."id", p."title", p."abstract", p."arxiv_id",
                       b."raw_payload",
                       b."raw_payload":citationCount::INT,
                       b."raw_payload":year::INT,
                       NULL, NULL, NULL
                FROM {silver_table} p
                LEFT JOIN {bronze_table} b
                  ON b."raw_payload":entry_id::STRING = CONCAT('https://arxiv.org/abs/', p."arxiv_id")
                LIMIT 100
            """)

        papers = []
        for r in cur.fetchall():
            payload = {}
            if isinstance(r[4], str):
                try: payload = json.loads(r[4])
                except: pass
            elif isinstance(r[4], dict):
                payload = r[4]

            authors_list = payload.get("authors") if isinstance(payload, dict) else None
            if isinstance(authors_list, list):
                authors = ", ".join(str(a) for a in authors_list[:5] if a)
                if len(authors_list) > 5: authors += " et al."
                authors = authors or "Unknown"
            else:
                authors = "Unknown"

            year = int(r[6]) if r[6] else 0
            if not year and isinstance(payload, dict) and payload.get("published"):
                try: year = int(str(payload["published"])[:4])
                except: pass

            papers.append({
                "id": int(r[0]),
                "title": r[1] or "Untitled",
                "shortTitle": (r[1] or "Untitled")[:20],
                "authors": authors,
                "year": year,
                "citations": int(r[5]) if r[5] else 0,
                "primaryTopic": "ML",
                "clusterId": int(r[7]) if r[7] is not None else None,
                "clusterName": r[8],
                "clusterDescription": r[9],
                "searchText": r[1] or "",
            })
        return papers
    finally:
        cur.close(); conn.close()


@api.get("/relationships")
async def get_relationships():
    conn = connect_to_snowflake(schema="GOLD")
    cur = conn.cursor()
    try:
        gold_table = qualify_table("GOLD_PAPER_RELATIONSHIPS", database=DATABASE)
        cur.execute(f'DESC TABLE {gold_table}')
        columns = {str(row[0]).lower() for row in cur.fetchall() if row and row[0]}
        has_reason = "reason" in columns
        reason_select = ', "reason"' if has_reason else ""
        cur.execute(f"""
            SELECT "source_paper_id", "target_paper_id", "relationship_type", "strength"{reason_select}
            FROM {gold_table} LIMIT 200
        """)
        rels = []
        for r in cur.fetchall():
            rels.append({
                "source_paper_id": int(r[0]),
                "target_paper_id": int(r[1]),
                "relationship_type": str(r[2]) if r[2] else "SIMILAR",
                "strength": float(r[3]) if r[3] else 0.5,
                "reason": r[4] if has_reason else None,
            })
        return rels
    finally:
        cur.close(); conn.close()


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
        cur.execute(
            f'SELECT "summary_json" FROM {summaries_table} WHERE "paper_id" = %s LIMIT 1',
            (pid,),
        )
        row = cur.fetchone()
        if not row:
            return {"found": False, "reason": "no_summary"}
        raw = row[0]
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return {"found": False, "reason": "invalid_json"}
        elif isinstance(raw, dict):
            return raw
        return {"found": False, "reason": "invalid_type"}
    finally:
        cur.close(); conn.close()


@api.get("/papers/search")
async def search_papers(query: str, limit: int = 3):
    url = "https://api.semanticscholar.org/graph/v1/paper/search"
    params = {"query": query, "limit": limit, "fields": "title,authors,year,citationCount,externalIds"}
    api_key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
    headers = {"x-api-key": api_key} if api_key else None
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            res = await client.get(url, params=params, headers=headers)
            if res.is_error:
                return {"data": [], "rate_limited": res.status_code == 429}
            return res.json()
        except Exception:
            return {"data": []}


@api.post("/clusters/rebuild")
async def rebuild_clusters(n_clusters: int = Query(default=5, ge=2, le=20)):
    raise HTTPException(status_code=503, detail="Cluster rebuild requires Modal deployment")
