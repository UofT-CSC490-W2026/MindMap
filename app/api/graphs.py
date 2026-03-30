import json

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.config import DATABASE, qualify_table
from app.services import graph_service
from app.services.contracts import GraphExpandResponse, GraphResponse
from app.utils import connect_to_snowflake

router = APIRouter()
SIMILAR_MIN_STRENGTH = 0.8


class GraphQueryRequest(BaseModel):
    query: str = Field(..., min_length=1)


class GraphExpandRequest(BaseModel):
    graph_id: str
    paper_id: int


@router.post("/graphs/query", response_model=GraphResponse)
async def query_graph(request: GraphQueryRequest):
    return await graph_service.query_graph(query=request.query)


@router.post("/graphs/expand", response_model=GraphExpandResponse)
async def expand_graph(request: GraphExpandRequest):
    return await graph_service.expand_graph(
        graph_id=request.graph_id, paper_id=request.paper_id
    )


@router.post("/graphs/clusters/rebuild")
@router.post("/clusters/rebuild")
async def rebuild_clusters(n_clusters: int = Query(default=5, ge=2, le=20)):
    try:
        from app.workers.graph_worker import run_topic_clustering
        result = await run_topic_clustering.remote.aio(
            n_clusters=n_clusters, database=DATABASE
        )
        return {"status": "ok", "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Cluster rebuild failed: {exc}") from exc


@router.get("/papers")
async def get_papers():
    conn = connect_to_snowflake(schema="SILVER", database=DATABASE)
    cur = conn.cursor()
    try:
        silver_table = qualify_table("SILVER_PAPERS", database=DATABASE)
        bronze_table = qualify_table("BRONZE_PAPERS", database=DATABASE)
        clusters_table = qualify_table("GOLD_PAPER_CLUSTERS", database=DATABASE)
        cur.execute(f"SHOW TABLES LIKE 'GOLD_PAPER_CLUSTERS' IN SCHEMA {DATABASE}.GOLD")
        has_clusters = cur.fetchone() is not None

        if has_clusters:
            cur.execute(
                f"""
                SELECT
                    p."id",
                    p."title",
                    p."abstract",
                    p."arxiv_id",
                    b."raw_payload",
                    b."raw_payload":citationCount::INT,
                    b."raw_payload":year::INT,
                    c."cluster_id",
                    c."cluster_name",
                    c."cluster_description"
                FROM {silver_table} p
                LEFT JOIN {bronze_table} b
                  ON b."raw_payload":entry_id::STRING = CONCAT('https://arxiv.org/abs/', p."arxiv_id")
                LEFT JOIN {clusters_table} c ON c."paper_id" = p."id"
                LIMIT 300
                """
            )
        else:
            cur.execute(
                f"""
                SELECT
                    p."id",
                    p."title",
                    p."abstract",
                    p."arxiv_id",
                    b."raw_payload",
                    b."raw_payload":citationCount::INT,
                    b."raw_payload":year::INT,
                    NULL,
                    NULL,
                    NULL
                FROM {silver_table} p
                LEFT JOIN {bronze_table} b
                  ON b."raw_payload":entry_id::STRING = CONCAT('https://arxiv.org/abs/', p."arxiv_id")
                LIMIT 300
                """
            )

        papers = []
        for r in cur.fetchall():
            payload = {}
            if isinstance(r[4], str):
                try:
                    payload = json.loads(r[4])
                except Exception:
                    payload = {}
            elif isinstance(r[4], dict):
                payload = r[4]

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

            papers.append(
                {
                    "id": int(r[0]),
                    "title": r[1] or "Untitled",
                    "shortTitle": (r[1] or "Untitled")[:20],
                    "authors": authors,
                    "year": year,
                    "citations": int(r[5]) if r[5] is not None else 0,
                    "primaryTopic": "ML",
                    "clusterId": int(r[7]) if r[7] is not None else None,
                    "clusterName": r[8],
                    "clusterDescription": r[9],
                }
            )

        return papers
    finally:
        cur.close()
        conn.close()


@router.get("/relationships")
async def get_relationships():
    conn = connect_to_snowflake(schema="GOLD", database=DATABASE)
    cur = conn.cursor()
    try:
        gold_table = qualify_table("GOLD_PAPER_RELATIONSHIPS", database=DATABASE)
        cur.execute(f"DESC TABLE {gold_table}")
        columns = {str(row[0]).lower() for row in cur.fetchall() if row and row[0]}
        has_reason = "reason" in columns
        reason_select = ', "reason"' if has_reason else ""
        cur.execute(
            f"""
            SELECT
                "source_paper_id",
                "target_paper_id",
                "relationship_type",
                "strength"
                {reason_select}
            FROM {gold_table}
            LIMIT 1000
            """
        )
        rows = cur.fetchall()
        relationships = []
        for r in rows:
            rel_type = str(r[2]) if r[2] else "SIMILAR"
            strength = float(r[3]) if r[3] is not None else 0.5
            if rel_type.upper() == "SIMILAR" and strength < SIMILAR_MIN_STRENGTH:
                continue
            relationships.append(
                {
                    "source_paper_id": int(r[0]),
                    "target_paper_id": int(r[1]),
                    "relationship_type": rel_type,
                    "strength": strength,
                    "reason": r[4] if has_reason else None,
                }
            )
        return relationships
    finally:
        cur.close()
        conn.close()
