"""Graph service: assembles query-scoped graphs from Silver/Gold data."""

from __future__ import annotations

import json
from typing import List, Optional
from uuid import uuid4

from fastapi import HTTPException

from app.config import DATABASE, qualify_table
from app.services.contracts import (
    GraphExpandResponse,
    GraphLink,
    GraphMeta,
    GraphNode,
    GraphResponse,
)
from app.utils import connect_to_snowflake
from app.workers.semantic_search_worker import get_related_papers, semantic_search


def _fetch_paper_rows(cur, paper_ids: List[int], database: str = DATABASE) -> List[dict]:
    """Fetch Silver + Bronze metadata for a list of paper IDs."""
    if not paper_ids:
        return []
    silver_table = qualify_table("SILVER_PAPERS", database=database)
    bronze_table = qualify_table("BRONZE_PAPERS", database=database)
    clusters_table = qualify_table("GOLD_PAPER_CLUSTERS", database=database)

    values_sql = ", ".join(["(%s)"] * len(paper_ids))
    cur.execute(
        f"""
        WITH ids(pid) AS (SELECT column1 FROM VALUES {values_sql})
        SELECT
            p."id",
            p."title",
            p."arxiv_id",
            b."raw_payload",
            c."cluster_id",
            c."cluster_name"
        FROM ids i
        JOIN {silver_table} p ON p."id" = i.pid
        LEFT JOIN {bronze_table} b
          ON b."raw_payload":entry_id::STRING = CONCAT('https://arxiv.org/abs/', p."arxiv_id")
        LEFT JOIN {clusters_table} c ON c."paper_id" = p."id"
        """,
        [int(pid) for pid in paper_ids],
    )
    rows = cur.fetchall()
    results = []
    for row in rows:
        pid, title, arxiv_id, raw_payload, cluster_id, cluster_name = row
        payload: dict = {}
        if isinstance(raw_payload, str):
            try:
                payload = json.loads(raw_payload)
            except Exception:
                payload = {}
        elif isinstance(raw_payload, dict):
            payload = raw_payload

        authors_raw = payload.get("authors") or []
        if isinstance(authors_raw, list):
            authors_str = ", ".join(str(a) for a in authors_raw[:5] if a)
            if len(authors_raw) > 5:
                authors_str += " et al."
        else:
            authors_str = "Unknown"

        year = payload.get("year")
        if year is None and payload.get("published"):
            try:
                year = int(str(payload["published"])[:4])
            except Exception:
                year = 0
        citations = payload.get("citationCount") or 0

        results.append({
            "id": int(pid),
            "title": title or "Untitled",
            "arxiv_id": arxiv_id,
            "authors": authors_str or "Unknown",
            "year": int(year) if year else 0,
            "citations": int(citations),
            "cluster_id": int(cluster_id) if cluster_id is not None else None,
            "cluster_name": cluster_name,
        })
    return results


def _fetch_edges(cur, paper_ids: List[int], database: str = DATABASE) -> List[GraphLink]:
    """Fetch GOLD_CONNECTIONS edges where source is in paper_ids."""
    if not paper_ids:
        return []
    gold_table = qualify_table("GOLD_CONNECTIONS", database=database)
    values_sql = ", ".join(["(%s)"] * len(paper_ids))
    cur.execute(
        f"""
        WITH ids(pid) AS (SELECT column1 FROM VALUES {values_sql})
        SELECT
            g."source_paper_id",
            g."target_paper_id",
            g."relationship_type",
            g."strength"
        FROM {gold_table} g
        JOIN ids i ON g."source_paper_id" = i.pid
        """,
        [int(pid) for pid in paper_ids],
    )
    rows = cur.fetchall()
    return [
        GraphLink(
            source=str(r[0]),
            target=str(r[1]),
            kind=str(r[2]) if r[2] else "SIMILAR",
            strength=float(r[3]) if r[3] is not None else 0.5,
        )
        for r in rows
    ]


def _build_node(row: dict) -> GraphNode:
    title = row["title"]
    return GraphNode(
        id=str(row["id"]),
        label=title[:40] if title else "",
        title=title,
        authors=row["authors"],
        year=row["year"],
        citations=row["citations"],
        arxiv_id=row["arxiv_id"],
        cluster_id=row["cluster_id"],
        cluster_name=row["cluster_name"],
    )


async def query_graph(query: str) -> GraphResponse:
    """Assemble a query-scoped graph from semantic search + Silver/Gold data."""
    search_results = await semantic_search.remote.aio(
        query=query, k=20, database=DATABASE
    )
    paper_ids = [int(r["id"]) for r in (search_results or []) if r.get("id")]

    if not paper_ids:
        return GraphResponse(
            graph_id=str(uuid4()),
            query=query,
            nodes=[],
            links=[],
            meta=GraphMeta(total_nodes=0, total_links=0, query=query),
        )

    conn = connect_to_snowflake(schema="SILVER", database=DATABASE)
    cur = conn.cursor()
    try:
        paper_rows = _fetch_paper_rows(cur, paper_ids, database=DATABASE)
        links = _fetch_edges(cur, paper_ids, database=DATABASE)
    finally:
        cur.close()
        conn.close()

    nodes = [_build_node(row) for row in paper_rows]
    graph_id = str(uuid4())
    return GraphResponse(
        graph_id=graph_id,
        query=query,
        nodes=nodes,
        links=links,
        meta=GraphMeta(total_nodes=len(nodes), total_links=len(links), query=query),
    )


async def expand_graph(graph_id: str, paper_id: int) -> GraphExpandResponse:
    """Expand the graph around a specific paper node."""
    neighbor_results = await get_related_papers.remote.aio(
        paper_id=paper_id, k=10, database=DATABASE
    )

    if neighbor_results is None:
        raise HTTPException(status_code=404, detail=f"Paper {paper_id} not found")

    neighbor_ids = [int(r["id"]) for r in (neighbor_results or []) if r.get("id")]

    conn = connect_to_snowflake(schema="SILVER", database=DATABASE)
    cur = conn.cursor()
    try:
        # Verify the source paper exists
        silver_table = qualify_table("SILVER_PAPERS", database=DATABASE)
        cur.execute(
            f'SELECT 1 FROM {silver_table} WHERE "id" = %s LIMIT 1',
            (int(paper_id),),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Paper {paper_id} not found")

        paper_rows = _fetch_paper_rows(cur, neighbor_ids, database=DATABASE) if neighbor_ids else []

        # Fetch edges from the source paper
        gold_table = qualify_table("GOLD_CONNECTIONS", database=DATABASE)
        cur.execute(
            f"""
            SELECT "source_paper_id", "target_paper_id", "relationship_type", "strength"
            FROM {gold_table}
            WHERE "source_paper_id" = %s
            """,
            (int(paper_id),),
        )
        edge_rows = cur.fetchall()
        new_links = [
            GraphLink(
                source=str(r[0]),
                target=str(r[1]),
                kind=str(r[2]) if r[2] else "SIMILAR",
                strength=float(r[3]) if r[3] is not None else 0.5,
            )
            for r in edge_rows
        ]
    finally:
        cur.close()
        conn.close()

    new_nodes = [_build_node(row) for row in paper_rows]
    return GraphExpandResponse(
        graph_id=graph_id,
        paper_id=str(paper_id),
        new_nodes=new_nodes,
        new_links=new_links,
    )
