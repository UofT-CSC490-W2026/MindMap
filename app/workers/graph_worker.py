"""
Build Gold layer relationships (citations + similarity) from Silver layer.
"""
import json
from typing import Iterable, List, Optional, Tuple

from config import app, image, snowflake_secret, DATABASE, qualify_table
from utils import connect_to_snowflake


def _silver_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPERS", database=database)


def _gold_table(database: str = DATABASE) -> str:
    return qualify_table("GOLD_PAPER_RELATIONSHIPS", database=database)


def _fetch_papers(cur, paper_id: Optional[int], database: str = DATABASE) -> List[Tuple[int, object, object]]:
    silver = _silver_table(database=database)
    if paper_id is not None:
        cur.execute(
            f'SELECT "id", "citation_list", "similar_embeddings_ids" FROM {silver} WHERE "id" = %s',
            (int(paper_id),),
        )
    else:
        cur.execute(
            f"""
            SELECT "id", "citation_list", "similar_embeddings_ids"
            FROM {silver}
            WHERE "citation_list" IS NOT NULL OR "similar_embeddings_ids" IS NOT NULL
            """
        )
    return cur.fetchall()


def _normalize_json_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    return value if isinstance(value, list) else []


def _normalize_ids(value) -> List[int]:
    ids = []
    for item in _normalize_json_list(value):
        try:
            ids.append(int(item))
        except (TypeError, ValueError):
            continue
    return ids


def _citation_targets(cur, citations: Iterable[dict], database: str = DATABASE) -> List[int]:
    ss_ids = []
    for citation in citations:
        if not isinstance(citation, dict):
            continue
        ss_id = citation.get("ss_paper_id")
        if ss_id:
            ss_ids.append(str(ss_id))

    if not ss_ids:
        return []

    values_sql = ", ".join(["(%s)"] * len(ss_ids))
    cur.execute(
        f"""
        WITH source_ss_ids(ss_id) AS (SELECT column1 FROM VALUES {values_sql})
            SELECT DISTINCT sp."id"
        FROM source_ss_ids src
        JOIN {_silver_table(database=database)} sp
            ON sp."ss_id" = src.ss_id
        """,
        ss_ids,
    )
    return [int(row[0]) for row in cur.fetchall()]


def _dedupe_edges(edges: Iterable[Tuple[int, int, str, float]]) -> List[Tuple[int, int, str, float]]:
    seen = {}
    for source_id, target_id, rel_type, strength in edges:
        if source_id == target_id:
            continue
        key = (int(source_id), int(target_id), rel_type)
        seen[key] = max(float(strength), seen.get(key, 0.0))
    return [(sid, tid, rel, strength) for (sid, tid, rel), strength in seen.items()]


def _bulk_merge_edges(cur, edges: List[Tuple[int, int, str, float]], database: str = DATABASE) -> int:
    if not edges:
        return 0

    values_sql = ", ".join(["(%s, %s, %s, %s)"] * len(edges))
    params = [value for edge in edges for value in edge]
    cur.execute(
        f"""
        MERGE INTO {_gold_table(database=database)} AS target
        USING (
            SELECT
                column1 AS \"source_paper_id\",
                column2 AS \"target_paper_id\",
                column3 AS \"relationship_type\",
                column4 AS \"strength\"
            FROM VALUES {values_sql}
        ) AS source
        ON target.\"source_paper_id\" = source.\"source_paper_id\"
           AND target.\"target_paper_id\" = source.\"target_paper_id\"
           AND target.\"relationship_type\" = source.\"relationship_type\"
        WHEN MATCHED THEN
            UPDATE SET target.\"strength\" = source.\"strength\"
        WHEN NOT MATCHED THEN
            INSERT (\"source_paper_id\", \"target_paper_id\", \"relationship_type\", \"strength\")
            VALUES (source.\"source_paper_id\", source.\"target_paper_id\", source.\"relationship_type\", source.\"strength\")
        """,
        params,
    )
    return len(edges)


@app.function(image=image, secrets=[snowflake_secret])
def build_knowledge_graph(paper_id: int = None, database: str = DATABASE):
    """
    Populate Gold layer with citation and semantic similarity relationships.
    If paper_id is None, process all papers with cached relationships.
    """
    conn = connect_to_snowflake(database=database, schema="GOLD")
    cur = conn.cursor()
    try:
        papers = _fetch_papers(cur, paper_id, database=database)
        edges: List[Tuple[int, int, str, float]] = []

        print(f"Processing {len(papers)} papers from SILVER to build relationships in GOLD...")

        for pid, citations, similar_ids in papers:
            print("----------------------------------------")
            print(f"paper {pid}")
            print(f"  citations: {citations}")
            print(f"  similar_ids: {similar_ids}")
            for target_id in _citation_targets(cur, _normalize_json_list(citations), database=database):
                edges.append((int(pid), target_id, "CITES", 1.0))

            for idx, sim_id in enumerate(_normalize_ids(similar_ids)):
                strength = max(0.0, 1.0 - (idx * 0.1))
                edges.append((int(pid), sim_id, "SIMILAR", strength))

        merged_count = _bulk_merge_edges(cur, _dedupe_edges(edges), database=database)
        conn.commit()
        return {"papers_processed": len(papers), "edges_merged": merged_count}
    finally:
        cur.close()
        conn.close()
