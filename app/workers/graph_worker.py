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


def _quote_ident(identifier: str) -> str:
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _resolve_table_columns(cur, table_name: str) -> dict[str, str]:
    cur.execute(f"DESC TABLE {table_name}")
    columns = [row[0] for row in cur.fetchall() if row and row[0]]
    return {str(name).lower(): _quote_ident(str(name)) for name in columns}


def _require_columns(column_map: dict[str, str], required: list[str], table_name: str) -> dict[str, str]:
    missing = [name for name in required if name not in column_map]
    if missing:
        raise RuntimeError(f"Missing required columns in {table_name}: {missing}")
    return {name: column_map[name] for name in required}


def _fetch_papers(cur, paper_id: Optional[int], database: str = DATABASE) -> List[Tuple[int, object, object]]:
    silver = _silver_table(database=database)
    cols = _require_columns(
        _resolve_table_columns(cur, silver),
        ["id", "citation_list", "similar_embeddings_ids"],
        silver,
    )
    if paper_id is not None:
        cur.execute(
            f'SELECT {cols["id"]} AS id, {cols["citation_list"]} AS citation_list, {cols["similar_embeddings_ids"]} AS similar_embeddings_ids FROM {silver} WHERE {cols["id"]} = %s',
            (int(paper_id),),
        )
    else:
        cur.execute(
            f"""
            SELECT {cols["id"]} AS id, {cols["citation_list"]} AS citation_list, {cols["similar_embeddings_ids"]} AS similar_embeddings_ids
            FROM {silver}
            WHERE {cols["citation_list"]} IS NOT NULL OR {cols["similar_embeddings_ids"]} IS NOT NULL
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

    silver = _silver_table(database=database)
    cols = _require_columns(
        _resolve_table_columns(cur, silver),
        ["id", "ss_id"],
        silver,
    )

    values_sql = ", ".join(["(%s)"] * len(ss_ids))
    cur.execute(
        f"""
        WITH source_ss_ids(ss_id) AS (SELECT column1 FROM VALUES {values_sql})
            SELECT DISTINCT sp.{cols["id"]}
        FROM source_ss_ids src
        JOIN {silver} sp
            ON sp.{cols["ss_id"]} = src.ss_id
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

    gold = _gold_table(database=database)
    cols = _require_columns(
        _resolve_table_columns(cur, gold),
        ["source_paper_id", "target_paper_id", "relationship_type", "strength"],
        gold,
    )

    values_sql = ", ".join(["(%s, %s, %s, %s)"] * len(edges))
    params = [value for edge in edges for value in edge]
    cur.execute(
        f"""
        MERGE INTO {gold} AS target
        USING (
            SELECT
                column1 AS source_paper_id,
                column2 AS target_paper_id,
                column3 AS relationship_type,
                column4 AS strength
            FROM VALUES {values_sql}
        ) AS source
        ON target.{cols["source_paper_id"]} = source.source_paper_id
           AND target.{cols["target_paper_id"]} = source.target_paper_id
           AND target.{cols["relationship_type"]} = source.relationship_type
        WHEN MATCHED THEN
            UPDATE SET target.{cols["strength"]} = source.strength
        WHEN NOT MATCHED THEN
            INSERT ({cols["source_paper_id"]}, {cols["target_paper_id"]}, {cols["relationship_type"]}, {cols["strength"]})
            VALUES (source.source_paper_id, source.target_paper_id, source.relationship_type, source.strength)
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

        for pid, citations, similar_ids in papers:
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
