"""
Build Gold layer relationships (citations + similarity) from Silver layer.
"""
import os
import snowflake.connector
from typing import Iterable, List, Optional, Tuple
#from utils import connect_to_snowflake
from config import app, image, snowflake_secret
import json


def connect_to_snowflake():
    env = "PROD"

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=f"MINDMAP_{env}",
        warehouse=f"MINDMAP_{env}_WH",
    )


SILVER = "MINDMAP_PROD.SILVER.SILVER_PAPERS"
GOLD = "MINDMAP_PROD.GOLD.GOLD_CONNECTIONS"

def _fetch_papers(cur, paper_id: Optional[int]) -> List[Tuple[int, list, list]]:
    print(f"Fetching papers for paper_id={paper_id}...")
    if paper_id:
        cur.execute(
            f'SELECT "id", "citation_list", "similar_embeddings_ids" FROM {SILVER} WHERE "id" = %s',
            (paper_id,),
        )
    else:
        cur.execute(
            f"""
            SELECT "id", "citation_list", "similar_embeddings_ids"
            FROM {SILVER}
            WHERE "citation_list" IS NOT NULL OR "similar_embeddings_ids" IS NOT NULL
            """
        )
    papers = cur.fetchall()
    print(f"Fetched {len(papers)} papers.")
    return papers

def _merge_relationship(cur, source_id: int, target_id: int, rel_type: str, strength: float):
    cur.execute(
        f"""
        MERGE INTO {GOLD} AS target
        USING (
            SELECT %s AS source_paper_id,
                   %s AS target_paper_id,
                   %s AS relationship_type,
                   %s AS strength
        ) AS source
        ON target."source_paper_id" = source.source_paper_id
           AND target."target_paper_id" = source.target_paper_id
           AND target."relationship_type" = source.relationship_type
        WHEN NOT MATCHED THEN
            INSERT ("source_paper_id", "target_paper_id", "relationship_type", "strength")
            VALUES (source.source_paper_id, source.target_paper_id, source.relationship_type, source.strength)
        """,
        (source_id, target_id, rel_type, strength),
    )

def _merge_citations(cur, source_id: int, citations: Iterable[dict]):
    print(f"Merging {len(citations)} citations for paper {source_id}...")
    for citation in citations:
        ss_paper_id = citation.get("ss_paper_id")
        if ss_paper_id:
            cur.execute(
                f"""
                MERGE INTO {GOLD} AS target
                USING (
                    SELECT %s AS source_paper_id, sp."id" AS target_paper_id,
                        'CITES' AS relationship_type, 1.0 AS strength
                    FROM {SILVER} sp
                    WHERE sp."ss_id" = %s
                ) AS source
                ON target."source_paper_id" = source.source_paper_id
                AND target."target_paper_id" = source.target_paper_id
                AND target."relationship_type" = source.relationship_type
                WHEN NOT MATCHED THEN
                    INSERT ("source_paper_id", "target_paper_id", "relationship_type", "strength")
                    VALUES (source.source_paper_id, source.target_paper_id, source.relationship_type, source.strength)
                """,
                (source_id, ss_paper_id),
            )
            print(f"  Merged citation by ss_paper_id: {ss_paper_id}")
        else:
            print(f"  Skipped citation with missing ss_paper_id: {citation}")

def _merge_similars(cur, source_id: int, similar_ids: Iterable[int]):
    print(f"Merging {len(similar_ids)} similars for paper {source_id}...")
    for idx, sim_id in enumerate(similar_ids):
        strength = max(0.0, 1.0 - (idx * 0.1))
        print(f"  Merged similar: {sim_id} with strength {strength}")
        _merge_relationship(cur, source_id, sim_id, "SIMILAR", strength)
        added += cur.rowcount
    return added

def _normalize_ids(value) -> List[int]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if isinstance(value, list):
        ids = []
        for v in value:
            try:
                ids.append(int(v))
            except (TypeError, ValueError):
                continue
        return ids
    return []

def _chunked(iterable, size: int):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def _bulk_merge_edges(cur, edges: List[Tuple[int, int, str, float]]) -> int:
    if not edges:
        return 0

    total = 0
    for chunk in _chunked(edges, 500):
        values_sql = ", ".join(["(%s, %s, %s, %s)"] * len(chunk))
        flat_params = [p for row in chunk for p in row]
        cur.execute(
            f"""
            MERGE INTO {GOLD} AS target
            USING (SELECT column1 AS source_paper_id,
                          column2 AS target_paper_id,
                          column3 AS relationship_type,
                          column4 AS strength
                   FROM VALUES {values_sql}) AS source
            ON target.source_paper_id = source.source_paper_id
               AND target.target_paper_id = source.target_paper_id
               AND target.relationship_type = source.relationship_type
            WHEN NOT MATCHED THEN
                INSERT (source_paper_id, target_paper_id, relationship_type, strength)
                VALUES (source.source_paper_id, source.target_paper_id, source.relationship_type, source.strength)
            """,
            flat_params,
        )
        total += cur.rowcount
    return total

# -----------------------------
# NEW FUNCTION: Hardcode some papers into SILVER for testing
# -----------------------------
def _insert_hardcoded_test_papers(cur):
    """
    Insert 2 dummy papers with known ss_ids so Gold can get at least 2 rows.
    """
    test_papers = [
        {"ss_id": "8d0835be89802021924c328d9fd3b10fa557c4a7", "title": "Dummy Paper 1"},
        {"ss_id": "462142049c247e69eaf9d903aae2301f81885645", "title": "Dummy Paper 2"},
    ]
    for paper in test_papers:
        cur.execute(
            f"""
            INSERT INTO {SILVER} ("ss_id", "title")
            SELECT %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM {SILVER} WHERE "ss_id" = %s
            )
            """,
            (paper["ss_id"], paper["title"], paper["ss_id"]),
        )
    print("Inserted hardcoded test papers into SILVER")

# -----------------------------
@app.function(image=image, secrets=[snowflake_secret])
def build_knowledge_graph(paper_id: int = None):
    """
    Populate Gold layer with citation and semantic similarity relationships.
    If paper_id is None, process all papers.
    """
    conn = connect_to_snowflake()
    cur = conn.cursor()
    try:
        # 🔥 Insert test papers so Gold can have connections
        _insert_hardcoded_test_papers(cur)

        papers = _fetch_papers(cur, paper_id)
        edges = []

        for pid, citations, similar_ids in papers:
            print(f"Processing paper {pid}...")
            if citations and isinstance(citations, str):
                try:
                    citations = json.loads(citations)
                except Exception as e:
                    print(f"Failed to parse citations for paper {pid}: {e}")
                    citations = []
            if citations:
                _merge_citations(cur, pid, citations)
            if similar_ids:
                _merge_similars(cur, pid, similar_ids)
        conn.commit()
        print(f"Built knowledge graph for {len(papers)} papers, edges added: {added}")
    finally:
        cur.close()
        conn.close()