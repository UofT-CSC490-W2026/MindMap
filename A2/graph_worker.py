"""
Build Gold layer relationships (citations + similarity) from Silver layer.
"""
import os
import snowflake.connector
from typing import Iterable, List, Optional, Tuple
#from utils import connect_to_snowflake
from config import app, image, snowflake_secret


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
    return cur.fetchall()

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

def _merge_citations(cur, source_id: int, citations: Iterable[str]):
    for arxiv_id in citations:
        cur.execute(
            f"""
            MERGE INTO {GOLD} AS target
            USING (
                SELECT %s AS source_paper_id, sp."id" AS target_paper_id,
                       'CITES' AS relationship_type, 1.0 AS strength
                FROM {SILVER} sp
                WHERE sp."arxiv_id" = %s
            ) AS source
            ON target."source_paper_id" = source.source_paper_id
               AND target."target_paper_id" = source.target_paper_id
               AND target."relationship_type" = source.relationship_type
            WHEN NOT MATCHED THEN
                INSERT ("source_paper_id", "target_paper_id", "relationship_type", "strength")
                VALUES (source.source_paper_id, source.target_paper_id, source.relationship_type, source.strength)
            """,
            (source_id, arxiv_id),
        )

def _merge_similars(cur, source_id: int, similar_ids: Iterable[int]):
    for idx, sim_id in enumerate(similar_ids):
        strength = max(0.0, 1.0 - (idx * 0.1))
        _merge_relationship(cur, source_id, sim_id, "SIMILAR", strength)

@app.function(image=image, secrets=[snowflake_secret])
def build_knowledge_graph(paper_id: int = None):
    """
    Populate Gold layer with citation and semantic similarity relationships.
    If paper_id is None, process all papers.
    """
    conn = connect_to_snowflake()
    cur = conn.cursor()
    try:
        papers = _fetch_papers(cur, paper_id)
        for pid, citations, similar_ids in papers:
            if citations:
                _merge_citations(cur, pid, citations)
            if similar_ids:
                _merge_similars(cur, pid, similar_ids)

        conn.commit()
        print(f"Built knowledge graph for {len(papers)} papers")
    finally:
        cur.close()
        conn.close()