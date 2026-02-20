"""
Build Gold layer relationships (citations + similarity) from Silver layer.
"""
from typing import Iterable, List, Optional, Tuple
from utils import connect_to_snowflake
from config import app, image, snowflake_secret
import json
from itertools import islice

SILVER = "MINDMAP_DB.PUBLIC.SILVER_PAPERS"
GOLD = "MINDMAP_DB.PUBLIC.GOLD_PAPER_RELATIONSHIPS"

def _fetch_papers(cur, paper_id: Optional[int]) -> List[Tuple[int, list, list]]:
    if paper_id:
        cur.execute(
            f"SELECT id, citation_list, similar_embeddings_ids FROM {SILVER} WHERE id = %s",
            (paper_id,),
        )
    else:
        cur.execute(
            f"""
            SELECT id, citation_list, similar_embeddings_ids
            FROM {SILVER}
            WHERE citation_list IS NOT NULL OR similar_embeddings_ids IS NOT NULL
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
        ON target.source_paper_id = source.source_paper_id
           AND target.target_paper_id = source.target_paper_id
           AND target.relationship_type = source.relationship_type
        WHEN NOT MATCHED THEN
            INSERT (source_paper_id, target_paper_id, relationship_type, strength)
            VALUES (source.source_paper_id, source.target_paper_id, source.relationship_type, source.strength)
        """,
        (source_id, target_id, rel_type, strength),
    )

def _merge_citations(cur, source_id: int, citations: Iterable[str]) -> int:
    added = 0
    for arxiv_id in citations:
        cur.execute(
            f"""
            MERGE INTO {GOLD} AS target
            USING (
                SELECT %s AS source_paper_id, sp.id AS target_paper_id,
                       'CITES' AS relationship_type, 1.0 AS strength
                FROM {SILVER} sp
                WHERE sp.arxiv_id = %s
            ) AS source
            ON target.source_paper_id = source.source_paper_id
               AND target.target_paper_id = source.target_paper_id
               AND target.relationship_type = source.relationship_type
            WHEN NOT MATCHED THEN
                INSERT (source_paper_id, target_paper_id, relationship_type, strength)
                VALUES (source.source_paper_id, source.target_paper_id, source.relationship_type, source.strength)
            """,
            (source_id, arxiv_id),
        )
        added += cur.rowcount
    return added

def _merge_similars(cur, source_id: int, similar_ids: Iterable[int]) -> int:
    added = 0
    for idx, sim_id in enumerate(similar_ids):
        strength = max(0.0, 1.0 - (idx * 0.1))
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
        edges = []

        for pid, citations, similar_ids in papers:
            # citations -> target ids via lookup
            if citations:
                for arxiv_id in set(citations):
                    cur.execute(f"SELECT id FROM {SILVER} WHERE arxiv_id = %s", (arxiv_id,))
                    row = cur.fetchone()
                    if row and row[0] != pid:
                        edges.append((pid, int(row[0]), "CITES", 1.0))

            # similars -> normalized ids
            for idx, sim_id in enumerate(_normalize_ids(similar_ids)):
                if sim_id != pid:
                    strength = max(0.0, 1.0 - (idx * 0.1))
                    edges.append((pid, sim_id, "SIMILAR", strength))

        # dedupe
        edges = list({(a, b, c, d) for a, b, c, d in edges})

        added = _bulk_merge_edges(cur, edges)
        conn.commit()
        print(f"Built knowledge graph for {len(papers)} papers, edges added: {added}")
    finally:
        cur.close()
        conn.close()