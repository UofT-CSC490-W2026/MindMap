import os
from typing import List, Dict, Any

def _connect_snowflake():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )

def get_related_papers(paper_id: str, k: int = 10) -> List[Dict[str, Any]]:
    """
    Returns top-k most similar papers by cosine similarity using embeddings in GOLD.PAPER_EMBEDDINGS.
    Assumes embeddings are normalized (recommended).
    """
    conn = _connect_snowflake()
    cur = conn.cursor()

    # Pull similar paper IDs + scores
    cur.execute(
        """
        WITH q AS (
          SELECT embedding AS qvec
          FROM GOLD.PAPER_EMBEDDINGS
          WHERE paper_id = %s
        )
        SELECT
          e.paper_id,
          VECTOR_COSINE_SIMILARITY(e.embedding, q.qvec) AS score
        FROM GOLD.PAPER_EMBEDDINGS e, q
        WHERE e.paper_id <> %s
        ORDER BY score DESC
        LIMIT %s
        """,
        (paper_id, paper_id, k),
    )

    rows = cur.fetchall()
    out = [{"paper_id": r[0], "score": float(r[1])} for r in rows]

    cur.close()
    conn.close()
    return out
