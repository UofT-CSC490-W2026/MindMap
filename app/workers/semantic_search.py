import modal
from typing import List, Dict, Any

from .snowflake_utils import connect_snowflake

image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install("snowflake-connector-python[pandas]==3.12.0", "pandas")
)

app = modal.App("mindmap-ml-workers")
secret = modal.Secret.from_name("mindmap-1")


@app.function(image=image, secrets=[secret], timeout=60 * 5)
def get_related_papers(paper_id: str, k: int = 10) -> List[Dict[str, Any]]:
    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH q AS (
              SELECT embedding AS qvec
              FROM PAPER_EMBEDDINGS
              WHERE paper_id = %s
            )
            SELECT
              e.paper_id,
              VECTOR_COSINE_SIMILARITY(e.embedding, q.qvec) AS score
            FROM PAPER_EMBEDDINGS e, q
            WHERE e.paper_id <> %s
            ORDER BY score DESC
            LIMIT %s
            """,
            (paper_id, paper_id, int(k)),
        )
        rows = cur.fetchall()
        return [{"paper_id": r[0], "score": float(r[1])} for r in rows]
    finally:
        cur.close()
        conn.close()
