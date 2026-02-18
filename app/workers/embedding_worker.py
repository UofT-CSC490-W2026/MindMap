import modal
from typing import List, Dict, Any, Tuple

from .snowflake_utils import connect_snowflake

# ---- Modal image ----
image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install(
        "sentence-transformers==2.7.0",
        "torch",
        "snowflake-connector-python[pandas]==3.12.0",
        "pandas",
    )
)

app = modal.App("mindmap-ml-workers")
secret = modal.Secret.from_name("mindmap-1")


def _ensure_embeddings_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS PAPER_EMBEDDINGS (
          paper_id STRING PRIMARY KEY,
          model_name STRING,
          embedding VECTOR(FLOAT, 384),
          updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )


def _fetch_unembedded_from_silver(cur, limit: int = 200) -> List[Dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          TO_VARCHAR(s.paper_id) AS paper_id,
          s.title,
          s.abstract
        FROM SILVER_PAPERS s
        LEFT JOIN PAPER_EMBEDDINGS e
          ON e.paper_id = TO_VARCHAR(s.paper_id)
        WHERE e.paper_id IS NULL
          AND s.abstract IS NOT NULL
        LIMIT {int(limit)}
        """
    )
    rows = cur.fetchall()
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _upsert_embeddings(cur, rows: List[Tuple[str, str, List[float]]]):
    if not rows:
        return

    _ensure_embeddings_table(cur)

    values_sql = ", ".join(["(%s, %s, %s)"] * len(rows))
    flat_params: List[Any] = []
    for paper_id, model_name, emb in rows:
        flat_params.extend([paper_id, model_name, emb])

    merge_sql = f"""
    MERGE INTO PAPER_EMBEDDINGS t
    USING (
      SELECT
        column1 AS paper_id,
        column2 AS model_name,
        column3 AS embedding
      FROM VALUES {values_sql}
    ) s
    ON t.paper_id = s.paper_id
    WHEN MATCHED THEN UPDATE SET
      t.model_name = s.model_name,
      t.embedding = s.embedding,
      t.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (paper_id, model_name, embedding)
    VALUES (s.paper_id, s.model_name, s.embedding)
    """
    cur.execute(merge_sql, flat_params)


@app.function(image=image, secrets=[secret], timeout=60 * 20)
def run_embedding_batch(
    limit: int = 200,
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
) -> Dict[str, Any]:
    from sentence_transformers import SentenceTransformer

    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        to_embed = _fetch_unembedded_from_silver(cur, limit=limit)
        if not to_embed:
            return {"status": "ok", "embedded": 0, "note": "No new rows in SILVER_PAPERS."}

        model = SentenceTransformer(model_name)

        ids: List[str] = []
        texts: List[str] = []

        for r in to_embed:
            pid = (r.get("paper_id") or "").strip()
            title = (r.get("title") or "").strip()
            abstract = (r.get("abstract") or "").strip()
            if not pid or not abstract:
                continue

            joined = f"{title}\n\n{abstract}" if title else abstract
            ids.append(pid)
            texts.append(joined)

        vectors = model.encode(
            texts,
            batch_size=32,
            show_progress_bar=False,
            normalize_embeddings=True,
        )

        payload: List[Tuple[str, str, List[float]]] = []
        for pid, vec in zip(ids, vectors):
            payload.append((pid, model_name, vec.tolist()))

        _upsert_embeddings(cur, payload)
        conn.commit()

        return {"status": "ok", "embedded": len(payload), "model": model_name}
    finally:
        cur.close()
        conn.close()
