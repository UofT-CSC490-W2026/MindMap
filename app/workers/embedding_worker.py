# app/workers/embedding_worker.py

# NOTE: Functionality implemented on this file:
# - worker that pulls abstracts and conclusions from the Silver layer and generates vector embeddings
# - upserts those embeddings into the Gold layer, associated with the paper_id and model name used
# - Use Snowflake Vector Search or a Modal-based similarity search to find related papers based on embeddings.
# - Build a function to parse the reference sections of papers (potentially using a library like ParsCit or an LLM).

import os
import hashlib
from typing import List, Dict, Any, Tuple

import modal

# Modal Image with necessary dependencies for embedding and Snowflake connectivity.
image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install(
        "sentence-transformers==2.7.0",
        "torch",
        "snowflake-connector-python[pandas]==3.12.0",
        "pandas",

        # Add any other deps you need for text processing, e.g. nltk, spacy, etc.
    )
)

# Define the Modal app
app = modal.App("mindmap-embedding-worker")

# You’ll likely use modal.Secret for Snowflake creds (your infra lead sets this up)
# and you read env vars like SNOWFLAKE_ACCOUNT, USER, PASSWORD, DATABASE, SCHEMA, WAREHOUSE.
secret = modal.Secret.from_name("mindmap-1")  # name depends on your team


def _sha1(text: str) -> str:
    """Helper to create a consistent hash for a given text, useful for caching or deduplication."""

    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def _connect_snowflake():
    """Establish a connection to Snowflake using credentials from environment variables.
    
    This function assumes a standard set of environment variables for Snowflake connectivity:
    """

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

def _fetch_unembedded(cur, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Pull rows from SILVER that don't have embeddings yet.
    Adjust table/field names to match your pipeline.
    """    
    cur.execute(f"""
        SELECT s.paper_id, s.title, s.abstract
        FROM SILVER_METADATA s
        LEFT JOIN GOLD_PAPER_EMBEDDINGS e
          ON s.paper_id = e.paper_id
        WHERE e.paper_id IS NULL
          AND s.abstract IS NOT NULL
        LIMIT {limit}
    """)
    rows = cur.fetchall()
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _upsert_embeddings(cur, rows: List[Tuple[str, str, List[float]]]):
    """
    rows: (paper_id, model_name, embedding_list)
    Snowflake VECTOR accepts VECTOR(FLOAT, dim). Connector can pass arrays/lists.
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS GOLD_PAPER_EMBEDDINGS (
          paper_id STRING PRIMARY KEY,
          model_name STRING,
          embedding VECTOR(FLOAT, 384),
          updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    # MERGE is clean for upsert.
    # We'll stage via VALUES.
    values_sql = ", ".join(["(%s, %s, %s)"] * len(rows))
    flat_params = []
    for paper_id, model_name, emb in rows:
        flat_params.extend([paper_id, model_name, emb])

    merge_sql = f"""
    MERGE INTO GOLD_PAPER_EMBEDDINGS t
    USING (SELECT column1 AS paper_id, column2 AS model_name, column3 AS embedding
           FROM VALUES {values_sql}) s
    ON t.paper_id = s.paper_id
    WHEN MATCHED THEN UPDATE SET
      t.model_name = s.model_name,
      t.embedding = s.embedding,
      t.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (paper_id, model_name, embedding)
    VALUES (s.paper_id, s.model_name, s.embedding)
    """
    cur.execute(merge_sql, flat_params)


@app.function(
    image=image,
    secrets=[secret],
    timeout=60 * 20,
)
def run_embedding_batch(limit: int = 200, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
    from sentence_transformers import SentenceTransformer

    conn = _connect_snowflake()
    cur = conn.cursor()

    to_embed = _fetch_unembedded(cur, limit=limit)
    if not to_embed:
        return {"status": "ok", "embedded": 0}

    model = SentenceTransformer(model_name)

    texts = []
    ids = []
    for r in to_embed:
        paper_id = r["paper_id"]
        abstract = (r.get("abstract") or "").strip()
        if not abstract:
            continue
        ids.append(paper_id)
        texts.append(abstract)

    # Batch encode
    vectors = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=False,
        normalize_embeddings=True,  # helpful for cosine similarity
    )

    payload = []
    for paper_id, vec in zip(ids, vectors):
        payload.append((paper_id, model_name, vec.tolist()))

    # Save the embeddings back to Snowflake (upsert)
    _upsert_embeddings(cur, payload)

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "ok", "embedded": len(payload)}

