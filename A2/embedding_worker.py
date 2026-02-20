# Offline worker to compute and backfill embeddings for papers in SILVER_PAPERS.
# Also computes and caches top-k similar paper ids based on embedding similarity.

import modal
from typing import List, Dict, Any, Tuple, Optional
import json
from utils import _connect_to_snowflake

# ---- Modal image ----
image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install(
        "sentence-transformers==2.7.0",
        "torch",
        "snowflake-connector-python[pandas]==3.12.0",
        "pandas",
        "numpy",
    )
)

app = modal.App("mindmap-ml-workers")
secret = modal.Secret.from_name("mindmap-1")


def _fetch_unembedded_from_silver(cur, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch papers in MINDMAP_DB.PUBLIC.SILVER_PAPERS that don't have embeddings yet.
    We use id (INT identity) as the key.
    """
    cur.execute(
        f"""
        SELECT
          id,
          title,
          abstract
        FROM MINDMAP_DB.PUBLIC.SILVER_PAPERS
        WHERE embedding IS NULL
          AND abstract IS NOT NULL
        LIMIT {int(limit)}
        """
    )
    rows = cur.fetchall()
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _update_embeddings(cur, rows: List[Tuple[int, List[float]]]):
    """
    rows: (id, embedding_list)
    Update SILVER_PAPERS.embedding for each id.
    """
    if not rows:
        return

    # executemany is simplest and readable
    cur.executemany(
        """
        UPDATE MINDMAP_DB.PUBLIC.SILVER_PAPERS
        SET embedding = %s
        WHERE id = %s
        """,
        [(emb, pid) for pid, emb in rows],
    )

def _compute_topk_in_snowflake(cur, pid: int, k: int) -> List[int]:
    cur.execute(
        """
        WITH q AS (
          SELECT embedding AS qvec
          FROM MINDMAP_DB.PUBLIC.SILVER_PAPERS
          WHERE id = %s
            AND embedding IS NOT NULL
        )
        SELECT e.id
        FROM MINDMAP_DB.PUBLIC.SILVER_PAPERS e, q
        WHERE e.id <> %s
          AND e.embedding IS NOT NULL
        ORDER BY VECTOR_COSINE_SIMILARITY(e.embedding, q.qvec) DESC
        LIMIT %s
        """,
        (pid, pid, int(k)),
    )
    return [int(r[0]) for r in cur.fetchall()]

def _write_similar_ids(cur, pid: int, sim_ids: List[int]):
    cur.execute(
        """
        UPDATE MINDMAP_DB.PUBLIC.SILVER_PAPERS
        SET similar_embddings_ids = PARSE_JSON(%s)
        WHERE id = %s
        """,
        (json.dumps(sim_ids), int(pid)),
    )

def _count_embedded_papers(cur) -> int:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM MINDMAP_DB.PUBLIC.SILVER_PAPERS
        WHERE embedding IS NOT NULL
        """
    )
    return int(cur.fetchone()[0])

@app.function(image=image, secrets=[secret], timeout=60 * 20)
def run_embedding_batch(
    limit: int = 200,
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
    populate_similar: bool = True,   # whether to also compute+store similar ids for each embedded paper
    min_corpus_size_for_neighbors: Optional[int] = None,   # if set, only populate similar ids if total embedded papers >= this threshold (to avoid OOM)
    k: int = 10,
) -> Dict[str, Any]:
    """
    - Pull up to `limit` rows with NULL embedding from SILVER_PAPERS
    - Embed title+abstract
    - Update SILVER_PAPERS.embedding

    Optional:
    - populate_similar=True will also compute top-k similar paper ids for each embedded paper
      and store into SILVER_PAPERS.similar_embddings_ids (VARIANT JSON array).
    """
    from sentence_transformers import SentenceTransformer

    conn = _connect_to_snowflake()
    cur = conn.cursor()
    try:
        to_embed = _fetch_unembedded_from_silver(cur, limit=limit)
        if not to_embed:
            return {"status": "ok", "embedded": 0, "note": "No new rows with NULL embedding."}

        model = SentenceTransformer(model_name)

        ids: List[int] = []
        texts: List[str] = []

        for r in to_embed:
            pid = int(r["id"])
            title = (r.get("title") or "").strip()
            abstract = (r.get("abstract") or "").strip()
            if not abstract:
                continue

            joined = f"{title}\n\n{abstract}" if title else abstract
            ids.append(pid)
            texts.append(joined)

        # Compute the vector embeddings for the batch
        vectors = model.encode(
            texts,
            batch_size=32,
            show_progress_bar=False,
            normalize_embeddings=True,
        )

        # Update embeddings in Silver
        payload: List[Tuple[int, List[float]]] = []
        for pid, vec in zip(ids, vectors):
            payload.append((pid, vec.tolist()))

        _update_embeddings(cur, payload)
        conn.commit()

        # Optionally compute similar paper ids (simple baseline)
        total = _count_embedded_papers(cur)

        # run similarity computation
        if populate_similar and min_corpus_size_for_neighbors and total >= min_corpus_size_for_neighbors:
            # Fetch all existing embeddings (including ones you just wrote)

            for pid in ids:
                sim_ids = _compute_topk_in_snowflake(cur, pid, k)
                _write_similar_ids(cur, pid, sim_ids)
            conn.commit()

        return {
            "status": "ok",
            "embedded": len(payload),
            "model": model_name,
            "k": int(k),
        }
    finally:
        cur.close()
        conn.close()

@app.function(image=image, secrets=[secret], timeout=60 * 20)
def backfill_similar_ids(limit: int = 200, k: int = 10) -> Dict[str, Any]:
    """
    OFFLINE job:
    Fill similar_embddings_ids for older papers that already have embeddings
    but do not have cached neighbors yet.
    """
    conn = _connect_to_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT id
            FROM MINDMAP_DB.PUBLIC.SILVER_PAPERS
            WHERE embedding IS NOT NULL
              AND similar_embddings_ids IS NULL
            LIMIT {int(limit)}
            """
        )
        ids = [int(r[0]) for r in cur.fetchall()]
        if not ids:
            return {"status": "ok", "backfilled": 0, "note": "No rows missing cache."}

        for pid in ids:
            sim_ids = _compute_topk_in_snowflake(cur, pid, k)
            cur.execute(
                """
                UPDATE MINDMAP_DB.PUBLIC.SILVER_PAPERS
                SET similar_embddings_ids = PARSE_JSON(%s)
                WHERE id = %s
                """,
                (json.dumps(sim_ids), int(pid)),
            )

        conn.commit()
        return {"status": "ok", "backfilled": len(ids), "k": int(k)}
    finally:
        cur.close()
        conn.close()
