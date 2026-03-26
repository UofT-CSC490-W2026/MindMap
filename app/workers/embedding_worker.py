# Offline worker to compute and backfill embeddings for papers in SILVER_PAPERS.
# Also computes and caches top-k similar paper ids based on embedding similarity.
from typing import List, Dict, Any, Tuple, Optional
import json

from app.utils import connect_to_snowflake
from app.config import app, ml_image, snowflake_secret, DATABASE, qualify_table

def _silver_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPERS", database=database)


def _chunks_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPER_CHUNKS", database=database)


def _fetch_unembedded_from_silver(cur, database: str = DATABASE, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch papers in SILVER_PAPERS that do not have embeddings yet.
    """
    silver = _silver_table(database=database)
    cur.execute(
        f"""
        SELECT
            "id",
            "title",
            "conclusion",
            "abstract"
        FROM {silver}
        WHERE "embedding" IS NULL
            AND ("abstract" IS NOT NULL OR "conclusion" IS NOT NULL)
        LIMIT {int(limit)}
        """
    )
    rows = cur.fetchall()
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _update_embeddings(cur, database: str, rows: List[Tuple[int, List[float]]], dim: int = 384):
    if not rows:
        return

    silver = _silver_table(database=database)
    sql = f"""
    UPDATE {silver}
    SET "embedding" = PARSE_JSON(%s)::VECTOR(FLOAT, {dim})
    WHERE "id" = %s
    """

    binds = [(json.dumps(emb), int(pid)) for pid, emb in rows]
    cur.executemany(sql, binds)


def _compute_topk_in_snowflake(cur, database: str, pid: int, k: int) -> List[int]:
    silver = _silver_table(database=database)
    cur.execute(
        f"""
        WITH q AS (
            SELECT "embedding" AS qvec
            FROM {silver}
            WHERE "id" = %s
                AND "embedding" IS NOT NULL
        )
        SELECT e."id"
        FROM {silver} e, q
        WHERE e."id" <> %s
            AND e."embedding" IS NOT NULL
        ORDER BY VECTOR_COSINE_SIMILARITY(e."embedding", q.qvec) DESC
        LIMIT %s
        """,
        (pid, pid, int(k)),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _write_similar_ids(cur, database: str, pid: int, sim_ids: List[int]):
    silver = _silver_table(database=database)
    cur.execute(
        f"""
        UPDATE {silver}
        SET "similar_embeddings_ids" = PARSE_JSON(%s)
        WHERE "id" = %s
        """,
        (json.dumps(sim_ids), int(pid)),
    )


def _count_embedded_papers(cur, database: str) -> int:
    silver = _silver_table(database=database)
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {silver}
        WHERE "embedding" IS NOT NULL
        """
    )
    return int(cur.fetchone()[0])


def _build_embedding_text(row: Dict[str, Any]) -> Optional[str]:
    title = (row.get("title") or "").strip()
    abstract = (row.get("abstract") or "").strip()
    # conclusion = (row.get("conclusion") or "").strip()

    # Currently, we focus on title and abstract for embedding. 
    # Conclusion is often missing and can be noisy, so we exclude it for now.

    sections = []
    if title:
        sections.append(f"Title: {title}")
    if abstract:
        sections.append(f"Abstract: {abstract}")
    # if conclusion:
    #     sections.append(f"Conclusion: {conclusion}")

    if not sections:
        return None
    return "\n\n".join(sections)


def _fetch_single_paper_by_arxiv_id(cur, database: str, arxiv_id: str) -> Optional[Dict[str, Any]]:
    silver = _silver_table(database=database)
    cur.execute(
        f"""
        SELECT
            "id",
            "title",
            "conclusion",
            "abstract",
            "embedding"
        FROM {silver}
        WHERE "arxiv_id" = %s
        LIMIT 1
        """,
        (str(arxiv_id),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "title": row[1],
        "conclusion": row[2],
        "abstract": row[3],
        "embedding": row[4],
    }


@app.function(image=ml_image, secrets=[snowflake_secret], timeout=60 * 10)
def process_single_embedding(
    arxiv_id: str,
    model_name: str = "sentence-transformers/all-MiniLM-L12-v2",
    populate_similar: bool = True,
    k: int = 10,
    overwrite_existing: bool = False,
    database: str = DATABASE,
) -> Dict[str, Any]:
    """
    Embed exactly one paper identified by arxiv_id and optionally populate neighbors.
    """
    import importlib
    sentence_transformers = importlib.import_module("sentence_transformers")
    SentenceTransformer = sentence_transformers.SentenceTransformer

    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    try:
        row = _fetch_single_paper_by_arxiv_id(cur, database=database, arxiv_id=arxiv_id)
        if not row:
            return {"status": "failed", "error": f"arxiv_id not found in SILVER_PAPERS: {arxiv_id}"}

        if row.get("embedding") is not None and not overwrite_existing:
            return {
                "status": "skipped",
                "reason": "embedding already exists",
                "arxiv_id": arxiv_id,
                "paper_id": int(row["id"]),
                "database": database,
            }

        text = _build_embedding_text(row)
        if not text:
            return {
                "status": "failed",
                "error": "No usable title/abstract text found for embedding.",
                "arxiv_id": arxiv_id,
                "paper_id": int(row["id"]),
                "database": database,
            }

        model = SentenceTransformer(model_name)
        vec = model.encode([text], normalize_embeddings=True)[0].tolist()
        pid = int(row["id"])

        _update_embeddings(cur, database=database, rows=[(pid, vec)])
        conn.commit()

        populated_neighbors = False
        if populate_similar:
            sim_ids = _compute_topk_in_snowflake(cur, database=database, pid=pid, k=int(k))
            _write_similar_ids(cur, database=database, pid=pid, sim_ids=sim_ids)
            conn.commit()
            populated_neighbors = True

        return {
            "status": "ok",
            "arxiv_id": arxiv_id,
            "paper_id": pid,
            "model": model_name,
            "neighbors_populated": populated_neighbors,
            "database": database,
        }
    finally:
        cur.close()
        conn.close()


@app.function(image=ml_image, secrets=[snowflake_secret], timeout=60 * 20)
def run_embedding_batch(
    limit: int = 200,
    model_name: str = "sentence-transformers/all-MiniLM-L12-v2",
    populate_similar: bool = True,
    min_corpus_size_for_neighbors: Optional[int] = None,
    k: int = 10,
    database: str = DATABASE,
) -> Dict[str, Any]:
    """
    Pull rows with NULL embedding from SILVER_PAPERS, generate embeddings,
    and optionally populate similar ids.
    """
    import importlib
    sentence_transformers = importlib.import_module("sentence_transformers")
    SentenceTransformer = sentence_transformers.SentenceTransformer

    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    try:
        to_embed = _fetch_unembedded_from_silver(cur, database=database, limit=limit)
        if not to_embed:
            return {"status": "ok", "embedded": 0, "note": "No new rows with NULL embedding."}

        model = SentenceTransformer(model_name)

        ids: List[int] = []
        texts: List[str] = []
        skipped_empty_text = 0

        for row in to_embed:
            pid = int(row["id"])
            text = _build_embedding_text(row)
            if not text:
                skipped_empty_text += 1
                continue
            ids.append(pid)
            texts.append(text)

        if not texts:
            return {
                "status": "ok",
                "embedded": 0,
                "note": "No rows contained usable text for embedding.",
                "skipped_empty_text": skipped_empty_text,
            }

        vectors = model.encode(
            texts,
            batch_size=32,
            show_progress_bar=False,
            normalize_embeddings=True,
        )

        payload: List[Tuple[int, List[float]]] = []
        for pid, vec in zip(ids, vectors):
            payload.append((pid, vec.tolist()))

        _update_embeddings(cur, database=database, rows=payload)
        conn.commit()

        total = _count_embedded_papers(cur, database=database)
        should_populate_neighbors = (
            populate_similar
            and (min_corpus_size_for_neighbors is None or total >= int(min_corpus_size_for_neighbors))
        )

        if should_populate_neighbors:
            for pid in ids:
                sim_ids = _compute_topk_in_snowflake(cur, database=database, pid=pid, k=k)
                _write_similar_ids(cur, database=database, pid=pid, sim_ids=sim_ids)
            conn.commit()

        return {
            "status": "ok",
            "embedded": len(payload),
            "model": model_name,
            "k": int(k),
            "database": database,
            "neighbors_populated": bool(should_populate_neighbors),
            "skipped_empty_text": skipped_empty_text,
        }
    finally:
        cur.close()
        conn.close()


@app.function(image=ml_image, secrets=[snowflake_secret], timeout=60 * 20)
def backfill_similar_ids(limit: int = 200, k: int = 10, database: str = DATABASE) -> Dict[str, Any]:
    """
    Fill similar_embeddings_ids for older papers that already have embeddings
    but do not yet have cached neighbors.
    """
    silver = _silver_table(database=database)

    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
                SELECT id
                FROM {silver}
                WHERE "embedding" IS NOT NULL
                    AND "similar_embeddings_ids" IS NULL
                LIMIT {int(limit)}
            """
        )
        ids = [int(r[0]) for r in cur.fetchall()]
        if not ids:
            return {"status": "ok", "backfilled": 0, "note": "No rows missing cache."}

        for pid in ids:
            sim_ids = _compute_topk_in_snowflake(cur, database=database, pid=pid, k=k)
            cur.execute(
                f"""
                UPDATE {silver}
                SET "similar_embeddings_ids" = PARSE_JSON(%s)
                WHERE "id" = %s
                """,
                (json.dumps(sim_ids), int(pid)),
            )

        conn.commit()
        return {"status": "ok", "backfilled": len(ids), "k": int(k), "database": database}
    finally:
        cur.close()
        conn.close()


def _fetch_unembedded_chunks(cur, database: str = DATABASE, limit: int = 500) -> List[Dict[str, Any]]:
    """
    Fetch chunks that do not have embeddings yet.
    """
    chunks = _chunks_table(database=database)
    cur.execute(
        f"""
        SELECT
            "chunk_id",
            "paper_id",
            "section_id",
            "chunk_text"
        FROM {chunks}
        WHERE "embedding" IS NULL
            AND "chunk_text" IS NOT NULL
        LIMIT {int(limit)}
        """
    )
    rows = cur.fetchall()
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _update_chunk_embeddings(cur, database: str, rows: List[Tuple[int, List[float]]], dim: int = 384):
    """
    Update embeddings for chunks.
    """
    if not rows:
        return

    chunks = _chunks_table(database=database)
    sql = f"""
    UPDATE {chunks}
    SET "embedding" = PARSE_JSON(%s)::VECTOR(FLOAT, {dim})
    WHERE "chunk_id" = %s
    """

    binds = [(json.dumps(emb), int(chunk_id)) for chunk_id, emb in rows]
    cur.executemany(sql, binds)


@app.function(image=ml_image, secrets=[snowflake_secret], timeout=60 * 30)
def run_chunk_embedding_batch(
    limit: int = 500,
    model_name: str = "sentence-transformers/all-MiniLM-L12-v2",
    database: str = DATABASE,
) -> Dict[str, Any]:
    """
    Embed chunks from SILVER_PAPER_CHUNKS.

    - Fetches chunks without embeddings
    - Encodes chunk_text using the same model as paper embeddings
    - Stores results in SILVER_PAPER_CHUNKS.embedding column
    - Independent of paper-level embeddings (both can coexist)
    """
    import importlib
    sentence_transformers = importlib.import_module("sentence_transformers")
    SentenceTransformer = sentence_transformers.SentenceTransformer

    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    try:
        chunks_to_embed = _fetch_unembedded_chunks(cur, database=database, limit=limit)
        if not chunks_to_embed:
            return {"status": "ok", "chunks_embedded": 0, "note": "No new chunks to embed."}

        model = SentenceTransformer(model_name)

        chunk_ids: List[int] = []
        texts: List[str] = []

        for chunk in chunks_to_embed:
            chunk_id = int(chunk["chunk_id"])
            text = (chunk.get("chunk_text") or "").strip()
            if not text:
                continue
            chunk_ids.append(chunk_id)
            texts.append(text)

        if not texts:
            return {
                "status": "ok",
                "chunks_embedded": 0,
                "note": "No chunks contained usable text for embedding.",
            }

        vectors = model.encode(
            texts,
            batch_size=32,
            show_progress_bar=False,
            normalize_embeddings=True,
        )

        payload: List[Tuple[int, List[float]]] = []
        for chunk_id, vec in zip(chunk_ids, vectors):
            payload.append((chunk_id, vec.tolist()))

        _update_chunk_embeddings(cur, database=database, rows=payload)
        conn.commit()

        return {
            "status": "ok",
            "chunks_embedded": len(payload),
            "model": model_name,
            "database": database,
        }
    finally:
        cur.close()
        conn.close()
