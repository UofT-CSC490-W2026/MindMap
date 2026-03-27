# Offline worker to compute and backfill embeddings for papers in SILVER_PAPERS.
# Also computes and caches top-k similar paper ids based on embedding similarity.
from typing import List, Dict, Any, Tuple, Optional
import cProfile
import io
import json
import pstats

from utils import connect_to_snowflake
from config import app, ml_image, snowflake_secret, DATABASE, qualify_table

_PROFILE_LOG = "/tmp/profile_output.txt"


def _write_profile(label: str, profiler: cProfile.Profile, top_n: int = 10) -> None:
    """Print cProfile stats to stdout so Modal streams them to the terminal."""
    s = io.StringIO()
    pstats.Stats(profiler, stream=s).sort_stats("cumulative").print_stats(top_n)
    output = (
        f"\n{'=' * 70}\n"
        f"  PROFILE: {label}\n"
        f"{'=' * 70}\n"
        + s.getvalue()
    )
    print(output)

def _silver_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPERS", database=database)


def _chunks_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPER_CHUNKS", database=database)


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


def _fetch_unembedded_from_silver(cur, database: str = DATABASE, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch papers in SILVER_PAPERS that do not have embeddings yet.
    """
    # Profiled because: DESC TABLE is called on every invocation to resolve
    # column names dynamically — this round-trip to Snowflake adds latency
    # before the actual SELECT even starts, and it repeats for every batch.
    profiler = cProfile.Profile()
    profiler.enable()

    silver = _silver_table(database=database)
    cols = _require_columns(
        _resolve_table_columns(cur, silver),
        ["id", "title", "conclusion", "abstract", "embedding"],
        silver,
    )
    cur.execute(
        f"""
        SELECT
            {cols["id"]} AS id,
            {cols["title"]} AS title,
            {cols["conclusion"]} AS conclusion,
            {cols["abstract"]} AS abstract
        FROM {silver}
        WHERE {cols["embedding"]} IS NULL
            AND ({cols["abstract"]} IS NOT NULL OR {cols["conclusion"]} IS NOT NULL)
        LIMIT {int(limit)}
        """
    )
    rows = cur.fetchall()
    cols = [c[0].lower() for c in cur.description]
    result = [dict(zip(cols, r)) for r in rows]

    profiler.disable()
    _write_profile("_fetch_unembedded_from_silver", profiler)

    return result

def _update_embeddings(cur, database: str, rows: List[Tuple[int, List[float]]], dim: int = 384):
    if not rows:
        return

    silver = _silver_table(database=database)
    cols = _require_columns(
        _resolve_table_columns(cur, silver),
        ["id", "embedding"],
        silver,
    )
    sql = f"""
    UPDATE {silver}
    SET {cols["embedding"]} = PARSE_JSON(%s)::VECTOR(FLOAT, {dim})
    WHERE {cols["id"]} = %s
    """

    binds = [(json.dumps(emb), int(pid)) for pid, emb in rows]
    cur.executemany(sql, binds)


def _compute_topk_in_snowflake(cur, database: str, pid: int, k: int) -> List[int]:
    # Profiled because: this runs a full cosine-similarity scan over every
    # embedded paper in Silver for each paper we process — O(n) Snowflake
    # compute per paper, so it scales badly as the corpus grows.
    profiler = cProfile.Profile()
    profiler.enable()

    silver = _silver_table(database=database)
    cols = _require_columns(
        _resolve_table_columns(cur, silver),
        ["id", "embedding"],
        silver,
    )
    cur.execute(
        f"""
        WITH q AS (
            SELECT {cols["embedding"]} AS qvec
            FROM {silver}
            WHERE {cols["id"]} = %s
                AND {cols["embedding"]} IS NOT NULL
        )
        SELECT e.{cols["id"]}
        FROM {silver} e, q
        WHERE e.{cols["id"]} <> %s
            AND e.{cols["embedding"]} IS NOT NULL
        ORDER BY VECTOR_COSINE_SIMILARITY(e.{cols["embedding"]}, q.qvec) DESC
        LIMIT %s
        """,
        (pid, pid, int(k)),
    )
    result = [int(r[0]) for r in cur.fetchall()]

    profiler.disable()
    _write_profile(f"_compute_topk_in_snowflake (pid={pid})", profiler)

    return result

def _write_similar_ids(cur, database: str, pid: int, sim_ids: List[int]):
    silver = _silver_table(database=database)
    cols = _require_columns(
        _resolve_table_columns(cur, silver),
        ["id", "similar_embeddings_ids"],
        silver,
    )
    cur.execute(
        f"""
        UPDATE {silver}
        SET {cols["similar_embeddings_ids"]} = PARSE_JSON(%s)
        WHERE {cols["id"]} = %s
        """,
        (json.dumps(sim_ids), int(pid)),
    )


def _count_embedded_papers(cur, database: str) -> int:
    silver = _silver_table(database=database)
    cols = _require_columns(
        _resolve_table_columns(cur, silver),
        ["embedding"],
        silver,
    )
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {silver}
        WHERE {cols["embedding"]} IS NOT NULL
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

        # Profiled because: SentenceTransformer model load is expensive (~2-5s),
        # and model.encode() over a batch is the dominant CPU/GPU cost — seeing
        # its share of total time tells us whether batching or model choice matters.
        profiler = cProfile.Profile()
        profiler.enable()

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

        profiler.disable()
        _write_profile("run_embedding_batch", profiler)

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
    cols = _require_columns(
        _resolve_table_columns(cur, silver),
        ["id", "embedding", "similar_embeddings_ids"],
        silver,
    )
    try:
        cur.execute(
            f"""
                SELECT {cols["id"]}
                FROM {silver}
                WHERE {cols["embedding"]} IS NOT NULL
                    AND {cols["similar_embeddings_ids"]} IS NULL
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
                SET {cols["similar_embeddings_ids"]} = PARSE_JSON(%s)
                WHERE {cols["id"]} = %s
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
    cols = _require_columns(
        _resolve_table_columns(cur, chunks),
        ["chunk_id", "paper_id", "section_id", "chunk_text", "embedding"],
        chunks,
    )
    cur.execute(
        f"""
        SELECT
            {cols["chunk_id"]} AS chunk_id,
            {cols["paper_id"]} AS paper_id,
            {cols["section_id"]} AS section_id,
            {cols["chunk_text"]} AS chunk_text
        FROM {chunks}
        WHERE {cols["embedding"]} IS NULL
            AND {cols["chunk_text"]} IS NOT NULL
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
    cols = _require_columns(
        _resolve_table_columns(cur, chunks),
        ["chunk_id", "embedding"],
        chunks,
    )
    sql = f"""
    UPDATE {chunks}
    SET {cols["embedding"]} = PARSE_JSON(%s)::VECTOR(FLOAT, {dim})
    WHERE {cols["chunk_id"]} = %s
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

        # Profiled because: chunk counts are typically 5-10× paper counts, so
        # model.encode() runs over a much larger list — this is likely the
        # single most expensive encode call in the whole pipeline.
        profiler = cProfile.Profile()
        profiler.enable()

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

        profiler.disable()
        _write_profile("run_chunk_embedding_batch", profiler)

        return {
            "status": "ok",
            "chunks_embedded": len(payload),
            "model": model_name,
            "database": database,
        }
    finally:
        cur.close()
        conn.close()
