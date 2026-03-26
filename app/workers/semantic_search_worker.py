# Online worker that computes and caches top-k similar paper ids based on embedding similarity.
# This runs on demand when a request needs similar papers for a given paper id.

from typing import List, Dict, Any, Optional
import json
import re

from app.utils import connect_to_snowflake
from app.config import app, image, ml_image, snowflake_secret, DATABASE, qualify_table


def _silver_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPERS", database=database)


def _chunks_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPER_CHUNKS", database=database)


def _parse_cached_ids(value: Any, k: int) -> List[int]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(value, list):
        return []

    parsed: List[int] = []
    for item in value:
        try:
            parsed.append(int(item))
        except (TypeError, ValueError):
            continue
    return parsed[: int(k)]


def _keyword_tokens(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]{3,}", (text or "").lower()))


def _hybrid_score(query_tokens: set[str], title: str, abstract: str, vector_score: float) -> float:
    if not query_tokens:
        return float(vector_score)
    doc_tokens = _keyword_tokens(f"{title or ''} {abstract or ''}")
    overlap = len(query_tokens & doc_tokens) / max(1, len(query_tokens))
    return 0.85 * float(vector_score) + 0.15 * float(overlap)


@app.function(image=image, secrets=[snowflake_secret], timeout=60 * 5)
def get_related_papers(
    paper_id: int,
    k: int = 10,
    score_threshold: float = 0.0,
    force_refresh: bool = False,
    database: str = DATABASE,
) -> List[Dict[str, Any]]:
    """
    ONLINE endpoint:
    1) Try cached similar_embeddings_ids first
    2) If missing, fallback to vector similarity query
    """
    silver = _silver_table(database=database)

    conn = connect_to_snowflake(database=database, schema="GOLD")
    cur = conn.cursor()
    try:
        if not force_refresh:
            cur.execute(
                f"""
                SELECT "similar_embeddings_ids"
                FROM {silver}
                WHERE "id" = %s
                """,
                (int(paper_id),),
            )
            row = cur.fetchone()
            cached_ids = _parse_cached_ids(row[0] if row else None, k)
            if cached_ids:
                values_sql = ", ".join(["(%s)"] * len(cached_ids))
                cur.execute(
                    f"""
                    WITH ids("id") AS (SELECT column1 FROM VALUES {values_sql})
                    SELECT s."id", s."arxiv_id", s."title"
                    FROM ids
                    JOIN {silver} s
                                            ON s.id = ids.id
                    """,
                    [int(x) for x in cached_ids],
                )
                rows = cur.fetchall()
                id_to_row = {int(r[0]): r for r in rows}

                ordered = []
                for cid in cached_ids:
                    cid = int(cid)
                    if cid in id_to_row:
                        r = id_to_row[cid]
                        ordered.append(
                            {
                                "id": cid,
                                "arxiv_id": r[1],
                                "title": r[2],
                                "source": "cache",
                                "database": database,
                            }
                        )
                return ordered

        cur.execute(
            f"""
            WITH q AS (
                            SELECT "embedding" AS qvec
              FROM {silver}
                            WHERE id = %s
                                AND embedding IS NOT NULL
            )
            SELECT
                            e.id,
                            e.arxiv_id,
                            e.title,
                            e.abstract,
                            VECTOR_COSINE_SIMILARITY(e.embedding, q.qvec) AS score
            FROM {silver} e, q
                        WHERE e.id <> %s
                            AND e.embedding IS NOT NULL
                            AND VECTOR_COSINE_SIMILARITY(e.embedding, q.qvec) >= %s
            ORDER BY score DESC
            LIMIT %s
            """,
            (int(paper_id), int(paper_id), float(score_threshold), int(k)),
        )
        rows = cur.fetchall()

        results = [
            {
                "id": int(r[0]),
                "arxiv_id": r[1],
                "title": r[2],
                "score": float(r[4]),
                "source": "fallback",
                "database": database,
                "schema": schema,
            }
            for r in rows
        ]

        if results:
            ids_only = [r["id"] for r in results]
            cur.execute(
                f"""
                UPDATE {silver}
                SET similar_embeddings_ids = PARSE_JSON(%s)
                WHERE id = %s
                """,
                (json.dumps(ids_only), int(paper_id)),
            )
            conn.commit()

        return results
    finally:
        cur.close()
        conn.close()


@app.function(image=ml_image, secrets=[snowflake_secret], timeout=60 * 8)
def semantic_search(
    query: str,
    k: int = 10,
    candidate_pool: int = 100,
    model_name: str = "sentence-transformers/all-MiniLM-L12-v2",
    score_threshold: float = 0.0,
    database: str = DATABASE,
) -> List[Dict[str, Any]]:
    """
    Query-based semantic search across papers with a lightweight hybrid rerank.
    """
    import importlib
    sentence_transformers = importlib.import_module("sentence_transformers")
    SentenceTransformer = sentence_transformers.SentenceTransformer

    q = (query or "").strip()
    if not q:
        return []

    silver = _silver_table(database=database)

    model = SentenceTransformer(model_name)
    qvec = model.encode([q], normalize_embeddings=True)[0].tolist()

    conn = connect_to_snowflake(database=database, schema="GOLD")
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                            s.id,
                            s.arxiv_id,
                            s.title,
                            s.abstract,
                            VECTOR_COSINE_SIMILARITY(s.embedding, PARSE_JSON(%s)::VECTOR(FLOAT, 384)) AS vec_score
            FROM {silver} s
                        WHERE s.embedding IS NOT NULL
                            AND VECTOR_COSINE_SIMILARITY(s.embedding, PARSE_JSON(%s)::VECTOR(FLOAT, 384)) >= %s
            ORDER BY vec_score DESC
            LIMIT %s
            """,
            (json.dumps(qvec), json.dumps(qvec), float(score_threshold), int(candidate_pool)),
        )
        rows = cur.fetchall()

        q_tokens = _keyword_tokens(q)
        ranked = []
        for pid, arxiv_id, title, abstract, vec_score in rows:
            hybrid = _hybrid_score(q_tokens, title or "", abstract or "", float(vec_score))
            ranked.append(
                {
                    "id": int(pid),
                    "arxiv_id": arxiv_id,
                    "title": title,
                    "score": float(vec_score),
                    "hybrid_score": float(hybrid),
                    "database": database,
                }
            )

        ranked.sort(key=lambda x: x["hybrid_score"], reverse=True)
        return ranked[: int(k)]
    finally:
        cur.close()
        conn.close()


@app.function(image=ml_image, secrets=[snowflake_secret], timeout=60 * 10)
def retrieve_similar_chunks(
    query_text: str,
    top_k: int = 5,
    paper_id: Optional[int] = None,
    score_threshold: float = 0.3,
    model_name: str = "sentence-transformers/all-MiniLM-L12-v2",
    database: str = DATABASE,
) -> List[Dict[str, Any]]:
    """
    RAG retrieval: find most similar chunks to a query.

    - Embeds the query text
    - Searches chunk embeddings by cosine similarity
    - Optionally filters by paper_id
    - Returns chunk metadata with scores
    """
    import importlib
    sentence_transformers = importlib.import_module("sentence_transformers")
    SentenceTransformer = sentence_transformers.SentenceTransformer

    q = (query_text or "").strip()
    if not q:
        return []

    chunks = _chunks_table(database=database)

    model = SentenceTransformer(model_name)
    qvec = model.encode([q], normalize_embeddings=True)[0].tolist()

    conn = connect_to_snowflake(database=database, schema="GOLD")
    cur = conn.cursor()
    try:
        paper_filter = ""
        params = [json.dumps(qvec), json.dumps(qvec), float(score_threshold)]

        if paper_id is not None:
            paper_filter = "AND c.paper_id = %s"
            params.append(int(paper_id))

        params.append(int(top_k))

        cur.execute(
            f"""
            SELECT
                            c.chunk_id,
                            c.paper_id,
                            c.section_id,
                            c.chunk_text,
                            c.chunk_type,
                            VECTOR_COSINE_SIMILARITY(c.embedding, PARSE_JSON(%s)::VECTOR(FLOAT, 384)) AS score
            FROM {chunks} c
                        WHERE c.embedding IS NOT NULL
                            AND VECTOR_COSINE_SIMILARITY(c.embedding, PARSE_JSON(%s)::VECTOR(FLOAT, 384)) >= %s
              {paper_filter}
            ORDER BY score DESC
            LIMIT %s
            """,
            params,
        )

        rows = cur.fetchall()
        results = [
            {
                "chunk_id": int(r[0]),
                "paper_id": int(r[1]),
                "section_id": int(r[2]) if r[2] else None,
                "chunk_text": r[3],
                "section_name": r[4],
                "score": float(r[5]),
                "database": database,
            }
            for r in rows
        ]

        return results
    finally:
        cur.close()
        conn.close()
