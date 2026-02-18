# app/workers/embedding_worker.py

"""
ML Lead responsibilities for A2 / Milestone 2:
- get_embedding: Pull text from SILVER_PAPERS and generate embeddings
- Semantic Search (get_related_papers): Use Snowflake vector similarity to find related papers
- Citation Worker (get_citations): Fetch citations/references (via Semantic Scholar; parsing can be future work)
"""

import os
from typing import List, Dict, Any, Tuple, Optional

import modal

# ---- Modal image (keep lean; add only what you need) ----
image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install(
        "sentence-transformers==2.7.0",
        "torch",
        "snowflake-connector-python[pandas]==3.12.0",
        "pandas",
        "requests",
    )
)

app = modal.App("mindmap-ml-workers")

# Your Modal secret contains SNOWFLAKE_* vars
secret = modal.Secret.from_name("mindmap-1")


# ------------------------------
# Snowflake helpers
# ------------------------------
def _connect_snowflake():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "MINDMAP_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )


def _ensure_embeddings_table(cur):
    # You already created this table, but keeping this makes the worker robust.
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
    """
    Fetch papers from SILVER_PAPERS that do not yet have embeddings in PAPER_EMBEDDINGS.

    Your schema:
      - SILVER_PAPERS(paper_id UUID, arxiv_id STRING, title STRING, abstract STRING, published_date DATE)
      - PAPER_EMBEDDINGS(paper_id STRING PRIMARY KEY, model_name STRING, embedding VECTOR(FLOAT,384), ...)
    """
    # NOTE: paper_id in SILVER is UUID; we cast to STRING for joining with PAPER_EMBEDDINGS.
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
    """
    Upsert embeddings into PAPER_EMBEDDINGS using MERGE.
    rows: (paper_id_str, model_name, embedding_list[float])
    """
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


# ------------------------------
# 1) Embedding worker (batch)
# ------------------------------
@app.function(image=image, secrets=[secret], timeout=60 * 20)
def run_embedding_batch(
    limit: int = 200,
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
) -> Dict[str, Any]:
    """
    Pull up to `limit` rows from SILVER_PAPERS that do not yet have embeddings,
    embed (title + abstract), and upsert into PAPER_EMBEDDINGS.
    """
    from sentence_transformers import SentenceTransformer

    conn = _connect_snowflake()
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

            # Embed title + abstract for better signal than abstract alone
            joined = f"{title}\n\n{abstract}" if title else abstract
            ids.append(pid)
            texts.append(joined)

        vectors = model.encode(
            texts,
            batch_size=32,
            show_progress_bar=False,
            normalize_embeddings=True,  # better for cosine similarity
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


# ------------------------------
# 2) Semantic Search (related papers)
# ------------------------------
@app.function(image=image, secrets=[secret], timeout=60 * 5)
def get_related_papers(paper_id: str, k: int = 10) -> List[Dict[str, Any]]:
    """
    Return top-k similar papers based on cosine similarity in Snowflake.
    Requires that PAPER_EMBEDDINGS already contains vectors.
    """
    conn = _connect_snowflake()
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


# ------------------------------
# 3) Citation Worker (Semantic Scholar)
# ------------------------------
@app.function(image=image, secrets=[secret], timeout=60 * 5)
def get_citations(
    s2_paper_id: Optional[str] = None,
    doi: Optional[str] = None,
    arxiv_id: Optional[str] = None,
    limit: int = 100,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch references + citations from Semantic Scholar Graph API.

    You can call this with:
      - s2_paper_id (best), OR
      - doi, OR
      - arxiv_id

    If you don't have a Semantic Scholar API key, it may still work for light usage.
    If you DO have one, set SEMANTIC_SCHOLAR_API_KEY in Modal secret/env.
    """
    import requests

    api_key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
    headers = {"x-api-key": api_key} if api_key else {}

    base = "https://api.semanticscholar.org/graph/v1"

    # Determine identifier format supported by S2:
    # - PaperId directly: {paperId}
    # - DOI: DOI:<doi>
    # - arXiv: arXiv:<id>
    if s2_paper_id:
        ident = s2_paper_id
    elif doi:
        ident = f"DOI:{doi}"
    elif arxiv_id:
        ident = f"arXiv:{arxiv_id}"
    else:
        raise ValueError("Provide one of: s2_paper_id, doi, or arxiv_id")

    fields = "paperId,title,year,authors"

    def _norm(p: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "paperId": p.get("paperId"),
            "title": p.get("title"),
            "year": p.get("year"),
            "authors": [a.get("name") for a in (p.get("authors") or [])],
        }

    # references (papers this paper cites)
    ref_url = f"{base}/paper/{ident}/references"
    ref_resp = requests.get(ref_url, headers=headers, params={"fields": fields, "limit": int(limit)}, timeout=30)
    ref_resp.raise_for_status()
    ref_data = ref_resp.json().get("data", [])
    references = [_norm(x.get("citedPaper") or {}) for x in ref_data if x.get("citedPaper")]

    # citations (papers that cite this paper)
    cit_url = f"{base}/paper/{ident}/citations"
    cit_resp = requests.get(cit_url, headers=headers, params={"fields": fields, "limit": int(limit)}, timeout=30)
    cit_resp.raise_for_status()
    cit_data = cit_resp.json().get("data", [])
    citations = [_norm(x.get("citingPaper") or {}) for x in cit_data if x.get("citingPaper")]

    return {"references": references, "citations": citations}
