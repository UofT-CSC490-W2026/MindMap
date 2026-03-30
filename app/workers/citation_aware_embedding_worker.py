from typing import List, Dict, Any, Optional, Tuple
import math
import re

from app.config import app, DATABASE, image_citation_aware, snowflake_secret
from app.utils import connect_to_snowflake
from .citation_worker import get_citations  # your Modal function
# NOTE: importing Modal functions across files is okay if both are in the same app name


def _l2_normalize(vec: List[float]) -> List[float]:
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [x / norm for x in vec]


def _ensure_tables(cur):
        cur.execute(
                """
                CREATE TABLE IF NOT EXISTS PAPER_EMBEDDINGS_CA (
                    "paper_id" STRING PRIMARY KEY,
                    "model_name" STRING,
                    "embedding" VECTOR(FLOAT, 384),
                    "alpha" FLOAT,
                    "updated_at" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
        )
        cur.execute(
                """
                CREATE TABLE IF NOT EXISTS GOLD_REFERENCES (
                    "paper_id" STRING,
                    "arxiv_id" STRING,
                    "ref_index" INT,
                    "ref_text" STRING,
                    "ref_arxiv_id" STRING,
                    "created_at" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
        )


def _upsert_ca_embedding(cur, paper_id: str, model_name: str, alpha: float, emb: List[float]):
        cur.execute(
                """
                MERGE INTO PAPER_EMBEDDINGS_CA t
                USING (SELECT %s AS "paper_id", %s AS "model_name", %s AS "embedding", %s AS "alpha") s
                ON t."paper_id" = s."paper_id"
                WHEN MATCHED THEN UPDATE SET
                    t."model_name" = s."model_name",
                    t."embedding" = s."embedding",
                    t."alpha" = s."alpha",
                    t."updated_at" = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT ("paper_id", "model_name", "embedding", "alpha")
                VALUES (s."paper_id", s."model_name", s."embedding", s."alpha")
                """,
                (paper_id, model_name, emb, float(alpha)),
        )


def _extract_ref_arxiv_id(ref) -> Optional[str]:
    if isinstance(ref, dict):
        return ref.get("ref_arxiv_id")
    if not isinstance(ref, str):
        return None
    match = re.search(r"\b(\d{4}\.\d{4,5})(?:v\d+)?\b", ref)
    return match.group(1) if match else None


def _extract_ref_text(ref) -> str:
    if isinstance(ref, dict):
        return ref.get("ref_text") or ""
    return str(ref)


def _insert_references(cur, paper_id: str, arxiv_id: str, refs: List[Any]):
    for i, ref in enumerate(refs):
        cur.execute(
            """
            INSERT INTO GOLD_REFERENCES("paper_id", "arxiv_id", "ref_index", "ref_text", "ref_arxiv_id")
            VALUES (%s, %s, %s, %s, %s)
            """,
            (paper_id, arxiv_id, int(i), _extract_ref_text(ref), _extract_ref_arxiv_id(ref)),
        )


def _resolve_ref_paper_ids(cur, ref_arxiv_ids: List[str]) -> List[str]:
    """
    Map referenced arXiv IDs → our own paper_id strings (UUIDs) using SILVER_PAPERS.arxiv_id.
    Returns list of paper_id strings we can fetch embeddings for.
    """
    if not ref_arxiv_ids:
        return []
    # Dedup and keep only non-empty
    ref_arxiv_ids = sorted({x for x in ref_arxiv_ids if x})
    # Build VALUES list for join
    values_sql = ", ".join(["(%s)"] * len(ref_arxiv_ids))
    cur.execute(
            f"""
            WITH refs("arxiv_id") AS (SELECT column1 FROM VALUES {values_sql})
            SELECT TO_VARCHAR(s."id") AS "paper_id"
            FROM refs r
            JOIN SILVER_PAPERS s
                ON s."arxiv_id" = r."arxiv_id"
            """,
            ref_arxiv_ids,
    )
    return [r[0] for r in cur.fetchall()]


def _fetch_embeddings(cur, paper_ids: List[str]) -> List[List[float]]:
    if not paper_ids:
        return []
    values_sql = ", ".join(["(%s)"] * len(paper_ids))
    cur.execute(
            f"""
            WITH ids("paper_id") AS (SELECT column1 FROM VALUES {values_sql})
            SELECT s."embedding"
            FROM ids i
            JOIN SILVER_PAPERS s
                ON TO_VARCHAR(s."id") = i."paper_id"
            WHERE s."embedding" IS NOT NULL
            """,
            paper_ids,
    )
    rows = cur.fetchall()
    # Each row[0] is a vector; connector returns it as a Python list-like
    return [list(r[0]) for r in rows]


@app.function(image=image_citation_aware, secrets=[snowflake_secret], timeout=60 * 30)
def run_citation_aware_embedding_batch(
    limit: int = 50,
    alpha: float = 0.8,
    base_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    max_refs: int = 80,
    database: str = DATABASE,
) -> Dict[str, Any]:
    """
    For each paper in SILVER_PAPERS (up to limit):
      - embed title+abstract => e_self
      - parse references from arXiv PDF
      - resolve some refs to known papers (by arxiv_id)
      - mean their baseline embeddings => e_refs_mean
      - e_final = alpha*e_self + (1-alpha)*e_refs_mean
      - store to PAPER_EMBEDDINGS_CA
    """
    import numpy as np
    from sentence_transformers import SentenceTransformer

    conn = connect_to_snowflake(database=database, schema="GOLD")
    cur = conn.cursor()
    try:
        _ensure_tables(cur)

        # Pick papers that do NOT yet have citation-aware embedding
        cur.execute(
                f"""
                SELECT
                    TO_VARCHAR(s."id") AS "paper_id",
                    s."arxiv_id",
                    s."title",
                    s."abstract"
                FROM SILVER_PAPERS s
                LEFT JOIN PAPER_EMBEDDINGS_CA ca
                    ON ca."paper_id" = TO_VARCHAR(s."id")
                WHERE ca."paper_id" IS NULL
                    AND s."abstract" IS NOT NULL
                    AND s."arxiv_id" IS NOT NULL
                LIMIT {int(limit)}
                """
        )
        rows = cur.fetchall()

        if not rows:
            return {"status": "ok", "updated": 0, "note": "No new papers to process."}

        model = SentenceTransformer(base_model)
        updated = 0
        skipped_no_refs = 0
        skipped_no_ref_embs = 0

        for paper_id, arxiv_id, title, abstract in rows:
            title = (title or "").strip()
            abstract = (abstract or "").strip()
            text = f"{title}\n\n{abstract}" if title else abstract

            # e_self
            e_self = model.encode([text], normalize_embeddings=True)[0].tolist()

            # parse references (Modal function call)
            cit = get_citations.remote(arxiv_id=arxiv_id, max_refs=max_refs)
            refs = cit.get("references", [])

            # store refs for A2 evidence
            _insert_references(cur, paper_id, arxiv_id, refs)

            ref_arxiv_ids = [ref_id for ref_id in (_extract_ref_arxiv_id(ref) for ref in refs) if ref_id]
            if not ref_arxiv_ids:
                skipped_no_refs += 1
                # still store a CA embedding identical to self (optional), or skip
                _upsert_ca_embedding(cur, paper_id, base_model + f"+cite_a{alpha}", alpha, e_self)
                updated += 1
                continue

            # resolve to known papers we have in SILVER
            ref_paper_ids = _resolve_ref_paper_ids(cur, ref_arxiv_ids)

            # get their baseline embeddings (must exist in PAPER_EMBEDDINGS)
            ref_embs = _fetch_embeddings(cur, ref_paper_ids)
            if not ref_embs:
                skipped_no_ref_embs += 1
                _upsert_ca_embedding(cur, paper_id, base_model + f"+cite_a{alpha}", alpha, e_self)
                updated += 1
                continue

            e_refs_mean = np.mean(np.array(ref_embs, dtype=np.float32), axis=0).tolist()

            # blend + normalize
            e_final = [
                float(alpha) * a + (1.0 - float(alpha)) * b
                for a, b in zip(e_self, e_refs_mean)
            ]
            e_final = _l2_normalize(e_final)

            _upsert_ca_embedding(cur, paper_id, base_model + f"+cite_a{alpha}", alpha, e_final)
            updated += 1

        conn.commit()
        return {
            "status": "ok",
            "updated": updated,
            "skipped_no_refs": skipped_no_refs,
            "skipped_no_ref_embs": skipped_no_ref_embs,
            "alpha": float(alpha),
        }
    finally:
        cur.close()
        conn.close()
