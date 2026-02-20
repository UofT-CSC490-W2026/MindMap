# Online worker that computes and caches top-k similar paper ids based on embedding similarity.
# This only runs on demand when the frontend needs to find similar papers for a given paper id but the cache is missing.
# It first tries to read from the similar_embddings_ids column, and if that’s null, it falls back to doing a vector 
# similarity query on the fly, returns results, and also writes the top-k ids back to the similar_embddings_ids column for next time.

import modal
from typing import List, Dict, Any

from utils import connect_to_snowflake
import json
from config import app, image, snowflake_secret

@app.function(image=image, secrets=[snowflake_secret], timeout=60 * 5)
def get_related_papers(paper_id: int, k: int = 10) -> List[Dict[str, Any]]:
    """
    ONLINE endpoint:
    1) Try cached similar_embddings_ids first
    2) If missing, fallback to vector similarity query
    """
    conn = connect_to_snowflake()
    cur = conn.cursor()
    try:
        # 1) Try cache
        cur.execute(
            """
            SELECT similar_embddings_ids
            FROM MINDMAP_DB.PUBLIC.SILVER_PAPERS
            WHERE id = %s
            """,
            (int(paper_id),),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            # row[0] is VARIANT → comes back as Python types or JSON-like
            cached_ids = list(row[0])
            cached_ids = cached_ids[: int(k)]
            if cached_ids:
                # Return details for cached ids
                values_sql = ", ".join(["(%s)"] * len(cached_ids))
                cur.execute(
                    f"""
                    WITH ids(id) AS (SELECT column1 FROM VALUES {values_sql})
                    SELECT s.id, s.arxiv_id, s.title
                    FROM ids
                    JOIN MINDMAP_DB.PUBLIC.SILVER_PAPERS s
                      ON s.id = ids.id
                    """,
                    [int(x) for x in cached_ids],
                )
                rows = cur.fetchall()
                # Preserve cached order (optional)
                id_to_row = {int(r[0]): r for r in rows}
                ordered = []
                for cid in cached_ids:
                    cid = int(cid)
                    if cid in id_to_row:
                        r = id_to_row[cid]
                        ordered.append({"id": cid, "arxiv_id": r[1], "title": r[2], "source": "cache"})
                return ordered

        # 2) Fallback: vector similarity
        cur.execute(
            """
            WITH q AS (
              SELECT embedding AS qvec
              FROM MINDMAP_DB.PUBLIC.SILVER_PAPERS
              WHERE id = %s
                AND embedding IS NOT NULL
            )
            SELECT
              e.id,
              e.arxiv_id,
              e.title,
              VECTOR_COSINE_SIMILARITY(e.embedding, q.qvec) AS score
            FROM MINDMAP_DB.PUBLIC.SILVER_PAPERS e, q
            WHERE e.id <> %s
              AND e.embedding IS NOT NULL
            ORDER BY score DESC
            LIMIT %s
            """,
            (int(paper_id), int(paper_id), int(k)),
        )
        rows = cur.fetchall()
        results = [{"id": int(r[0]), "arxiv_id": r[1], "title": r[2], "score": float(r[3]), "source": "fallback"} for r in rows]

        # Optional: write cache so next time it’s instant
        if results:
            ids_only = [r["id"] for r in results]
            cur.execute(
                """
                UPDATE MINDMAP_DB.PUBLIC.SILVER_PAPERS
                SET similar_embddings_ids = PARSE_JSON(%s)
                WHERE id = %s
                """,
                (json.dumps(ids_only), int(paper_id)),
            )
            conn.commit()

        return results
    finally:
        cur.close()
        conn.close()

