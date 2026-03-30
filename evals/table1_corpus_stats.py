"""
Table 1: Pipeline Corpus Statistics
=====================================
Queries Snowflake directly to show how many papers made it through
each layer of the Bronze -> Silver -> Gold pipeline.

Usage (from repo root, with venv active):
    python evals/table1_corpus_stats.py

Requires env vars:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE (optional, defaults to MINDMAP_DEV_WH)

Output:
    prints a Markdown table to stdout
    writes evals/results/table1_pipeline_stats.csv
"""

import csv
import os
from pathlib import Path

import snowflake.connector

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "MINDMAP_DEV")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "MINDMAP_DEV_WH")
OUT_PATH = Path("evals/results/table1_pipeline_stats.csv")


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=DATABASE,
        warehouse=WAREHOUSE,
        schema="SILVER",
    )


def q(cur, sql: str) -> list:
    cur.execute(sql)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def collect_stats(cur) -> list[dict]:
    rows = []

    def add(layer, metric, value):
        rows.append({"Layer": layer, "Metric": metric, "Value": value})

    # --- Bronze ---
    (bronze_total,) = q(cur, f"SELECT COUNT(*) FROM {DATABASE}.BRONZE.BRONZE_PAPERS")[0]
    add("Bronze", "Raw papers ingested", bronze_total)

    # --- Silver ---
    S = f"{DATABASE}.SILVER.SILVER_PAPERS"

    (silver_total,) = q(cur, f"SELECT COUNT(*) FROM {S}")[0]
    add("Silver", "Papers normalized", silver_total)

    (with_abstract,) = q(cur, f'SELECT COUNT(*) FROM {S} WHERE "abstract" IS NOT NULL')[0]
    add("Silver", "Papers with abstract", with_abstract)

    (with_conclusion,) = q(cur, f"""
        SELECT COUNT(*) FROM {S}
        WHERE "conclusion" IS NOT NULL AND LENGTH(TRIM("conclusion")) > 0
    """)[0]
    add("Silver", "Papers with conclusion (PDF extracted)", with_conclusion)

    (with_embedding,) = q(cur, f'SELECT COUNT(*) FROM {S} WHERE "embedding" IS NOT NULL')[0]
    add("Silver", "Papers with embedding", with_embedding)

    (with_ss_id,) = q(cur, f'SELECT COUNT(*) FROM {S} WHERE "ss_id" IS NOT NULL')[0]
    add("Silver", "Papers with Semantic Scholar ID", with_ss_id)

    # --- Chunks ---
    C = f"{DATABASE}.SILVER.SILVER_PAPER_CHUNKS"

    (total_chunks,) = q(cur, f"SELECT COUNT(*) FROM {C}")[0]
    add("Silver", "Total chunks", total_chunks)

    (chunks_embedded,) = q(cur, f'SELECT COUNT(*) FROM {C} WHERE "embedding" IS NOT NULL')[0]
    add("Silver", "Chunks with embedding", chunks_embedded)

    # --- Gold ---
    G = f"{DATABASE}.GOLD.GOLD_PAPER_RELATIONSHIPS"

    (total_edges,) = q(cur, f"SELECT COUNT(*) FROM {G}")[0]
    add("Gold", "Total graph edges", total_edges)

    edge_types = q(cur, f"""
        SELECT "relationship_type", COUNT(*)
        FROM {G}
        GROUP BY "relationship_type"
        ORDER BY 2 DESC
    """)
    for rel_type, count in edge_types:
        add("Gold", f"  {rel_type} edges", count)

    return rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_md_table(rows: list[dict]) -> None:
    headers = ["Layer", "Metric", "Value"]
    col_w = [max(len(h), max(len(str(r[h])) for r in rows)) for h in headers]

    def fmt_row(vals):
        return "| " + " | ".join(str(v).ljust(w) for v, w in zip(vals, col_w)) + " |"

    sep = "| " + " | ".join("-" * w for w in col_w) + " |"
    print(fmt_row(headers))
    print(sep)
    for r in rows:
        print(fmt_row([r["Layer"], r["Metric"], r["Value"]]))


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["Layer", "Metric", "Value"])
        w.writeheader()
        w.writerows(rows)
    print(f"\nWrote {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Connecting to {DATABASE} on {WAREHOUSE}...")
    conn = connect()
    cur = conn.cursor()
    try:
        rows = collect_stats(cur)
    finally:
        cur.close()
        conn.close()

    print(f"\n{'='*60}")
    print("TABLE 1: Pipeline Corpus Statistics")
    print(f"{'='*60}\n")
    print_md_table(rows)
    write_csv(rows, OUT_PATH)
