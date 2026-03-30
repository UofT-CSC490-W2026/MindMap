"""
Pipeline benchmarking script for Table 1 in the MindMap report.

Runs the full ingestion pipeline for a set of queries and records
per-stage wall-clock latency, paper counts, and cache hit rates.

Usage (run from repo root):
    modal run evals/benchmark_pipeline.py --queries "model quantization" "graph neural networks"

Or with defaults:
    modal run evals/benchmark_pipeline.py

Output: prints a Markdown table + writes evals/pipeline_benchmark_results.csv
"""

import csv
import time
from typing import Any, Dict, List

import modal

from app.config import app, DATABASE

# Import workers at module level so Modal hydrates them before the entrypoint runs
from app.workers.ingestion import ingest_from_semantic_scholar
from app.workers.transformation import main as transform_main
from app.workers.embedding_worker import run_embedding_batch, run_chunk_embedding_batch
from app.workers.chunking_worker import chunk_papers
from app.workers.graph_worker import build_knowledge_graph

# ---------------------------------------------------------------------------
# Default queries to benchmark (override via CLI)
# ---------------------------------------------------------------------------
DEFAULT_QUERIES_STR = "model quantization,graph neural networks,retrieval augmented generation"
DEFAULT_MAX_RESULTS = 10  # keep small so the run is fast


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _elapsed(start: float) -> float:
    return round(time.time() - start, 2)


# ---------------------------------------------------------------------------
# Local entrypoint
# ---------------------------------------------------------------------------

@app.local_entrypoint()
def benchmark(
    queries: str = DEFAULT_QUERIES_STR,
    max_results: int = DEFAULT_MAX_RESULTS,
    database: str = DATABASE,
    skip_ingestion: bool = False,
    skip_transformation: bool = False,
):
    """
    Benchmark each pipeline stage for each query and emit Table 1 data.

    --queries is a comma-separated string, e.g.:
        --queries "model quantization,graph neural networks"

    Flags:
        --skip-ingestion        Skip Bronze ingestion (use already-ingested data)
        --skip-transformation   Skip Silver transformation
    """
    query_list = [q.strip() for q in queries.split(",") if q.strip()]

    rows: List[Dict[str, Any]] = []

    for query in query_list:
        print(f"\n{'='*60}")
        print(f"BENCHMARKING QUERY: {query!r}")
        print(f"{'='*60}")

        row: Dict[str, Any] = {
            "query": query,
            "max_results": max_results,
            "papers_ingested": 0,
            "papers_skipped_dupe": 0,
            "ingestion_s": None,
            "transformation_s": None,
            "embedding_s": None,
            "chunking_s": None,
            "chunk_embedding_s": None,
            "graph_build_s": None,
            "total_s": None,
            "edges_merged": None,
            "cache_hit_pct": None,
        }

        total_start = time.time()

        # ------------------------------------------------------------------
        # Stage 1: Ingestion (Bronze)
        # ------------------------------------------------------------------
        if not skip_ingestion:
            print("\n[1/6] Ingestion (Bronze)...")
            t = time.time()
            ingest_result = ingest_from_semantic_scholar.remote(
                query=query,
                max_results=max_results,
                database=database,
            )
            row["ingestion_s"] = _elapsed(t)
            print(f"      done in {row['ingestion_s']}s | result={ingest_result}")
        else:
            print("\n[1/6] Ingestion skipped.")

        # ------------------------------------------------------------------
        # Stage 2: Transformation (Silver)
        # ------------------------------------------------------------------
        if not skip_transformation:
            print("\n[2/6] Transformation (Silver)...")
            t = time.time()
            transform_result = transform_main.remote(database=database)
            row["transformation_s"] = _elapsed(t)
            print(f"      done in {row['transformation_s']}s | result={transform_result}")
        else:
            print("\n[2/6] Transformation skipped.")

        # ------------------------------------------------------------------
        # Stage 3: Paper Embeddings
        # ------------------------------------------------------------------
        print("\n[3/6] Paper embeddings...")
        t = time.time()
        embed_result = run_embedding_batch.remote(limit=200, database=database)
        row["embedding_s"] = _elapsed(t)
        print(f"      done in {row['embedding_s']}s | result={embed_result}")

        # ------------------------------------------------------------------
        # Stage 4: Chunking
        # ------------------------------------------------------------------
        print("\n[4/6] Chunking...")
        t = time.time()
        chunk_result = chunk_papers.remote(limit=200, database=database)
        row["chunking_s"] = _elapsed(t)
        print(f"      done in {row['chunking_s']}s | result={chunk_result}")

        # ------------------------------------------------------------------
        # Stage 5: Chunk Embeddings
        # ------------------------------------------------------------------
        print("\n[5/6] Chunk embeddings...")
        t = time.time()
        chunk_embed_result = run_chunk_embedding_batch.remote(limit=500, database=database)
        row["chunk_embedding_s"] = _elapsed(t)
        print(f"      done in {row['chunk_embedding_s']}s | result={chunk_embed_result}")

        # ------------------------------------------------------------------
        # Stage 6: Graph Build
        # ------------------------------------------------------------------
        print("\n[6/6] Graph build (Gold)...")
        t = time.time()
        graph_result = build_knowledge_graph.remote(database=database)
        row["graph_build_s"] = _elapsed(t)
        row["edges_merged"] = (graph_result or {}).get("edges_merged")
        print(f"      done in {row['graph_build_s']}s | result={graph_result}")

        row["total_s"] = _elapsed(total_start)
        rows.append(row)

    # -----------------------------------------------------------------------
    # Second pass: measure cache hit rate by re-running ingestion on same queries
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("CACHE HIT RATE: re-running ingestion on same queries...")
    print(f"{'='*60}")
    for row in rows:
        t = time.time()
        rerun_result = ingest_from_semantic_scholar.remote(
            query=row["query"],
            max_results=row["max_results"],
            database=database,
        )
        rerun_s = _elapsed(t)
        # The worker prints inserted/skipped counts but doesn't return them as a dict.
        # We infer cache hit rate from the fact that a fast rerun with 0 inserts = 100% cache hit.
        # If you want exact counts, update ingest_from_semantic_scholar to return a dict.
        print(f"  query={row['query']!r} rerun_s={rerun_s}s result={rerun_result}")
        # Heuristic: if rerun is >50% faster than original ingestion, mark as cached
        orig = row["ingestion_s"]
        if orig and rerun_s < orig * 0.5:
            row["cache_hit_pct"] = "~100%"
        else:
            row["cache_hit_pct"] = "partial"

    # -----------------------------------------------------------------------
    # Print Markdown table
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("TABLE 1: Pipeline Runtime by Query")
    print(f"{'='*60}\n")

    headers = [
        "Query", "Ingestion (s)", "Transform (s)", "Embed (s)",
        "Chunk (s)", "Chunk Embed (s)", "Graph (s)", "Total (s)",
        "Edges", "Cache Hit",
    ]
    col_keys = [
        "query", "ingestion_s", "transformation_s", "embedding_s",
        "chunking_s", "chunk_embedding_s", "graph_build_s", "total_s",
        "edges_merged", "cache_hit_pct",
    ]

    # Markdown table
    header_row = "| " + " | ".join(headers) + " |"
    sep_row = "| " + " | ".join(["---"] * len(headers)) + " |"
    print(header_row)
    print(sep_row)
    for row in rows:
        vals = [str(row.get(k, "—")) for k in col_keys]
        print("| " + " | ".join(vals) + " |")

    # -----------------------------------------------------------------------
    # Write CSV
    # -----------------------------------------------------------------------
    out_path = "evals/pipeline_benchmark_results.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=col_keys)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nResults written to {out_path}")
