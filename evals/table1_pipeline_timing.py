"""
Table 1: Pipeline Runtime Benchmark
=====================================
Runs the full ingestion pipeline for a set of queries, times each stage,
then re-runs the same queries to measure cache effectiveness.

Usage (from repo root):
    modal run evals/table1_pipeline_timing.py::benchmark

To run only specific queries:
    modal run evals/table1_pipeline_timing.py::benchmark --queries "model quantization,transformers"

To skip ingestion/transform if already done:
    modal run evals/table1_pipeline_timing.py::benchmark --skip-ingestion --skip-transform

Output:
    prints Markdown table to stdout
    writes evals/results/table1_pipeline_timing.csv
"""

import csv
import time
from pathlib import Path
from typing import Any

from app.config import app, DATABASE
from app.workers.ingestion import ingest_from_semantic_scholar
from app.workers.transformation import main as transform_main
from app.workers.embedding_worker import run_embedding_batch
from app.workers.chunking_worker import chunk_papers
from app.workers.embedding_worker import run_chunk_embedding_batch
from app.workers.graph_worker import build_knowledge_graph

OUT_PATH = Path("evals/results/table1_pipeline_timing.csv")

DEFAULT_QUERIES = "model quantization,graph neural networks,retrieval augmented generation"
DEFAULT_MAX_RESULTS = 3


def _elapsed(start: float) -> float:
    return round(time.time() - start, 2)


def _run_pipeline_for_query(
    query: str,
    max_results: int,
    database: str,
    skip_ingestion: bool,
    skip_transform: bool,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "query": query,
        "max_results": max_results,
        "papers_inserted": "—",
        "papers_skipped": "—",
        "ingestion_s": "—",
        "transform_s": "—",
        "embedding_s": "—",
        "chunking_s": "—",
        "chunk_embed_s": "—",
        "graph_s": "—",
        "total_s": "—",
        "edges_merged": "—",
    }

    total_start = time.time()

    # Stage 1: Ingestion
    if not skip_ingestion:
        print(f"  [1/6] ingestion...", flush=True)
        t = time.time()
        ingest_from_semantic_scholar.remote(query=query, max_results=max_results, database=database)
        row["ingestion_s"] = _elapsed(t)
        print(f"        {row['ingestion_s']}s", flush=True)
    else:
        print(f"  [1/6] ingestion skipped")

    # Stage 2: Transformation
    if not skip_transform:
        print(f"  [2/6] transformation...", flush=True)
        t = time.time()
        transform_main.remote(database=database)
        row["transform_s"] = _elapsed(t)
        print(f"        {row['transform_s']}s", flush=True)
    else:
        print(f"  [2/6] transformation skipped")

    # Stage 3: Paper embeddings
    print(f"  [3/6] paper embeddings...", flush=True)
    t = time.time()
    run_embedding_batch.remote(limit=200, database=database)
    row["embedding_s"] = _elapsed(t)
    print(f"        {row['embedding_s']}s", flush=True)

    # Stage 4: Chunking
    print(f"  [4/6] chunking...", flush=True)
    t = time.time()
    chunk_papers.remote(limit=200, database=database)
    row["chunking_s"] = _elapsed(t)
    print(f"        {row['chunking_s']}s", flush=True)

    # Stage 5: Chunk embeddings
    print(f"  [5/6] chunk embeddings...", flush=True)
    t = time.time()
    run_chunk_embedding_batch.remote(limit=500, database=database)
    row["chunk_embed_s"] = _elapsed(t)
    print(f"        {row['chunk_embed_s']}s", flush=True)

    # Stage 6: Graph build
    print(f"  [6/6] graph build...", flush=True)
    t = time.time()
    result = build_knowledge_graph.remote(database=database)
    row["graph_s"] = _elapsed(t)
    row["edges_merged"] = (result or {}).get("edges_merged", "—")
    print(f"        {row['graph_s']}s | edges={row['edges_merged']}", flush=True)

    row["total_s"] = _elapsed(total_start)
    return row


@app.local_entrypoint()
def benchmark(
    queries: str = DEFAULT_QUERIES,
    max_results: int = DEFAULT_MAX_RESULTS,
    database: str = DATABASE,
    skip_ingestion: bool = False,
    skip_transform: bool = False,
):
    query_list = [q.strip() for q in queries.split(",") if q.strip()]
    rows = []

    # --- First pass: fresh run ---
    print(f"\n{'='*60}")
    print("PASS 1: Fresh pipeline run")
    print(f"{'='*60}")
    for query in query_list:
        print(f"\nQuery: {query!r}")
        row = _run_pipeline_for_query(
            query=query,
            max_results=max_results,
            database=database,
            skip_ingestion=skip_ingestion,
            skip_transform=skip_transform,
        )
        row["run"] = "fresh"
        rows.append(row)

    # --- Second pass: rerun same queries (cache hit measurement) ---
    print(f"\n{'='*60}")
    print("PASS 2: Rerun (cache hit measurement)")
    print(f"{'='*60}")
    for query in query_list:
        print(f"\nQuery: {query!r}")
        # Only re-time ingestion and transform — those are the cached stages
        print(f"  [1/2] ingestion (rerun)...", flush=True)
        t = time.time()
        ingest_from_semantic_scholar.remote(query=query, max_results=max_results, database=database)
        rerun_ingest_s = _elapsed(t)
        print(f"        {rerun_ingest_s}s", flush=True)

        print(f"  [2/2] transformation (rerun)...", flush=True)
        t = time.time()
        transform_main.remote(database=database)
        rerun_transform_s = _elapsed(t)
        print(f"        {rerun_transform_s}s", flush=True)

        # Find the matching fresh row to compute speedup
        fresh = next((r for r in rows if r["query"] == query and r["run"] == "fresh"), None)
        fresh_ingest = fresh["ingestion_s"] if fresh and fresh["ingestion_s"] != "—" else None
        speedup = round(fresh_ingest / rerun_ingest_s, 1) if fresh_ingest and rerun_ingest_s > 0 else "—"

        rows.append({
            "query": query,
            "max_results": max_results,
            "run": "cached",
            "ingestion_s": rerun_ingest_s,
            "transform_s": rerun_transform_s,
            "embedding_s": "skipped",
            "chunking_s": "skipped",
            "chunk_embed_s": "skipped",
            "graph_s": "skipped",
            "total_s": round(rerun_ingest_s + rerun_transform_s, 2),
            "edges_merged": "—",
            "cache_speedup": f"{speedup}x faster",
        })

    # --- Print Markdown table ---
    print(f"\n{'='*60}")
    print("TABLE 1: Pipeline Runtime by Query")
    print(f"{'='*60}\n")

    headers = ["run", "query", "ingestion_s", "transform_s", "embedding_s",
               "chunking_s", "chunk_embed_s", "graph_s", "total_s", "edges_merged"]
    print("| " + " | ".join(headers) + " |")
    print("| " + " | ".join(["---"] * len(headers)) + " |")
    for r in rows:
        print("| " + " | ".join(str(r.get(k, "—")) for k in headers) + " |")

    # --- Write CSV ---
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    all_keys = list({k for r in rows for k in r})
    with open(OUT_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"\nWrote {OUT_PATH}")
