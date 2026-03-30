"""
Full offline pipeline runner for cache/database population.

Usage examples:
  modal run app/job_test.py --query "model quantization" --source semantic_scholar --max-results 50
  modal run app/job_test.py --query "graph neural networks" --source arxiv --max-results 30
  modal run app/job_test.py --query "transformers" --skip-ingestion --skip-transformation
"""

from typing import Any, Dict

from app.config import DATABASE, app
from app.workers.chunking_worker import chunk_papers
from app.workers.embedding_worker import backfill_similar_ids, run_chunk_embedding_batch, run_embedding_batch
from app.workers.graph_worker import build_knowledge_graph
from app.workers.ingestion import ingest_from_arxiv, ingest_from_openalex, ingest_from_semantic_scholar
from app.workers.summary_worker import batch_summarize_papers
from app.workers.transformation import main as transform_main


@app.local_entrypoint()
def pipeline(
    query: str = "transformers",
    source: str = "semantic_scholar",
    max_results: int = 50,
    database: str = DATABASE,
    embed_limit: int = 200,
    chunk_limit: int = 500,
    summary_limit: int = 100,
    k: int = 10,
    embedding_model_name: str = "sentence-transformers/all-MiniLM-L12-v2",
    summary_model_name: str = "gpt-4o-mini",
    force_reprocess: bool = False,
    skip_ingestion: bool = False,
    skip_transformation: bool = False,
    skip_paper_embeddings: bool = False,
    skip_similar_backfill: bool = False,
    skip_chunking: bool = False,
    skip_chunk_embeddings: bool = False,
    skip_graph: bool = False,
    skip_summary: bool = True,
) -> Dict[str, Any]:
    """
    Run the full offline data pipeline to populate Snowflake tables used by the app cache.

    The default behavior skips summarization because it is the most expensive stage.
    """
    results: Dict[str, Any] = {
        "database": database,
        "query": query,
        "source": source,
        "max_results": max_results,
    }

    print("[pipeline] starting full cache-population run", flush=True)
    print(f"[pipeline] database={database} source={source} query={query}", flush=True)

    if not skip_ingestion:
        print("[pipeline] step 1/8: ingestion", flush=True)
        if source == "semantic_scholar":
            ingestion_result = ingest_from_semantic_scholar.remote(
                query=query,
                max_results=max_results,
                database=database,
            )
        elif source == "openalex":
            ingestion_result = ingest_from_openalex.remote(
                query=query,
                max_results=max_results,
                database=database,
            )
        else:
            ingestion_result = ingest_from_arxiv.remote(
                query=query,
                max_results=max_results,
                database=database,
            )
        results["ingestion"] = ingestion_result
    else:
        results["ingestion"] = {"status": "skipped"}

    if not skip_transformation:
        print("[pipeline] step 2/8: bronze -> silver transformation", flush=True)
        results["transformation"] = transform_main.remote(
            force_reprocess=force_reprocess,
            database=database,
        )
    else:
        results["transformation"] = {"status": "skipped"}

    if not skip_paper_embeddings:
        print("[pipeline] step 3/8: paper embeddings", flush=True)
        results["paper_embeddings"] = run_embedding_batch.remote(
            limit=embed_limit,
            model_name=embedding_model_name,
            populate_similar=True,
            k=k,
            database=database,
        )
    else:
        results["paper_embeddings"] = {"status": "skipped"}

    if not skip_similar_backfill:
        print("[pipeline] step 4/8: similar-id backfill", flush=True)
        results["similar_backfill"] = backfill_similar_ids.remote(
            limit=embed_limit,
            k=k,
            database=database,
        )
    else:
        results["similar_backfill"] = {"status": "skipped"}

    if not skip_chunking:
        print("[pipeline] step 5/8: chunking", flush=True)
        results["chunking"] = chunk_papers.remote(limit=chunk_limit, database=database)
    else:
        results["chunking"] = {"status": "skipped"}

    if not skip_chunk_embeddings:
        print("[pipeline] step 6/8: chunk embeddings", flush=True)
        results["chunk_embeddings"] = run_chunk_embedding_batch.remote(
            limit=chunk_limit,
            model_name=embedding_model_name,
            database=database,
        )
    else:
        results["chunk_embeddings"] = {"status": "skipped"}

    if not skip_graph:
        print("[pipeline] step 7/8: graph build", flush=True)
        results["graph"] = build_knowledge_graph.remote(database=database)
    else:
        results["graph"] = {"status": "skipped"}

    if not skip_summary:
        print("[pipeline] step 8/8: paper summarization", flush=True)
        results["summary"] = batch_summarize_papers.remote(
            limit=summary_limit,
            model_name=summary_model_name,
            database=database,
        )
    else:
        results["summary"] = {"status": "skipped"}

    print("[pipeline] complete", flush=True)
    return results
