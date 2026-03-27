from typing import Any, Dict

from app.config import DATABASE, app, image, snowflake_secret


@app.function(image=image, secrets=[snowflake_secret], timeout=60 * 20)
def run_single_ingestion_job(arxiv_id: str, database: str = DATABASE) -> Dict[str, Any]:
    """
    Detached Modal job: bronze -> silver -> single-paper embedding -> gold.
    """
    from app.workers.ingestion import ingest_single_paper
    from app.workers.transformation import process_single_silver
    from app.workers.embedding_worker import process_single_embedding
    from app.workers.graph_worker import build_knowledge_graph

    print(f"[job] start arxiv_id={arxiv_id} database={database}", flush=True)
    bronze_result = ingest_single_paper.remote(arxiv_id=arxiv_id, database=database)
    print(f"[job] bronze done: {bronze_result}", flush=True)

    silver_result = process_single_silver.remote(arxiv_id=arxiv_id, database=database)
    print(f"[job] silver done: {silver_result}", flush=True)

    embedding_result = process_single_embedding.remote(arxiv_id=arxiv_id, database=database)
    print(f"[job] embedding done: {embedding_result}", flush=True)

    graph_result = build_knowledge_graph.remote(database=database)
    print(f"[job] gold done: {graph_result}", flush=True)

    return {
        "status": "done",
        "database": database,
        "bronze_result": bronze_result,
        "silver_result": silver_result,
        "embedding_result": embedding_result,
        "graph_result": graph_result,
    }


@app.function(image=image, secrets=[snowflake_secret], timeout=60 * 20)
def run_post_bronze_job(arxiv_id: str, database: str = DATABASE) -> Dict[str, Any]:
    """
    Detached Modal job: silver -> single-paper embedding -> gold.
    Intended to run after Bronze has already succeeded.
    """
    from app.workers.transformation import process_single_silver
    from app.workers.embedding_worker import process_single_embedding
    from app.workers.graph_worker import build_knowledge_graph

    print(f"[post-bronze-job] start arxiv_id={arxiv_id} database={database}", flush=True)

    silver_result = process_single_silver.remote(arxiv_id=arxiv_id, database=database)
    print(f"[post-bronze-job] silver done: {silver_result}", flush=True)

    embedding_result = process_single_embedding.remote(arxiv_id=arxiv_id, database=database)
    print(f"[post-bronze-job] embedding done: {embedding_result}", flush=True)

    paper_id = None
    if isinstance(embedding_result, dict):
        try:
            paper_id = int(embedding_result.get("paper_id")) if embedding_result.get("paper_id") is not None else None
        except (TypeError, ValueError):
            paper_id = None

    graph_result = build_knowledge_graph.remote(paper_id=paper_id, database=database)
    print(f"[post-bronze-job] gold done: {graph_result}", flush=True)

    return {
        "status": "done",
        "database": database,
        "silver_result": silver_result,
        "embedding_result": embedding_result,
        "graph_result": graph_result,
    }
