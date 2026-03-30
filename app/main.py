import modal

# Import worker modules so Modal can discover functions used by API jobs.
from app.workers import embedding_worker, graph_worker, ingestion, transformation  # noqa: F401
from app.workers import chunking_worker, summary_worker, qa_worker  # noqa: F401
from app.config import app, snowflake_secret
from app.api import api as web_app
from app import jobs  # noqa: F401


api_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install("fastapi", "httpx", "modal", "snowflake-connector-python")
    .add_local_dir("app", remote_path="/root/app")
)


@app.function(image=api_image, secrets=[snowflake_secret], timeout=60 * 20)
@modal.asgi_app()
def fastapi_app():
    return web_app


@app.local_entrypoint()
def run_chunking():
    from app.workers.chunking_worker import chunk_papers
    result = chunk_papers.remote()
    print(result)


@app.local_entrypoint()
def run_summarization():
    from app.workers.summary_worker import batch_summarize_papers
    result = batch_summarize_papers.remote()
    print(result)


@app.local_entrypoint()
def run_full_pipeline():
    """Run steps 1-5 via main.py (same app context), then summarization and graph separately."""
    from app.workers.ingestion import ingest_from_semantic_scholar
    from app.workers.transformation import main as transform_main
    from app.workers.embedding_worker import run_embedding_batch, run_chunk_embedding_batch
    from app.workers.chunking_worker import chunk_papers
    from app.workers.graph_worker import build_knowledge_graph

    print("=== Step 1: Ingest papers ===")
    print(ingest_from_semantic_scholar.remote(query="machine learning", max_results=20))

    print("=== Step 2: Transform to Silver ===")
    print(transform_main.remote())

    print("=== Step 3: Embed papers ===")
    print(run_embedding_batch.remote(limit=200))

    print("=== Step 4: Chunk papers ===")
    print(chunk_papers.remote(limit=200))

    print("=== Step 5: Embed chunks ===")
    print(run_chunk_embedding_batch.remote(limit=500))

    print("=== Step 6: Build knowledge graph ===")
    print(build_knowledge_graph.remote())

    print("=== Steps 1-6 complete. Run: modal run app/main.py::run_summarization ===")
