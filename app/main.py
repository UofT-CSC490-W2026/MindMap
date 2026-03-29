import modal

from app.config import app, snowflake_secret

# Import all workers at module level so Modal registers them in the same app context
from app.workers import embedding_worker, graph_worker, ingestion, transformation  # noqa: F401
from app.workers import semantic_search_worker, qa_worker, summary_worker  # noqa: F401
from app import jobs  # noqa: F401


api_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install("fastapi", "httpx", "modal", "snowflake-connector-python")
    .add_local_dir("app", remote_path="/root/app")
)


@app.function(image=api_image, secrets=[snowflake_secret], timeout=60 * 20)
@modal.asgi_app()
def fastapi_app():
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from app.api.router import router
    from app.workers import embedding_worker, graph_worker, ingestion, transformation  # noqa: F401
    from app import jobs  # noqa: F401

    web_app = FastAPI(title="MindMap API")
    web_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    web_app.include_router(router)
    return web_app
