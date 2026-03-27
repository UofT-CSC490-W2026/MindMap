import modal

# Import worker modules so Modal can discover functions used by API jobs.
from app.workers import embedding_worker, graph_worker, ingestion, transformation  # noqa: F401
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
