import modal

# Import only workers needed by the current API pipeline to keep deploys fast.
from app.workers import (
    embedding_worker,
    graph_worker,
    ingestion,
    transformation,
)

from app.config import app
from app.api import api as web_app
from app import jobs

# API image
api_image = modal.Image.debian_slim().pip_install("fastapi", "httpx", "modal", "snowflake-connector-python").add_local_dir("app", remote_path="/root/app")

@app.function(
    image=api_image,
    secrets=[modal.Secret.from_name("mindmap-1")]
)
@modal.asgi_app()
def fastapi_app():
    return web_app
