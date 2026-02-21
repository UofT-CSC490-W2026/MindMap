import modal
from pathlib import Path

app = modal.App("mindmap-pipeline")

# Base image with common dependencies
base_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install(
        "arxiv",
        "snowflake-connector-python",
        "httpx",
        "pymupdf",
    )
)

# Add local files to base image
def _add_local_files(img):
    return (
        img.add_local_file("A2/config.py", remote_path="/root/config.py")
        .add_local_file("A2/utils.py", remote_path="/root/utils.py")
    )

# Ingestion image (minimal)
image = _add_local_files(base_image)

# ML image (with ML dependencies)
ml_image = _add_local_files(
    base_image.pip_install(
        "sentence-transformers==2.7.0",
        "torch",
        "pandas",
        "numpy",
    )
)

# Shared secret
snowflake_secret = modal.Secret.from_name("snowflake-creds")