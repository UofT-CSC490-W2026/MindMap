import modal
from pathlib import Path

app = modal.App("mindmap-pipeline")
APP_DIR = Path(__file__).resolve().parent
DATABASE = "MINDMAP_DB"
SCHEMA = "PUBLIC"


def qualify_table(table_name: str, database: str = DATABASE, schema: str = SCHEMA) -> str:
    return f'"{database}"."{schema}"."{table_name}"'

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
        img.add_local_file(APP_DIR / "config.py", remote_path="/root/config.py")
        .add_local_file(APP_DIR / "utils.py", remote_path="/root/utils.py")
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

# Citation parsing image (PDF + feed parsing)
image_citation = _add_local_files(
    base_image.pip_install(
        "requests",
        "feedparser",
    )
)

# Citation-aware embedding image (ML + citation parsing)
image_citation_aware = _add_local_files(
    ml_image.pip_install(
        "requests",
        "feedparser",
    )
)

# Shared secret
snowflake_secret = modal.Secret.from_name("snowflake-creds")
semantic_scholar_secret = modal.Secret.from_name("semantic-scholar-api")
