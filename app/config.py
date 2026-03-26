import modal
from pathlib import Path
import os

app = modal.App("mindmap-pipeline")
APP_DIR = Path(__file__).resolve().parent
DATABASE = f"MINDMAP_{os.getenv('MINDMAP_ENV', 'DEV').upper()}" # e.g. MINDMAP_DEV, MINDMAP_PROD
WAREHOUSE = f"MINDMAP_{os.getenv('MINDMAP_ENV', 'DEV').upper()}_WH" # e.g. MINDMAP_DEV_WH, MINDMAP_PROD_WH

def resolve_schema_for_table(table_name: str) -> str:
    """
    Returns the schema name based on table naming convention.
    E.g. BRONZE_PAPERS -> BRONZE, SILVER_PAPERS -> SILVER, GOLD_CONNECTIONS -> GOLD
    Defaults to BRONZE if prefix not found.
    """
    upper_name = table_name.upper()
    if upper_name.startswith("BRONZE_"):
        return "BRONZE"
    elif upper_name.startswith("SILVER_"):
        return "SILVER"
    elif upper_name.startswith("GOLD_"):
        return "GOLD"
    else:
        return "BRONZE"  # Default fallback
    
def qualify_table(table_name: str, database: str = DATABASE) -> str:
    """
    Returns the fully qualified table name, automatically resolving schema if not provided.
    Example usage:
        bronze_table = qualify_table("BRONZE_PAPERS")
        silver_table = qualify_table("SILVER_PAPERS")
    """
    schema = resolve_schema_for_table(table_name)
    return f'{database}.{schema}.{table_name}'

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

# Add local files and services (with copy=True so we can run .env() after)
def _add_local_files(img):
    """Add config and utils files with copy=True, then set PYTHONPATH."""
    return (
        img.add_local_file(APP_DIR / "config.py", remote_path="/root/config.py", copy=True)
        .add_local_file(APP_DIR / "utils.py", remote_path="/root/utils.py", copy=True)
        .add_local_dir(APP_DIR / "services", remote_path="/root/services", copy=True)
        .env({"PYTHONPATH": "/root"})
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

# Clustering image (ML + scikit-learn for K-Means topic clustering)
clustering_image = _add_local_files(
    base_image.pip_install(
        "numpy",
        "scikit-learn",
    )
)

# LLM image (with LLM dependencies for summarization)
llm_image = _add_local_files(
    base_image.pip_install(
        "openai",
        "pydantic",
    )
)

# RAG + LLM image for retrieval-backed QA workers.
rag_image = _add_local_files(
    base_image.pip_install(
        "sentence-transformers==2.7.0",
        "torch",
        "numpy",
        "pydantic",
    )
)

# Shared secrets
snowflake_secret = modal.Secret.from_name("snowflake-creds")
semantic_scholar_secret = modal.Secret.from_name("semantic-scholar-api")
openai_secret = modal.Secret.from_name("openai-api")
