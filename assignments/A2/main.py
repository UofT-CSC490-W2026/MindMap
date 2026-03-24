"""
MindMap Pipeline Orchestrator

Coordinates the end-to-end workflow for building a knowledge graph of research papers:

1. INGESTION: Fetches papers from arXiv based on a search query
2. TRANSFORMATION: Processes raw papers into structured metadata (Bronze → Silver layer)
3. EMBEDDING: Computes vector embeddings and identifies similar papers
4. KNOWLEDGE GRAPH: Builds semantic relationships between papers

Usage:
    modal run A2/main.py --query "transformers" --max-results 100

Parameters:
    query (str): arXiv search query (default: "transformers")
    max_results (int): number of papers to ingest (default: 50)
    threshold (int): min papers before computing similarities to avoid OOM (default: 50)
    k (int): number of similar papers to find per paper (default: 10)

All functions execute remotely in Modal containers.
"""

from config import app
from ingestion import ingest_from_arxiv
from transformation import main as transform_main
from embedding_worker import run_embedding_batch, backfill_similar_ids
from graph_worker import build_knowledge_graph

@app.local_entrypoint()
def pipeline(
    query: str = "transformers",
    max_results: int = 50,
    threshold: int = 50,
    k: int = 10
):
    """Full pipeline orchestrator"""
    print("Step 1: Ingesting papers from arXiv...")
    ingest_from_arxiv.remote(query=query, max_results=max_results)
    
    print("Step 2: Transforming Bronze → Silver...")
    transform_main.remote()
    
    print("Step 3: Generating embeddings...")
    run_embedding_batch.remote(
        limit=max_results,
        populate_similar=True,
        min_corpus_size_for_neighbors=threshold,
        k=k
    )
    
    print("Step 4: Backfill older papers...")
    backfill_similar_ids.remote(limit=max_results, k=k)
    
    print("Step 5: Building knowledge graph...")
    build_knowledge_graph.remote()
    
    print("✓ Pipeline complete!")