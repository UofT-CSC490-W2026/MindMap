"""
MindMap Pipeline Orchestrator: arXiv -> Knowledge Graph + RAG Chunks

Coordinates the complete workflow for building a knowledge graph and RAG-ready embeddings:

1. INGESTION: Fetches papers from arXiv based on a search query
2. TRANSFORMATION: Processes raw papers into structured metadata (Bronze -> Silver layer)
3. EMBEDDING: Computes vector embeddings and identifies similar papers
4. CHUNKING: Splits papers into semantic sections and chunks for RAG
5. CHUNK EMBEDDING: Generates embeddings for chunks to enable dense retrieval
6. KNOWLEDGE GRAPH: Builds semantic relationships between papers

Usage:
    modal run app/main.py --query "transformers" --max-results 100

Parameters:
    query (str): search query (default: "transformers")
    source (str): ingestion source, one of: semantic_scholar, arxiv
    max_results (int): number of papers to ingest (default: 50)
    threshold (int): min papers before computing similarities to avoid OOM (default: 50)
    k (int): number of similar papers to find per paper (default: 10)

All functions execute remotely in Modal containers.
"""

from config import app, DATABASE, SCHEMA
from workers.ingestion import ingest_from_arxiv, ingest_from_semantic_scholar
from workers.transformation import main as transform_main, backfill_missing_ss_ids
from workers.embedding_worker import run_embedding_batch, backfill_similar_ids, run_chunk_embedding_batch
from workers.chunking_worker import chunk_papers
from workers.graph_worker import build_knowledge_graph

@app.local_entrypoint()
def pipeline(
    query: str = "transformers",
    source: str = "semantic_scholar",
    max_results: int = 50,
    threshold: int = 50,
    k: int = 10,
    ss_backfill_limit: int = 1000,
    database: str = DATABASE,
    schema: str = SCHEMA,
    skip_ingestion: bool = False,
    skip_transformation: bool = False,
    skip_ss_id_backfill: bool = False,
    skip_embedding: bool = False,
    skip_chunking: bool = False,
    skip_chunk_embedding: bool = False,
    skip_backfill: bool = False,
    skip_graph: bool = False,
):
    """
    Full RAG-ready pipeline orchestrator with optional step skipping.
    
    Usage:
        # Full pipeline (all steps, Semantic Scholar source)
        modal run app/main.py --query "transformers" --max-results 50
        
        # Skip already-run steps
        modal run app/main.py --query "transformers" --max-results 50 --skip-ingestion --skip-transformation
    """
    if not skip_ingestion:
        if source == "semantic_scholar":
            print("Step 1: Ingesting papers from Semantic Scholar...")
            ingest_from_semantic_scholar.remote(
                query=query,
                max_results=max_results,
                database=database,
                schema=schema,
            )
        else:
            print("Step 1: Ingesting papers from arXiv...")
            ingest_from_arxiv.remote(query=query, max_results=max_results, database=database, schema=schema)
    else:
        print("Step 1: Skipped (ingestion already complete)")
    
    if not skip_transformation:
        print("Step 2: Transforming Bronze -> Silver...")
        transform_main.remote(database=database, schema=schema)
    else:
        print("Step 2: Skipped (transformation already complete)")

    if not skip_ss_id_backfill:
        print("Step 2b: Backfilling missing ss_id values...")
        backfill_missing_ss_ids.remote(limit=ss_backfill_limit, database=database, schema=schema)
    else:
        print("Step 2b: Skipped (ss_id backfill already complete)")
    
    if not skip_embedding:
        print("Step 3: Generating paper-level embeddings...")
        run_embedding_batch.remote(
            limit=max_results,
            populate_similar=True,
            min_corpus_size_for_neighbors=threshold,
            k=k,
            database=database,
            schema=schema,
        )
    else:
        print("Step 3: Skipped (paper embeddings already complete)")
    
    if not skip_chunking:
        print("Step 4: Chunking papers into RAG sections...")
        chunk_papers.remote(limit=max_results, database=database, schema=schema)
    else:
        print("Step 4: Skipped (chunking already complete)")
    
    if not skip_chunk_embedding:
        print("Step 5: Embedding chunks for dense retrieval...")
        run_chunk_embedding_batch.remote(limit=max_results * 10, database=database, schema=schema)
    else:
        print("Step 5: Skipped (chunk embeddings already complete)")
    
    if not skip_backfill:
        print("Step 6: Backfill older papers' similar ids...")
        backfill_similar_ids.remote(limit=max_results, k=k, database=database, schema=schema)
    else:
        print("Step 6: Skipped (backfill already complete)")
    
    if not skip_graph:
        print("Step 7: Building knowledge graph...")
        build_knowledge_graph.remote(database=database, schema=schema)
    else:
        print("Step 7: Skipped (knowledge graph already complete)")
    
    print("✓ RAG-ready pipeline complete!")
