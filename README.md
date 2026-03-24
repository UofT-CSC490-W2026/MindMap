# MindMap

MindMap is an AI-assisted system for exploring academic literature through semantic retrieval and interactive visualization. Instead of returning a flat list of search results, MindMap helps users understand how papers relate, discover relevant work, and navigate research areas more effectively.

## Table of Contents

- [Installation / Getting Started](#installation--getting-started)
- [Codebase Overview](#codebase-overview)
- [Pipeline Overview](#pipeline-overview)
- [Features](#features)
- [Architecture](#architecture)
  - [1. Data Ingestion Layer](#1-data-ingestion-layer)
  - [2. Representation Layer](#2-representation-layer)
  - [3. Retrieval Layer](#3-retrieval-layer)
  - [4. Ranking & Reasoning Layer](#4-ranking--reasoning-layer)
  - [5. Graph Construction Layer](#5-graph-construction-layer)
  - [6. Serving Layer](#6-serving-layer)
  - [7. Infrastructure Layer](#7-infrastructure-layer)
- [Tech Stack](#tech-stack)
- [Inspiration](#inspiration)
- [Status](#status)

## Getting Started

### Prerequisites
- Node.js 18+
- npm

### 1. Quick Start

Use this if you only want to run the UI.

```bash
# clone the repo
git clone https://github.com/UofT-CSC490-W2026/MindMap.git
cd MindMap

# install and run frontend
cd react
npm install
npm run dev
```

Open the local URL shown in your terminal (usually `http://localhost:5173`).

### 2. Developer / Admin Setup 

Use this if you are developing backend features or refreshing pipeline data.

Additional prerequisites:
- Python 3.10+
- uv
- Modal CLI configured (`modal setup`)
- Snowflake credentials available through Modal secrets

Backend setup:
```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

Modal + Snowflake credential setup:

```bash
# 1) authenticate Modal (one-time)
modal setup

# 2) export Snowflake credentials in your shell
export SNOWFLAKE_ACCOUNT="<your_account>"
export SNOWFLAKE_USER="<your_user>"
export SNOWFLAKE_PASSWORD="<your_password>"
export SNOWFLAKE_WAREHOUSE="MINDMAP_WH"
export SEMANTIC_SCHOLAR_API_KEY="<your_semantic_scholar_api_key>"

# 3) create/update Modal secrets used by workers

modal secret create snowflake-creds \
  SNOWFLAKE_ACCOUNT="$SNOWFLAKE_ACCOUNT" \
  SNOWFLAKE_USER="$SNOWFLAKE_USER" \
  SNOWFLAKE_PASSWORD="$SNOWFLAKE_PASSWORD" \
  SNOWFLAKE_WAREHOUSE="$SNOWFLAKE_WAREHOUSE" \
  --force

modal secret create semantic-scholar-api \
  SEMANTIC_SCHOLAR_API_KEY="$SEMANTIC_SCHOLAR_API_KEY" \
  --force

# 4) verify secrets exist
modal secret list
```

Terraform setup (Snowflake infrastructure):

```bash
# from repo root
cd terraform

# 1) initialize terraform
terraform init

# 2) choose workspace
# use dev for local testing
terraform workspace select dev || terraform workspace new dev

# (use prod only when deploying production resources)
# terraform workspace select prod # uncomment for prod

# 3) create schemas
export TF_VAR_snowflake_account="$SNOWFLAKE_ACCOUNT"
export TF_VAR_snowflake_user="$SNOWFLAKE_USER"
export TF_VAR_snowflake_password="$SNOWFLAKE_PASSWORD"

terraform plan # preview changes
terraform apply # apply changes
```

Example run:

```bash
QUERY="model quantization"

# run from repo root
cd /path/to/MindMap

# end-to-end pipeline (matches app/main.py local_entrypoint)
modal run app/main.py \
  --query "$QUERY" \
  --source semantic_scholar \
  --max-results 50

# rerun without expensive early stages
modal run app/main.py \
  --query "$QUERY" \
  --max-results 50 \
  --skip-ingestion \
  --skip-transformation \
  --skip-ss-id-backfill

# arXiv ingestion source
modal run app/main.py \
  --query "graph neural networks" \
  --source arxiv \
  --max-results 30

# target a specific Snowflake database/schema
modal run app/main.py \
  --query "$QUERY" \
  --database MINDMAP_DB \
  --schema PUBLIC
```

## Codebase Overview

```text
MindMap/
├── app/                                 # Backend pipeline + API
│   ├── api.py                           # FastAPI routes
│   ├── config.py                        # Shared Modal app/images/secrets + DB constants
│   ├── create_schemas.sql               # Snowflake schema/table bootstrap SQL
│   ├── main.py                          # Modal local entrypoint pipeline orchestrator
│   ├── setup_schema.py                  # One-time schema setup runner
│   ├── useful_prompts.txt               # Prompt templates
│   ├── utils.py                         # Snowflake connection helper
│   └── workers/                         # Modal workers by stage
│       ├── chunking_worker.py
│       ├── citation_aware_embedding_worker.py
│       ├── citation_worker.py
│       ├── embedding_worker.py
│       ├── graph_worker.py
│       ├── ingestion.py
│       ├── reasoning.py
│       ├── semantic_search_worker.py
│       └── transformation.py
├── react/                               # Frontend (Vite + React + TypeScript)
│   ├── src/                             # UI source
│   └── package.json                     # Frontend scripts/deps
├── terraform/                           # Infrastructure as Code
│   ├── main.tf
│   ├── providers.tf
│   └── variables.tf
├── assignments/                         # Course milestone artifacts
├── evals/                               # Evaluation assets/scripts
├── finetuning/                          # Fine-tuning assets/scripts
├── requirements.txt                     # Python dependencies
└── README.md                            # Project documentation
```

## Pipeline Overview

MindMap has an offline build pipeline plus online retrieval APIs.

Offline pipeline (`modal run app/main.py`):

1. Ingestion: Pull papers from Semantic Scholar or arXiv into Bronze (`BRONZE_PAPERS`).
2. Transformation: Normalize Bronze payloads into Silver paper records (`SILVER_PAPERS`) and enrich metadata.
3. SS-ID backfill: Fill missing Semantic Scholar IDs for existing Silver rows.
4. Paper embeddings: Compute paper-level vectors and initialize similar-paper caches.
5. Chunking: Split papers into structured sections/chunks for RAG (`SILVER_PAPER_SECTIONS`, `SILVER_PAPER_CHUNKS`).
6. Chunk embeddings + neighbor backfill: Embed chunks for dense retrieval and backfill similar IDs for older papers.
7. Graph build: Materialize Gold-layer relationship edges from citations/similarity.

Online retrieval (API/worker path):

1. Related-paper lookup (`get_related_papers`) uses cached neighbors when available and falls back to vector search.
2. Query semantic search (`semantic_search`) combines vector similarity with lightweight lexical overlap reranking.
3. RAG chunk retrieval (`retrieve_similar_chunks`) returns top matching chunks for grounded QA/summarization.

The pipeline supports step-level skip flags (`--skip-*`) so maintainers can rerun only changed stages.

## Features

- Dual-source ingestion from Semantic Scholar and arXiv into Bronze storage  
- Idempotent Bronze -> Silver transformation with metadata enrichment/backfills  
- Paper-level embedding generation with cached similar-paper neighbors  
- RAG-ready section/chunk generation plus chunk-level dense retrieval  
- Hybrid semantic search (vector score + lexical overlap rerank)  
- Knowledge graph edge construction from citation and similarity signals  
- Configurable step skipping in the pipeline for fast iterative reruns

## Architecture

MindMap follows a layered retrieval-and-reasoning architecture designed for scalable literature exploration.

### 1. Data Ingestion Layer
Paper metadata is ingested from Semantic Scholar and arXiv into `BRONZE_PAPERS` as raw JSON payloads for traceability and reprocessing. Reruns do not duplicate ingestion.
Transformation normalizes Bronze records into `SILVER_PAPERS`, with enrichment such as Semantic Scholar IDs and extracted metadata fields.  

### 2. Representation Layer
The embedding workers generate paper-level vectors and chunk-level vectors, storing them directly in Silver tables for retrieval and graph construction.

### 3. Retrieval Layer
Online retrieval supports three paths: related-paper lookup, free-text semantic search, and chunk-level retrieval for RAG.  
Retrieval uses vector similarity in Snowflake and supports configurable thresholds, candidate pool sizes, and top-k output.

### 4. Ranking & Reasoning Layer
Ranking is currently a lightweight hybrid strategy that blends vector similarity with token-overlap signals for query search.  
Reasoning helpers and prompt templates exist for higher-level analysis/summarization workflows, but the core retrieval path is deterministic and data-driven.

### 5. Graph Construction Layer
The graph worker materializes `GOLD_PAPER_RELATIONSHIPS` edges from two concrete signals in Silver: citation links and cached similar-paper neighbors.  
Edges are deduplicated and merged to keep relationship data incremental and consistent across reruns.

### 6. Serving Layer
Heavy pipeline stages run as Modal jobs through the orchestrator (`app/main.py`) with per-step skip flags for partial reruns. 

### 7. Infrastructure Layer
Snowflake stores Bronze/Silver/Gold data and vectors, while Terraform provisions and manages the database resources/environment setup.  
Modal provides remote execution for workers, and secrets are managed via Modal Secret resources for secure runtime credentials.


## Tech Stack

MindMap is built as a layered ML application rather than a single framework.

### Application Layer
- **Frontend UI** — interactive visualization and exploration of paper relationships
- **API layer (Modal endpoints)** — orchestration of retrieval, reranking, and graph construction

### ML & Retrieval
- **Embedding models (Transformer-based)** — semantic representation of papers and queries  
- **LLM integration** — contextual reranking, reasoning, and content generation  
- **Vector similarity search** — candidate retrieval from embedding space

### Data Platform
- **Snowflake** — storage of raw data, normalized metadata, embeddings, and derived features  
- **Medallion-style tables (Bronze/Silver)** — reproducible ingestion and feature preparation  
- **Semi-structured storage (VARIANT)** — flexible handling of evolving paper schemas

### Compute & Pipelines
- **Modal** — batch ingestion, embedding jobs, scheduled refresh, and API serving  
- **Asynchronous pipelines** — separation of heavy offline processing from online requests

### Infrastructure & DevOps
- **Terraform** — reproducible provisioning of Snowflake resources and environment configuration  
- **GitHub** — version control and collaboration  
- **Environment-based configuration** — secrets and deployment settings management

### Supporting Tooling
- **Python ecosystem (pandas, requests, etc.)** — ingestion and preprocessing  
- **Graph construction utilities** — building mind map structure from retrieval outputs  
- **Logging and monitoring** — debugging and evaluation of retrieval quality

## Inspiration

MindMap draws inspiration from existing platforms that rethink how researchers discover and navigate academic knowledge.

- 🗺️ **Litmaps** — interactive citation maps that visualize how papers connect, helping researchers discover related work and track evolving literature.  
  https://www.litmaps.com/

- 🔎 **Connected Papers** — visual graphs that reveal relationships between papers and help users explore unfamiliar research areas.  
  https://www.connectedpapers.com/

- 🧠 **Elicit** — AI-assisted literature review workflows that move beyond keyword search toward structured reasoning over papers.  
  https://elicit.com/

- 📚 **Semantic Scholar** — rich metadata, citation context, and AI signals that improve academic search and discovery.  
  https://www.semanticscholar.org/

- 🕸️ **ResearchRabbit** — continuous discovery through recommendation graphs and collection-based exploration.  
  https://www.researchrabbit.ai/

- ✨ **Perplexity (Academic workflows)** — conversational exploration that blends retrieval with synthesis, influencing how users expect to interact with information.  
  https://www.perplexity.ai/

MindMap combines ideas from these systems — semantic retrieval, graph exploration, and AI reasoning — into a unified workflow designed for interactive understanding rather than static search.


## Status

Active development as part of the CSC490 Machine Learning Engineering capstone at the University of Toronto.
