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

### 1. Quick Start (Frontend Users)

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

### 2. Developer / Admin Setup (Backend + Data Pipeline)

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
export SNOWFLAKE_ACCOUNT="SCZSGQN-SG49598"
export SNOWFLAKE_USER="hclover2003"
export SNOWFLAKE_PASSWORD="mindMap20262026"

# 3) create/update Modal secrets used by workers

modal secret create snowflake-creds \
  SNOWFLAKE_ACCOUNT="$SNOWFLAKE_ACCOUNT" \
  SNOWFLAKE_USER="$SNOWFLAKE_USER" \
  SNOWFLAKE_PASSWORD="$SNOWFLAKE_PASSWORD" \
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
# terraform workspace select prod

# 3) define variables
export TF_VAR_snowflake_account="$SNOWFLAKE_ACCOUNT"
export TF_VAR_snowflake_user="$SNOWFLAKE_USER"
export TF_VAR_snowflake_password="$SNOWFLAKE_PASSWORD"

# 4) 
# preview changes
terraform plan

# or apply changes
terraform apply
```

Example run:

```bash
QUERY="model quantization"

# end-to-end run
python -m app.main pipeline --query "$QUERY" --max-results 10 --embed-limit 20

# you can also run individual steps
python -m app.main ingest --query "$QUERY" --max-results 10
python -m app.main transform
python -m app.main embed --limit 200
```

## Codebase Overview

```text
MindMap/
├── app/                                 # Backend code and pipeline orchestration
│   ├── main.py                          #   CLI entry point (pipeline, ingest, transform, embed)
│   ├── create.sql                       #   Snowflake schema/table setup SQL
│   ├── workers/                         #   Modal worker functions for each pipeline stage
│   │   ├── ingestion.py                 #     arXiv ingestion into Bronze
│   │   ├── transformation.py            #     Bronze -> Silver transformation
│   │   ├── embedding_worker.py          #     baseline embedding generation
│   │   ├── semantic_search.py           #     vector similarity retrieval
│   │   ├── citation_worker.py           #     citation/reference extraction from PDFs
│   │   └── citation_aware_embedding_worker.py  # citation-aware embedding pipeline
│   └── utils/
│       ├── snowflake_utils.py           #   Snowflake connection helpers
│       └── modal_config.py              #   Shared Modal app/image/secret config
├── react/                               # Frontend UI (Vite + React + TypeScript)
│   ├── src/
│   │   ├── App.tsx                      #   Main graph UI
│   │   └── mock/graphData.ts            #   Current mock graph data source
│   └── package.json                     #   Frontend scripts and dependencies
├── terraform/                           # Infrastructure as Code
│   ├── main.tf                          #   Core infrastructure resources
│   ├── providers.tf                     #   Terraform providers
│   └── variables.tf                     #   Input variables
├── assignments/                         # Course milestone/assignment artifacts
└── README.md                            # Project documentation
```

## Pipeline Overview

MindMap runs as a staged retrieval pipeline:

1. Ingest: Pull paper metadata/content from arXiv and write raw records to Bronze tables.
2. Transform: Normalize raw payloads into structured Silver tables with paper-level fields.
3. Embed: Generate vector embeddings for papers and store them for semantic retrieval.
4. Retrieve: Given a paper or query, run vector similarity search to find related candidates.
5. Build graph: Convert related papers and links into node/edge data for the frontend graph view.

The `pipeline` command in `app/main.py` runs the first three stages end-to-end; retrieval and graph rendering happen at query time in the app experience. End users interact with the frontend, while backend pipeline commands are for maintainers/developers.

## Features

- Semantic paper retrieval using embeddings  
- Context-aware reranking with metadata and LLM reasoning  
- Graph-based mind map visualization of research topics  
- Incremental ingestion pipeline for new papers  
- Modular architecture for experimentation

## Architecture

MindMap follows a layered retrieval-and-reasoning architecture designed for scalable literature exploration.

### 1. Data Ingestion Layer
Paper metadata and content are collected from external sources and stored in append-only **Bronze tables** to preserve raw inputs.  
Normalization, deduplication, and feature preparation produce structured **Silver tables** used by downstream components.

### 2. Representation Layer
Papers and queries are converted into vector representations using transformer-based embedding models.  
Embeddings are versioned and stored alongside metadata to support reproducible experiments and retrieval consistency.

### 3. Retrieval Layer
Given a user query, the system performs semantic candidate generation via vector similarity search.  
This stage prioritizes recall and produces a manageable set of relevant papers.

### 4. Ranking & Reasoning Layer
Candidate papers are reranked using a combination of similarity signals, metadata features, and optional LLM-assisted reasoning.  
This stage focuses on precision and contextual relevance.

### 5. Graph Construction Layer
Selected papers are organized into a lightweight graph that captures relationships such as topical similarity, shared authorship, or citation structure.  
The graph forms the basis of the mind map visualization.

### 6. Serving Layer
API endpoints orchestrate embedding, retrieval, ranking, and graph generation at request time.  
Heavy processing is handled asynchronously, enabling responsive interaction in the UI.

### 7. Infrastructure Layer
Compute, storage, and environment configuration are managed through a serverless architecture with reproducible provisioning, allowing the system to scale while remaining easy to deploy.


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
