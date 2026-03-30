# MindMap

![Coverage](./coverage.svg)

MindMap is an AI-assisted academic literature explorer that helps researchers understand how papers relate, discover relevant work, and navigate research areas through an interactive graph interface. Instead of returning flat search results, it builds a knowledge graph from ingested papers and exposes it through semantic search, citation-aware retrieval, and a React-based visualization UI. The backend runs as Modal workers and a FastAPI app, Snowflake stores all Bronze/Silver/Gold data and embeddings, and Terraform provisions the Snowflake infrastructure.

---

## Demo / Grading

> **For graders / reviewers: you only need to open the frontend URL. No credentials, no setup.**
>
> The backend is already deployed on Modal and the frontend is pointed at it. Just open the app and use it.

The full pipeline — ingestion through graph construction — is working end-to-end on the `main` branch. The React frontend connects to a deployed Modal API and renders the paper relationship graph. Semantic search, related-paper lookup, and chunk-level RAG retrieval are all functional. Summarization (Step 8) is implemented but skipped by default due to LLM cost.

**How the live demo works:** We run `modal deploy app/main.py` once from our machine. Modal hosts the FastAPI backend at a persistent public URL that stays live even after our terminal closes. The frontend is built with that URL baked in as `VITE_API_URL` and deployed statically. The grading team opens one URL and the full app is live — no Python, no credentials, no local setup required on their end.

Note: Modal containers go cold after a period of inactivity. The first request after a cold start may take 5–10 seconds to respond — this is normal. Subsequent requests are fast.

---

## Repository Structure

```
MindMap/
├── app/
│   ├── api/          # FastAPI route handlers
│   ├── services/     # Business logic (search, graph, ingestion, chat, paper)
│   ├── workers/      # Modal pipeline workers (ingestion → graph)
│   ├── config.py     # Modal app/images/secrets + DB constants
│   ├── main.py       # Pipeline orchestrator (local_entrypoint)
│   └── server.py     # FastAPI app definition
├── react/            # Vite + React + TypeScript frontend
├── tests/
│   ├── api/          # API route tests
│   ├── properties/   # Property-based tests (Hypothesis)
│   └── test_*.py     # Worker and service unit tests
├── terraform/        # Snowflake infrastructure (IaC)
└── evals/            # Evaluation scripts and assets
```

---

## Quick Start

There are three paths depending on what you need:

| Path | Who it's for |
|---|---|
| Path 1 — Frontend only | Browsing the UI locally with mock data |
| Path 2 — Backend API | Deploying or serving the live backend (team members only) |
| Path 3 — Full pipeline | Running the data pipeline or developing backend features |

**Graders/reviewers:** you don't need any of these. The live app is already deployed — see [Demo / Grading](#demo--grading) above.

---

### Path 1 — Frontend Only (local, mock data)

Prerequisites: Node.js 18+, npm

```bash
git clone https://github.com/UofT-CSC490-W2026/MindMap.git
cd MindMap/react
npm install
npm run dev
```

Open `http://localhost:5173`. The UI will use mock data if `VITE_API_URL` is not set. To point it at the live backend, add `VITE_API_URL=<deployed-modal-url>` to `react/.env` before running.

---

### Path 2 — Backend API (team deployment)

Prerequisites: Modal CLI configured (`modal setup`), Snowflake credentials

```bash
# Create required Modal secrets
modal secret create snowflake-creds \
  SNOWFLAKE_ACCOUNT="<account>" \
  SNOWFLAKE_USER="<user>" \
  SNOWFLAKE_PASSWORD="<password>" \
  SNOWFLAKE_WAREHOUSE="MINDMAP_WH" \
  --force

modal secret create openai-api \
  OPENAI_API_KEY="<key>" --force

modal secret create semantic-scholar-api \
  SEMANTIC_SCHOLAR_API_KEY="<key>" --force

# Serve the API (ephemeral — stops when terminal closes)
modal serve app/main.py

# Or deploy persistently
modal deploy app/main.py
```

Modal prints a URL like `https://<workspace>--mindmap-pipeline-fastapi-app.modal.run`.
Set it in `react/.env`:

```
VITE_API_URL=https://<your-modal-url>
```

Restart the Vite dev server after updating `.env`.

---

### Path 3 — Full Pipeline / Developer Setup (team only)

Prerequisites: Python 3.10+, `uv`, Modal CLI, Snowflake credentials

```bash
# Python environment
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt

# Authenticate Modal (one-time)
modal setup

# Export credentials
export SNOWFLAKE_ACCOUNT="<account>"
export SNOWFLAKE_USER="<user>"
export SNOWFLAKE_PASSWORD="<password>"
export SNOWFLAKE_WAREHOUSE="MINDMAP_WH"
export SEMANTIC_SCHOLAR_API_KEY="<key>"
export OPENAI_API_KEY="<key>"

# Create Modal secrets
modal secret create snowflake-creds \
  SNOWFLAKE_ACCOUNT="$SNOWFLAKE_ACCOUNT" \
  SNOWFLAKE_USER="$SNOWFLAKE_USER" \
  SNOWFLAKE_PASSWORD="$SNOWFLAKE_PASSWORD" \
  SNOWFLAKE_WAREHOUSE="$SNOWFLAKE_WAREHOUSE" --force

modal secret create semantic-scholar-api \
  SEMANTIC_SCHOLAR_API_KEY="$SEMANTIC_SCHOLAR_API_KEY" --force

modal secret create openai-api \
  OPENAI_API_KEY="$OPENAI_API_KEY" --force

# Optional: HuggingFace token for gated models
modal secret create huggingface-secret HF_TOKEN="<token>" --force

# Verify
modal secret list
```

Run the full pipeline:

```bash
modal run app/main.py --query "model quantization" --source semantic_scholar --max-results 20
```

Skip expensive early stages on reruns:

```bash
modal run app/main.py --query "model quantization" --max-results 50 \
  --skip-ingestion --skip-transformation --skip-ss-id-backfill
```

Other options:

```bash
# arXiv source
modal run app/main.py --query "graph neural networks" --source arxiv --max-results 30

# Target a specific Snowflake database
modal run app/main.py --query "model quantization" --database MINDMAP_DB

# Enable optional summarization (skipped by default)
modal run app/main.py --query "model quantization" --max-results 20 --skip-summary false
```

---

## Configuration

| Variable | Required | Example | Purpose |
|---|---|---|---|
| `SNOWFLAKE_ACCOUNT` | Yes | `abc12345.us-east-1` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Yes | `mindmap_user` | Snowflake login user |
| `SNOWFLAKE_PASSWORD` | Yes | `...` | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | Yes | `MINDMAP_WH` | Compute warehouse |
| `OPENAI_API_KEY` | Yes | `sk-...` | LLM calls (reasoning, summarization) |
| `SEMANTIC_SCHOLAR_API_KEY` | Yes | `...` | Paper ingestion from Semantic Scholar |
| `HF_TOKEN` | No | `hf_...` | HuggingFace token for gated models |
| `VITE_API_URL` | Frontend | `https://...modal.run` | Modal API URL for the React app |
| `TF_VAR_snowflake_organization_name` | Terraform | `myorg` | Snowflake org for Terraform provider |
| `TF_VAR_snowflake_account_name` | Terraform | `myaccount` | Snowflake account for Terraform provider |

All backend secrets are passed to Modal workers via `modal secret create`. The frontend reads `VITE_API_URL` from `react/.env` (copy from `react/.env.example`).

---

## Testing

### Backend

```bash
# Run all tests
pytest

# With coverage report
pytest --cov=app

# Specific file
pytest tests/test_ingestion.py

# Property-based tests only
pytest tests/properties/
```

Backend tests cover all pipeline workers (ingestion, transformation, chunking, embedding, citation, graph, QA, summary, semantic search), API routes, and service-layer logic. Property-based tests use Hypothesis to verify correctness properties across the chat, graph, ingestion, paper, and search services.

### Frontend

```bash
cd react

# Run all tests
npm test

# With coverage
npm run test:coverage
```

Frontend tests cover React components (`AddPaperModal`, `PaperPanel`), custom hooks (`useGraphData`, `useIngest`, `semanticSearch`), all service modules (`apiClient`, `graphService`, `ingestionService`, `paperService`, `searchService`, `paperDetailService`), and graph utilities.

---

## Infrastructure and Deployment

### Terraform (Snowflake)

Terraform provisions the Snowflake database, schemas, warehouse, and roles used by the pipeline.

```bash
cd terraform

# Set provider variables
export TF_VAR_snowflake_organization_name="<org>"
export TF_VAR_snowflake_account_name="<account>"
export TF_VAR_snowflake_user="$SNOWFLAKE_USER"
export TF_VAR_snowflake_password="$SNOWFLAKE_PASSWORD"

terraform init
terraform workspace select dev || terraform workspace new dev
terraform plan
terraform apply
```

Use `terraform workspace select prod` for production resources.

### Modal (Backend)

```bash
# Ephemeral serve (stops when terminal closes)
modal serve app/main.py

# Persistent deployment
modal deploy app/main.py

# Run the full offline pipeline
modal run app/main.py --query "<query>" --source semantic_scholar --max-results 20
```

---

## Benchmarking / Reproducibility

Pipeline performance was profiled using Python's `cProfile` across the major worker stages. Results are saved in `profile_summary.txt` at the repo root.

To reproduce:

```bash
# Profile output is generated during a modal run with profiling enabled in the worker
# Results are written to profile_summary.txt
cat profile_summary.txt
```

Key findings from the profile:

- `build_knowledge_graph` (`_fetch_papers`): ~108s — dominated by a single Snowflake fetch of the full paper set; a candidate for pagination or incremental processing
- `chunk_papers` (`_fetch_unchunked_papers`): ~73s — full-table scan for unchunked papers; indexing on `chunked` status would reduce this
- `run_chunk_embedding_batch`: ~45s — network-bound (OpenAI embedding API); batching is already applied, latency is inherent
- `run_embedding_batch`: ~6.5s — similar network-bound pattern for paper-level embeddings
- `ingest_from_semantic_scholar`: ~4.6s — dominated by Snowflake write latency across 13 queries

The main bottleneck is Snowflake I/O in the graph and chunking workers. Snowflake connection setup adds ~4s per worker cold start.

---

## Architecture / Pipeline Overview

MindMap follows a medallion-style data architecture (Bronze → Silver → Gold) with a separate online serving layer.

### Offline Pipeline (`modal run app/main.py`)

1. Ingestion — pull papers from Semantic Scholar or arXiv into `BRONZE_PAPERS` as raw JSON
2. Transformation — normalize Bronze into `SILVER_PAPERS`, enrich metadata, backfill Semantic Scholar IDs
3. Paper embeddings — compute paper-level vectors, initialize similar-paper neighbor caches
4. Chunking — split papers into sections/chunks for RAG (`SILVER_PAPER_SECTIONS`, `SILVER_PAPER_CHUNKS`)
5. Chunk embeddings — embed chunks for dense retrieval, backfill neighbor IDs for older papers
6. Graph build — materialize `GOLD_PAPER_RELATIONSHIPS` edges from citation links and similarity signals
7. Summarization (optional) — generate structured Gold-layer summaries via LLM (skipped by default)

Each stage has a `--skip-*` flag for partial reruns.

### Online Retrieval (API)

- Related-paper lookup — uses cached neighbors, falls back to vector search
- Semantic search — vector similarity + lexical overlap reranking
- Chunk retrieval — top-k chunk matches for grounded QA

### Serving

The FastAPI app (`app/server.py`) is deployed on Modal and exposes REST endpoints consumed by the React frontend. The frontend uses `react-force-graph-2d` to render the paper relationship graph interactively.

---

## Known Limitations / Next Steps

- All external services (Snowflake, Modal, OpenAI, Semantic Scholar) require active credentials — the system cannot run fully offline
- Summarization is expensive and skipped by default; enabling it on large paper sets will incur significant LLM cost
- Graph and chunking workers do full-table scans; large datasets will see increased Snowflake latency
- The `evals/` directory is present but evaluation scripts are not yet populated