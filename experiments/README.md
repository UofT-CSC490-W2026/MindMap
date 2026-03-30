# Experiments and Benchmarking

This folder contains isolated evaluation pipelines for MindMap retrieval and QA experiments. Nothing in this package writes back to production MindMap tables; it only reads from the existing backend tables and writes structured outputs under `experiments/results/`.

## Design

### Knowledge Graph Retrieval

`experiments/kg/run_kg_eval.py` evaluates semantic retrieval quality across:

- Source compositions:
  - `title_abstract`
  - `title_abstract_tldr`
  - `title_abstract_conclusion`
  - `tldr_only`
- Embedding models:
  - `all-MiniLM-L12-v2`
  - `text-embedding-3-small`
  - `text-embedding-3-large`
  - `bge-large-en-v1.5`

Anchor papers are selected from `SILVER_PAPERS` with title, abstract, and at least one resolvable citation/reference target. Citation resolution reuses the same identifier matching path used by the graph worker.

Metrics:

- `Recall@k`: fraction of resolvable citations recovered in the top-k retrieved papers
- `Hits@k`: whether at least one cited paper appears in the top-k retrieved papers
- `MRR`: reciprocal rank of the first cited paper in the full ranked list

Outputs:

- `experiments/results/kg/kg_auto_summary.csv`
- `experiments/results/kg/kg_auto_per_anchor.csv`
- `experiments/results/kg/kg_detailed_results.json`
- `experiments/results/kg_bridge_candidates.csv`
- `experiments/results/kg/intermediate/`

### RAG + LLM

`experiments/rag/run_rag_eval.py` evaluates paper understanding with:

- Stage 1 methods:
  - `abstract_only`
  - `llm_only`
  - `rag_llm`
- Stage 2 models:
  - `gpt-4o-mini`
  - `gpt-4.1-mini`
  - `gpt-4.1`

Question inputs come from `experiments/config/rag_questions.csv` with columns:

```csv
paper_id,question_id,question_text
```

The `llm_only` baseline answers from the paper abstract only. The `rag_llm` method retrieves chunks with `retrieve_similar_chunks_local` and uses only those chunks as context. No experiment writes to `APP_QA_LOGS`.

Metrics:

- `similarity_score`: max cosine similarity between the generated answer and retrieved context chunks
- `support_rate`: fraction of answer sentences whose best chunk similarity exceeds the support threshold
- `stability`: average pairwise similarity across repeated runs for the same prompt

Outputs:

- `experiments/results/rag/rag_auto_summary_stage1.csv`
- `experiments/results/rag/rag_auto_summary_stage2.csv`
- `experiments/results/rag/rag_per_question.csv`
- `experiments/results/rag/rag_outputs.json`
- `experiments/results/rag_human_eval.csv`
- `experiments/results/rag/intermediate/`

## Backend Reuse Map

- Local experiment wrappers: `experiments/common.py`
- Citation resolution logic: aligned with `app.workers.graph_worker._citation_targets`
- Chunk retrieval logic: aligned with `app.workers.semantic_search_worker.retrieve_similar_chunks_local`
- Chunk formatting: `app.services.prompt_templates.format_chunk_context`
- LLM calls: `app.services.llm_client.LLMClient`

The experiment package intentionally uses local wrappers for Snowflake access and table qualification so the scripts can run outside the Modal production runtime while still following the same backend logic.

## Setup

Local dependencies typically needed for these scripts:

- `snowflake-connector-python`
- `sentence-transformers`
- `httpx`
- `tqdm`

Environment variables:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `OPENAI_API_KEY`

Optional overrides:

- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_WAREHOUSE`

## Running

KG retrieval:

```bash
python -m experiments.kg.run_kg_eval \
  --database MINDMAP_DEV \
  --anchor-count 24 \
  --seed 13 \
  --reuse-cache
```

RAG evaluation:

```bash
python -m experiments.rag.run_rag_eval \
  --database MINDMAP_DEV \
  --questions-file experiments/config/rag_questions.csv \
  --stage all \
  --top-k 5 \
  --runs-per-question 3
```

`--max-questions` limits the run by question rows, not by unique papers. For example, if the CSV contains four questions per paper, `--max-questions 24` corresponds to six papers.

## Commands We Used

KG evaluation on the current corpus:

```bash
python -m experiments.kg.run_kg_eval \
  --database MINDMAP_DEV \
  --anchor-count 24 \
  --seed 13 \
  --reuse-cache
```

RAG stage-1 pilot:

```bash
python -m experiments.rag.run_rag_eval \
  --database MINDMAP_DEV \
  --questions-file experiments/config/rag_questions.csv \
  --stage stage1 \
  --methods llm_only,rag_llm \
  --top-k 3 \
  --runs-per-question 1 \
  --max-questions 24
```

Reduced full RAG evaluation:

```bash
python -m experiments.rag.run_rag_eval \
  --database MINDMAP_DEV \
  --questions-file experiments/config/rag_questions.csv \
  --stage all \
  --top-k 3 \
  --runs-per-question 3 \
  --max-questions 24
```

Corpus expansion runs used to grow the database before evaluation:

```bash
modal run app/job_test.py \
  --query "machine learning" \
  --source semantic_scholar \
  --max-results 100 \
  --database MINDMAP_DEV \
  --embed-limit 500 \
  --chunk-limit 1000 \
  --k 10
```

```bash
modal run app/job_test.py \
  --query "graph neural networks" \
  --source semantic_scholar \
  --max-results 100 \
  --database MINDMAP_DEV \
  --embed-limit 500 \
  --chunk-limit 1000 \
  --k 10
```

```bash
modal run app/job_test.py \
  --query "representation learning" \
  --source semantic_scholar \
  --max-results 100 \
  --database MINDMAP_DEV \
  --embed-limit 500 \
  --chunk-limit 1000 \
  --k 10
```

If you only want to populate up to `similar_embeddings_ids` and stop before chunking and graph construction:

```bash
modal run app/job_test.py \
  --query "representation learning" \
  --source semantic_scholar \
  --max-results 100 \
  --database MINDMAP_DEV \
  --embed-limit 500 \
  --chunk-limit 1000 \
  --k 10 \
  --skip-chunking \
  --skip-chunk-embeddings \
  --skip-graph
```

## Human Annotation

### KG bridge review

Annotate `experiments/results/kg_bridge_candidates.csv` by adding either:

- `score`: numeric quality score, or
- `label`: positive/negative binary annotation

Then run:

```bash
python -m experiments.kg.summarize_bridge experiments/results/kg_bridge_candidates.csv
```

This writes `experiments/results/kg_bridge_summary.csv`.

### RAG answer review

Annotate `experiments/results/rag_human_eval.csv` with a `score` column using:

- `0`: incorrect / unsupported
- `1`: partially correct
- `2`: correct / well grounded

Then run:

```bash
python -m experiments.rag.summarize_human experiments/results/rag_human_eval.csv
```

Tie handling for `win_rate` is deterministic: tied top-scoring methods split the win equally for that paper-question pair.

This writes `experiments/results/rag_human_summary.csv`.

## Assumptions And Limitations

- KG metrics exclude citations that cannot be resolved to local `SILVER_PAPERS.id` values.
- OpenAI models require `OPENAI_API_KEY`.
- Sentence-transformer similarity scoring uses `all-MiniLM-L12-v2`.
- `rag_questions.csv` must contain valid `paper_id,question_id,question_text` rows before running the RAG evaluation pipeline.
