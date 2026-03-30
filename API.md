# MindMap API Reference

The MindMap API is a FastAPI application deployed on Modal. All endpoints are served under the base URL printed by `modal serve` or `modal deploy`.

```
Base URL: https://<workspace>--mindmap-pipeline-fastapi-app.modal.run
```

All request and response bodies are JSON. Successful responses return HTTP `200`. Errors return standard HTTP status codes with a `detail` field.

---

## Table of Contents

- [Health](#health)
- [Search](#search)
- [Graphs](#graphs)
- [Papers](#papers)
- [Ingestion](#ingestion)

---

## Health

### `GET /health`

Liveness check. Returns immediately with no dependencies.

**Response**

```json
{ "status": "ok" }
```

---

## Search

### `GET /search/papers`

Search for papers by free-text query using vector similarity and lexical overlap reranking. Also accessible at the alias `GET /papers/search` for Semantic Scholar API compatibility.

**Query Parameters**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `query` | string | Yes | — | Free-text search query (min length 1) |
| `limit` | integer | No | `10` | Max results to return (1–50) |
| `fields` | string | No | — | Accepted but ignored (compatibility param) |

**Example Request**

```
GET /search/papers?query=transformer+attention&limit=5
```

**Response** — array of paper objects

```json
[
  {
    "title": "Attention Is All You Need",
    "authors": ["Ashish Vaswani", "Noam Shazeer"],
    "year": 2017,
    "citation_count": 98000,
    "arxiv_id": "1706.03762",
    "external_url": "https://arxiv.org/abs/1706.03762"
  }
]
```

| Field | Type | Description |
|---|---|---|
| `title` | string | Paper title |
| `authors` | string[] | Author names |
| `year` | integer \| null | Publication year |
| `citation_count` | integer \| null | Total citation count |
| `arxiv_id` | string \| null | arXiv identifier |
| `external_url` | string \| null | Link to the paper |

---

## Graphs

### `POST /graphs/query`

Build a knowledge graph for a research query. Returns nodes (papers) and links (relationships) ready for visualization.

**Request Body**

```json
{ "query": "model quantization" }
```

| Field | Type | Required | Description |
|---|---|---|---|
| `query` | string | Yes | Research topic or question (min length 1) |

**Response**

```json
{
  "graph_id": "a1b2c3d4",
  "query": "model quantization",
  "nodes": [
    {
      "id": "42",
      "label": "LLM.int8(): 8-bit Matrix...",
      "title": "LLM.int8(): 8-bit Matrix Multiplication for Transformers at Scale",
      "authors": "Tim Dettmers, Mike Lewis",
      "year": 2022,
      "citations": 1200,
      "arxiv_id": "2208.07339",
      "cluster_id": 2,
      "cluster_name": "Quantization Methods"
    }
  ],
  "links": [
    {
      "source": "42",
      "target": "17",
      "kind": "CITES",
      "strength": 0.91
    }
  ],
  "meta": {
    "total_nodes": 24,
    "total_links": 38,
    "query": "model quantization"
  }
}
```

**Node fields**

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique paper ID (stringified integer) |
| `label` | string | Truncated title for graph display |
| `title` | string | Full paper title |
| `authors` | string | Comma-separated author names |
| `year` | integer | Publication year |
| `citations` | integer | Citation count |
| `arxiv_id` | string \| null | arXiv identifier |
| `cluster_id` | integer \| null | Topic cluster assignment |
| `cluster_name` | string \| null | Human-readable cluster label |

**Link fields**

| Field | Type | Description |
|---|---|---|
| `source` | string | Source node ID |
| `target` | string | Target node ID |
| `kind` | string | Relationship type: `CITES`, `SIMILAR`, `SUPPORT`, `CONTRADICT`, `NEUTRAL` |
| `strength` | float | Edge weight (0.0–1.0) |

**Meta fields**

| Field | Type | Description |
|---|---|---|
| `total_nodes` | integer | Total nodes in the graph |
| `total_links` | integer | Total edges in the graph |
| `query` | string \| null | The original query |

---

### `POST /graphs/expand`

Expand an existing graph by adding the neighbors of a specific paper. Use this when a user clicks a node to explore its connections.

**Request Body**

```json
{
  "graph_id": "a1b2c3d4",
  "paper_id": 42
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `graph_id` | string | Yes | ID returned by `POST /graphs/query` |
| `paper_id` | integer | Yes | ID of the node to expand |

**Response**

```json
{
  "graph_id": "a1b2c3d4",
  "paper_id": "42",
  "new_nodes": [ /* GraphNode objects */ ],
  "new_links": [ /* GraphLink objects */ ]
}
```

Only the newly added nodes and links are returned. Merge them into the existing graph on the client side.

---

### `POST /graphs/clusters/rebuild`

Trigger a background re-clustering of all papers in the knowledge graph. This is an admin/maintenance operation — it re-runs topic clustering and updates `cluster_id` and `cluster_name` on all nodes.

**Query Parameters**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `n_clusters` | integer | No | `5` | Number of topic clusters to produce (2–20) |

**Example Request**

```
POST /graphs/clusters/rebuild?n_clusters=8
```

**Response**

```json
{ "status": "ok", "result": { ... } }
```

Returns `500` with a `detail` message if the cluster rebuild fails.

---

## Papers

### `GET /papers/{paper_id}`

Fetch full metadata for a single paper by its internal ID.

**Path Parameters**

| Parameter | Type | Description |
|---|---|---|
| `paper_id` | integer | Internal paper ID |

**Response**

```json
{
  "paper_id": 42,
  "title": "LLM.int8(): 8-bit Matrix Multiplication for Transformers at Scale",
  "authors": ["Tim Dettmers", "Mike Lewis"],
  "year": 2022,
  "citations": 1200,
  "arxiv_id": "2208.07339",
  "abstract": "We develop a procedure for Int8 matrix multiplication..."
}
```

| Field | Type | Description |
|---|---|---|
| `paper_id` | integer | Internal ID |
| `title` | string | Full paper title |
| `authors` | string[] | Author names |
| `year` | integer \| null | Publication year |
| `citations` | integer \| null | Citation count |
| `arxiv_id` | string \| null | arXiv identifier |
| `abstract` | string \| null | Paper abstract |

---

### `GET /papers/{paper_id}/summary`

Fetch a structured AI-generated summary of a paper. Summaries are pre-computed during the offline pipeline (Step 7). Returns `null` fields if summarization was skipped for this paper.

**Path Parameters**

| Parameter | Type | Description |
|---|---|---|
| `paper_id` | integer | Internal paper ID |

**Response**

```json
{
  "paper_id": 42,
  "research_question": "Can 8-bit quantization match full-precision LLM performance?",
  "methods": ["Int8 matrix multiplication", "vector-wise quantization"],
  "main_claims": ["Int8 inference matches fp16 perplexity on large models"],
  "key_findings": ["175B parameter models run on a single GPU with Int8"],
  "limitations": ["Requires models with >6.7B parameters for full benefit"],
  "conclusion": "Int8 quantization is viable for large-scale LLM inference."
}
```

| Field | Type | Description |
|---|---|---|
| `paper_id` | integer | Internal ID |
| `research_question` | string \| null | Core research question addressed |
| `methods` | string[] | Methods and techniques used |
| `main_claims` | string[] | Primary claims made by the paper |
| `key_findings` | string[] | Key empirical or theoretical findings |
| `limitations` | string[] | Stated or inferred limitations |
| `conclusion` | string \| null | Summary conclusion |

---

### `POST /papers/{paper_id}/chat`

Ask a question about a specific paper. Uses chunk-level RAG retrieval to ground the answer in the paper's content. Supports multi-turn sessions via `session_id`.

**Path Parameters**

| Parameter | Type | Description |
|---|---|---|
| `paper_id` | integer | Internal paper ID |

**Request Body**

```json
{
  "question": "What quantization method does this paper propose?",
  "session_id": "sess_abc123"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `question` | string | Yes | Question to ask about the paper (min length 1) |
| `session_id` | string | No | Pass the value returned by a previous call to continue a conversation |

**Response**

```json
{
  "paper_id": 42,
  "session_id": "sess_abc123",
  "answer": "The paper proposes Int8 matrix multiplication using vector-wise quantization...",
  "cited_chunk_ids": [101, 204, 317],
  "rewritten_query": "What is the quantization approach in LLM.int8()?"
}
```

| Field | Type | Description |
|---|---|---|
| `paper_id` | integer | Paper the answer is grounded in |
| `session_id` | string | Session ID — pass this back for follow-up questions |
| `answer` | string | RAG-grounded answer |
| `cited_chunk_ids` | integer[] | IDs of the chunks used to generate the answer |
| `rewritten_query` | string \| null | Query rewrite applied before retrieval, if any |

---

## Ingestion

### `POST /ingestions`

Kick off ingestion of a paper by arXiv ID. The paper is fetched, normalized, embedded, and added to the knowledge graph asynchronously. If the paper already exists in the database, the job is skipped.

**Request Body**

```json
{ "arxiv_id": "2208.07339" }
```

| Field | Type | Required | Description |
|---|---|---|---|
| `arxiv_id` | string | Yes | arXiv paper identifier (e.g. `2208.07339`) |

**Response**

```json
{
  "job_id": "job_xyz789",
  "arxiv_id": "2208.07339",
  "status": "processing",
  "stage": "ingestion",
  "bronze_status": "inserted"
}
```

| Field | Type | Description |
|---|---|---|
| `job_id` | string | Use this to poll `GET /ingestions/{job_id}` |
| `arxiv_id` | string | The arXiv ID that was submitted |
| `status` | `"processing"` \| `"skipped"` | `skipped` means the paper already exists |
| `stage` | string | Current pipeline stage |
| `bronze_status` | string | Whether the raw record was inserted or already present |

---

### `GET /ingestions/{job_id}`

Poll the status of an ingestion job.

**Path Parameters**

| Parameter | Type | Description |
|---|---|---|
| `job_id` | string | Job ID returned by `POST /ingestions` |

**Response**

```json
{
  "job_id": "job_xyz789",
  "status": "done",
  "result": { ... },
  "error": null
}
```

| Field | Type | Description |
|---|---|---|
| `job_id` | string | The job ID |
| `status` | `"processing"` \| `"done"` \| `"failed"` | Current job state |
| `result` | object \| null | Final result payload when `status` is `"done"` |
| `error` | string \| null | Error message when `status` is `"failed"` |

**Polling pattern**

```
POST /ingestions          → { job_id: "job_xyz789", status: "processing" }
GET  /ingestions/job_xyz789 → { status: "processing" }
GET  /ingestions/job_xyz789 → { status: "done", result: { ... } }
```

Poll every few seconds until `status` is `"done"` or `"failed"`.

---

## Error Responses

All errors follow FastAPI's standard format:

```json
{ "detail": "descriptive error message" }
```

| Status | Meaning |
|---|---|
| `400` | Bad request — invalid or missing parameters |
| `404` | Resource not found |
| `422` | Validation error — request body failed schema validation |
| `500` | Internal server error — check `detail` for context |
