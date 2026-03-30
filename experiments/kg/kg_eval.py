"""Knowledge-graph retrieval evaluation pipeline."""

from __future__ import annotations

import csv
import importlib
import json
import logging
import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence, Union

from experiments.common import (
    DATABASE,
    citation_targets,
    connect_to_snowflake,
    qualify_table,
)

try:
    from tqdm.auto import tqdm
except ModuleNotFoundError:
    def tqdm(iterable=None, total=None, desc=None, **kwargs):
        return iterable if iterable is not None else _NoOpProgress(total=total)

    class _NoOpProgress:
        def __init__(self, total=None):
            self.total = total

        def update(self, n=1):
            return None

        def set_description(self, desc=None):
            return None

        def set_postfix(self, ordered_dict=None, refresh=True, **kwargs):
            return None

        def close(self):
            return None

logger = logging.getLogger(__name__)

DEFAULT_SOURCE_TYPES = [
    "title_abstract",
    "title_abstract_tldr",
    "title_abstract_conclusion",
    "tldr_only",
]
DEFAULT_EMBEDDING_MODELS = [
    "all-MiniLM-L12-v2",
    "text-embedding-3-small",
    "text-embedding-3-large",
    "bge-large-en-v1.5",
]
DEFAULT_TOP_K_VALUES = [5, 10]
DEFAULT_OUTPUT_DIR = Path("experiments/results/kg")
DEFAULT_ANCHOR_COUNT = 24
OPENAI_EMBEDDING_MODELS = {"text-embedding-3-small", "text-embedding-3-large"}
MODEL_NAME_MAP = {
    "all-MiniLM-L12-v2": "sentence-transformers/all-MiniLM-L12-v2",
    "bge-large-en-v1.5": "BAAI/bge-large-en-v1.5",
}
OPENAI_EMBEDDING_URL = "https://api.openai.com/v1/embeddings"


@dataclass
class PaperRecord:
    """Minimal paper payload needed for KG experiments."""

    paper_id: int
    title: str
    abstract: str
    tldr: str
    conclusion: str
    citation_payload: list[dict[str, Any]]
    resolved_citation_ids: list[int]


def _quote_ident(identifier: str) -> str:
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _resolve_table_columns(cur, table_name: str) -> dict[str, str]:
    cur.execute(f"DESC TABLE {table_name}")
    columns = [row[0] for row in cur.fetchall() if row and row[0]]
    return {str(name).lower(): _quote_ident(str(name)) for name in columns}


def _require_columns(column_map: dict[str, str], required: list[str], table_name: str) -> dict[str, str]:
    missing = [name for name in required if name not in column_map]
    if missing:
        raise RuntimeError(f"Missing required columns in {table_name}: {missing}")
    return {name: column_map[name] for name in required}


def _parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    return value if isinstance(value, list) else []


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _source_text_for_paper(paper: PaperRecord, source_type: str) -> str:
    title = _normalize_text(paper.title)
    abstract = _normalize_text(paper.abstract)
    tldr = _normalize_text(paper.tldr)
    conclusion = _normalize_text(paper.conclusion)

    sections: list[str] = []
    if source_type == "title_abstract":
        if title:
            sections.append(f"Title: {title}")
        if abstract:
            sections.append(f"Abstract: {abstract}")
    elif source_type == "title_abstract_tldr":
        if title:
            sections.append(f"Title: {title}")
        if abstract:
            sections.append(f"Abstract: {abstract}")
        if tldr:
            sections.append(f"TLDR: {tldr}")
    elif source_type == "title_abstract_conclusion":
        if title:
            sections.append(f"Title: {title}")
        if abstract:
            sections.append(f"Abstract: {abstract}")
        if conclusion:
            sections.append(f"Conclusion: {conclusion}")
    elif source_type == "tldr_only":
        if tldr:
            sections.append(f"TLDR: {tldr}")
    else:
        raise ValueError(f"Unknown source_type: {source_type}")

    return "\n\n".join(section for section in sections if section).strip()


def _metadata_richness(paper: PaperRecord) -> tuple[int, int]:
    score = 0
    if _normalize_text(paper.tldr):
        score += 1
    if _normalize_text(paper.conclusion):
        score += 1
    score += min(5, len(paper.resolved_citation_ids))
    return score, len(_normalize_text(paper.abstract))


def _fetch_papers(database: str = DATABASE) -> list[PaperRecord]:
    table = qualify_table("SILVER_PAPERS", database=database)
    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    try:
        col_map = _resolve_table_columns(cur, table)
        cols = _require_columns(col_map, ["id", "title", "abstract"], table)
        tldr_col = col_map.get("tldr")
        conclusion_col = col_map.get("conclusion")
        references_col = col_map.get("reference_list")
        citations_col = col_map.get("citation_list")
        citation_expr = "NULL"
        if references_col and citations_col:
            citation_expr = f"COALESCE({references_col}, {citations_col})"
        elif references_col or citations_col:
            citation_expr = str(references_col or citations_col)

        cur.execute(
            f"""
            SELECT
                {cols["id"]} AS id,
                {cols["title"]} AS title,
                {cols["abstract"]} AS abstract,
                {tldr_col or 'NULL'} AS tldr,
                {conclusion_col or 'NULL'} AS conclusion,
                {citation_expr} AS citation_payload
            FROM {table}
            WHERE {cols["title"]} IS NOT NULL OR {cols["abstract"]} IS NOT NULL
            """
        )
        rows = cur.fetchall()
        papers: list[PaperRecord] = []
        for row in rows:
            citation_payload = _parse_json_list(row[5])
            if citation_payload:
                resolved = sorted(
                    {
                        int(pid)
                        for pid in citation_targets(cur, citation_payload, database=database)
                        if int(pid) != int(row[0])
                    }
                )
            else:
                resolved = []
            papers.append(
                PaperRecord(
                    paper_id=int(row[0]),
                    title=_normalize_text(row[1]),
                    abstract=_normalize_text(row[2]),
                    tldr=_normalize_text(row[3]),
                    conclusion=_normalize_text(row[4]),
                    citation_payload=citation_payload,
                    resolved_citation_ids=resolved,
                )
            )
        return papers
    finally:
        cur.close()
        conn.close()


def select_anchor_papers(
    papers: Sequence[PaperRecord],
    anchor_count: int = DEFAULT_ANCHOR_COUNT,
    seed: int = 13,
) -> list[PaperRecord]:
    """Select anchors deterministically while preferring richer metadata."""
    eligible = [
        paper
        for paper in papers
        if paper.title and paper.abstract and paper.resolved_citation_ids
    ]
    rng = random.Random(seed)
    decorated = []
    for paper in eligible:
        decorated.append((rng.random(), _metadata_richness(paper), paper.paper_id, paper))
    decorated.sort(key=lambda item: (-item[1][0], -item[1][1], item[0], item[2]))
    return [item[3] for item in decorated[: max(1, int(anchor_count))]]


def _normalize_vector(vector: Sequence[float]) -> list[float]:
    norm = math.sqrt(sum(float(x) * float(x) for x in vector))
    if norm == 0:
        return [0.0 for _ in vector]
    return [float(x) / norm for x in vector]


def _embed_with_sentence_transformers(
    texts: Sequence[str],
    model_name: str,
    batch_size: int,
) -> list[list[float]]:
    sentence_transformers = importlib.import_module("sentence_transformers")
    SentenceTransformer = sentence_transformers.SentenceTransformer
    model = SentenceTransformer(MODEL_NAME_MAP.get(model_name, model_name))
    vectors = model.encode(
        list(texts),
        batch_size=int(batch_size),
        show_progress_bar=False,
        normalize_embeddings=True,
    )
    output: list[list[float]] = []
    for vec in vectors:
        if hasattr(vec, "tolist"):
            output.append([float(x) for x in vec.tolist()])
        else:
            output.append([float(x) for x in vec])
    return output


def _embed_with_openai(
    texts: Sequence[str],
    model_name: str,
    batch_size: int,
) -> list[list[float]]:
    import httpx

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is required for OpenAI embedding models.")

    headers = {"Authorization": f"Bearer {api_key}"}
    vectors: list[list[float]] = []
    with httpx.Client(timeout=60.0, headers=headers) as client:
        for start in range(0, len(texts), max(1, int(batch_size))):
            batch = list(texts[start : start + max(1, int(batch_size))])
            response = client.post(
                OPENAI_EMBEDDING_URL,
                json={"model": model_name, "input": batch},
            )
            response.raise_for_status()
            payload = response.json()
            data = sorted(payload.get("data", []), key=lambda item: item["index"])
            for item in data:
                vectors.append(_normalize_vector(item["embedding"]))
    return vectors


def embed_texts(
    texts: Sequence[str],
    model_name: str,
    batch_size: int = 16,
) -> list[list[float]]:
    """Embed texts with the selected experiment model."""
    if not texts:
        return []
    if model_name in OPENAI_EMBEDDING_MODELS:
        return _embed_with_openai(texts=texts, model_name=model_name, batch_size=batch_size)
    return _embed_with_sentence_transformers(texts=texts, model_name=model_name, batch_size=batch_size)


def _cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    return float(sum(float(a) * float(b) for a, b in zip(vec_a, vec_b)))


def _mean(values: Iterable[float]) -> float:
    values = list(values)
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def compute_anchor_metrics(retrieved_ids: Sequence[int], relevant_ids: Sequence[int], k: int) -> dict[str, float]:
    """Compute hits, recall, and reciprocal rank for one anchor."""
    relevant = {int(pid) for pid in relevant_ids}
    top_ids = [int(pid) for pid in retrieved_ids[: int(k)]]
    hit_count = sum(1 for pid in top_ids if pid in relevant)
    first_rank = next((idx for idx, pid in enumerate(retrieved_ids, start=1) if pid in relevant), None)
    return {
        "hits": 1.0 if hit_count > 0 else 0.0,
        "recall": float(hit_count / len(relevant)) if relevant else 0.0,
        "mrr": float(1.0 / first_rank) if first_rank else 0.0,
        "hit_count": float(hit_count),
        "relevant_count": float(len(relevant)),
        "first_relevant_rank": float(first_rank or 0),
    }


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def run_kg_evaluation(
    database: str = DATABASE,
    anchor_count: int = DEFAULT_ANCHOR_COUNT,
    seed: int = 13,
    top_k_values: Sequence[int] = DEFAULT_TOP_K_VALUES,
    source_types: Sequence[str] = DEFAULT_SOURCE_TYPES,
    embedding_models: Sequence[str] = DEFAULT_EMBEDDING_MODELS,
    output_dir: Union[str, Path] = DEFAULT_OUTPUT_DIR,
    batch_size: int = 16,
    reuse_cache: bool = False,
) -> dict[str, Any]:
    """Run KG retrieval evaluation and write CSV/JSON outputs."""
    output_path = Path(output_dir)
    intermediate_dir = output_path / "intermediate"
    intermediate_dir.mkdir(parents=True, exist_ok=True)

    random.seed(seed)
    papers = _fetch_papers(database=database)
    anchors = select_anchor_papers(papers=papers, anchor_count=anchor_count, seed=seed)

    anchor_cache = intermediate_dir / "anchor_selection.json"
    anchor_cache.write_text(
        json.dumps(
            {
                "database": database,
                "seed": seed,
                "anchor_count": len(anchors),
                "anchor_ids": [paper.paper_id for paper in anchors],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    papers_by_id = {paper.paper_id: paper for paper in papers}
    summary_rows: list[dict[str, Any]] = []
    per_anchor_rows: list[dict[str, Any]] = []
    bridge_rows: list[dict[str, Any]] = []
    detailed_results: dict[str, Any] = {
        "metadata": {
            "database": database,
            "seed": seed,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "anchor_count_requested": anchor_count,
            "anchor_count_selected": len(anchors),
        },
        "configurations": [],
    }

    max_requested_k = max(int(k) for k in top_k_values)
    total_progress_units = len(source_types) * len(embedding_models) * max(1, len(anchors))
    progress = tqdm(total=total_progress_units, desc="KG eval", unit="anchor")

    try:
        for source_type in source_types:
            paper_texts: list[str] = []
            paper_ids: list[int] = []
            for paper in papers:
                text = _source_text_for_paper(paper, source_type)
                if text:
                    paper_ids.append(paper.paper_id)
                    paper_texts.append(text)

            if not paper_ids:
                logger.warning("Skipping source_type=%s because no usable papers were found.", source_type)
                continue

            for embedding_model in embedding_models:
                progress.set_description(f"KG eval [{source_type} | {embedding_model}]")
                cache_file = intermediate_dir / f"embeddings_{source_type}_{embedding_model.replace('/', '_')}.json"
                embeddings_payload: dict[str, Any]
                if reuse_cache and cache_file.exists():
                    embeddings_payload = json.loads(cache_file.read_text(encoding="utf-8"))
                else:
                    vectors = embed_texts(texts=paper_texts, model_name=embedding_model, batch_size=batch_size)
                    embeddings_payload = {
                        "source_type": source_type,
                        "embedding_model": embedding_model,
                        "paper_ids": paper_ids,
                        "vectors": vectors,
                    }
                    cache_file.write_text(json.dumps(embeddings_payload), encoding="utf-8")

                candidate_vectors = {
                    int(pid): [float(x) for x in vec]
                    for pid, vec in zip(embeddings_payload["paper_ids"], embeddings_payload["vectors"])
                }

                anchor_details: list[dict[str, Any]] = []
                for anchor in anchors:
                    anchor_vector = candidate_vectors.get(anchor.paper_id)
                    if anchor_vector is None:
                        progress.update(1)
                        continue

                    ranked = []
                    for candidate_id, vector in candidate_vectors.items():
                        if candidate_id == anchor.paper_id:
                            continue
                        ranked.append(
                            {
                                "paper_id": candidate_id,
                                "title": papers_by_id[candidate_id].title if candidate_id in papers_by_id else "",
                                "score": _cosine_similarity(anchor_vector, vector),
                            }
                        )
                    ranked.sort(key=lambda item: item["score"], reverse=True)
                    ranked_ids = [int(item["paper_id"]) for item in ranked]

                    anchor_result = {
                        "anchor_paper_id": anchor.paper_id,
                        "anchor_title": anchor.title,
                        "relevant_citation_ids": anchor.resolved_citation_ids,
                        "retrieved": ranked[:max_requested_k],
                        "metrics": {},
                    }

                    for k in top_k_values:
                        metrics = compute_anchor_metrics(
                            retrieved_ids=ranked_ids,
                            relevant_ids=anchor.resolved_citation_ids,
                            k=int(k),
                        )
                        anchor_result["metrics"][str(k)] = metrics
                        per_anchor_rows.append(
                            {
                                "source_type": source_type,
                                "embedding_model": embedding_model,
                                "k": int(k),
                                "anchor_paper_id": anchor.paper_id,
                                "anchor_title": anchor.title,
                                "relevant_citation_count": len(anchor.resolved_citation_ids),
                                "hits": metrics["hits"],
                                "recall": metrics["recall"],
                                "mrr": metrics["mrr"],
                                "first_relevant_rank": int(metrics["first_relevant_rank"]),
                                "retrieved_ids": json.dumps(ranked_ids[: int(k)]),
                            }
                        )

                    for rank, candidate in enumerate(ranked[:max_requested_k], start=1):
                        bridge_rows.append(
                            {
                                "source_type": source_type,
                                "embedding_model": embedding_model,
                                "anchor_paper_id": anchor.paper_id,
                                "anchor_title": anchor.title,
                                "candidate_paper_id": candidate["paper_id"],
                                "candidate_title": candidate["title"],
                                "similarity_score": candidate["score"],
                                "rank": rank,
                                "is_cited": candidate["paper_id"] in set(anchor.resolved_citation_ids),
                            }
                        )

                    anchor_details.append(anchor_result)
                    progress.set_postfix(anchor_id=anchor.paper_id, evaluated=len(anchor_details), refresh=False)
                    progress.update(1)

                for k in top_k_values:
                    k_metrics = [
                        detail["metrics"][str(int(k))]
                        for detail in anchor_details
                        if str(int(k)) in detail["metrics"]
                    ]
                    if not k_metrics:
                        continue
                    summary_rows.append(
                        {
                            "source_type": source_type,
                            "embedding_model": embedding_model,
                            "k": int(k),
                            "recall": _mean(item["recall"] for item in k_metrics),
                            "hits": _mean(item["hits"] for item in k_metrics),
                            "mrr": _mean(item["mrr"] for item in k_metrics),
                            "anchor_count": len(k_metrics),
                        }
                    )

                detailed_results["configurations"].append(
                    {
                        "source_type": source_type,
                        "embedding_model": embedding_model,
                        "paper_count": len(candidate_vectors),
                        "anchors_evaluated": len(anchor_details),
                        "anchors": anchor_details,
                    }
                )
    finally:
        progress.close()

    _write_csv(
        output_path / "kg_auto_summary.csv",
        ["source_type", "embedding_model", "k", "recall", "hits", "mrr", "anchor_count"],
        summary_rows,
    )
    _write_csv(
        output_path / "kg_auto_per_anchor.csv",
        [
            "source_type",
            "embedding_model",
            "k",
            "anchor_paper_id",
            "anchor_title",
            "relevant_citation_count",
            "hits",
            "recall",
            "mrr",
            "first_relevant_rank",
            "retrieved_ids",
        ],
        per_anchor_rows,
    )
    _write_csv(
        output_path.parent / "kg_bridge_candidates.csv",
        [
            "source_type",
            "embedding_model",
            "anchor_paper_id",
            "anchor_title",
            "candidate_paper_id",
            "candidate_title",
            "similarity_score",
            "rank",
            "is_cited",
        ],
        bridge_rows,
    )
    (output_path / "kg_detailed_results.json").write_text(
        json.dumps(detailed_results, indent=2, default=_json_default),
        encoding="utf-8",
    )
    return {
        "summary_path": str(output_path / "kg_auto_summary.csv"),
        "per_anchor_path": str(output_path / "kg_auto_per_anchor.csv"),
        "details_path": str(output_path / "kg_detailed_results.json"),
        "bridge_candidates_path": str(output_path.parent / "kg_bridge_candidates.csv"),
        "configurations": len(detailed_results["configurations"]),
    }
