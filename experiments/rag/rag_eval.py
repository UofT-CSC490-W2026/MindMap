"""RAG and LLM experiment pipeline."""

from __future__ import annotations

import csv
import importlib
import json
import logging
import random
import re
from collections import defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Union

from app.services.llm_client import LLMClient
from app.services.prompt_templates import format_chunk_context

from experiments.common import (
    DATABASE,
    connect_to_snowflake,
    qualify_table,
    retrieve_similar_chunks_local,
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

DEFAULT_OUTPUT_DIR = Path("experiments/results/rag")
DEFAULT_METHODS_STAGE1 = ["abstract_only", "llm_only", "rag_llm"]
DEFAULT_MODELS_STAGE2 = ["gpt-4o-mini", "gpt-4.1-mini", "gpt-4.1"]
DEFAULT_STAGE1_MODEL = "gpt-4o-mini"
SIMILARITY_MODEL = "sentence-transformers/all-MiniLM-L12-v2"
_GLOBAL_TEXT_EMBEDDING_CACHE: dict[str, list[float]] = {}


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


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    return float(sum(float(a) * float(b) for a, b in zip(vec_a, vec_b)))


def _mean(values: Iterable[float]) -> float:
    values = list(values)
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _split_sentences(text: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+", _normalize_text(text)) if part.strip()]


@lru_cache(maxsize=1)
def _get_similarity_model():
    """Load the sentence-transformer model once per process."""
    sentence_transformers = importlib.import_module("sentence_transformers")
    SentenceTransformer = sentence_transformers.SentenceTransformer
    return SentenceTransformer(SIMILARITY_MODEL)


def _embed_texts(texts: Sequence[str], batch_size: int = 16) -> list[list[float]]:
    if not texts:
        return []
    model = _get_similarity_model()
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


def _embed_texts_cached(
    texts: Sequence[str],
    batch_size: int = 16,
    embedding_cache: Optional[dict[str, list[float]]] = None,
) -> list[list[float]]:
    """Embed texts while reusing previously computed vectors."""
    cache = embedding_cache if embedding_cache is not None else _GLOBAL_TEXT_EMBEDDING_CACHE
    normalized_texts = [_normalize_text(text) for text in texts]

    missing_texts = []
    for text in normalized_texts:
        if text and text not in cache:
            missing_texts.append(text)

    if missing_texts:
        vectors = _embed_texts(missing_texts, batch_size=batch_size)
        for text, vector in zip(missing_texts, vectors):
            cache[text] = vector

    return [cache.get(text, [0.0]) for text in normalized_texts]


def compute_similarity_metrics(
    answer: str,
    chunk_texts: Sequence[str],
    support_threshold: float = 0.6,
    batch_size: int = 16,
    embedding_cache: Optional[dict[str, list[float]]] = None,
) -> dict[str, float]:
    """Compute answer-level similarity and sentence support."""
    chunk_texts = [_normalize_text(text) for text in chunk_texts if _normalize_text(text)]
    if not chunk_texts:
        return {"similarity_score": 0.0, "mean_chunk_similarity": 0.0, "support_rate": 0.0}

    answer_text = _normalize_text(answer)
    if not answer_text:
        return {"similarity_score": 0.0, "mean_chunk_similarity": 0.0, "support_rate": 0.0}

    answer_vector = _embed_texts_cached([answer_text], batch_size=batch_size, embedding_cache=embedding_cache)[0]
    chunk_vectors = _embed_texts_cached(chunk_texts, batch_size=batch_size, embedding_cache=embedding_cache)
    similarities = [_cosine_similarity(answer_vector, chunk_vector) for chunk_vector in chunk_vectors]

    sentences = _split_sentences(answer_text) or [answer_text]
    sentence_vectors = _embed_texts_cached(sentences, batch_size=batch_size, embedding_cache=embedding_cache)
    supported = 0
    for sentence_vector in sentence_vectors:
        max_sentence_similarity = max(_cosine_similarity(sentence_vector, chunk_vector) for chunk_vector in chunk_vectors)
        if max_sentence_similarity >= float(support_threshold):
            supported += 1

    return {
        "similarity_score": max(similarities) if similarities else 0.0,
        "mean_chunk_similarity": _mean(similarities),
        "support_rate": float(supported / len(sentences)) if sentences else 0.0,
    }


def compute_stability(
    answers: Sequence[str],
    batch_size: int = 16,
    embedding_cache: Optional[dict[str, list[float]]] = None,
) -> float:
    """Compute average pairwise answer similarity across repeated runs."""
    normalized = [_normalize_text(answer) for answer in answers]
    if len(normalized) <= 1:
        return 1.0
    vectors = _embed_texts_cached(normalized, batch_size=batch_size, embedding_cache=embedding_cache)
    pairwise = []
    for idx in range(len(vectors)):
        for jdx in range(idx + 1, len(vectors)):
            pairwise.append(_cosine_similarity(vectors[idx], vectors[jdx]))
    return _mean(pairwise) if pairwise else 1.0


def _read_questions_csv(path: Union[str, Path], max_questions: Optional[int] = None) -> list[dict[str, str]]:
    with Path(path).open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"paper_id", "question_id", "question_text"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Question CSV missing required columns: {sorted(missing)}")
        rows = [row for row in reader if _normalize_text(row.get("paper_id"))]
    if max_questions is not None:
        return rows[: max(0, int(max_questions))]
    return rows


def _fetch_paper_lookup(paper_ids: Sequence[int], database: str = DATABASE) -> dict[int, dict[str, str]]:
    if not paper_ids:
        return {}
    table = qualify_table("SILVER_PAPERS", database=database)
    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    try:
        col_map = _resolve_table_columns(cur, table)
        cols = _require_columns(col_map, ["id", "title", "abstract"], table)
        values_sql = ", ".join(["(%s)"] * len(paper_ids))
        cur.execute(
            f"""
            WITH source_ids(id) AS (SELECT column1 FROM VALUES {values_sql})
            SELECT p.{cols["id"]}, p.{cols["title"]}, p.{cols["abstract"]}
            FROM source_ids s
            JOIN {table} p
              ON p.{cols["id"]} = s.id
            """,
            [int(pid) for pid in paper_ids],
        )
        return {
            int(row[0]): {
                "title": _normalize_text(row[1]),
                "abstract": _normalize_text(row[2]),
            }
            for row in cur.fetchall()
        }
    finally:
        cur.close()
        conn.close()


def _build_llm_only_prompt(question: str, title: str, abstract: str) -> str:
    return (
        "You are answering a question about a single research paper.\n\n"
        "Use only the title and abstract below. If the answer is not supported, say the "
        "information is not available in the provided abstract.\n\n"
        f"TITLE:\n{title}\n\n"
        f"ABSTRACT:\n{abstract}\n\n"
        f"QUESTION:\n{question}\n\n"
        "Answer in plain text."
    )


def _build_rag_prompt(question: str, title: str, context: str) -> str:
    return (
        "You are answering a question about a single research paper.\n\n"
        "Use only the retrieved chunks below. If the answer is not supported, say the "
        "information is not available in the provided paper context.\n\n"
        f"TITLE:\n{title}\n\n"
        f"QUESTION:\n{question}\n\n"
        f"RETRIEVED CHUNKS:\n{context}\n\n"
        "Answer in plain text."
    )


def _generate_llm_answer(prompt: str, model_name: str, max_tokens: int = 600) -> dict[str, Any]:
    llm = LLMClient(model=model_name, max_tokens=max_tokens)
    return llm.generate_text(prompt=prompt, retry_count=1, temperature=0.2, max_tokens=max_tokens)


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def run_rag_evaluation(
    database: str = DATABASE,
    questions_file: Union[str, Path] = "experiments/config/rag_questions.csv",
    seed: int = 13,
    top_k: int = 5,
    support_threshold: float = 0.6,
    runs_per_question: int = 3,
    stage: str = "all",
    models: Optional[Sequence[str]] = None,
    methods: Optional[Sequence[str]] = None,
    output_dir: Union[str, Path] = DEFAULT_OUTPUT_DIR,
    batch_size: int = 16,
    reuse_cache: bool = False,
    max_questions: Optional[int] = None,
) -> dict[str, Any]:
    """Run Stage 1 and/or Stage 2 RAG evaluations."""
    random.seed(seed)
    questions = _read_questions_csv(questions_file, max_questions=max_questions)
    paper_lookup = _fetch_paper_lookup(sorted({int(row["paper_id"]) for row in questions}), database=database)

    output_path = Path(output_dir)
    intermediate_dir = output_path / "intermediate"
    intermediate_dir.mkdir(parents=True, exist_ok=True)
    embedding_cache: dict[str, list[float]] = {}

    per_question_rows: list[dict[str, Any]] = []
    human_eval_rows: list[dict[str, Any]] = []
    outputs_json: dict[str, Any] = {
        "metadata": {
            "database": database,
            "seed": seed,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "top_k": top_k,
            "support_threshold": support_threshold,
            "runs_per_question": runs_per_question,
            "reuse_cache": reuse_cache,
            "max_questions": max_questions,
        },
        "records": [],
    }

    stage1_methods = list(methods or DEFAULT_METHODS_STAGE1)
    stage2_models = list(models or DEFAULT_MODELS_STAGE2)
    retrieval_cache: dict[tuple[int, str], list[dict[str, Any]]] = {}
    stage1_summary_inputs: list[dict[str, Any]] = []
    stage2_summary_inputs: list[dict[str, Any]] = []
    total_progress_units = 0
    if stage in {"all", "stage1"}:
        total_progress_units += len(questions) * len(stage1_methods)
    if stage in {"all", "stage2"}:
        total_progress_units += len(questions) * len(stage2_models)
    progress = tqdm(total=total_progress_units, desc="RAG eval", unit="item")

    try:
        for question_row in questions:
            paper_id = int(question_row["paper_id"])
            question_id = question_row["question_id"]
            question_text = _normalize_text(question_row["question_text"])
            paper = paper_lookup.get(paper_id, {"title": "", "abstract": ""})
            title = paper.get("title", "")
            abstract = paper.get("abstract", "")

            if stage in {"all", "stage1"}:
                for method in stage1_methods:
                    progress.set_description(f"RAG eval [stage1:{method}]")
                    cache_key = (paper_id, question_text)
                    chunk_rows: list[dict[str, Any]] = []
                    if method == "rag_llm":
                        if cache_key not in retrieval_cache:
                            retrieval_cache[cache_key] = retrieve_similar_chunks_local(
                                query_vector=_embed_texts_cached(
                                    [question_text],
                                    batch_size=batch_size,
                                    embedding_cache=embedding_cache,
                                )[0],
                                top_k=top_k,
                                paper_id=paper_id,
                                score_threshold=0.05,
                                database=database,
                            )
                        chunk_rows = retrieval_cache[cache_key]
                    answers: list[str] = []
                    usage: list[dict[str, Any]] = []
                    chunk_texts = [row.get("chunk_text", "") for row in chunk_rows]
                    for run_index in range(max(1, int(runs_per_question))):
                        if method == "abstract_only":
                            answer = abstract
                            usage.append({})
                        elif method == "llm_only":
                            response = _generate_llm_answer(
                                prompt=_build_llm_only_prompt(question_text, title, abstract),
                                model_name=DEFAULT_STAGE1_MODEL,
                            )
                            answer = response["text"]
                            usage.append(response.get("usage", {}))
                        elif method == "rag_llm":
                            response = _generate_llm_answer(
                                prompt=_build_rag_prompt(
                                    question_text,
                                    title,
                                    format_chunk_context(chunk_rows),
                                ),
                                model_name=DEFAULT_STAGE1_MODEL,
                            )
                            answer = response["text"]
                            usage.append(response.get("usage", {}))
                        else:
                            raise ValueError(f"Unknown method: {method}")
                        answers.append(answer)

                    primary_answer = answers[0] if answers else ""
                    metrics = compute_similarity_metrics(
                        answer=primary_answer,
                        chunk_texts=chunk_texts or [abstract],
                        support_threshold=support_threshold,
                        batch_size=batch_size,
                        embedding_cache=embedding_cache,
                    )
                    stability = 1.0 if method == "abstract_only" else compute_stability(
                        answers,
                        batch_size=batch_size,
                        embedding_cache=embedding_cache,
                    )
                    row = {
                        "stage": "stage1",
                        "method": method,
                        "model": DEFAULT_STAGE1_MODEL,
                        "paper_id": paper_id,
                        "question_id": question_id,
                        "question_text": question_text,
                        "answer": primary_answer,
                        "similarity_score": metrics["similarity_score"],
                        "support_rate": metrics["support_rate"],
                        "stability": stability,
                        "chunks_used": len(chunk_rows),
                        "run_count": len(answers),
                    }
                    per_question_rows.append(row)
                    stage1_summary_inputs.append(row)
                    human_eval_rows.append(
                        {
                            "paper_id": paper_id,
                            "question": question_text,
                            "method_model": f"{method}:{DEFAULT_STAGE1_MODEL}",
                            "generated_answer": primary_answer,
                        }
                    )
                    outputs_json["records"].append(
                        {
                            **row,
                            "answers": answers,
                            "usage": usage,
                            "chunk_ids": [row.get("chunk_id") for row in chunk_rows],
                        }
                    )
                    progress.set_postfix(question_id=question_id, paper_id=paper_id, refresh=False)
                    progress.update(1)

            if stage in {"all", "stage2"}:
                cache_key = (paper_id, question_text)
                if cache_key not in retrieval_cache:
                    retrieval_cache[cache_key] = retrieve_similar_chunks_local(
                        query_vector=_embed_texts_cached(
                            [question_text],
                            batch_size=batch_size,
                            embedding_cache=embedding_cache,
                        )[0],
                        top_k=top_k,
                        paper_id=paper_id,
                        score_threshold=0.05,
                        database=database,
                    )
                chunk_rows = retrieval_cache[cache_key]
                chunk_texts = [row.get("chunk_text", "") for row in chunk_rows]
                prompt = _build_rag_prompt(question_text, title, format_chunk_context(chunk_rows))

                for model_name in stage2_models:
                    progress.set_description(f"RAG eval [stage2:{model_name}]")
                    answers = []
                    usage = []
                    for _ in range(max(1, int(runs_per_question))):
                        response = _generate_llm_answer(prompt=prompt, model_name=model_name)
                        answers.append(response["text"])
                        usage.append(response.get("usage", {}))
                    primary_answer = answers[0] if answers else ""
                    metrics = compute_similarity_metrics(
                        answer=primary_answer,
                        chunk_texts=chunk_texts or [abstract],
                        support_threshold=support_threshold,
                        batch_size=batch_size,
                        embedding_cache=embedding_cache,
                    )
                    stability = compute_stability(
                        answers,
                        batch_size=batch_size,
                        embedding_cache=embedding_cache,
                    )
                    row = {
                        "stage": "stage2",
                        "method": "rag_llm",
                        "model": model_name,
                        "paper_id": paper_id,
                        "question_id": question_id,
                        "question_text": question_text,
                        "answer": primary_answer,
                        "similarity_score": metrics["similarity_score"],
                        "support_rate": metrics["support_rate"],
                        "stability": stability,
                        "chunks_used": len(chunk_rows),
                        "run_count": len(answers),
                    }
                    per_question_rows.append(row)
                    stage2_summary_inputs.append(row)
                    human_eval_rows.append(
                        {
                            "paper_id": paper_id,
                            "question": question_text,
                            "method_model": f"rag_llm:{model_name}",
                            "generated_answer": primary_answer,
                        }
                    )
                    outputs_json["records"].append(
                        {
                            **row,
                            "answers": answers,
                            "usage": usage,
                            "chunk_ids": [row.get("chunk_id") for row in chunk_rows],
                        }
                    )
                    progress.set_postfix(question_id=question_id, paper_id=paper_id, refresh=False)
                    progress.update(1)
    finally:
        progress.close()

    stage1_summary_rows = []
    for method in sorted({row["method"] for row in stage1_summary_inputs}):
        items = [row for row in stage1_summary_inputs if row["method"] == method]
        if not items:
            continue
        stage1_summary_rows.append(
            {
                "method": method,
                "model": DEFAULT_STAGE1_MODEL,
                "question_count": len(items),
                "similarity_score": _mean(row["similarity_score"] for row in items),
                "support_rate": _mean(row["support_rate"] for row in items),
                "stability": _mean(row["stability"] for row in items),
            }
        )

    stage2_summary_rows = []
    for model_name in sorted({row["model"] for row in stage2_summary_inputs}):
        items = [row for row in stage2_summary_inputs if row["model"] == model_name]
        if not items:
            continue
        stage2_summary_rows.append(
            {
                "method": "rag_llm",
                "model": model_name,
                "question_count": len(items),
                "similarity_score": _mean(row["similarity_score"] for row in items),
                "support_rate": _mean(row["support_rate"] for row in items),
                "stability": _mean(row["stability"] for row in items),
            }
        )

    _write_csv(
        output_path / "rag_auto_summary_stage1.csv",
        ["method", "model", "question_count", "similarity_score", "support_rate", "stability"],
        stage1_summary_rows,
    )
    _write_csv(
        output_path / "rag_auto_summary_stage2.csv",
        ["method", "model", "question_count", "similarity_score", "support_rate", "stability"],
        stage2_summary_rows,
    )
    _write_csv(
        output_path / "rag_per_question.csv",
        [
            "stage",
            "method",
            "model",
            "paper_id",
            "question_id",
            "question_text",
            "answer",
            "similarity_score",
            "support_rate",
            "stability",
            "chunks_used",
            "run_count",
        ],
        per_question_rows,
    )
    _write_csv(
        output_path.parent / "rag_human_eval.csv",
        ["paper_id", "question", "method_model", "generated_answer"],
        human_eval_rows,
    )
    (output_path / "rag_outputs.json").write_text(json.dumps(outputs_json, indent=2), encoding="utf-8")
    (intermediate_dir / "retrieval_cache.json").write_text(
        json.dumps(
            {
                f"{paper_id}:{question}": rows
                for (paper_id, question), rows in retrieval_cache.items()
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "stage1_summary_path": str(output_path / "rag_auto_summary_stage1.csv"),
        "stage2_summary_path": str(output_path / "rag_auto_summary_stage2.csv"),
        "per_question_path": str(output_path / "rag_per_question.csv"),
        "outputs_path": str(output_path / "rag_outputs.json"),
        "human_eval_path": str(output_path.parent / "rag_human_eval.csv"),
        "records": len(outputs_json["records"]),
    }
