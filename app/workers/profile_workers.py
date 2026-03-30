"""
Profiling script for the computationally significant pure-Python functions
in the MindMap pipeline using cProfile + pstats.

Profiles ONLY the real functions from the codebase — no optimized variants.
Public @app.function workers all require Snowflake/GPU/LLM I/O and cannot be
profiled in isolation; this script covers every pure-Python helper they call.

Functions profiled:
  embedding_worker:
    1. _build_embedding_text

  chunking_worker:
    2. _split_into_chunks
    3. _build_sections_for_paper

  graph_worker:
    4. _dedupe_edges
    5. _normalize_json_list
    6. _normalize_ids

  summary_worker:
    7. _estimate_token_count
    8. _format_history (builds the conversation string passed to the LLM)

  qa_worker:
    9. _looks_ambiguous  (decides whether to rewrite a follow-up question)
   10. _looks_unrelated  (guards against off-topic questions)

Run on Modal:
    modal run app/workers/profile_workers.py

Run locally (no Snowflake needed — all synthetic data):
    cd app && python workers/profile_workers.py
"""

import cProfile
import pstats
import io
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Any, Tuple

# ── path setup ────────────────────────────────────────────────────────────────
APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from workers.chunking_worker import _split_into_chunks, _build_sections_for_paper
from workers.embedding_worker import _build_embedding_text
from workers.graph_worker import _dedupe_edges, _normalize_json_list, _normalize_ids
from workers.summary_worker import _estimate_token_count

# Copied verbatim from qa_worker — importing that module fails locally because
# it pulls in config.SCHEMA and rag_image which require Modal secrets at import time.
_QA_MAX_HISTORY = 6
_QA_UNRELATED_KEYWORDS = {
    "weather", "restaurant", "movie", "stock", "sports",
    "recipe", "vacation", "dating", "politics", "bitcoin",
}

def _format_history(history: List[Dict[str, Any]]) -> str:
    lines = []
    for item in history[-_QA_MAX_HISTORY:]:
        lines.append(f"{item['role'].upper()}: {item['message']}")
    return "\n".join(lines)

def _looks_ambiguous(question: str) -> bool:
    lowered = (question or "").strip().lower()
    if not lowered:
        return False
    tokens = lowered.split()
    pronouns = {"it", "they", "this", "that", "these", "those", "he", "she"}
    return len(tokens) <= 8 or any(token in pronouns for token in tokens)

def _looks_unrelated(question: str) -> bool:
    lowered = (question or "").lower()
    return any(keyword in lowered for keyword in _QA_UNRELATED_KEYWORDS)

import modal

_profile_app = modal.App("mindmap-profiling")
_profile_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install("snowflake-connector-python", "openai")
    .add_local_file(APP_ROOT / "config.py",  remote_path="/root/config.py",  copy=True)
    .add_local_file(APP_ROOT / "utils.py",   remote_path="/root/utils.py",   copy=True)
    .add_local_dir(APP_ROOT / "services",    remote_path="/root/services",   copy=True)
    .add_local_dir(APP_ROOT / "workers",     remote_path="/root/workers",    copy=True)
    .env({"PYTHONPATH": "/root"})
)

# ═══════════════════════════════════════════════════════════════════════════════
# SYNTHETIC DATA
# ═══════════════════════════════════════════════════════════════════════════════

def _make_paper() -> Dict[str, Any]:
    """
    Realistic paper dict that exercises all branches in the chunking/embedding
    workers: full_text with labelled sections + abstract + conclusion.
    """
    word = "research "
    abstract   = (word * 150).strip()
    conclusion = (word * 100).strip()
    full_text  = (
        "Abstract\n"     + abstract               + "\n\n"
        "Introduction\n" + (word * 400).strip()   + "\n\n"
        "Methods\n"      + (word * 600).strip()   + "\n\n"
        "Results\n"      + (word * 500).strip()   + "\n\n"
        "Discussion\n"   + (word * 400).strip()   + "\n\n"
        "Conclusion\n"   + conclusion
    )
    return {
        "id": 1,
        "arxiv_id": "2401.00001",
        "title": "Deep Learning for Knowledge Graph Construction",
        "abstract": abstract,
        "conclusion": conclusion,
        "full_text": full_text,
    }


def _make_edges(n: int = 5000) -> List[Tuple]:
    """
    5 000 edges cycling through 200 node IDs — produces many duplicates so
    _dedupe_edges has real deduplication work to do, matching production load.
    """
    rels = ["CITES", "SIMILAR", "SUPPORT", "CONTRADICT", "NEUTRAL"]
    return [
        (i % 200, (i + 1) % 200, rels[i % 5], round(1.0 - (i % 10) * 0.1, 1), None)
        for i in range(n)
    ]


def _make_json_pairs(n: int = 100) -> List[Tuple[str, str]]:
    """
    Simulate the (citations_json, similar_ids_json) pairs returned by
    _fetch_papers from Snowflake. Each paper has 5 citation dicts and 10
    similar IDs — same shape as real data.
    """
    pairs = []
    for i in range(n):
        citations = json.dumps([
            {"ss_paper_id": f"ss_{(i + j) % n}"} for j in range(1, 6)
        ])
        similar_ids = json.dumps([(i + j) % n for j in range(1, 11)])
        pairs.append((citations, similar_ids))
    return pairs


def _make_chunk_text(n_words: int = 500) -> str:
    """Realistic chunk text used by _estimate_token_count."""
    return ("neural network transformer attention mechanism " * (n_words // 5)).strip()


def _make_conversation_history(n_turns: int = 6) -> List[Dict[str, Any]]:
    """
    Simulate a multi-turn QA session history as stored in APP_QA_LOGS and
    returned by _load_history. Each turn has a user question and assistant
    answer, matching the dict shape _format_history expects.
    """
    history = []
    for i in range(n_turns):
        history.append({
            "role": "user",
            "message": f"What does the paper say about method {i}?",
            "rewritten_query": None,
            "cited_chunk_ids": [],
        })
        history.append({
            "role": "assistant",
            "message": f"The paper describes method {i} as a novel approach to knowledge graph construction.",
            "rewritten_query": None,
            "cited_chunk_ids": [i * 2, i * 2 + 1],
        })
    return history


def _make_questions() -> List[str]:
    """
    Mix of ambiguous follow-ups, unrelated questions, and clear questions —
    exercises all branches of _looks_ambiguous and _looks_unrelated.
    """
    return [
        # ambiguous follow-ups (short + pronouns → should trigger rewrite)
        "What does it say?",
        "How does this work?",
        "Can you explain that?",
        "What are they?",
        # unrelated (should be refused)
        "What's the weather today?",
        "Recommend a restaurant near me.",
        "What's the bitcoin price?",
        # clear, on-topic questions (should pass through)
        "What is the main contribution of this paper?",
        "How does the proposed method compare to baselines?",
        "What datasets were used in the experiments?",
        "What are the limitations of this approach?",
        "What future work do the authors suggest?",
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# PROFILING HARNESS
# ═══════════════════════════════════════════════════════════════════════════════

def _profile(label: str, fn, top_n: int = 10):
    """Run fn() under cProfile and print cumulative stats + wall time."""
    print(f"\n{'#' * 70}")
    print(f"  FUNCTION: {label}")
    print(f"{'#' * 70}")

    t0 = time.perf_counter()
    profiler = cProfile.Profile()
    profiler.enable()
    fn()
    profiler.disable()
    elapsed = time.perf_counter() - t0

    s = io.StringIO()
    pstats.Stats(profiler, stream=s).sort_stats("cumulative").print_stats(top_n)
    print(s.getvalue())
    print(f"  Wall time: {elapsed:.4f}s")


# ═══════════════════════════════════════════════════════════════════════════════
# MODAL FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════

@_profile_app.function(image=_profile_image, timeout=60 * 10)
def run_profiling():
    paper      = _make_paper()
    edges      = _make_edges(5_000)
    long_text  = " ".join(["word"] * 5_000)
    json_pairs = _make_json_pairs(100)
    chunk_text = _make_chunk_text(500)
    history    = _make_conversation_history(6)
    questions  = _make_questions()

    # ── embedding_worker ──────────────────────────────────────────────────────
    # Called once per paper in run_embedding_batch.
    # 10 000 calls for stable timing (function is very fast).
    _profile(
        "_build_embedding_text  (×10 000 calls)",
        lambda: [_build_embedding_text(paper) for _ in range(10_000)],
    )

    # ── chunking_worker ───────────────────────────────────────────────────────
    # _split_into_chunks: called once per section per paper in chunk_papers.
    # 500 calls on a 5 000-word input (realistic large section).
    _profile(
        "_split_into_chunks  (5 000-word input, ×500 calls)",
        lambda: [_split_into_chunks(long_text, target_words=500) for _ in range(500)],
    )

    # _build_sections_for_paper: called once per paper in chunk_papers.
    # 200 calls on a full paper with abstract + full_text + conclusion.
    _profile(
        "_build_sections_for_paper  (×200 calls, full paper with all fields)",
        lambda: [_build_sections_for_paper(paper) for _ in range(200)],
    )

    # ── graph_worker ──────────────────────────────────────────────────────────
    # _dedupe_edges: called once per build_knowledge_graph run over all edges.
    # 1 000 calls on 5 000 edges for stable timing.
    _profile(
        "_dedupe_edges  (5 000 edges, ×1 000 calls)",
        lambda: [_dedupe_edges(edges) for _ in range(1_000)],
    )

    # _normalize_json_list: called twice per paper in build_knowledge_graph
    # (citations + similar_ids). 100 papers × 500 iterations.
    _profile(
        "_normalize_json_list  (citations + similar_ids, 100 papers, ×500 iterations)",
        lambda: [
            (_normalize_json_list(cit), _normalize_json_list(sim))
            for _ in range(500)
            for cit, sim in json_pairs
        ],
    )

    # _normalize_ids: called on similar_ids in build_knowledge_graph.
    # 100 papers × 500 iterations.
    _profile(
        "_normalize_ids  (similar_ids, 100 papers, ×500 iterations)",
        lambda: [
            _normalize_ids(sim)
            for _ in range(500)
            for _, sim in json_pairs
        ],
    )

    # ── summary_worker ────────────────────────────────────────────────────────
    # _estimate_token_count: called once per paper in generate_paper_summary
    # to estimate context size before the LLM call.
    # 100 000 calls on a 500-word chunk for stable timing.
    _profile(
        "_estimate_token_count  (500-word chunk, ×100 000 calls)",
        lambda: [_estimate_token_count(chunk_text) for _ in range(100_000)],
    )

    # _format_history: called once per QA turn in answer_paper_question to
    # build the conversation string injected into the LLM prompt.
    # 50 000 calls on a 6-turn (12-message) history.
    _profile(
        "_format_history  (6-turn history, ×50 000 calls)",
        lambda: [_format_history(history) for _ in range(50_000)],
    )

    # ── qa_worker ─────────────────────────────────────────────────────────────
    # _looks_ambiguous: called once per question in answer_paper_question to
    # decide whether to rewrite the query. Mix of ambiguous + clear questions.
    # 12 questions × 50 000 iterations.
    _profile(
        "_looks_ambiguous  (12 questions, ×50 000 iterations)",
        lambda: [
            _looks_ambiguous(q)
            for _ in range(50_000)
            for q in questions
        ],
    )

    # _looks_unrelated: called once per question before any DB work.
    # Same question set, same iteration count.
    _profile(
        "_looks_unrelated  (12 questions, ×50 000 iterations)",
        lambda: [
            _looks_unrelated(q)
            for _ in range(50_000)
            for q in questions
        ],
    )

    print("\n\nAll profiling runs complete.")


@_profile_app.local_entrypoint()
def main():
    run_profiling.remote()
