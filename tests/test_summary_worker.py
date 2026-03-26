"""
Tests for workers/summary_worker.py

LLMClient is mocked in sys.modules before the module is imported.
summary_schema is imported naturally since it exists and has no heavy deps.
"""

import sys
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Inject stubs before summary_worker is imported
# ---------------------------------------------------------------------------

mock_llm_module = MagicMock()
sys.modules["services.llm_client"] = mock_llm_module

# Mock summary_schema since pydantic may not be installed in test env
mock_schema_module = MagicMock()
mock_schema_module.PaperSummary = MagicMock
mock_schema_module.SummaryContext = MagicMock
sys.modules["services.summary_schema"] = mock_schema_module

if "services" not in sys.modules:
    sys.modules["services"] = MagicMock()

import workers.summary_worker as sw  # noqa: E402
from workers.summary_worker import (  # noqa: E402
    generate_paper_summary,
    batch_summarize_papers,
)


# ---------------------------------------------------------------------------
# generate_paper_summary — summary already exists (skipped)
# ---------------------------------------------------------------------------

def test_generate_paper_summary_already_exists():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        (1,),  # summary exists check returns a row
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
        result = generate_paper_summary(paper_id=1, force=False)

    assert result["status"] == "skipped"


# ---------------------------------------------------------------------------
# generate_paper_summary — paper not found
# ---------------------------------------------------------------------------

def test_generate_paper_summary_paper_not_found():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        None,  # summary check: no existing summary
        None,  # paper lookup: paper not found
    ]
    mock_cursor.fetchall.return_value = []

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
        result = generate_paper_summary(paper_id=999)

    assert result["status"] == "error"
    assert result["error"] == "paper_not_found"


# ---------------------------------------------------------------------------
# generate_paper_summary — no chunks found
# ---------------------------------------------------------------------------

def test_generate_paper_summary_no_chunks():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        None,                          # no existing summary
        (1, "Test Title", "Abstract"), # paper found
    ]
    mock_cursor.fetchall.return_value = []  # no chunks

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
        result = generate_paper_summary(paper_id=1)

    assert result["status"] == "error"
    assert result["error"] == "no_chunks_found"


# ---------------------------------------------------------------------------
# generate_paper_summary — success path
# ---------------------------------------------------------------------------

def test_generate_paper_summary_success():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        None,                          # no existing summary
        (1, "Test Title", "Abstract"), # paper found
    ]
    mock_cursor.fetchall.return_value = [
        (1, "This is chunk text about methods.", "methods", 50),
    ]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Build a mock PaperSummary with all required fields
    mock_summary = MagicMock()
    mock_summary.research_question = "RQ"
    mock_summary.methods = "Methods"
    mock_summary.main_claims = "Claims"
    mock_summary.key_findings = "Findings"
    mock_summary.limitations = "Limitations"
    mock_summary.conclusion = "Conclusion"
    mock_summary.to_dict.return_value = {}

    mock_llm_instance = MagicMock()
    mock_llm_instance.generate_structured_summary.return_value = {
        "result": mock_summary,
        "attempts": 1,
        "usage": {},
    }
    mock_llm_cls = MagicMock(return_value=mock_llm_instance)

    with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.summary_worker.LLMClient", mock_llm_cls):
            result = generate_paper_summary(paper_id=1)

    assert result["status"] == "ok"
    assert "chunks_used" in result


# ---------------------------------------------------------------------------
# batch_summarize_papers — no papers to summarize
# ---------------------------------------------------------------------------

def test_batch_summarize_papers_no_papers():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = [("id",), ("arxiv_id",), ("title",), ("abstract",)]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Patch generate_paper_summary.remote at module level so batch doesn't fail
    original = sw.generate_paper_summary
    sw.generate_paper_summary = MagicMock(return_value={"status": "ok"})

    try:
        with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
            result = batch_summarize_papers(limit=10)
    finally:
        sw.generate_paper_summary = original

    assert result["status"] == "ok"
    assert result["papers_to_summarize"] == 0


# ---------------------------------------------------------------------------
# Additional coverage tests
# ---------------------------------------------------------------------------

def test_estimate_token_count():
    # Line 12 (sys.path insert) + line 109/111/122 (_estimate_token_count)
    from workers.summary_worker import _estimate_token_count
    assert _estimate_token_count("hello world") >= 1
    assert _estimate_token_count("a" * 400) == 100


def test_fetch_paper_chunks_empty():
    # Lines 109/111: _fetch_paper_chunks returns [] when no rows
    from workers.summary_worker import _fetch_paper_chunks
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    result = _fetch_paper_chunks(mock_cursor, paper_id=1)
    assert result == []


def test_insert_evidence_empty_chunk_ids():
    # Line 185-186: _insert_evidence skips when chunk_ids is empty
    from workers.summary_worker import _insert_evidence
    mock_cursor = MagicMock()
    _insert_evidence(mock_cursor, paper_id=1, chunk_ids=[])
    mock_cursor.execute.assert_not_called()


def test_generate_paper_summary_llm_failure():
    # Lines 341-343: LLM raises → returns error with llm_generation_failed
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        None,                          # no existing summary
        (1, "Test Title", "Abstract"), # paper found
    ]
    mock_cursor.fetchall.return_value = [
        (1, "Chunk text about methods.", "methods", 50),
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_llm_instance = MagicMock()
    mock_llm_instance.generate_structured_summary.side_effect = RuntimeError("LLM timeout")

    with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.summary_worker.LLMClient", return_value=mock_llm_instance):
            result = generate_paper_summary(paper_id=1)

    assert result["status"] == "error"
    assert "llm_generation_failed" in result["error"]


def test_batch_summarize_papers_with_one_paper():
    # Lines 441-486: batch loop with one paper that succeeds
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, "2301.00001", "Test Title", "Abstract"),
    ]
    mock_cursor.description = [("id",), ("arxiv_id",), ("title",), ("abstract",)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    original = sw.generate_paper_summary
    mock_fn = MagicMock()
    mock_fn.remote = MagicMock(return_value={"status": "ok"})
    sw.generate_paper_summary = mock_fn

    try:
        with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
            result = batch_summarize_papers(limit=10)
    finally:
        sw.generate_paper_summary = original

    assert result["status"] == "ok"
    assert result["papers_to_summarize"] == 1
    assert result["papers_successful"] == 1


def test_batch_summarize_papers_with_failed_paper():
    # Lines 441-486: batch loop where paper fails
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, "2301.00001", "Test Title", "Abstract"),
    ]
    mock_cursor.description = [("id",), ("arxiv_id",), ("title",), ("abstract",)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    original = sw.generate_paper_summary
    mock_fn = MagicMock()
    mock_fn.remote = MagicMock(return_value={"status": "error", "error": "no_chunks_found"})
    sw.generate_paper_summary = mock_fn

    try:
        with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
            result = batch_summarize_papers(limit=10)
    finally:
        sw.generate_paper_summary = original

    assert result["papers_failed"] == 1


def test_fetch_paper_chunks_respects_context_limit_and_skips_blank_text():
    from workers.summary_worker import _fetch_paper_chunks

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, "short text", "abstract", 3),
        (2, "   ", "methods", 4),
        (3, "x" * 100, "results", 5),
    ]

    result = _fetch_paper_chunks(mock_cursor, paper_id=1, max_context_chars=20)

    assert [row["chunk_id"] for row in result] == [1]


def test_fetch_paper_chunks_breaks_at_limit():
    from workers.summary_worker import _fetch_paper_chunks

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, "a", "abstract", 1),
        (2, "b", "methods", 1),
        (3, "c", "results", 1),
    ]

    result = _fetch_paper_chunks(mock_cursor, paper_id=1, limit=2, max_context_chars=100)

    assert [row["chunk_id"] for row in result] == [1, 2]


def test_insert_summary_calls_merge():
    from workers.summary_worker import _insert_summary

    summary = MagicMock()
    summary.to_dict.return_value = {"research_question": "Q"}
    mock_cursor = MagicMock()

    _insert_summary(mock_cursor, paper_id=1, summary=summary)

    mock_cursor.execute.assert_called_once()


def test_batch_summarize_papers_top_level_exception():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = RuntimeError("db failed")
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
        result = batch_summarize_papers(limit=1)

    assert result["status"] == "error"
    assert "db failed" in result["error"]


def test_generate_paper_summary_outer_exception_rolls_back():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.summary_worker._fetch_paper_chunks", side_effect=RuntimeError("chunk fetch failed")):
            result = generate_paper_summary(paper_id=1, force=True)

    assert result["status"] == "error"
    assert "chunk fetch failed" in result["error"]
    mock_conn.rollback.assert_called_once()


def test_batch_summarize_papers_remote_exception_is_recorded():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, "2301.00001", "Test Title", "Abstract"),
    ]
    mock_cursor.description = [("id",), ("arxiv_id",), ("title",), ("abstract",)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    original = sw.generate_paper_summary
    mock_fn = MagicMock()
    mock_fn.remote = MagicMock(side_effect=RuntimeError("worker boom"))
    sw.generate_paper_summary = mock_fn

    try:
        with patch("workers.summary_worker.connect_to_snowflake", return_value=mock_conn):
            result = batch_summarize_papers(limit=10)
    finally:
        sw.generate_paper_summary = original

    assert result["papers_failed"] == 1
    assert "worker boom" in result["errors"][0]
