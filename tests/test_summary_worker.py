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
