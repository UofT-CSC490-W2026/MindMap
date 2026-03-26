"""
Tests for workers/qa_worker.py

LLMClient and semantic_search_worker are mocked in sys.modules before
the module is imported so those heavy deps are never loaded.
"""

import sys
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Inject stubs before qa_worker is imported
# ---------------------------------------------------------------------------

mock_llm_module = MagicMock()
sys.modules["services.llm_client"] = mock_llm_module
sys.modules.setdefault("services", MagicMock())

# Mock workers.semantic_search_worker to avoid its SCHEMA issue
mock_ssw = MagicMock()
sys.modules["workers.semantic_search_worker"] = mock_ssw

# Add SCHEMA to config before qa_worker imports it
import config  # noqa: E402
config.SCHEMA = "SILVER"

from workers.qa_worker import (  # noqa: E402
    _looks_ambiguous,
    _looks_unrelated,
    _format_history,
    answer_paper_question,
)


# ---------------------------------------------------------------------------
# _looks_unrelated
# ---------------------------------------------------------------------------

def test_looks_unrelated_true():
    assert _looks_unrelated("what is the weather today") is True
    assert _looks_unrelated("best restaurant near me") is True


def test_looks_unrelated_false():
    assert _looks_unrelated("what is the main contribution of this paper") is False


# ---------------------------------------------------------------------------
# _looks_ambiguous
# ---------------------------------------------------------------------------

def test_looks_ambiguous_true():
    # Short query with pronouns
    assert _looks_ambiguous("what does it do") is True


def test_looks_ambiguous_false():
    # Long specific query without pronouns
    assert _looks_ambiguous(
        "what are the main contributions of the transformer architecture in the paper"
    ) is False


# ---------------------------------------------------------------------------
# _format_history
# ---------------------------------------------------------------------------

def test_format_history_empty():
    result = _format_history([])
    assert result == ""


def test_format_history_with_messages():
    history = [
        {"role": "user", "message": "Hello"},
        {"role": "assistant", "message": "Hi there"},
    ]
    result = _format_history(history)
    assert "USER: Hello" in result
    assert "ASSISTANT: Hi there" in result


# ---------------------------------------------------------------------------
# answer_paper_question — edge cases
# ---------------------------------------------------------------------------

def test_answer_paper_question_empty_question():
    result = answer_paper_question(paper_id=1, question="")
    assert result["status"] == "error"
    assert result["error"] == "empty_question"


def test_answer_paper_question_unrelated():
    result = answer_paper_question(paper_id=1, question="what is the weather today")
    assert result["status"] == "refused"


# ---------------------------------------------------------------------------
# answer_paper_question — no chunks found
# ---------------------------------------------------------------------------

def test_answer_paper_question_no_chunks_found():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []   # no history
    mock_cursor.fetchone.return_value = None
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.qa_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.qa_worker.retrieve_similar_chunks_local", return_value=[]):
            with patch("workers.qa_worker.qualify_table", return_value="DB.SILVER.APP_QA_LOGS"):
                result = answer_paper_question(paper_id=1, question="what is the main method?")

    assert result["status"] == "ok"
    assert result["chunks_used"] == 0


# ---------------------------------------------------------------------------
# answer_paper_question — with chunks (happy path)
# ---------------------------------------------------------------------------

def test_answer_paper_question_with_chunks():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []   # no history
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    chunks = [
        {
            "chunk_id": 1,
            "chunk_text": "The method uses transformers.",
            "chunk_type": "methods",
            "paper_id": 1,
            "section_id": 1,
            "score": 0.9,
        }
    ]

    mock_grounded_answer = MagicMock()
    mock_grounded_answer.answer = "The method uses transformers."
    mock_grounded_answer.cited_chunk_ids = [1]

    mock_llm_instance = MagicMock()
    mock_llm_instance.answer_grounded_question.return_value = {"result": mock_grounded_answer}

    mock_llm_cls = MagicMock(return_value=mock_llm_instance)

    with patch("workers.qa_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.qa_worker.retrieve_similar_chunks_local", return_value=chunks):
            with patch("workers.qa_worker.LLMClient", mock_llm_cls):
                with patch("workers.qa_worker.qualify_table", return_value="DB.SILVER.APP_QA_LOGS"):
                    result = answer_paper_question(paper_id=1, question="what is the main method?")

    assert result["status"] == "ok"
    assert "answer" in result
