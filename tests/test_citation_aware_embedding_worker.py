"""
Tests for workers/citation_aware_embedding_worker.py

The module uses a relative import `from .citation_worker import get_citations`,
so we inject a mock for `workers.citation_worker` into sys.modules before
importing the module under test.
"""

import math
import sys
from unittest.mock import MagicMock, patch

# --- Mock citation_worker before importing citation_aware_embedding_worker ---
mock_citation_worker = MagicMock()
mock_get_citations = MagicMock()
mock_citation_worker.get_citations = mock_get_citations
sys.modules["workers.citation_worker"] = mock_citation_worker

# --- Mock sentence_transformers globally (also imported inside the function) ---
mock_sentence_transformers = MagicMock()
mock_st_model = MagicMock()
mock_sentence_transformers.SentenceTransformer.return_value = mock_st_model
sys.modules["sentence_transformers"] = mock_sentence_transformers

from workers.citation_aware_embedding_worker import (  # noqa: E402
    _l2_normalize,
    _extract_ref_arxiv_id,
    _extract_ref_text,
    run_citation_aware_embedding_batch,
)


# ---------------------------------------------------------------------------
# _l2_normalize
# ---------------------------------------------------------------------------

def test_l2_normalize_unit_length():
    vec = [3.0, 4.0]
    result = _l2_normalize(vec)
    norm = math.sqrt(sum(x * x for x in result))
    assert abs(norm - 1.0) < 1e-6


def test_l2_normalize_zero_vector():
    vec = [0.0, 0.0, 0.0]
    result = _l2_normalize(vec)
    # Should not raise; norm defaults to 1.0 so each element stays 0
    assert len(result) == 3


# ---------------------------------------------------------------------------
# _extract_ref_arxiv_id
# ---------------------------------------------------------------------------

def test_extract_ref_arxiv_id_from_dict():
    ref = {"ref_arxiv_id": "2301.00001"}
    assert _extract_ref_arxiv_id(ref) == "2301.00001"


def test_extract_ref_arxiv_id_from_string():
    ref = "Smith et al. 2301.00001 Some paper title"
    result = _extract_ref_arxiv_id(ref)
    assert result == "2301.00001"


def test_extract_ref_arxiv_id_none_for_non_matching():
    ref = "No arxiv id here"
    assert _extract_ref_arxiv_id(ref) is None


# ---------------------------------------------------------------------------
# _extract_ref_text
# ---------------------------------------------------------------------------

def test_extract_ref_text_from_dict():
    ref = {"ref_text": "Some reference text"}
    assert _extract_ref_text(ref) == "Some reference text"


def test_extract_ref_text_from_string():
    ref = "Plain string reference"
    assert _extract_ref_text(ref) == "Plain string reference"


# ---------------------------------------------------------------------------
# run_citation_aware_embedding_batch — happy path, no papers
# ---------------------------------------------------------------------------

def test_run_citation_aware_embedding_batch_no_papers():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None
    mock_conn.close.return_value = None

    with patch("workers.citation_aware_embedding_worker.connect_to_snowflake", return_value=mock_conn):
        result = run_citation_aware_embedding_batch(limit=1)

    assert result["status"] == "ok"
    assert result.get("updated") == 0
