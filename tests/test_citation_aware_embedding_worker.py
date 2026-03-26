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


def test_extract_ref_arxiv_id_none_for_non_string_non_dict():
    assert _extract_ref_arxiv_id(123) is None


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


# ---------------------------------------------------------------------------
# _ensure_tables, _upsert_ca_embedding, _insert_references helpers
# ---------------------------------------------------------------------------

def test_ensure_tables_calls_execute():
    from workers.citation_aware_embedding_worker import _ensure_tables
    mock_cursor = MagicMock()
    _ensure_tables(mock_cursor)
    assert mock_cursor.execute.call_count == 2


def test_upsert_ca_embedding_calls_execute():
    from workers.citation_aware_embedding_worker import _upsert_ca_embedding
    mock_cursor = MagicMock()
    _upsert_ca_embedding(mock_cursor, "paper1", "model-v1", 0.8, [0.1] * 384)
    mock_cursor.execute.assert_called_once()


def test_insert_references_inserts_rows():
    from workers.citation_aware_embedding_worker import _insert_references
    mock_cursor = MagicMock()
    refs = [{"ref_text": "Ref 1", "ref_arxiv_id": "2301.00001"}, "plain string ref"]
    _insert_references(mock_cursor, "paper1", "2301.00001", refs)
    assert mock_cursor.execute.call_count == 2


def test_resolve_ref_paper_ids_empty():
    from workers.citation_aware_embedding_worker import _resolve_ref_paper_ids
    mock_cursor = MagicMock()
    result = _resolve_ref_paper_ids(mock_cursor, [])
    assert result == []


def test_resolve_ref_paper_ids_with_ids():
    from workers.citation_aware_embedding_worker import _resolve_ref_paper_ids
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("uuid-1",), ("uuid-2",)]
    result = _resolve_ref_paper_ids(mock_cursor, ["2301.00001", "2301.00002"])
    assert result == ["uuid-1", "uuid-2"]


def test_fetch_embeddings_empty():
    from workers.citation_aware_embedding_worker import _fetch_embeddings
    mock_cursor = MagicMock()
    result = _fetch_embeddings(mock_cursor, [])
    assert result == []


def test_fetch_embeddings_with_ids():
    from workers.citation_aware_embedding_worker import _fetch_embeddings
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [([0.1] * 384,)]
    result = _fetch_embeddings(mock_cursor, ["uuid-1"])
    assert len(result) == 1
    assert len(result[0]) == 384


# ---------------------------------------------------------------------------
# run_citation_aware_embedding_batch — with one paper, no refs
# ---------------------------------------------------------------------------

def test_run_citation_aware_embedding_batch_with_paper_no_refs():
    """Covers the skipped_no_refs path in run_citation_aware_embedding_batch."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("pid1", "2301.00001", "Title", "Abstract")]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_vec = MagicMock()
    mock_vec.tolist.return_value = [0.1] * 384
    mock_model = MagicMock()
    mock_model.encode.return_value = [mock_vec]
    mock_st_local = MagicMock()
    mock_st_local.SentenceTransformer.return_value = mock_model
    sys.modules["sentence_transformers"] = mock_st_local

    # get_citations returns no references
    mock_get_citations.remote.return_value = {"references": []}

    with patch("workers.citation_aware_embedding_worker.connect_to_snowflake", return_value=mock_conn):
        result = run_citation_aware_embedding_batch(limit=1)

    assert result["status"] == "ok"
    assert result["updated"] == 1
    assert result["skipped_no_refs"] == 1


def test_run_citation_aware_embedding_batch_with_reference_embeddings():
    import types

    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("pid1", "2301.00001", "Title", "Abstract")],
        [("ref-paper",)],
        [([0.0, 1.0],)],
    ]
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_vec = MagicMock()
    mock_vec.tolist.return_value = [1.0, 0.0]
    mock_model = MagicMock()
    mock_model.encode.return_value = [mock_vec]
    sys.modules["sentence_transformers"] = types.SimpleNamespace(
        SentenceTransformer=MagicMock(return_value=mock_model)
    )
    sys.modules["numpy"] = types.SimpleNamespace(
        float32="float32",
        array=lambda values, dtype=None: values,
        mean=lambda values, axis=0: MagicMock(tolist=MagicMock(return_value=[0.0, 1.0])),
    )

    mock_get_citations.remote.return_value = {
        "references": [{"ref_text": "Ref", "ref_arxiv_id": "2301.00002"}]
    }

    with patch("workers.citation_aware_embedding_worker.connect_to_snowflake", return_value=mock_conn):
        result = run_citation_aware_embedding_batch(limit=1, alpha=0.75)

    assert result["status"] == "ok"
    assert result["updated"] == 1
    assert result["skipped_no_refs"] == 0
    assert result["skipped_no_ref_embs"] == 0
