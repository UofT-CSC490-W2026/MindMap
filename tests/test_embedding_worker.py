"""
Tests for workers/embedding_worker.py
"""

import importlib
import sys
from unittest.mock import MagicMock, patch

from workers.embedding_worker import (
    _build_embedding_text,
    run_embedding_batch,
    run_chunk_embedding_batch,
)


# ---------------------------------------------------------------------------
# _build_embedding_text
# ---------------------------------------------------------------------------

def test_build_embedding_text_none_when_empty():
    row = {"title": "", "abstract": "", "conclusion": ""}
    assert _build_embedding_text(row) is None


def test_build_embedding_text_with_title_and_abstract():
    row = {"title": "My Title", "abstract": "My Abstract", "conclusion": ""}
    result = _build_embedding_text(row)
    assert result is not None
    assert "My Title" in result
    assert "My Abstract" in result


# ---------------------------------------------------------------------------
# run_embedding_batch — no rows
# ---------------------------------------------------------------------------

def test_run_embedding_batch_no_rows():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("TITLE",), ("CONCLUSION",), ("ABSTRACT",), ("EMBEDDING",)],  # DESC TABLE
        [],  # SELECT returns no rows
    ]
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_st = MagicMock()
    mock_st.SentenceTransformer.return_value = MagicMock()

    with patch("workers.embedding_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("importlib.import_module", return_value=mock_st):
            result = run_embedding_batch(limit=1)

    assert result["status"] == "ok"
    assert result.get("embedded") == 0


# ---------------------------------------------------------------------------
# run_embedding_batch — one paper
# ---------------------------------------------------------------------------

def test_run_embedding_batch_with_one_paper():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        # DESC TABLE for _fetch_unembedded_from_silver
        [("ID",), ("TITLE",), ("CONCLUSION",), ("ABSTRACT",), ("EMBEDDING",), ("SIMILAR_EMBEDDINGS_IDS",)],
        # data rows
        [(1, "Test Title", "Test Conclusion", "Test Abstract")],
        # DESC TABLE for _update_embeddings
        [("ID",), ("EMBEDDING",)],
        # DESC TABLE for _count_embedded_papers
        [("EMBEDDING",)],
    ]
    mock_cursor.fetchone.return_value = (5,)
    mock_cursor.description = [("id",), ("title",), ("conclusion",), ("abstract",)]
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_vec = MagicMock()
    mock_vec.tolist.return_value = [0.1] * 384

    mock_model = MagicMock()
    mock_model.encode.return_value = [mock_vec]

    mock_st = MagicMock()
    mock_st.SentenceTransformer.return_value = mock_model

    def fake_import_module(name):
        if name == "sentence_transformers":
            return mock_st
        return importlib.import_module(name)

    with patch("workers.embedding_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("importlib.import_module", side_effect=fake_import_module):
            result = run_embedding_batch(limit=1, populate_similar=False)

    assert result["status"] == "ok"
    assert result["embedded"] == 1


# ---------------------------------------------------------------------------
# run_chunk_embedding_batch — no chunks
# ---------------------------------------------------------------------------

def test_run_chunk_embedding_batch_no_chunks():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        # DESC TABLE for SILVER_PAPER_CHUNKS
        [("CHUNK_ID",), ("PAPER_ID",), ("SECTION_ID",), ("CHUNK_TEXT",), ("EMBEDDING",)],
        # no chunks
        [],
    ]
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_st = MagicMock()
    mock_st.SentenceTransformer.return_value = MagicMock()

    with patch("workers.embedding_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("importlib.import_module", return_value=mock_st):
            result = run_chunk_embedding_batch(limit=1)

    assert result["status"] == "ok"
    assert result.get("chunks_embedded") == 0
