"""
Tests for workers/embedding_worker.py
"""

import importlib
import sys
import pytest
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


# ---------------------------------------------------------------------------
# backfill_similar_ids — no rows
# ---------------------------------------------------------------------------

def test_backfill_similar_ids_no_rows():
    from workers.embedding_worker import backfill_similar_ids
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("EMBEDDING",), ("SIMILAR_EMBEDDINGS_IDS",)],  # DESC TABLE
        [],  # no rows missing cache
    ]
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.embedding_worker.connect_to_snowflake", return_value=mock_conn):
        result = backfill_similar_ids(limit=10)

    assert result["status"] == "ok"
    assert result["backfilled"] == 0


def test_backfill_similar_ids_with_rows():
    from workers.embedding_worker import backfill_similar_ids
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("EMBEDDING",), ("SIMILAR_EMBEDDINGS_IDS",)],  # DESC TABLE
        [(1,), (2,)],   # two papers missing cache
        [("ID",), ("EMBEDDING",)],  # DESC TABLE for _compute_topk (paper 1)
        [(3,), (4,)],   # top-k results for paper 1
        [("ID",), ("EMBEDDING",)],  # DESC TABLE for _compute_topk (paper 2)
        [(5,), (6,)],   # top-k results for paper 2
    ]
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.embedding_worker.connect_to_snowflake", return_value=mock_conn):
        result = backfill_similar_ids(limit=10)

    assert result["status"] == "ok"
    assert result["backfilled"] == 2


def test_run_embedding_batch_with_populate_similar():
    """Test run_embedding_batch with populate_similar=True to cover neighbor population path."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("TITLE",), ("CONCLUSION",), ("ABSTRACT",), ("EMBEDDING",), ("SIMILAR_EMBEDDINGS_IDS",)],
        [(1, "Test Title", "Test Conclusion", "Test Abstract")],
        [("ID",), ("EMBEDDING",)],   # DESC TABLE for _update_embeddings
        [("EMBEDDING",)],            # DESC TABLE for _count_embedded_papers
        [("ID",), ("EMBEDDING",)],   # DESC TABLE for _compute_topk
        [(2,), (3,)],                # top-k results
        [("ID",), ("SIMILAR_EMBEDDINGS_IDS",)],  # DESC TABLE for _write_similar_ids
    ]
    mock_cursor.fetchone.return_value = (10,)  # count >= min_corpus_size
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

    def fake_import(name):
        if name == "sentence_transformers":
            return mock_st
        return importlib.import_module(name)

    with patch("workers.embedding_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("importlib.import_module", side_effect=fake_import):
            result = run_embedding_batch(limit=1, populate_similar=True, min_corpus_size_for_neighbors=5)

    assert result["status"] == "ok"
    assert result["neighbors_populated"] is True


# ---------------------------------------------------------------------------
# _require_columns raises on missing
# ---------------------------------------------------------------------------

def test_require_columns_raises():
    from workers.embedding_worker import _require_columns
    with pytest.raises(RuntimeError, match="Missing required columns"):
        _require_columns({"id": '"ID"'}, ["id", "nonexistent"], "MY_TABLE")


# ---------------------------------------------------------------------------
# _update_embeddings — empty rows is a no-op
# ---------------------------------------------------------------------------

def test_update_embeddings_empty_rows():
    from workers.embedding_worker import _update_embeddings
    mock_cursor = MagicMock()
    _update_embeddings(mock_cursor, database="DB", rows=[])
    mock_cursor.executemany.assert_not_called()


# ---------------------------------------------------------------------------
# run_embedding_batch — all rows have empty text (skipped_empty_text path)
# ---------------------------------------------------------------------------

def test_run_embedding_batch_all_empty_text():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("TITLE",), ("CONCLUSION",), ("ABSTRACT",), ("EMBEDDING",)],
        [(1, "", "", "")],  # row with no usable text
    ]
    mock_cursor.description = [("id",), ("title",), ("conclusion",), ("abstract",)]
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_st = MagicMock()
    with patch("workers.embedding_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("importlib.import_module", return_value=mock_st):
            result = run_embedding_batch(limit=1)

    assert result["status"] == "ok"
    assert result["embedded"] == 0
    assert result.get("skipped_empty_text") == 1


# ---------------------------------------------------------------------------
# run_chunk_embedding_batch — chunks with empty text are skipped
# ---------------------------------------------------------------------------

def test_run_chunk_embedding_batch_empty_text_chunks():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("CHUNK_ID",), ("PAPER_ID",), ("SECTION_ID",), ("CHUNK_TEXT",), ("EMBEDDING",)],
        [(1, 10, 5, "   ")],  # whitespace-only chunk text
    ]
    mock_cursor.description = [("chunk_id",), ("paper_id",), ("section_id",), ("chunk_text",)]
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_st = MagicMock()
    with patch("workers.embedding_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("importlib.import_module", return_value=mock_st):
            result = run_chunk_embedding_batch(limit=1)

    assert result["status"] == "ok"
    assert result["chunks_embedded"] == 0
