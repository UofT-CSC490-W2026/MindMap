"""
Tests for workers/semantic_search_worker.py

sentence_transformers is mocked in sys.modules before the module is imported.
SCHEMA is patched onto config before import to satisfy the default parameter
in retrieve_similar_chunks_local.
"""

import json
import sys
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Inject stubs before semantic_search_worker is imported
# ---------------------------------------------------------------------------

# Mock sentence_transformers so it's available via importlib.import_module
_st_mock = MagicMock()
sys.modules["sentence_transformers"] = _st_mock

# SCHEMA is used as a bare name default parameter in retrieve_similar_chunks_local
# (schema: str = SCHEMA). It is not imported from config, so we inject it into
# builtins before the module is loaded so it resolves at function-definition time.
import builtins as _builtins
_builtins.SCHEMA = "SILVER"  # type: ignore[attr-defined]

import config  # noqa: E402
config.SCHEMA = "SILVER"

# Remove any previously injected mock so we import the real module
sys.modules.pop("workers.semantic_search_worker", None)

from workers.semantic_search_worker import (  # noqa: E402
    _parse_cached_ids,
    _keyword_tokens,
    _hybrid_score,
    semantic_search,
    get_related_papers,
)


# ---------------------------------------------------------------------------
# _parse_cached_ids
# ---------------------------------------------------------------------------

def test_parse_cached_ids_none():
    assert _parse_cached_ids(None, 10) == []


def test_parse_cached_ids_valid_json():
    assert _parse_cached_ids('[1, 2, 3]', 10) == [1, 2, 3]


def test_parse_cached_ids_invalid_json():
    assert _parse_cached_ids('not json', 10) == []


def test_parse_cached_ids_non_list():
    assert _parse_cached_ids('{"key": "val"}', 10) == []


def test_parse_cached_ids_respects_k():
    assert _parse_cached_ids('[1, 2, 3, 4, 5]', 3) == [1, 2, 3]


# ---------------------------------------------------------------------------
# _keyword_tokens
# ---------------------------------------------------------------------------

def test_keyword_tokens_basic():
    tokens = _keyword_tokens("Hello World foo")
    assert "hello" in tokens
    assert "world" in tokens
    assert "foo" in tokens


def test_keyword_tokens_filters_short():
    tokens = _keyword_tokens("a bb ccc dddd")
    assert "a" not in tokens
    assert "bb" not in tokens
    assert "ccc" in tokens


# ---------------------------------------------------------------------------
# _hybrid_score
# ---------------------------------------------------------------------------

def test_hybrid_score_empty_query_tokens():
    # When query_tokens is empty, should return the vector score unchanged
    score = _hybrid_score(set(), "Some Title", "Some Abstract", 0.75)
    assert score == 0.75


# ---------------------------------------------------------------------------
# semantic_search — empty query
# ---------------------------------------------------------------------------

def test_semantic_search_empty_query():
    result = semantic_search(query="", k=5)
    assert result == []


# ---------------------------------------------------------------------------
# get_related_papers — with cached ids
# ---------------------------------------------------------------------------

def test_get_related_papers_with_cached_ids():
    mock_cursor = MagicMock()
    # fetchone returns the cached similar_embeddings_ids row
    mock_cursor.fetchone.return_value = (json.dumps([101, 102]),)
    # fetchall returns the JOIN result for the cached ids
    mock_cursor.fetchall.return_value = [
        (101, "2301.00001", "Paper One"),
        (102, "2301.00002", "Paper Two"),
    ]
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.semantic_search_worker.connect_to_snowflake", return_value=mock_conn):
        result = get_related_papers(paper_id=1, k=2)

    assert isinstance(result, list)
    assert len(result) == 2
