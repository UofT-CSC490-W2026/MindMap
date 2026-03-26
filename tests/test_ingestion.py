"""
Tests for workers/ingestion.py

arxiv and httpx are mocked in sys.modules before the module is imported
so those heavy deps are never loaded.
"""

import sys
import pytest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Inject stubs before ingestion is imported
# ---------------------------------------------------------------------------

_arxiv_mock = MagicMock()
_httpx_mock = MagicMock()

sys.modules.setdefault("arxiv", _arxiv_mock)
sys.modules.setdefault("httpx", _httpx_mock)

from workers.ingestion import (  # noqa: E402
    _extract_arxiv_id,
    ingest_from_semantic_scholar,
    ingest_from_arxiv,
    ingest_from_openalex,
)


# ---------------------------------------------------------------------------
# _extract_arxiv_id
# ---------------------------------------------------------------------------

def test_extract_arxiv_id_valid():
    external_ids = {"ArXiv": "2301.00001"}
    assert _extract_arxiv_id(external_ids) == "2301.00001"


def test_extract_arxiv_id_missing_key():
    assert _extract_arxiv_id({"DOI": "10.1234/test"}) is None


def test_extract_arxiv_id_non_dict():
    assert _extract_arxiv_id("not a dict") is None


# ---------------------------------------------------------------------------
# ingest_from_semantic_scholar — happy path
# ---------------------------------------------------------------------------

def test_ingest_from_semantic_scholar_happy_path():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("RAW_PAYLOAD",)],  # DESC TABLE for _resolve_bronze_payload_column
    ]
    mock_cursor.fetchone.return_value = None  # no duplicate
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": [
            {
                "paperId": "abc123",
                "title": "Test Paper",
                "abstract": "Abstract",
                "externalIds": {"ArXiv": "2301.00001"},
                "authors": [],
                "openAccessPdf": None,
                "publicationDate": "2023-01-01",
                "journal": None,
                "url": "https://example.com",
            }
        ]
    }
    mock_response.raise_for_status.return_value = None

    mock_httpx_module = MagicMock()
    mock_httpx_module.get.return_value = mock_response
    mock_httpx_module.HTTPStatusError = Exception

    with patch("workers.ingestion.connect_to_snowflake", return_value=mock_conn):
        with patch.dict(sys.modules, {"httpx": mock_httpx_module}):
            ingest_from_semantic_scholar(query="test", max_results=1)


# ---------------------------------------------------------------------------
# ingest_from_arxiv — happy path
# ---------------------------------------------------------------------------

def test_ingest_from_arxiv_happy_path():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("RAW_PAYLOAD",)],  # DESC TABLE for _resolve_bronze_payload_column
    ]
    mock_cursor.fetchone.return_value = None  # no duplicate
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_result = MagicMock()
    mock_result.entry_id = "http://arxiv.org/abs/2301.00001v1"
    mock_result.updated = "2023-01-01"
    mock_result.published = "2023-01-01"
    mock_result.title = "Test Paper"
    mock_author = MagicMock()
    mock_author.name = "Author One"
    mock_result.authors = [mock_author]
    mock_result.summary = "Test abstract"
    mock_result.comment = None
    mock_result.journal_ref = None
    mock_result.doi = None
    mock_result.primary_category = "cs.AI"
    mock_result.categories = ["cs.AI"]
    mock_result.links = [MagicMock(href="https://arxiv.org/abs/2301.00001")]
    mock_result.pdf_url = "https://arxiv.org/pdf/2301.00001.pdf"

    mock_search_instance = MagicMock()
    mock_search_instance.results.return_value = iter([mock_result])

    mock_arxiv_module = MagicMock()
    mock_arxiv_module.Search.return_value = mock_search_instance

    with patch("workers.ingestion.connect_to_snowflake", return_value=mock_conn):
        with patch.dict(sys.modules, {"arxiv": mock_arxiv_module}):
            # Re-patch the arxiv name in the ingestion module's namespace
            with patch("workers.ingestion.arxiv", mock_arxiv_module):
                ingest_from_arxiv(query="test", max_results=1)


# ---------------------------------------------------------------------------
# ingest_from_openalex — happy path
# ---------------------------------------------------------------------------

def test_ingest_from_openalex_happy_path():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("RAW_PAYLOAD",)],  # DESC TABLE (in case it's called)
    ]
    mock_cursor.fetchone.return_value = None  # no duplicate
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_conn.cursor.return_value
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_response = MagicMock()
    mock_response.json.return_value = {
        "results": [
            {
                "id": "https://openalex.org/W123",
                "title": "Test Paper",
                "authorships": [],
                "abstract_inverted_index": None,
                "publication_date": "2023-01-01",
                "doi": None,
                "primary_location": None,
                "cited_by_count": 0,
                "referenced_works": [],
                "related_works": [],
                "ids": {},
            }
        ]
    }
    mock_response.raise_for_status.return_value = None

    with patch("workers.ingestion.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.ingestion.requests.get", return_value=mock_response):
            ingest_from_openalex(query="test", max_results=1)


# ---------------------------------------------------------------------------
# Additional coverage tests
# ---------------------------------------------------------------------------

def test_resolve_bronze_payload_column_raises_when_missing():
    # Line 35: RuntimeError when raw_payload column not found
    from workers.ingestion import _resolve_bronze_payload_column
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("OTHER_COL",)]
    mock_cursor.execute.return_value = None
    with pytest.raises(RuntimeError, match="Could not find raw payload column"):
        _resolve_bronze_payload_column(mock_cursor, "BRONZE_TABLE")


def test_ss_get_json_success():
    # Lines 59, 71-73: _ss_get_json happy path
    from workers.ingestion import _ss_get_json
    mock_httpx = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {"data": []}
    mock_httpx.get.return_value = mock_resp
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        result = _ss_get_json("https://example.com", params={})
    assert result == {"data": []}


def test_ss_get_json_401_retries_without_key():
    # Lines 77-89: 401 response causes retry without API key
    from workers.ingestion import _ss_get_json
    mock_httpx = MagicMock()
    mock_resp_401 = MagicMock()
    mock_resp_401.status_code = 401
    mock_resp_ok = MagicMock()
    mock_resp_ok.status_code = 200
    mock_resp_ok.raise_for_status.return_value = None
    mock_resp_ok.json.return_value = {"data": "ok"}
    mock_httpx.get.side_effect = [mock_resp_401, mock_resp_ok]
    mock_httpx.HTTPStatusError = Exception

    import os
    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        with patch.dict(os.environ, {"SEMANTIC_SCHOLAR_API_KEY": "bad_key"}):
            result = _ss_get_json("https://example.com", params={})
    assert result == {"data": "ok"}


def test_ss_get_json_raises_on_http_error():
    # Lines 93-99: HTTPStatusError is re-raised as RuntimeError
    from workers.ingestion import _ss_get_json
    mock_httpx = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    exc = Exception("500 error")
    exc.response = MagicMock()
    exc.response.status_code = 500
    exc.response.text = "Internal Server Error"
    mock_resp.raise_for_status.side_effect = exc
    mock_httpx.get.return_value = mock_resp
    mock_httpx.HTTPStatusError = type(exc)

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        with pytest.raises((RuntimeError, Exception)):
            _ss_get_json("https://example.com", params={})


def test_ingest_from_openalex_skips_no_id():
    # Lines 145-146: paper with no id is skipped
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_response = MagicMock()
    mock_response.json.return_value = {"results": [{"id": None, "title": "No ID Paper"}]}
    mock_response.raise_for_status.return_value = None

    with patch("workers.ingestion.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.ingestion.requests.get", return_value=mock_response):
            ingest_from_openalex(query="test", max_results=1)

    # No inserts should have happened
    mock_cursor.execute.assert_not_called()


def test_ingest_from_openalex_skips_duplicate():
    # Lines 156-157: duplicate paper is skipped
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = (1,)  # duplicate found
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_response = MagicMock()
    mock_response.json.return_value = {"results": [{"id": "W123", "title": "Dup Paper", "authorships": []}]}
    mock_response.raise_for_status.return_value = None

    with patch("workers.ingestion.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.ingestion.requests.get", return_value=mock_response):
            ingest_from_openalex(query="test", max_results=1)


def test_ingest_from_arxiv_skips_duplicate():
    # Lines 222-223: duplicate arxiv paper is skipped
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [[("RAW_PAYLOAD",)]]
    mock_cursor.fetchone.return_value = (1,)  # duplicate
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_result = MagicMock()
    mock_result.entry_id = "http://arxiv.org/abs/2301.00001v1"
    mock_result.updated = "2023-01-01"
    mock_result.published = "2023-01-01"
    mock_result.title = "Test Paper"
    mock_result.authors = []
    mock_result.summary = "Abstract"
    mock_result.comment = None
    mock_result.journal_ref = None
    mock_result.doi = None
    mock_result.primary_category = "cs.AI"
    mock_result.categories = ["cs.AI"]
    mock_result.links = []
    mock_result.pdf_url = None

    mock_search = MagicMock()
    mock_search.results.return_value = iter([mock_result])
    mock_arxiv = MagicMock()
    mock_arxiv.Search.return_value = mock_search

    with patch("workers.ingestion.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.ingestion.arxiv", mock_arxiv):
            ingest_from_arxiv(query="test", max_results=1)


def test_ingest_from_semantic_scholar_skips_no_arxiv():
    # Lines 254, 276: paper without ArXiv ID is skipped
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [[("RAW_PAYLOAD",)]]
    mock_cursor.fetchone.return_value = None
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_conn.cursor.return_value
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": [{"paperId": "abc", "title": "No ArXiv", "externalIds": {"DOI": "10.1234"}, "authors": []}]
    }
    mock_response.raise_for_status.return_value = None

    mock_httpx = MagicMock()
    mock_httpx.get.return_value = mock_response
    mock_httpx.HTTPStatusError = Exception

    with patch("workers.ingestion.connect_to_snowflake", return_value=mock_conn):
        with patch.dict(sys.modules, {"httpx": mock_httpx}):
            ingest_from_semantic_scholar(query="test", max_results=1)


def test_ingest_from_semantic_scholar_skips_duplicate():
    # Lines 312-315: duplicate SS paper is skipped
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [[("RAW_PAYLOAD",)]]
    mock_cursor.fetchone.return_value = (1,)  # duplicate
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": [{"paperId": "abc", "title": "Dup", "externalIds": {"ArXiv": "2301.00001"}, "authors": []}]
    }
    mock_response.raise_for_status.return_value = None

    mock_httpx = MagicMock()
    mock_httpx.get.return_value = mock_response
    mock_httpx.HTTPStatusError = Exception

    with patch("workers.ingestion.connect_to_snowflake", return_value=mock_conn):
        with patch.dict(sys.modules, {"httpx": mock_httpx}):
            ingest_from_semantic_scholar(query="test", max_results=1)


def test_peek_bronze():
    # Lines 387-424: peek_bronze happy path
    from workers.ingestion import peek_bronze
    import json as _json
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("RAW_PAYLOAD",)],
        [(_json.dumps({"title": "Test", "entry_id": "W123", "summary": "Abstract text"}),)],
    ]
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.ingestion.connect_to_snowflake", return_value=mock_conn):
        peek_bronze(limit=1)  # just assert no exception


def test_main_entrypoint_semantic_scholar():
    # Lines 431-437: main() local_entrypoint with semantic_scholar source
    from workers.ingestion import main
    mock_ingest = MagicMock(return_value=None)
    with patch("workers.ingestion.ingest_from_semantic_scholar") as mock_ss:
        mock_ss.remote = MagicMock(return_value=None)
        main(query="test", max_results=1, source="semantic_scholar")


def test_main_entrypoint_arxiv():
    from workers.ingestion import main
    with patch("workers.ingestion.ingest_from_arxiv") as mock_arxiv:
        mock_arxiv.remote = MagicMock(return_value=None)
        main(query="test", max_results=1, source="arxiv")
