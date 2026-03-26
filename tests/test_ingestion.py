"""
Tests for workers/ingestion.py

arxiv and httpx are mocked in sys.modules before the module is imported
so those heavy deps are never loaded.
"""

import sys
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
