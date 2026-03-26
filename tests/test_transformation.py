"""
Tests for workers/transformation.py

httpx is mocked in sys.modules before the module is imported
so the heavy dep is never loaded.
"""

import sys
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Inject stubs before transformation is imported
# ---------------------------------------------------------------------------

_httpx_mock = MagicMock()
sys.modules.setdefault("httpx", _httpx_mock)

import workers.transformation  # noqa: E402

from workers.transformation import (  # noqa: E402
    _retry_delay_from_response,
    _clean_extracted_text,
    _truncate_text,
    _extract_conclusion_from_text,
    _extract_arxiv_id_from_external_ids,
    _normalize_connection_entry,
    extract_full_text_pdf,
    transform_to_silver,
)


# ---------------------------------------------------------------------------
# _retry_delay_from_response
# ---------------------------------------------------------------------------

def test_retry_delay_from_response_none_response():
    delay = _retry_delay_from_response(None, attempt=0)
    assert delay > 0


def test_retry_delay_from_response_with_retry_after_header():
    mock_response = MagicMock()
    mock_response.headers.get.return_value = "5"
    delay = _retry_delay_from_response(mock_response, attempt=0)
    assert delay == 5.0


# ---------------------------------------------------------------------------
# _clean_extracted_text
# ---------------------------------------------------------------------------

def test_clean_extracted_text_null_bytes():
    result = _clean_extracted_text("hello\x00world")
    assert "\x00" not in result


def test_clean_extracted_text_multiple_spaces():
    result = _clean_extracted_text("hello   world")
    assert "   " not in result


def test_clean_extracted_text_multiple_newlines():
    result = _clean_extracted_text("line1\n\n\nline2")
    assert "\n\n\n" not in result


# ---------------------------------------------------------------------------
# _truncate_text
# ---------------------------------------------------------------------------

def test_truncate_text_within_limit():
    text = "short text"
    assert _truncate_text(text, max_chars=100) == text


def test_truncate_text_over_limit():
    text = "word " * 1000  # 5000 chars
    result = _truncate_text(text, max_chars=50)
    assert len(result) <= 50


# ---------------------------------------------------------------------------
# _extract_conclusion_from_text
# ---------------------------------------------------------------------------

def test_extract_conclusion_from_text_with_header():
    text = (
        "Introduction\nSome intro text.\n\n"
        "Conclusion\nThis is the conclusion text.\n\n"
        "References\nRef 1."
    )
    result = _extract_conclusion_from_text(text)
    assert "conclusion" in result.lower() or "conclusion text" in result.lower()


def test_extract_conclusion_from_text_no_header():
    text = "This text has no conclusion section header."
    result = _extract_conclusion_from_text(text)
    assert result == ""


# ---------------------------------------------------------------------------
# _extract_arxiv_id_from_external_ids
# ---------------------------------------------------------------------------

def test_extract_arxiv_id_from_external_ids_valid():
    assert _extract_arxiv_id_from_external_ids({"ArXiv": "2301.00001"}) == "2301.00001"


def test_extract_arxiv_id_from_external_ids_missing():
    assert _extract_arxiv_id_from_external_ids({"DOI": "10.1234"}) is None


def test_extract_arxiv_id_from_external_ids_non_dict():
    assert _extract_arxiv_id_from_external_ids("not a dict") is None


# ---------------------------------------------------------------------------
# _normalize_connection_entry
# ---------------------------------------------------------------------------

def test_normalize_connection_entry_valid():
    node = {
        "title": "Test",
        "year": 2023,
        "externalIds": {"ArXiv": "2301.00001"},
        "paperId": "abc",
    }
    result = _normalize_connection_entry(node)
    assert result["title"] == "Test"
    assert result["arxiv_id"] == "2301.00001"


def test_normalize_connection_entry_non_dict():
    result = _normalize_connection_entry("not a dict")
    assert result is None


# ---------------------------------------------------------------------------
# extract_full_text_pdf — happy path
# ---------------------------------------------------------------------------

def test_extract_full_text_pdf_happy_path():
    mock_page = MagicMock()
    mock_page.get_text.return_value = "Sample text content here"

    mock_doc = MagicMock()
    mock_doc.__len__ = MagicMock(return_value=1)
    mock_doc.__iter__ = MagicMock(return_value=iter([mock_page]))
    mock_doc.__getitem__ = MagicMock(return_value=mock_page)

    mock_pymupdf = MagicMock()
    mock_pymupdf.open.return_value = mock_doc

    with patch.dict(sys.modules, {"pymupdf": mock_pymupdf}):
        with patch("workers.transformation._arxiv_get_pdf_bytes", return_value=b"%PDF fake"):
            result = extract_full_text_pdf("2301.00001")

    assert result["full_text"] != ""
    assert result["source"] == "pdf"


# ---------------------------------------------------------------------------
# transform_to_silver — happy path
# ---------------------------------------------------------------------------

def test_transform_to_silver_happy_path():
    mock_extract = MagicMock()
    mock_extract.local.return_value = {
        "full_text": "Some text",
        "source": "pdf",
        "truncated": False,
        "pages_processed": 5,
    }

    mock_get_refs = MagicMock()
    mock_get_refs.local.return_value = {"source": "api", "data": []}

    mock_fetch_conns = MagicMock()
    mock_fetch_conns.local.return_value = []

    mock_extract_conclusion = MagicMock()
    mock_extract_conclusion.local.return_value = ""

    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("RAW_PAYLOAD",)],  # DESC TABLE BRONZE for _resolve_bronze_payload_column
        [
            ("ARXIV_ID",),
            ("SS_ID",),
            ("CONCLUSION",),
            ("FULL_TEXT",),
            ("FULL_TEXT_SOURCE",),
            ("FULL_TEXT_EXTRACTED_AT",),
            ("TLDR",),
            ("REFERENCE_LIST",),
            ("CITATION_LIST",),
            ("TITLE",),
            ("ABSTRACT",),
        ],  # DESC TABLE SILVER
        [(1, "2301.00001")],  # bronze SELECT for MERGE
    ]
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None
    mock_conn.rollback.return_value = None

    original_extract = workers.transformation.extract_full_text_pdf
    original_get_refs = workers.transformation.get_references
    original_fetch_conns = workers.transformation.fetch_connections_ss
    original_extract_conclusion = workers.transformation.extract_conclusion

    try:
        workers.transformation.extract_full_text_pdf = mock_extract
        workers.transformation.get_references = mock_get_refs
        workers.transformation.fetch_connections_ss = mock_fetch_conns
        workers.transformation.extract_conclusion = mock_extract_conclusion

        with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
            with patch("workers.transformation._ss_get_json", return_value={"paperId": "ss_abc"}):
                transform_to_silver("2301.00001")

    finally:
        workers.transformation.extract_full_text_pdf = original_extract
        workers.transformation.get_references = original_get_refs
        workers.transformation.fetch_connections_ss = original_fetch_conns
        workers.transformation.extract_conclusion = original_extract_conclusion
