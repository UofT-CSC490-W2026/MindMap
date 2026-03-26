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


# ---------------------------------------------------------------------------
# _chunks helper
# ---------------------------------------------------------------------------

def test_chunks_splits_correctly():
    from workers.transformation import _chunks
    result = list(_chunks([1, 2, 3, 4, 5], 2))
    assert result == [[1, 2], [3, 4], [5]]


def test_chunks_empty():
    from workers.transformation import _chunks
    result = list(_chunks([], 3))
    assert result == []


# ---------------------------------------------------------------------------
# _fetch_ss_batch_rows_resilient
# ---------------------------------------------------------------------------

def test_fetch_ss_batch_rows_resilient_success():
    from workers.transformation import _fetch_ss_batch_rows_resilient
    mock_httpx = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = [{"paperId": "abc"}, {"paperId": "def"}]
    mock_httpx.post.return_value = mock_resp
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        result = _fetch_ss_batch_rows_resilient(
            batch_url="https://example.com",
            ids=["ARXIV:2301.00001", "ARXIV:2301.00002"],
            params={},
        )
    assert len(result) == 2


def test_fetch_ss_batch_rows_resilient_failure_single():
    """When a single-id batch fails, returns [None]."""
    from workers.transformation import _fetch_ss_batch_rows_resilient
    with patch("workers.transformation._ss_post_json", side_effect=RuntimeError("fail")):
        result = _fetch_ss_batch_rows_resilient(
            batch_url="https://example.com",
            ids=["ARXIV:2301.00001"],
            params={},
        )
    assert result == [None]


# ---------------------------------------------------------------------------
# _fetch_ss_batch_metadata — empty input
# ---------------------------------------------------------------------------

def test_fetch_ss_batch_metadata_empty():
    from workers.transformation import _fetch_ss_batch_metadata
    result = _fetch_ss_batch_metadata([])
    assert result == {}


def test_fetch_ss_batch_metadata_with_ids():
    from workers.transformation import _fetch_ss_batch_metadata
    mock_row = {
        "paperId": "ss123",
        "externalIds": {"ArXiv": "2301.00001"},
        "tldr": {"text": "A summary"},
        "references": [],
        "citations": [],
    }
    with patch("workers.transformation._fetch_ss_batch_rows_resilient", return_value=[mock_row]):
        result = _fetch_ss_batch_metadata(["2301.00001"])
    assert "2301.00001" in result
    assert result["2301.00001"]["ss_id"] == "ss123"
    assert result["2301.00001"]["tldr"] == "A summary"


# ---------------------------------------------------------------------------
# _fetch_ss_batch_tldr — empty input
# ---------------------------------------------------------------------------

def test_fetch_ss_batch_tldr_empty():
    from workers.transformation import _fetch_ss_batch_tldr
    result = _fetch_ss_batch_tldr([])
    assert result == {}


# ---------------------------------------------------------------------------
# _extract_connections
# ---------------------------------------------------------------------------

def test_extract_connections_wrapped_nodes():
    from workers.transformation import _extract_connections
    items = [
        {"citedPaper": {"title": "Paper A", "year": 2023, "externalIds": {"ArXiv": "2301.00001"}, "paperId": "abc"}},
        {"citedPaper": {"title": "Paper B", "year": 2022, "externalIds": {}, "paperId": "def"}},
    ]
    result = _extract_connections(items, relation_key="citedPaper", limit=10)
    assert len(result) == 2
    assert result[0]["title"] == "Paper A"


def test_extract_connections_respects_limit():
    from workers.transformation import _extract_connections
    items = [{"title": f"Paper {i}", "year": 2023, "externalIds": {}, "paperId": str(i)} for i in range(5)]
    result = _extract_connections(items, relation_key="nonexistent", limit=2)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# fetch_connections_ss
# ---------------------------------------------------------------------------

def test_fetch_connections_ss_references():
    from workers.transformation import fetch_connections_ss
    mock_data = {
        "data": [
            {"citedPaper": {"title": "Ref Paper", "year": 2022, "externalIds": {"ArXiv": "2301.00001"}, "paperId": "abc"}}
        ]
    }
    with patch("workers.transformation._ss_get_json", return_value=mock_data):
        result = fetch_connections_ss("2301.00001", mode=0)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["title"] == "Ref Paper"


def test_fetch_connections_ss_api_error():
    from workers.transformation import fetch_connections_ss
    with patch("workers.transformation._ss_get_json", side_effect=RuntimeError("API down")):
        result = fetch_connections_ss("2301.00001", mode=0)
    assert result is None


# ---------------------------------------------------------------------------
# extract_references_pdf
# ---------------------------------------------------------------------------

def test_extract_references_pdf_happy_path():
    from workers.transformation import extract_references_pdf
    ref_text = "References [1] Smith et al. A long enough reference entry here. [2] Jones et al. Another long reference."

    mock_page = MagicMock()
    mock_page.get_text.return_value = ref_text
    mock_doc = MagicMock()
    mock_doc.__len__ = MagicMock(return_value=3)
    mock_doc.__iter__ = MagicMock(return_value=iter([mock_page]))
    mock_doc.__enter__ = MagicMock(return_value=mock_doc)
    mock_doc.__exit__ = MagicMock(return_value=False)
    mock_doc.__getitem__ = MagicMock(return_value=mock_page)

    mock_fitz = MagicMock()
    mock_fitz.open.return_value = mock_doc

    with patch("workers.transformation._arxiv_get_pdf_bytes", return_value=b"%PDF fake"):
        with patch.dict(sys.modules, {"fitz": mock_fitz}):
            result = extract_references_pdf("2301.00001")

    assert isinstance(result, list)


def test_extract_references_pdf_download_error():
    from workers.transformation import extract_references_pdf
    mock_fitz = MagicMock()
    with patch("workers.transformation._arxiv_get_pdf_bytes", side_effect=RuntimeError("download failed")):
        with patch.dict(sys.modules, {"fitz": mock_fitz}):
            result = extract_references_pdf("2301.00001")
    assert result == []


# ---------------------------------------------------------------------------
# get_references — API path and fallback path
# ---------------------------------------------------------------------------

def test_get_references_api_path():
    from workers.transformation import get_references
    mock_fetch = MagicMock()
    mock_fetch.remote.return_value = [{"title": "Paper", "arxiv_id": "2301.00001"}]
    workers.transformation.fetch_connections_ss = mock_fetch

    result = get_references("2301.00001")
    assert result["source"] == "api"
    assert len(result["data"]) == 1


def test_get_references_fallback_pdf():
    from workers.transformation import get_references
    mock_fetch = MagicMock()
    mock_fetch.remote.return_value = []  # API returns nothing

    mock_pdf = MagicMock()
    mock_pdf.remote.return_value = ["[1] Some reference text here"]

    workers.transformation.fetch_connections_ss = mock_fetch
    workers.transformation.extract_references_pdf = mock_pdf

    result = get_references("2301.00001")
    assert result["source"] == "pdf_parsed_list"


# ---------------------------------------------------------------------------
# get_bronze_worklist
# ---------------------------------------------------------------------------

def test_get_bronze_worklist_returns_new_ids():
    from workers.transformation import get_bronze_worklist
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("RAW_PAYLOAD",)],                          # DESC TABLE BRONZE
        [("ARXIV_ID",)],                             # DESC TABLE SILVER
        [("https://arxiv.org/abs/2301.00001v1",)],   # bronze entry_ids
        [("9999.99999",)],                           # existing silver arxiv_ids
    ]
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
        result = get_bronze_worklist()

    assert "2301.00001" in result


# ---------------------------------------------------------------------------
# backfill_missing_ss_ids — no rows
# ---------------------------------------------------------------------------

def test_backfill_missing_ss_ids_no_rows():
    from workers.transformation import backfill_missing_ss_ids
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("SS_ID",)],  # DESC TABLE
        [],  # no rows with missing ss_id
    ]
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
        result = backfill_missing_ss_ids(limit=10)

    assert result["status"] == "ok"
    assert result["updated"] == 0


# ---------------------------------------------------------------------------
# backfill_conclusions_from_tldr — no rows
# ---------------------------------------------------------------------------

def test_backfill_conclusions_from_tldr_no_rows():
    from workers.transformation import backfill_conclusions_from_tldr
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("TLDR",), ("SS_ID",)],  # DESC TABLE
        [],  # no eligible rows
    ]
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
        result = backfill_conclusions_from_tldr(limit=10)

    assert result["status"] == "ok"
    assert result["updated"] == 0


# ---------------------------------------------------------------------------
# _ss_get_json — success path
# ---------------------------------------------------------------------------

def test_ss_get_json_success():
    from workers.transformation import _ss_get_json
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


# ---------------------------------------------------------------------------
# _ss_post_json — success path
# ---------------------------------------------------------------------------

def test_ss_post_json_success():
    from workers.transformation import _ss_post_json
    mock_httpx = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = [{"paperId": "abc"}]
    mock_httpx.post.return_value = mock_resp
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        result = _ss_post_json("https://example.com", payload={"ids": ["ARXIV:2301.00001"]})
    assert result == [{"paperId": "abc"}]
