"""
Tests for workers/transformation.py

httpx is mocked in sys.modules before the module is imported
so the heavy dep is never loaded.
"""

import sys
import pytest
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
    with patch.object(workers.transformation, "fetch_connections_ss", mock_fetch):
        result = get_references("2301.00001")
    assert result["source"] == "api"
    assert len(result["data"]) == 1


def test_get_references_fallback_pdf():
    from workers.transformation import get_references
    mock_fetch = MagicMock()
    mock_fetch.remote.return_value = []  # API returns nothing

    mock_pdf = MagicMock()
    mock_pdf.remote.return_value = ["[1] Some reference text here"]

    with patch.object(workers.transformation, "fetch_connections_ss", mock_fetch):
        with patch.object(workers.transformation, "extract_references_pdf", mock_pdf):
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


def test_retry_delay_from_response_invalid_retry_after_header():
    response = MagicMock()
    response.headers = {"Retry-After": "bad-value"}
    delay = _retry_delay_from_response(response, attempt=1, base=1.0, cap=5.0)
    assert 0.0 <= delay <= 5.0


def test_ss_get_json_retries_transient_error_then_succeeds():
    from workers.transformation import _ss_get_json

    first = MagicMock(status_code=500)
    first.raise_for_status.side_effect = Exception("should not be raised")
    second = MagicMock(status_code=200)
    second.raise_for_status.return_value = None
    second.json.return_value = {"ok": True}
    mock_httpx = MagicMock()
    mock_httpx.get.side_effect = [first, second]
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        with patch("workers.transformation.time.sleep"):
            result = _ss_get_json("https://example.com", params={})

    assert result == {"ok": True}


def test_ss_post_json_retries_transient_error_then_succeeds():
    from workers.transformation import _ss_post_json

    first = MagicMock(status_code=429)
    first.raise_for_status.side_effect = Exception("should not be raised")
    second = MagicMock(status_code=200)
    second.raise_for_status.return_value = None
    second.json.return_value = [{"paperId": "abc"}]
    mock_httpx = MagicMock()
    mock_httpx.post.side_effect = [first, second]
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        with patch("workers.transformation.time.sleep"):
            result = _ss_post_json("https://example.com", payload={"ids": ["ARXIV:1"]})

    assert result == [{"paperId": "abc"}]


def test_arxiv_get_pdf_bytes_retries_http_error_then_succeeds():
    from workers.transformation import _arxiv_get_pdf_bytes

    class FakeHTTPError(Exception):
        pass

    success = MagicMock(status_code=200, content=b"pdf-bytes", text="")
    success.raise_for_status.return_value = None
    mock_httpx = MagicMock()
    mock_httpx.get.side_effect = [FakeHTTPError("boom"), success]
    mock_httpx.HTTPError = FakeHTTPError
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        with patch("workers.transformation.time.sleep"):
            result = _arxiv_get_pdf_bytes("2301.00001", max_attempts=2)

    assert result == b"pdf-bytes"


def test_fetch_ss_batch_rows_resilient_non_list_response_returns_nones():
    from workers.transformation import _fetch_ss_batch_rows_resilient

    with patch("workers.transformation._ss_post_json", return_value={"unexpected": True}):
        result = _fetch_ss_batch_rows_resilient("https://example.com", ["a", "b"], params={})

    assert result == [None, None]


def test_fetch_ss_batch_rows_resilient_recurses_on_batch_failure():
    from workers.transformation import _fetch_ss_batch_rows_resilient

    calls = []

    def fake_post(url, payload, params, timeout):
        calls.append(tuple(payload["ids"]))
        if len(payload["ids"]) > 1:
            raise RuntimeError("batch failed")
        return [{"paperId": payload["ids"][0]}]

    with patch("workers.transformation._ss_post_json", side_effect=fake_post):
        result = _fetch_ss_batch_rows_resilient("https://example.com", ["a", "b"], params={})

    assert result == [{"paperId": "a"}, {"paperId": "b"}]
    assert calls[0] == ("a", "b")


def test_resolve_bronze_payload_column_raises_when_missing():
    from workers.transformation import _resolve_bronze_payload_column

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("OTHER_COL",)]

    with pytest.raises(RuntimeError, match="Could not find raw payload column"):
        _resolve_bronze_payload_column(mock_cursor)


def test_fetch_connections_ss_citations_mode():
    from workers.transformation import fetch_connections_ss

    with patch(
        "workers.transformation._ss_get_json",
        return_value={
            "data": [
                {"citingPaper": {"title": "T", "year": 2024, "paperId": "ss1", "externalIds": {"DOI": "10.1"}}},
                None,
            ]
        },
    ):
        result = fetch_connections_ss("2301.00001", mode=1)

    assert result == [{"title": "T", "year": 2024, "arxiv_id": None, "doi": "10.1", "ss_paper_id": "ss1"}]


def test_get_references_returns_none_source_when_api_and_pdf_empty():
    from workers.transformation import get_references

    with patch("workers.transformation.fetch_connections_ss", MagicMock(remote=MagicMock(return_value=None))):
        with patch("workers.transformation.extract_references_pdf", MagicMock(remote=MagicMock(return_value=None))):
            result = get_references("2301.00001")

    assert result == {"source": "none", "data": []}


def test_main_returns_when_no_ids_to_process():
    from workers.transformation import main

    with patch("workers.transformation.get_bronze_worklist", MagicMock(remote=MagicMock(return_value=[]))):
        assert main(parallel=1) is None


def test_backfill_missing_ss_ids_with_updates():
    from workers.transformation import backfill_missing_ss_ids

    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("SS_ID",)],
        [(1, "2301.00001"), (2, "2301.00002")],
    ]
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
        with patch(
            "workers.transformation._ss_post_json",
            return_value=[{"paperId": "ss1"}, {"paperId": "ss2"}],
        ):
            result = backfill_missing_ss_ids(limit=10, batch_size=2)

    assert result["updated"] == 2
    mock_cursor.executemany.assert_called_once()


def test_backfill_conclusions_from_tldr_with_updates_and_overwrite():
    from workers.transformation import backfill_conclusions_from_tldr

    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("TLDR",), ("SS_ID",)],
        [(1, "2301.00001"), (2, "2301.00002")],
    ]
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
        with patch(
            "workers.transformation._fetch_ss_batch_tldr",
            return_value={
                "2301.00001": {"tldr": "Summary A", "ss_id": "ss1"},
                "2301.00002": {"tldr": "Summary B", "ss_id": "ss2"},
            },
        ):
            result = backfill_conclusions_from_tldr(limit=10, batch_size=2, overwrite_existing=True)

    assert result["updated"] == 2
    assert result["ss_id_updated"] == 2


def test_ss_get_json_401_then_success_without_key():
    from workers.transformation import _ss_get_json

    unauthorized = MagicMock(status_code=401)
    unauthorized.raise_for_status.return_value = None
    success = MagicMock(status_code=200)
    success.raise_for_status.return_value = None
    success.json.return_value = {"ok": True}
    mock_httpx = MagicMock()
    mock_httpx.get.side_effect = [unauthorized, success]
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        result = _ss_get_json("https://example.com", params={})

    assert result == {"ok": True}


def test_ss_get_json_raises_runtime_error_on_non_retryable_http_error():
    from workers.transformation import _ss_get_json

    class FakeHTTPStatusError(Exception):
        def __init__(self, response):
            self.response = response

    response = MagicMock(status_code=404)
    response.text = "missing"
    response.raise_for_status.side_effect = FakeHTTPStatusError(response)
    mock_httpx = MagicMock()
    mock_httpx.get.return_value = response
    mock_httpx.HTTPStatusError = FakeHTTPStatusError

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        with pytest.raises(RuntimeError, match="Semantic Scholar GET failed"):
            _ss_get_json("https://example.com", params={})


def test_ss_post_json_401_then_success_without_key():
    from workers.transformation import _ss_post_json

    unauthorized = MagicMock(status_code=403)
    unauthorized.raise_for_status.return_value = None
    success = MagicMock(status_code=200)
    success.raise_for_status.return_value = None
    success.json.return_value = {"ok": True}
    mock_httpx = MagicMock()
    mock_httpx.post.side_effect = [unauthorized, success]
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        result = _ss_post_json("https://example.com", payload={})

    assert result == {"ok": True}


def test_ss_post_json_raises_runtime_error_on_non_retryable_http_error():
    from workers.transformation import _ss_post_json

    class FakeHTTPStatusError(Exception):
        def __init__(self, response):
            self.response = response

    response = MagicMock(status_code=404)
    response.text = "missing"
    response.raise_for_status.side_effect = FakeHTTPStatusError(response)
    mock_httpx = MagicMock()
    mock_httpx.post.return_value = response
    mock_httpx.HTTPStatusError = FakeHTTPStatusError

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        with pytest.raises(RuntimeError, match="Semantic Scholar POST failed"):
            _ss_post_json("https://example.com", payload={})


def test_arxiv_get_pdf_bytes_raises_after_http_error():
    from workers.transformation import _arxiv_get_pdf_bytes

    class FakeHTTPError(Exception):
        pass

    mock_httpx = MagicMock()
    mock_httpx.get.side_effect = FakeHTTPError("boom")
    mock_httpx.HTTPError = FakeHTTPError
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        with patch("workers.transformation.time.sleep"):
            with pytest.raises(RuntimeError, match="request failed"):
                _arxiv_get_pdf_bytes("2301.00001", max_attempts=1)


def test_arxiv_get_pdf_bytes_raises_after_http_status_error():
    from workers.transformation import _arxiv_get_pdf_bytes

    response = MagicMock(status_code=404, text="missing")
    response.raise_for_status.side_effect = Exception("status")
    mock_httpx = MagicMock()
    mock_httpx.get.return_value = response
    mock_httpx.HTTPError = Exception
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        with patch("workers.transformation.time.sleep"):
            with pytest.raises(RuntimeError, match="download failed"):
                _arxiv_get_pdf_bytes("2301.00001", max_attempts=1)


def test_arxiv_get_pdf_bytes_retries_transient_status_then_succeeds():
    from workers.transformation import _arxiv_get_pdf_bytes

    transient = MagicMock(status_code=429, text="")
    transient.raise_for_status.return_value = None
    success = MagicMock(status_code=200, content=b"pdf", text="")
    success.raise_for_status.return_value = None
    mock_httpx = MagicMock()
    mock_httpx.get.side_effect = [transient, success]
    mock_httpx.HTTPError = Exception
    mock_httpx.HTTPStatusError = Exception

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        with patch("workers.transformation.time.sleep"):
            result = _arxiv_get_pdf_bytes("2301.00001", max_attempts=2)

    assert result == b"pdf"


def test_extract_connections_skips_non_dict_items():
    from workers.transformation import _extract_connections

    result = _extract_connections([None, "bad", {"citedPaper": {"title": "T"}}], relation_key="citedPaper", limit=5)

    assert len(result) == 1


def test_fetch_ss_batch_metadata_skips_non_dict_rows_and_non_dict_tldr():
    from workers.transformation import _fetch_ss_batch_metadata

    with patch(
        "workers.transformation._fetch_ss_batch_rows_resilient",
        return_value=[None, {"paperId": "ss2", "tldr": "plain", "references": [], "citations": []}],
    ):
        result = _fetch_ss_batch_metadata(["a1", "a2"], batch_size=2)

    assert "a1" not in result
    assert result["a2"]["tldr"] is None


def test_fetch_ss_batch_tldr_skips_non_dict_rows_and_non_dict_tldr():
    from workers.transformation import _fetch_ss_batch_tldr

    with patch(
        "workers.transformation._fetch_ss_batch_rows_resilient",
        return_value=[None, {"paperId": "ss2", "tldr": "plain"}],
    ):
        result = _fetch_ss_batch_tldr(["a1", "a2"], batch_size=2)

    assert "a1" not in result
    assert result["a2"]["tldr"] is None


def test_require_columns_raises():
    from workers.transformation import _require_columns

    with pytest.raises(RuntimeError, match="Missing required columns"):
        _require_columns({"id": '"ID"'}, ["id", "missing"], "TABLE")


def test_clean_extracted_text_empty():
    assert _clean_extracted_text("") == ""


def test_extract_conclusion_from_text_empty():
    assert _extract_conclusion_from_text("") == ""


def test_extract_conclusion_stops_at_bibliography():
    text = "\nConclusion\n" + ("Useful finding " * 10) + "\nBibliography\nignored"
    result = _extract_conclusion_from_text(text)
    assert "Bibliography" not in result


def test_extract_full_text_pdf_parse_failed():
    mock_pymupdf = MagicMock()
    mock_pymupdf.open.side_effect = RuntimeError("bad pdf")

    with patch("workers.transformation._arxiv_get_pdf_bytes", return_value=b"pdf"):
        with patch.dict(sys.modules, {"pymupdf": mock_pymupdf}):
            result = extract_full_text_pdf("2301.00001")

    assert result["source"] == "parse_failed"


def test_extract_full_text_pdf_empty_pdf_and_skips_blank_pages():
    blank_page = MagicMock()
    blank_page.get_text.return_value = "   "
    mock_doc = MagicMock()
    mock_doc.__len__.return_value = 1
    mock_doc.__getitem__.return_value = blank_page

    mock_pymupdf = MagicMock()
    mock_pymupdf.open.return_value = mock_doc

    with patch("workers.transformation._arxiv_get_pdf_bytes", return_value=b"pdf"):
        with patch.dict(sys.modules, {"pymupdf": mock_pymupdf}):
            result = extract_full_text_pdf("2301.00001")

    assert result["source"] == "empty_pdf"
    assert result["pages_processed"] == 1


def test_extract_full_text_pdf_truncates_when_page_exceeds_limit():
    long_text = "word " * 80000
    page = MagicMock()
    page.get_text.return_value = long_text
    mock_doc = MagicMock()
    mock_doc.__len__.return_value = 1
    mock_doc.__getitem__.return_value = page

    mock_pymupdf = MagicMock()
    mock_pymupdf.open.return_value = mock_doc

    with patch("workers.transformation._arxiv_get_pdf_bytes", return_value=b"pdf"):
        with patch.dict(sys.modules, {"pymupdf": mock_pymupdf}):
            result = extract_full_text_pdf("2301.00001")

    assert result["truncated"] is True
    assert result["source"] == "pdf"


def test_extract_full_text_pdf_download_error_returns_unavailable():
    with patch("workers.transformation._arxiv_get_pdf_bytes", side_effect=RuntimeError("download failed")):
        with patch.dict(sys.modules, {"pymupdf": MagicMock()}):
            result = extract_full_text_pdf("2301.00001")

    assert result["source"] == "unavailable"


def test_extract_conclusion_success_path():
    from workers.transformation import extract_conclusion

    with patch(
        "workers.transformation.extract_full_text_pdf",
        MagicMock(local=MagicMock(return_value={"full_text": "\nConclusion\nKey finding"})),
    ):
        result = extract_conclusion("2301.00001")

    assert "Conclusion" in result


def test_extract_conclusion_handles_exception():
    from workers.transformation import extract_conclusion

    with patch("workers.transformation.extract_full_text_pdf", MagicMock(local=MagicMock(side_effect=RuntimeError("boom")))):
        result = extract_conclusion("2301.00001")

    assert result == ""


def test_fetch_connections_ss_skips_none_ids_and_missing_paper():
    from workers.transformation import fetch_connections_ss

    with patch(
        "workers.transformation._ss_get_json",
        return_value={"data": [{"citedPaper": None}, {"citedPaper": {"externalIds": None}}]},
    ):
        result = fetch_connections_ss("2301.00001", mode=0)

    assert result == []


def test_extract_references_pdf_parse_error_returns_empty_list():
    from workers.transformation import extract_references_pdf

    mock_doc_ctx = MagicMock()
    mock_doc_ctx.__enter__.side_effect = RuntimeError("parse boom")
    mock_fitz = MagicMock()
    mock_fitz.open.return_value = mock_doc_ctx

    with patch("workers.transformation._arxiv_get_pdf_bytes", return_value=b"pdf"):
        with patch.dict(sys.modules, {"fitz": mock_fitz}):
            result = extract_references_pdf("2301.00001")

    assert result == []


def test_transform_to_silver_prefetched_without_tldr_uses_conclusion_fallback():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("RAW_PAYLOAD",)],
        [("ARXIV_ID",), ("SS_ID",), ("CONCLUSION",), ("FULL_TEXT",), ("FULL_TEXT_SOURCE",), ("FULL_TEXT_EXTRACTED_AT",), ("TLDR",), ("REFERENCE_LIST",), ("CITATION_LIST",), ("TITLE",), ("ABSTRACT",)],
    ]
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.transformation.extract_full_text_pdf", MagicMock(local=MagicMock(return_value={"full_text": "\nConclusion\nUseful text", "source": "pdf"}))):
        with patch("workers.transformation.get_references", MagicMock(local=MagicMock(return_value={"data": []}))):
            with patch("workers.transformation.fetch_connections_ss", MagicMock(local=MagicMock(return_value=[]))):
                with patch("workers.transformation.extract_conclusion", MagicMock(local=MagicMock(return_value="fallback conclusion"))):
                    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
                        transform_to_silver("2301.00001", ss_prefetched={"references": [], "citations": [], "ss_id": "ss1", "tldr": ""})

    args = mock_cursor.execute.call_args[0][1]
    assert args[5] == "Conclusion\nUseful text"


def test_transform_to_silver_database_error_rolls_back():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("RAW_PAYLOAD",)],
        [("ARXIV_ID",), ("SS_ID",), ("CONCLUSION",), ("FULL_TEXT",), ("FULL_TEXT_SOURCE",), ("FULL_TEXT_EXTRACTED_AT",), ("TLDR",), ("REFERENCE_LIST",), ("CITATION_LIST",), ("TITLE",), ("ABSTRACT",)],
    ]
    mock_cursor.execute.side_effect = [None, None, RuntimeError("db merge failed")]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.transformation.extract_full_text_pdf", MagicMock(local=MagicMock(return_value={"full_text": "", "source": "unavailable"}))):
        with patch("workers.transformation.get_references", MagicMock(local=MagicMock(return_value={"data": []}))):
            with patch("workers.transformation.fetch_connections_ss", MagicMock(local=MagicMock(return_value=[]))):
                with patch("workers.transformation.extract_conclusion", MagicMock(local=MagicMock(return_value="fallback"))):
                    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
                        with pytest.raises(RuntimeError, match="Database Error"):
                            transform_to_silver("2301.00001", ss_prefetched={"references": [], "citations": [], "ss_id": "ss1", "tldr": ""})

    mock_conn.rollback.assert_called_once()


def test_transform_to_silver_non_prefetched_ss_lookup_failure_is_ignored():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("RAW_PAYLOAD",)],
        [("ARXIV_ID",), ("SS_ID",), ("CONCLUSION",), ("FULL_TEXT",), ("FULL_TEXT_SOURCE",), ("FULL_TEXT_EXTRACTED_AT",), ("TLDR",), ("REFERENCE_LIST",), ("CITATION_LIST",), ("TITLE",), ("ABSTRACT",)],
    ]
    mock_cursor.execute.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.transformation.extract_full_text_pdf", MagicMock(local=MagicMock(return_value={"full_text": "", "source": "unavailable"}))):
        with patch("workers.transformation.get_references", MagicMock(local=MagicMock(return_value={"data": []}))):
            with patch("workers.transformation.fetch_connections_ss", MagicMock(local=MagicMock(return_value=[]))):
                with patch("workers.transformation.extract_conclusion", MagicMock(local=MagicMock(return_value="fallback"))):
                    with patch("workers.transformation._ss_get_json", side_effect=RuntimeError("lookup failed")):
                        with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
                            transform_to_silver("2301.00001", ss_prefetched=None)

    args = mock_cursor.execute.call_args[0][1]
    assert args[1] is None


def test_transform_to_silver_outer_exception_is_swallowed():
    with patch("workers.transformation.extract_full_text_pdf", MagicMock(local=MagicMock(side_effect=RuntimeError("boom")))):
        with pytest.raises(RuntimeError, match="boom"):
            transform_to_silver("2301.00001")


def test_main_parallel_prefetch_and_sequential_error_continue():
    from workers.transformation import main

    with patch("workers.transformation.get_bronze_worklist", MagicMock(remote=MagicMock(return_value=["a1", "a2"]))):
        with patch("workers.transformation._fetch_ss_batch_metadata", return_value={"a1": {"ss_id": "x"}}):
            remote = MagicMock(side_effect=[RuntimeError("bad"), None])
            with patch("workers.transformation.transform_to_silver", MagicMock(remote=remote)):
                assert main(parallel=0) is None


def test_main_parallel_branch_dispatches_remote_calls():
    from workers.transformation import main

    remote = MagicMock()
    with patch("workers.transformation.get_bronze_worklist", MagicMock(remote=MagicMock(return_value=["a1", "a2"]))):
        with patch("workers.transformation._fetch_ss_batch_metadata", return_value={"a1": {"ss_id": "x"}, "a2": {"ss_id": "y"}}):
            with patch("workers.transformation.transform_to_silver", MagicMock(remote=remote)):
                assert main(parallel=1) is None

    assert remote.call_count == 2


def test_process_single_silver_orchestrates_prefetch_and_transform():
    from workers.transformation import process_single_silver

    with patch("workers.transformation._fetch_ss_batch_metadata", return_value={"x1": {"ss_id": "ssx"}}):
        remote = MagicMock(return_value={"status": "ok"})
        with patch("workers.transformation.transform_to_silver", MagicMock(remote=remote)):
            result = process_single_silver("x1", database="DB")

    assert result == {"status": "ok"}
    remote.assert_called_once()


def test_backfill_missing_ss_ids_batch_failure_and_invalid_rows():
    from workers.transformation import backfill_missing_ss_ids

    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("SS_ID",)],
        [(1, "2301.00001"), (2, "2301.00002")],
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.transformation._ss_post_json", side_effect=RuntimeError("batch failed")):
            result = backfill_missing_ss_ids(limit=10, batch_size=2)

    assert result["resolved"] == 0
    assert result["updated"] == 0


def test_backfill_missing_ss_ids_skips_items_without_paper_id():
    from workers.transformation import backfill_missing_ss_ids

    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("SS_ID",)],
        [(1, "2301.00001")],
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.transformation._ss_post_json", return_value=[{"externalIds": {}}]):
            result = backfill_missing_ss_ids(limit=10, batch_size=1)

    assert result["resolved"] == 0
    assert result["updated"] == 0


def test_backfill_conclusions_from_tldr_updates_only_ss_id_when_tldr_missing():
    from workers.transformation import backfill_conclusions_from_tldr

    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("TLDR",), ("SS_ID",)],
        [(1, "2301.00001")],
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.transformation._fetch_ss_batch_tldr", return_value={"2301.00001": {"tldr": "", "ss_id": "ss1"}}):
            result = backfill_conclusions_from_tldr(limit=10, batch_size=1, overwrite_existing=False)

    assert result["updated"] == 0
    assert result["ss_id_updated"] == 1


def test_backfill_conclusions_from_tldr_non_overwrite_updates_tldr():
    from workers.transformation import backfill_conclusions_from_tldr

    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("TLDR",), ("SS_ID",)],
        [(1, "2301.00001")],
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.transformation.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.transformation._fetch_ss_batch_tldr", return_value={"2301.00001": {"tldr": "Summary", "ss_id": None}}):
            result = backfill_conclusions_from_tldr(limit=10, batch_size=1, overwrite_existing=False)

    assert result["updated"] == 1
