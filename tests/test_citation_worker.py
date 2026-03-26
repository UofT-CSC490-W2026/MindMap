"""
Tests for workers/citation_worker.py — get_citations function.

feedparser, requests, and fitz are all imported inside the function body,
so we patch sys.modules directly so each `import X` inside the function
resolves to our mock objects.
"""

import sys
import pytest
from unittest.mock import MagicMock, patch

# Remove any previously injected mock for citation_worker so we import the real module
sys.modules.pop("workers.citation_worker", None)

from workers.citation_worker import get_citations


REF_TEXT = (
    "References\n"
    "[1] Smith et al. This is a reference entry that is long enough to pass the filter.\n"
    "[2] Jones et al. Another reference entry that is also long enough.\n"
)


def _make_feed_entry():
    mock_entry = MagicMock()
    mock_entry.get.side_effect = lambda key, default=None: {
        "title": "Test Paper",
        "summary": "Test abstract",
        "authors": [MagicMock(name="Author One")],
        "links": [MagicMock(type="application/pdf", href="https://arxiv.org/pdf/2301.00001.pdf")],
    }.get(key, default)
    return mock_entry


def _make_mock_doc(page_text):
    mock_page = MagicMock()
    mock_page.get_text.return_value = page_text

    mock_doc = MagicMock()
    mock_doc.__len__ = MagicMock(return_value=3)
    mock_doc.load_page.return_value = mock_page
    return mock_doc


def test_get_citations_happy_path():
    mock_entry = _make_feed_entry()

    mock_feedparser = MagicMock()
    mock_feed = MagicMock()
    mock_feed.entries = [mock_entry]
    mock_feedparser.parse.return_value = mock_feed

    mock_requests = MagicMock()
    mock_response = MagicMock()
    mock_response.content = b"%PDF-1.4 fake pdf content"
    mock_response.raise_for_status.return_value = None
    mock_requests.get.return_value = mock_response

    mock_fitz = MagicMock()
    mock_doc = _make_mock_doc(REF_TEXT)
    mock_fitz.open.return_value = mock_doc

    with patch.dict(sys.modules, {
        "feedparser": mock_feedparser,
        "requests": mock_requests,
        "fitz": mock_fitz,
    }):
        result = get_citations("2301.00001")

    assert isinstance(result, dict)
    assert "arxiv_metadata" in result
    assert "references" in result
    assert result["arxiv_metadata"]["arxiv_id"] == "2301.00001"


def test_get_citations_no_arxiv_entry():
    mock_feedparser = MagicMock()
    mock_feed = MagicMock()
    mock_feed.entries = []
    mock_feedparser.parse.return_value = mock_feed

    with patch.dict(sys.modules, {
        "feedparser": mock_feedparser,
        "requests": MagicMock(),
        "fitz": MagicMock(),
    }):
        with pytest.raises(ValueError):
            get_citations("9999.99999")


def test_get_citations_no_references_section():
    mock_entry = _make_feed_entry()

    mock_feedparser = MagicMock()
    mock_feed = MagicMock()
    mock_feed.entries = [mock_entry]
    mock_feedparser.parse.return_value = mock_feed

    mock_requests = MagicMock()
    mock_response = MagicMock()
    mock_response.content = b"%PDF-1.4 fake pdf content"
    mock_response.raise_for_status.return_value = None
    mock_requests.get.return_value = mock_response

    mock_fitz = MagicMock()
    mock_doc = _make_mock_doc("This page has no references section at all.")
    mock_fitz.open.return_value = mock_doc

    with patch.dict(sys.modules, {
        "feedparser": mock_feedparser,
        "requests": mock_requests,
        "fitz": mock_fitz,
    }):
        result = get_citations("2301.00001")

    assert result["references"] == []
