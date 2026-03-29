# Feature: mindmap-serving-layer-refactor, Property 3
# Feature: mindmap-serving-layer-refactor, Property 4
"""Property-based tests for paper_service."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from app.services.contracts import PaperDetailResponse, PaperSummaryResponse
from app.services import paper_service


def _make_detail_cursor(paper_id: int):
    """Return a mock cursor that yields a valid paper detail row."""
    raw_payload = json.dumps({
        "authors": ["Author A", "Author B"],
        "year": 2020,
        "citationCount": 42,
    })
    cursor = MagicMock()
    cursor.fetchone.return_value = (paper_id, f"Title {paper_id}", "Abstract text.", f"arxiv_{paper_id}", raw_payload)
    cursor.execute.return_value = None
    cursor.close.return_value = None
    return cursor


def _make_summary_cursor(paper_id: int):
    """Return a mock cursor that yields a valid summary row."""
    summary_json = json.dumps({
        "research_question": "What is the effect?",
        "methods": ["Method A"],
        "main_claims": ["Claim 1"],
        "key_findings": ["Finding 1"],
        "limitations": ["Limitation 1"],
        "conclusion": "Conclusion text.",
    })
    cursor = MagicMock()
    cursor.fetchone.return_value = (summary_json,)
    cursor.execute.return_value = None
    cursor.close.return_value = None
    return cursor


def _make_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.close.return_value = None
    return conn


# ---------------------------------------------------------------------------
# Property 3: PaperDetailResponse schema conformance
# Validates: Requirements 4.1, 4.3
# ---------------------------------------------------------------------------

@given(paper_id=st.integers(min_value=1))
@settings(max_examples=100)
def test_paper_detail_response_schema_conformance(paper_id):
    """For any valid paper_id, get_paper_detail returns a valid PaperDetailResponse."""
    cursor = _make_detail_cursor(paper_id)
    conn = _make_conn(cursor)

    with patch("app.services.paper_service.connect_to_snowflake", return_value=conn):
        result = asyncio.get_event_loop().run_until_complete(
            paper_service.get_paper_detail(paper_id)
        )

    validated = PaperDetailResponse.model_validate(result.model_dump())
    assert validated.paper_id == paper_id
    assert isinstance(validated.title, str)
    assert isinstance(validated.authors, list)


# ---------------------------------------------------------------------------
# Property 4: PaperSummaryResponse schema conformance
# Validates: Requirements 5.1, 5.3
# ---------------------------------------------------------------------------

@given(paper_id=st.integers(min_value=1))
@settings(max_examples=100)
def test_paper_summary_response_schema_conformance(paper_id):
    """For any valid paper_id with a summary, get_paper_summary returns a valid PaperSummaryResponse."""
    cursor = _make_summary_cursor(paper_id)
    conn = _make_conn(cursor)

    with patch("app.services.paper_service.connect_to_snowflake", return_value=conn):
        result = asyncio.get_event_loop().run_until_complete(
            paper_service.get_paper_summary(paper_id)
        )

    validated = PaperSummaryResponse.model_validate(result.model_dump())
    assert validated.paper_id == paper_id
    assert isinstance(validated.methods, list)
    assert isinstance(validated.main_claims, list)
    assert isinstance(validated.key_findings, list)
    assert isinstance(validated.limitations, list)
