# Feature: mindmap-serving-layer-refactor, Property 7
"""Property-based tests for search_service."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from app.services.contracts import SearchPaperResponse
from app.services import search_service


def _make_httpx_response(limit: int):
    """Build a mock httpx response with exactly `limit` results."""
    data = [
        {
            "title": f"Paper {i}",
            "authors": [{"name": f"Author {i}"}],
            "year": 2020 + (i % 5),
            "citationCount": i * 10,
            "externalIds": {"ArXiv": f"2301.{i:05d}"},
            "url": f"https://example.com/paper/{i}",
        }
        for i in range(limit)
    ]
    response = MagicMock()
    response.status_code = 200
    response.is_error = False
    response.json.return_value = {"data": data}
    return response


# ---------------------------------------------------------------------------
# Property 7: Search result count bounded by limit
# Validates: Requirements 7.1, 7.4, 7.5
# ---------------------------------------------------------------------------

@given(
    query=st.text(min_size=1),
    limit=st.integers(min_value=1, max_value=50),
)
@settings(max_examples=100)
def test_search_result_count_bounded_by_limit(query, limit):
    """For any query and limit, search_papers returns at most `limit` results."""
    mock_response = _make_httpx_response(limit)

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("app.services.search_service.httpx.AsyncClient", return_value=mock_client):
        results = asyncio.run(
            search_service.search_papers(query, limit)
        )

    assert len(results) <= limit, f"Expected <= {limit} results, got {len(results)}"
    for item in results:
        SearchPaperResponse.model_validate(item.model_dump())
