"""Tests for app/services/search_service.py"""
import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.search_service import search_papers, _map_result


# --- _map_result ---

def test_map_result_basic():
    item = {
        "title": "Attention Is All You Need",
        "authors": [{"name": "Vaswani"}, {"name": "Shazeer"}],
        "year": 2017,
        "citationCount": 50000,
        "externalIds": {"ArXiv": "1706.03762"},
        "url": "https://arxiv.org/abs/1706.03762",
    }
    result = _map_result(item)
    assert result.title == "Attention Is All You Need"
    assert result.authors == ["Vaswani", "Shazeer"]
    assert result.year == 2017
    assert result.citation_count == 50000
    assert result.arxiv_id == "1706.03762"
    assert result.external_url == "https://arxiv.org/abs/1706.03762"


def test_map_result_missing_fields():
    result = _map_result({})
    assert result.title == ""
    assert result.authors == []
    assert result.year is None
    assert result.arxiv_id is None


def test_map_result_filters_empty_author_names():
    item = {"authors": [{"name": "Alice"}, {"name": ""}, {}]}
    result = _map_result(item)
    assert result.authors == ["Alice"]


def test_map_result_no_arxiv_id():
    item = {"externalIds": {"DOI": "10.1234/xyz"}}
    result = _map_result(item)
    assert result.arxiv_id is None


# --- search_papers ---

@pytest.mark.anyio
async def test_search_papers_returns_results():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.is_error = False
    mock_response.json.return_value = {
        "data": [
            {
                "title": "Test Paper",
                "authors": [{"name": "Author A"}],
                "year": 2023,
                "citationCount": 10,
                "externalIds": {"ArXiv": "2301.00001"},
                "url": "https://arxiv.org/abs/2301.00001",
            }
        ]
    }

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_response):
        results = await search_papers("transformers", limit=5)

    assert len(results) == 1
    assert results[0].title == "Test Paper"


@pytest.mark.anyio
async def test_search_papers_returns_empty_on_error():
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.is_error = True

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_response):
        results = await search_papers("query", limit=5)

    assert results == []


@pytest.mark.anyio
async def test_search_papers_retries_on_429_then_succeeds():
    rate_limited = MagicMock()
    rate_limited.status_code = 429
    rate_limited.is_error = False
    rate_limited.headers = {"Retry-After": "0"}

    success = MagicMock()
    success.status_code = 200
    success.is_error = False
    success.json.return_value = {"data": [{"title": "Paper", "authors": [], "externalIds": {}}]}

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, side_effect=[rate_limited, success]):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            results = await search_papers("query", limit=5)

    assert len(results) == 1


@pytest.mark.anyio
async def test_search_papers_returns_empty_after_all_429():
    rate_limited = MagicMock()
    rate_limited.status_code = 429
    rate_limited.is_error = False
    rate_limited.headers = {}

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=rate_limited):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            results = await search_papers("query", limit=5)

    assert results == []


@pytest.mark.anyio
async def test_search_papers_returns_empty_on_http_error():
    with patch(
        "httpx.AsyncClient.get",
        new_callable=AsyncMock,
        side_effect=httpx.HTTPError("connection failed"),
    ):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            results = await search_papers("query", limit=5)

    assert results == []


@pytest.mark.anyio
async def test_search_papers_respects_limit():
    items = [{"title": f"Paper {i}", "authors": [], "externalIds": {}} for i in range(10)]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.is_error = False
    mock_response.json.return_value = {"data": items}

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_response):
        results = await search_papers("query", limit=3)

    assert len(results) == 3
