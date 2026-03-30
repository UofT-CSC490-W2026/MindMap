"""Search service: queries Semantic Scholar with retry/backoff logic."""

from __future__ import annotations

import asyncio
import os
from typing import List

import httpx

from app.services.contracts import SearchPaperResponse

_SS_SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
_MAX_ATTEMPTS = 3
_RETRY_DELAYS = [0.6, 1.2]


async def search_papers(query: str, limit: int) -> List[SearchPaperResponse]:
    """Query Semantic Scholar and return up to `limit` results.

    Retries up to 3 times on 429 and 5xx responses with backoff.
    Returns [] on exhaustion rather than raising.
    """
    params = {
        "query": query,
        "limit": limit,
        "fields": "title,authors,year,citationCount,externalIds,url",
    }
    api_key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
    headers = {"x-api-key": api_key} if api_key else None

    async with httpx.AsyncClient(timeout=30.0) as client:
        for attempt in range(_MAX_ATTEMPTS):
            try:
                res = await client.get(_SS_SEARCH_URL, params=params, headers=headers)
            except httpx.HTTPError:
                if attempt < _MAX_ATTEMPTS - 1:
                    await asyncio.sleep(_RETRY_DELAYS[attempt])
                    continue
                return []

            if res.status_code == 429:
                if attempt < _MAX_ATTEMPTS - 1:
                    retry_after = res.headers.get("Retry-After")
                    try:
                        wait_s = float(retry_after) if retry_after is not None else _RETRY_DELAYS[attempt]
                    except (TypeError, ValueError):
                        wait_s = _RETRY_DELAYS[attempt]
                    await asyncio.sleep(max(0.0, wait_s))
                    continue
                return []

            if res.status_code >= 500 and attempt < _MAX_ATTEMPTS - 1:
                await asyncio.sleep(_RETRY_DELAYS[attempt])
                continue

            if res.is_error:
                return []

            data = res.json().get("data", [])
            return [_map_result(item) for item in data[:limit]]

    return []


def _map_result(item: dict) -> SearchPaperResponse:
    authors = [a.get("name", "") for a in (item.get("authors") or []) if a.get("name")]
    external_ids = item.get("externalIds") or {}
    arxiv_id = external_ids.get("ArXiv")
    return SearchPaperResponse(
        title=item.get("title") or "",
        authors=authors,
        year=item.get("year"),
        citation_count=item.get("citationCount"),
        arxiv_id=arxiv_id,
        external_url=item.get("url"),
    )
