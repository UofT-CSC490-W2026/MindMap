from typing import Any, Dict, List

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.services import search_service
from app.services.contracts import SearchPaperResponse

router = APIRouter()


@router.get("/search/papers")
@router.get("/papers/search")
async def search_papers(
    query: str = Query(..., min_length=1),
    limit: int = Query(default=10, ge=1, le=50),
    fields: str = Query(default=None),
) -> JSONResponse:
    results = await search_service.search_papers(query=query, limit=limit)
    # Return SS-compatible shape: { data: [ { paperId, title, authors, year, citationCount, externalIds } ] }
    data = [
        {
            "paperId": r.arxiv_id or "",
            "title": r.title,
            "authors": [{"name": a} for a in r.authors],
            "year": r.year,
            "citationCount": r.citation_count,
            "externalIds": {"ArXiv": r.arxiv_id} if r.arxiv_id else {},
            "url": r.external_url,
        }
        for r in results
    ]
    return JSONResponse({"data": data})
