from typing import List

from fastapi import APIRouter, Query

from app.services import search_service
from app.services.contracts import SearchPaperResponse

router = APIRouter()


@router.get("/search/papers", response_model=List[SearchPaperResponse])
@router.get("/papers/search", response_model=List[SearchPaperResponse])
async def search_papers(
    query: str = Query(..., min_length=1),
    limit: int = Query(default=10, ge=1, le=50),
    fields: str = Query(default=None),  # ignored, accepted for Semantic Scholar API compatibility
):
    return await search_service.search_papers(query=query, limit=limit)
