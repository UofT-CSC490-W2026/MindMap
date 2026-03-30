from fastapi import APIRouter

from app.services import chat_service, paper_service
from app.services.contracts import (
    PaperChatRequest,
    PaperChatResponse,
    PaperDetailResponse,
    PaperSummaryResponse,
)

router = APIRouter()


@router.get("/papers/{paper_id}", response_model=PaperDetailResponse)
async def get_paper(paper_id: int):
    return await paper_service.get_paper_detail(paper_id=paper_id)


@router.get("/papers/{paper_id}/summary", response_model=PaperSummaryResponse)
async def get_paper_summary(paper_id: int):
    return await paper_service.get_paper_summary(paper_id=paper_id)


@router.post("/papers/{paper_id}/chat", response_model=PaperChatResponse)
async def chat_with_paper(paper_id: int, request: PaperChatRequest):
    return await chat_service.answer_question(
        paper_id=paper_id,
        question=request.question,
        session_id=request.session_id,
    )
