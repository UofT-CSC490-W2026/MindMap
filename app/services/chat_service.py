"""Chat service: grounded paper Q&A via the qa_worker."""

from __future__ import annotations

from fastapi import HTTPException

from app.config import DATABASE
from app.services.contracts import PaperChatResponse
from app.workers.qa_worker import answer_paper_question


async def answer_question(
    paper_id: int, question: str, session_id: str | None
) -> PaperChatResponse:
    """Delegate to answer_paper_question worker and map result to PaperChatResponse."""
    result = await answer_paper_question.remote.aio(
        paper_id=paper_id,
        question=question,
        session_id=session_id,
        database=DATABASE,
    )

    if not isinstance(result, dict):
        raise HTTPException(status_code=500, detail="QA worker returned unexpected response")

    if result.get("status") == "error":
        raise HTTPException(
            status_code=500,
            detail=result.get("error", "QA worker failed"),
        )

    return PaperChatResponse(
        paper_id=int(result.get("paper_id", paper_id)),
        session_id=result.get("session_id") or "",
        answer=result.get("answer") or "",
        cited_chunk_ids=result.get("cited_chunk_ids") or [],
        rewritten_query=result.get("rewritten_query"),
    )
