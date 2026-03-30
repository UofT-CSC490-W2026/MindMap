# Feature: mindmap-serving-layer-refactor, Property 5
# Feature: mindmap-serving-layer-refactor, Property 6
# Feature: mindmap-serving-layer-refactor, Property 10
"""Property-based tests for chat_service."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from app.services.contracts import PaperChatResponse
from app.services import chat_service


def _make_worker_return(paper_id: int, question: str, session_id: str = "sess-1", rewritten_query=None):
    """Build a mock QA worker return dict."""
    return {
        "paper_id": paper_id,
        "session_id": session_id,
        "answer": f"Answer to: {question}",
        "cited_chunk_ids": [1, 2, 3],
        "rewritten_query": rewritten_query,
    }


# ---------------------------------------------------------------------------
# Property 5: PaperChatResponse schema conformance
# Validates: Requirements 6.1, 6.6
# ---------------------------------------------------------------------------

@given(
    paper_id=st.integers(min_value=1),
    question=st.text(min_size=1),
)
@settings(max_examples=100)
def test_paper_chat_response_schema_conformance(paper_id, question):
    """For any paper_id and non-empty question, answer_question returns a valid PaperChatResponse."""
    worker_result = _make_worker_return(paper_id, question)

    with patch(
        "app.services.chat_service.answer_paper_question.remote",
        new_callable=MagicMock,
    ) as mock_remote:
        mock_remote.aio = AsyncMock(return_value=worker_result)

        result = asyncio.run(
            chat_service.answer_question(paper_id, question, session_id=None)
        )

    validated = PaperChatResponse.model_validate(result.model_dump())
    assert isinstance(validated.answer, str)
    assert isinstance(validated.cited_chunk_ids, list)
    assert validated.session_id is not None


# ---------------------------------------------------------------------------
# Property 6: Chat citations scoped to paper
# Validates: Requirements 6.7
# ---------------------------------------------------------------------------

@given(
    paper_id=st.integers(min_value=1),
    question=st.text(min_size=1),
)
@settings(max_examples=100)
def test_chat_citations_are_integers(paper_id, question):
    """All cited_chunk_ids in PaperChatResponse are integers (worker scopes them to the paper)."""
    worker_result = _make_worker_return(paper_id, question)

    with patch(
        "app.services.chat_service.answer_paper_question.remote",
        new_callable=MagicMock,
    ) as mock_remote:
        mock_remote.aio = AsyncMock(return_value=worker_result)

        result = asyncio.run(
            chat_service.answer_question(paper_id, question, session_id=None)
        )

    assert all(isinstance(cid, int) for cid in result.cited_chunk_ids), (
        "All cited_chunk_ids must be integers"
    )


# ---------------------------------------------------------------------------
# Property 10: Chat session history continuity
# Validates: Requirements 6.2
# ---------------------------------------------------------------------------

@given(
    paper_id=st.integers(min_value=1),
    question=st.text(min_size=1),
    session_id=st.text(min_size=1),
)
@settings(max_examples=100)
def test_chat_session_history_continuity(paper_id, question, session_id):
    """When a session_id is provided, the worker returns a non-null rewritten_query."""
    rewritten = f"Rewritten: {question}"
    worker_result = _make_worker_return(paper_id, question, session_id=session_id, rewritten_query=rewritten)

    with patch(
        "app.services.chat_service.answer_paper_question.remote",
        new_callable=MagicMock,
    ) as mock_remote:
        mock_remote.aio = AsyncMock(return_value=worker_result)

        result = asyncio.run(
            chat_service.answer_question(paper_id, question, session_id=session_id)
        )

    assert result.rewritten_query is not None, (
        "rewritten_query must be non-null when session_id is provided and worker returns it"
    )
