"""Pydantic models for grounded QA responses."""

from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class GroundedAnswer(BaseModel):
    answer: str = Field(default="", description="Answer grounded only in provided context")
    cited_chunk_ids: List[int] = Field(default_factory=list, description="Chunk ids supporting the answer")

    @field_validator("answer", mode="before")
    @classmethod
    def ensure_answer(cls, value):
        if value is None:
            return ""
        return str(value).strip()

    @field_validator("cited_chunk_ids", mode="before")
    @classmethod
    def ensure_chunk_ids(cls, value):
        if value is None:
            return []
        if isinstance(value, list):
            cleaned = []
            for item in value:
                try:
                    cleaned.append(int(item))
                except (TypeError, ValueError):
                    continue
            return cleaned
        return []


class ConversationTurn(BaseModel):
    role: str
    message: str
    rewritten_query: Optional[str] = None
    cited_chunk_ids: List[int] = Field(default_factory=list)
