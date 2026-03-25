"""Pydantic models for structured paper summaries."""

from typing import Optional, List
from pydantic import BaseModel, Field, field_validator


class PaperSummary(BaseModel):
    """Structured summary extracted from a paper using LLM."""
    
    research_question: Optional[str] = Field(
        default="",
        description="Main research question or problem statement"
    )
    methods: List[str] = Field(
        default_factory=list,
        description="List of research methods used"
    )
    main_claims: List[str] = Field(
        default_factory=list,
        description="Primary claims or hypotheses"
    )
    key_findings: List[str] = Field(
        default_factory=list,
        description="Key empirical findings or results"
    )
    limitations: List[str] = Field(
        default_factory=list,
        description="Acknowledged limitations of the work"
    )
    conclusion: Optional[str] = Field(
        default="",
        description="Overall conclusion or implications"
    )
    
    @field_validator("methods", "main_claims", "key_findings", "limitations", mode="before")
    @classmethod
    def ensure_list(cls, v):
        """Ensure list fields are always lists, even if empty."""
        if v is None:
            return []
        if isinstance(v, str):
            # If single string provided, wrap in list
            return [v] if v.strip() else []
        if isinstance(v, list):
            # Filter out empty strings
            return [item for item in v if isinstance(item, str) and item.strip()]
        return []
    
    @field_validator("research_question", "conclusion", mode="before")
    @classmethod
    def ensure_string(cls, v):
        """Ensure string fields are always strings."""
        if v is None:
            return ""
        if isinstance(v, str):
            return v.strip()
        return str(v).strip()
    
    def is_empty(self) -> bool:
        """Check if summary contains any substantive content."""
        return (
            not self.research_question and
            not self.methods and
            not self.main_claims and
            not self.key_findings and
            not self.limitations and
            not self.conclusion
        )
    
    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
        return {
            "research_question": self.research_question,
            "methods": self.methods,
            "main_claims": self.main_claims,
            "key_findings": self.key_findings,
            "limitations": self.limitations,
            "conclusion": self.conclusion,
        }


class SummaryContext(BaseModel):
    """Context passed to LLM for summary generation."""
    
    paper_title: Optional[str] = Field(default=None, description="Paper title for context")
    chunks: List[str] = Field(default_factory=list, description="Retrieved chunk texts")
    chunk_ids: List[int] = Field(default_factory=list, description="Corresponding chunk IDs")
    chunk_types: List[str] = Field(default_factory=list, description="Corresponding chunk types")
    
    def build_context_string(self) -> str:
        """Build formatted context string for prompt injection."""
        context_parts = []
        
        if self.paper_title:
            context_parts.append(f"TITLE: {self.paper_title}")

        if self.chunks:
            context_parts.append("PAPER CONTENT:")
            for idx, chunk_text in enumerate(self.chunks):
                chunk_id = self.chunk_ids[idx] if idx < len(self.chunk_ids) else idx + 1
                chunk_type = self.chunk_types[idx] if idx < len(self.chunk_types) else "body"
                context_parts.append(
                    f"\n[Chunk {idx + 1} | ID: {chunk_id} | Type: {chunk_type}]:\n{chunk_text}"
                )
        
        return "\n\n".join(context_parts)
