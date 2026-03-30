"""Typed contract schemas shared by all layers of the serving architecture."""

from typing import Optional, List, Literal

from pydantic import BaseModel, Field


class GraphNode(BaseModel):
    id: str                       # str(paper_id)
    label: str                    # paper title (truncated for display)
    title: str
    authors: str
    year: int
    citations: int
    arxiv_id: Optional[str]
    cluster_id: Optional[int]
    cluster_name: Optional[str]


class GraphLink(BaseModel):
    source: str                   # GraphNode.id
    target: str                   # GraphNode.id
    kind: str                     # "CITES" | "SIMILAR" | "SUPPORT" | "CONTRADICT" | "NEUTRAL"
    strength: float


class GraphMeta(BaseModel):
    total_nodes: int
    total_links: int
    query: Optional[str]


class GraphResponse(BaseModel):
    graph_id: str
    query: Optional[str]
    nodes: List[GraphNode]
    links: List[GraphLink]
    meta: GraphMeta


class GraphExpandResponse(BaseModel):
    graph_id: str
    paper_id: str
    new_nodes: List[GraphNode]
    new_links: List[GraphLink]


class PaperDetailResponse(BaseModel):
    paper_id: int
    title: str
    authors: List[str]
    year: Optional[int]
    citations: Optional[int]
    arxiv_id: Optional[str]
    abstract: Optional[str]


class PaperSummaryResponse(BaseModel):
    paper_id: int
    research_question: Optional[str]
    methods: List[str]
    main_claims: List[str]
    key_findings: List[str]
    limitations: List[str]
    conclusion: Optional[str]


class PaperChatRequest(BaseModel):
    question: str = Field(..., min_length=1)
    session_id: Optional[str] = None


class PaperChatResponse(BaseModel):
    paper_id: int
    session_id: str
    answer: str
    cited_chunk_ids: List[int]
    rewritten_query: Optional[str] = None


class SearchPaperResponse(BaseModel):
    title: str
    authors: List[str]
    year: Optional[int]
    citation_count: Optional[int]
    arxiv_id: Optional[str]
    external_url: Optional[str]


class IngestionCreateResponse(BaseModel):
    job_id: str
    arxiv_id: str
    status: Literal["processing", "skipped"]
    stage: str
    bronze_status: str


class IngestionStatusResponse(BaseModel):
    job_id: str
    status: Literal["processing", "done", "failed"]
    result: Optional[dict] = None
    error: Optional[str] = None
