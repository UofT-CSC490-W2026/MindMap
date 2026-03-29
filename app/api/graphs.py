from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.config import DATABASE
from app.services import graph_service
from app.services.contracts import GraphExpandResponse, GraphResponse

router = APIRouter()


class GraphQueryRequest(BaseModel):
    query: str = Field(..., min_length=1)


class GraphExpandRequest(BaseModel):
    graph_id: str
    paper_id: int


@router.post("/graphs/query", response_model=GraphResponse)
async def query_graph(request: GraphQueryRequest):
    return await graph_service.query_graph(query=request.query)


@router.post("/graphs/expand", response_model=GraphExpandResponse)
async def expand_graph(request: GraphExpandRequest):
    return await graph_service.expand_graph(
        graph_id=request.graph_id, paper_id=request.paper_id
    )


@router.post("/graphs/clusters/rebuild")
async def rebuild_clusters(n_clusters: int = Query(default=5, ge=2, le=20)):
    try:
        from app.workers.graph_worker import run_topic_clustering

        result = await run_topic_clustering.remote.aio(
            n_clusters=n_clusters, database=DATABASE
        )
        return {"status": "ok", "result": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Cluster rebuild failed: {exc}") from exc
