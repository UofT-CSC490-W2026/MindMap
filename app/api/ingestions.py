from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services import ingestion_service
from app.services.contracts import IngestionCreateResponse, IngestionStatusResponse

router = APIRouter()


class IngestionCreateRequest(BaseModel):
    arxiv_id: str = Field(..., min_length=1)


@router.post("/ingestions", response_model=IngestionCreateResponse)
async def create_ingestion(request: IngestionCreateRequest):
    return await ingestion_service.create_ingestion(arxiv_id=request.arxiv_id)


@router.get("/ingestions/{job_id}", response_model=IngestionStatusResponse)
async def get_ingestion_status(job_id: str):
    return await ingestion_service.get_ingestion_status(job_id=job_id)
