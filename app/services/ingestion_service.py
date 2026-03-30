"""Ingestion service: starts and tracks paper ingestion jobs via Modal workers."""

from __future__ import annotations

import modal
from fastapi import HTTPException

from app.config import DATABASE
from app.jobs import run_post_bronze_job
from app.services.contracts import IngestionCreateResponse, IngestionStatusResponse
from app.workers.ingestion import ingest_single_paper


async def create_ingestion(arxiv_id: str) -> IngestionCreateResponse:
    """Run Bronze ingestion synchronously, then spawn remaining pipeline stages async."""
    try:
        bronze_result = await ingest_single_paper.remote.aio(
            arxiv_id=arxiv_id, database=DATABASE
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Bronze ingestion failed: {exc}") from exc

    bronze_status = bronze_result.get("status") if isinstance(bronze_result, dict) else "failed"
    if bronze_status not in {"ok", "skipped"}:
        raise HTTPException(
            status_code=500,
            detail=f"Bronze ingestion returned unexpected status: {bronze_status}",
        )

    if bronze_status == "skipped":
        # Paper already exists; return a stable job_id placeholder
        return IngestionCreateResponse(
            job_id=f"skipped-{arxiv_id}",
            arxiv_id=arxiv_id,
            status="skipped",
            stage="bronze",
            bronze_status=bronze_status,
        )

    call = await run_post_bronze_job.spawn.aio(arxiv_id=arxiv_id, database=DATABASE)
    return IngestionCreateResponse(
        job_id=call.object_id,
        arxiv_id=arxiv_id,
        status="processing",
        stage="bronze",
        bronze_status=bronze_status,
    )


async def get_ingestion_status(job_id: str) -> IngestionStatusResponse:
    """Poll a Modal FunctionCall by job_id and return its current status."""
    try:
        call = modal.FunctionCall.from_id(job_id)
    except Exception:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    try:
        result = await call.get.aio(timeout=0)
        if isinstance(result, dict):
            return IngestionStatusResponse(job_id=job_id, status="done", result=result)
        return IngestionStatusResponse(job_id=job_id, status="done", result={"result": result})
    except TimeoutError:
        return IngestionStatusResponse(job_id=job_id, status="processing")
    except Exception as exc:
        return IngestionStatusResponse(job_id=job_id, status="failed", error=str(exc))
