# Feature: mindmap-serving-layer-refactor, Property 8
# Feature: mindmap-serving-layer-refactor, Property 9
"""Property-based tests for ingestion_service."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from app.services.contracts import IngestionCreateResponse, IngestionStatusResponse
from app.services import ingestion_service


# ---------------------------------------------------------------------------
# Property 8: IngestionCreateResponse contains job_id
# Validates: Requirements 8.1, 8.2, 8.5
# ---------------------------------------------------------------------------

@given(arxiv_id=st.from_regex(r'\d{4}\.\d{4,5}', fullmatch=True))
@settings(max_examples=100)
def test_ingestion_create_response_contains_job_id(arxiv_id):
    """For any valid arxiv_id, create_ingestion returns a valid IngestionCreateResponse with non-empty job_id."""
    bronze_result = {"status": "ok"}
    fake_call = MagicMock()
    fake_call.object_id = f"job-{arxiv_id}"

    with patch(
        "app.services.ingestion_service.ingest_single_paper.remote",
        new_callable=MagicMock,
        create=True,
    ) as mock_ingest, patch(
        "app.services.ingestion_service.run_post_bronze_job.spawn",
        new_callable=MagicMock,
        create=True,
    ) as mock_spawn:
        mock_ingest.aio = AsyncMock(return_value=bronze_result)
        mock_spawn.aio = AsyncMock(return_value=fake_call)

        result = asyncio.run(
            ingestion_service.create_ingestion(arxiv_id)
        )

    validated = IngestionCreateResponse.model_validate(result.model_dump())
    assert validated.job_id, "job_id must be non-empty"
    assert validated.status in {"processing", "skipped"}
    assert validated.arxiv_id == arxiv_id


# ---------------------------------------------------------------------------
# Property 9: IngestionStatusResponse status is a valid enum value
# Validates: Requirements 9.1, 9.3
# ---------------------------------------------------------------------------

@given(job_id=st.text(min_size=1))
@settings(max_examples=100)
def test_ingestion_status_response_valid_enum(job_id):
    """For any job_id, get_ingestion_status returns a valid IngestionStatusResponse with status in the enum."""
    fake_call = MagicMock()
    # Simulate a completed job
    fake_call.get = MagicMock()
    fake_call.get.aio = AsyncMock(return_value={"result": "done"})

    with patch("app.services.ingestion_service.modal.FunctionCall.from_id", return_value=fake_call):
        result = asyncio.run(
            ingestion_service.get_ingestion_status(job_id)
        )

    validated = IngestionStatusResponse.model_validate(result.model_dump())
    assert validated.status in {"processing", "done", "failed"}, (
        f"status must be one of processing/done/failed, got {validated.status!r}"
    )
    assert validated.job_id == job_id
