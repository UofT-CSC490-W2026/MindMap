"""Tests for app/jobs.py"""
import pytest
from unittest.mock import MagicMock, patch


def _make_remote_mock(return_value):
    m = MagicMock()
    m.remote = MagicMock(return_value=return_value)
    return m


def test_run_single_ingestion_job_returns_done():
    mock_bronze = {"status": "ok", "inserted": 1}
    mock_silver = {"status": "ok"}
    mock_embedding = {"status": "ok", "paper_id": 42}
    mock_graph = {"status": "ok"}

    with patch.dict("sys.modules", {
        "app.workers.ingestion": MagicMock(ingest_single_paper=_make_remote_mock(mock_bronze)),
        "app.workers.transformation": MagicMock(process_single_silver=_make_remote_mock(mock_silver)),
        "app.workers.embedding_worker": MagicMock(process_single_embedding=_make_remote_mock(mock_embedding)),
        "app.workers.graph_worker": MagicMock(build_knowledge_graph=_make_remote_mock(mock_graph)),
    }):
        from app.jobs import run_single_ingestion_job
        result = run_single_ingestion_job("2301.00001", database="MINDMAP_TEST")

    assert result["status"] == "done"
    assert result["database"] == "MINDMAP_TEST"
    assert result["bronze_result"] == mock_bronze
    assert result["silver_result"] == mock_silver
    assert result["embedding_result"] == mock_embedding
    assert result["graph_result"] == mock_graph


def test_run_post_bronze_job_returns_done():
    mock_silver = {"status": "ok"}
    mock_embedding = {"status": "ok", "paper_id": 7}
    mock_graph = {"status": "ok"}

    with patch.dict("sys.modules", {
        "app.workers.transformation": MagicMock(process_single_silver=_make_remote_mock(mock_silver)),
        "app.workers.embedding_worker": MagicMock(process_single_embedding=_make_remote_mock(mock_embedding)),
        "app.workers.graph_worker": MagicMock(build_knowledge_graph=_make_remote_mock(mock_graph)),
    }):
        from app.jobs import run_post_bronze_job
        result = run_post_bronze_job("2301.00001", database="MINDMAP_TEST")

    assert result["status"] == "done"
    assert result["silver_result"] == mock_silver
    assert result["embedding_result"] == mock_embedding
    assert result["graph_result"] == mock_graph


def test_run_post_bronze_job_handles_missing_paper_id():
    with patch.dict("sys.modules", {
        "app.workers.transformation": MagicMock(process_single_silver=_make_remote_mock({"status": "ok"})),
        "app.workers.embedding_worker": MagicMock(process_single_embedding=_make_remote_mock({"status": "ok"})),
        "app.workers.graph_worker": MagicMock(build_knowledge_graph=_make_remote_mock({"status": "ok"})),
    }):
        from app.jobs import run_post_bronze_job
        result = run_post_bronze_job("2301.00001")

    assert result["status"] == "done"


def test_run_post_bronze_job_handles_invalid_paper_id():
    with patch.dict("sys.modules", {
        "app.workers.transformation": MagicMock(process_single_silver=_make_remote_mock({"status": "ok"})),
        "app.workers.embedding_worker": MagicMock(process_single_embedding=_make_remote_mock({"status": "ok", "paper_id": "not-a-number"})),
        "app.workers.graph_worker": MagicMock(build_knowledge_graph=_make_remote_mock({"status": "ok"})),
    }):
        from app.jobs import run_post_bronze_job
        result = run_post_bronze_job("2301.00001")

    assert result["status"] == "done"
