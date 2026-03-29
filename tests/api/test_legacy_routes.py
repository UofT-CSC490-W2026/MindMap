"""
Route-level tests verifying legacy routes return 404.
Requirements: 14.1–14.7
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.router import router

app = FastAPI()
app.include_router(router)

client = TestClient(app, raise_server_exceptions=False)


def test_legacy_papers_list_returns_404():
    """GET /papers → 404 (Requirement 14.1)"""
    response = client.get("/papers")
    assert response.status_code == 404


def test_legacy_relationships_returns_404():
    """GET /relationships → 404 (Requirement 14.2)"""
    response = client.get("/relationships")
    assert response.status_code == 404


def test_legacy_pipeline_run_returns_404():
    """POST /api/pipeline/run → 404 (Requirement 14.3)"""
    response = client.post("/api/pipeline/run")
    assert response.status_code == 404


def test_legacy_api_related_returns_404():
    """GET /api/related → 404 (Requirement 14.4)"""
    response = client.get("/api/related")
    assert response.status_code == 404


def test_legacy_api_graph_returns_404():
    """GET /api/graph → 404 (Requirement 14.5)"""
    response = client.get("/api/graph")
    assert response.status_code == 404


def test_legacy_papers_ingest_returns_404():
    """POST /api/papers/ingest → 404 (Requirement 14.6)"""
    response = client.post("/api/papers/ingest")
    assert response.status_code == 404
