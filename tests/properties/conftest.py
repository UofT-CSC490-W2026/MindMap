"""Shared fixtures and Hypothesis configuration for property-based tests."""

from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import HealthCheck, settings


# ---------------------------------------------------------------------------
# Stub out Modal worker modules and Snowflake before any service is imported.
# This mirrors the pattern in tests/api/conftest.py.
# ---------------------------------------------------------------------------

def _make_stub_module(name: str) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.__spec__ = None  # type: ignore[attr-defined]
    return mod


# Stub app.config
_config = _make_stub_module("app.config")
_config.DATABASE = "test_db"  # type: ignore[attr-defined]
_config.SCHEMA = "PUBLIC"  # type: ignore[attr-defined]
_config.app = MagicMock()  # type: ignore[attr-defined]
_config.image = MagicMock()  # type: ignore[attr-defined]
_config.ml_image = MagicMock()  # type: ignore[attr-defined]
_config.snowflake_secret = MagicMock()  # type: ignore[attr-defined]
_config.qualify_table = lambda table, database=None: table  # type: ignore[attr-defined]
sys.modules.setdefault("app.config", _config)

# Stub modal
_modal = _make_stub_module("modal")
_modal.FunctionCall = MagicMock()  # type: ignore[attr-defined]
sys.modules.setdefault("modal", _modal)

# Stub worker modules
for _worker in (
    "app.workers.semantic_search_worker",
    "app.workers.graph_worker",
    "app.workers.summary_worker",
    "app.workers.qa_worker",
    "app.workers.ingestion",
):
    _mod = _make_stub_module(_worker)
    _mod.semantic_search = MagicMock()  # type: ignore[attr-defined]
    _mod.get_related_papers = MagicMock()  # type: ignore[attr-defined]
    _mod.run_topic_clustering = MagicMock()  # type: ignore[attr-defined]
    _mod.generate_paper_summary = MagicMock()  # type: ignore[attr-defined]
    _mod.answer_paper_question = MagicMock()  # type: ignore[attr-defined]
    _mod.ingest_single_paper = MagicMock()  # type: ignore[attr-defined]
    _mod.run_post_bronze_job = MagicMock()  # type: ignore[attr-defined]
    sys.modules.setdefault(_worker, _mod)

# Stub snowflake connector
for _sf in ("snowflake", "snowflake.connector"):
    sys.modules.setdefault(_sf, _make_stub_module(_sf))

# Stub app.utils
_utils = _make_stub_module("app.utils")
_utils.connect_to_snowflake = MagicMock()  # type: ignore[attr-defined]
sys.modules.setdefault("app.utils", _utils)

# Stub app.jobs (used by ingestion_service)
_jobs = _make_stub_module("app.jobs")
_jobs.run_post_bronze_job = MagicMock()  # type: ignore[attr-defined]
sys.modules.setdefault("app.jobs", _jobs)

# ---------------------------------------------------------------------------
# Hypothesis default profile: 100 examples, suppress slow-test health check
# ---------------------------------------------------------------------------
settings.register_profile(
    "default",
    max_examples=100,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("default")


# ---------------------------------------------------------------------------
# Snowflake cursor factory fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_snowflake_cursor():
    """Factory that returns a mock Snowflake cursor with configurable return values.

    Usage::

        def test_something(mock_snowflake_cursor):
            cursor = mock_snowflake_cursor(
                fetchone=("value",),
                fetchall=[("row1",), ("row2",)],
            )
    """

    def _factory(fetchone=None, fetchall=None):
        cursor = MagicMock()
        cursor.fetchone.return_value = fetchone
        cursor.fetchall.return_value = fetchall if fetchall is not None else []
        cursor.execute.return_value = None
        cursor.executemany.return_value = None
        cursor.description = []
        cursor.close.return_value = None
        return cursor

    return _factory


# ---------------------------------------------------------------------------
# Modal function factory fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_modal_fn():
    """Factory that returns an async mock simulating a Modal remote function.

    The returned object supports `.remote.aio(...)` calls, matching the
    pattern used by all service modules::

        result = await some_worker.remote.aio(...)

    Usage::

        def test_something(mock_modal_fn):
            worker = mock_modal_fn(return_value={"status": "ok", "answer": "42"})
            # patch the worker in the service under test, then call the service
    """

    def _factory(return_value=None, side_effect=None):
        remote_aio = AsyncMock(return_value=return_value, side_effect=side_effect)
        remote = MagicMock()
        remote.aio = remote_aio
        fn = MagicMock()
        fn.remote = remote
        # Also support spawn.aio pattern used by ingestion_service
        spawn_aio = AsyncMock(return_value=MagicMock(object_id="fake-job-id"))
        spawn = MagicMock()
        spawn.aio = spawn_aio
        fn.spawn = spawn
        return fn

    return _factory
