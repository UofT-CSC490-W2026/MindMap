"""
Conftest for tests/api: stub out Modal worker modules that fail to import
in a plain test environment (no Modal runtime, no Snowflake credentials).
"""
import sys
import types
from unittest.mock import MagicMock


def _make_stub_module(name: str) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.__spec__ = None  # type: ignore[attr-defined]
    return mod


# Stub app.config before anything else imports it
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

# Stub worker modules that graph_service / other services import
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
