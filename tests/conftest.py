import os
import sys
import pytest
from unittest.mock import MagicMock

# --- Env vars must be set before any import of config or utils ---
os.environ.setdefault("SNOWFLAKE_ACCOUNT", "test_account")
os.environ.setdefault("SNOWFLAKE_USER", "test_user")
os.environ.setdefault("SNOWFLAKE_PASSWORD", "test_password")
os.environ.setdefault("OPENAI_API_KEY", "test_openai_key")
os.environ.setdefault("SEMANTIC_SCHOLAR_API_KEY", "test_ss_key")
os.environ.setdefault("MINDMAP_ENV", "TEST")

# --- Patch modal before config.py is imported ---
_modal_mock = MagicMock()
_modal_app_mock = MagicMock()
_modal_mock.App.return_value = _modal_app_mock


def _passthrough(*args, **kwargs):
    def decorator(fn):
        return fn
    return decorator


_modal_app_mock.function = _passthrough
_modal_app_mock.cls = _passthrough
_modal_app_mock.local_entrypoint = _passthrough

_modal_mock.Image.debian_slim = MagicMock()
_modal_mock.Secret.from_name = MagicMock()
_modal_mock.method = MagicMock()
_modal_mock.enter = MagicMock()

sys.modules["modal"] = _modal_mock

# --- Patch snowflake.connector before utils.py is imported ---
_sf_mock = MagicMock()
sys.modules["snowflake"] = _sf_mock
sys.modules["snowflake.connector"] = _sf_mock.connector


@pytest.fixture(scope="session")
def mock_cursor():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = None
    cursor.execute.return_value = None
    cursor.executemany.return_value = None
    cursor.description = []
    return cursor


@pytest.fixture(scope="session")
def mock_conn(mock_cursor):
    conn = MagicMock()
    conn.cursor.return_value = mock_cursor
    conn.commit.return_value = None
    conn.rollback.return_value = None
    conn.close.return_value = None
    return conn
