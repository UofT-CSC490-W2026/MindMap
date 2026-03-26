"""
Tests for workers/semantic_search.py (legacy semantic search module).

This module imports `connect_snowflake` from `app.utils.snowflake_utils`
using the full `app.` prefix. We inject stubs into sys.modules before
importing so the module loads cleanly under pytest.
"""

import sys
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Inject app.utils.snowflake_utils stub before the module is imported
# ---------------------------------------------------------------------------
_mock_sf_utils = MagicMock()
sys.modules.setdefault("app", MagicMock())
sys.modules.setdefault("app.utils", MagicMock())
sys.modules["app.utils.snowflake_utils"] = _mock_sf_utils

from workers.semantic_search import get_related_papers  # noqa: E402


# ---------------------------------------------------------------------------
# get_related_papers
# ---------------------------------------------------------------------------

def test_get_related_papers_returns_list():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(101, 0.95), (102, 0.88)]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.semantic_search.connect_snowflake", return_value=mock_conn):
        result = get_related_papers("paper_1", k=2)

    assert isinstance(result, list)
    assert len(result) == 2
    for item in result:
        assert "paper_id" in item
        assert "score" in item
