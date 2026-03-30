"""Tests for app/utils.py"""
import pytest
from unittest.mock import MagicMock, patch


def test_connect_to_snowflake_passes_correct_args():
    mock_conn = MagicMock()
    with patch("app.utils.snowflake.connector.connect", return_value=mock_conn) as mock_connect:
        from app.utils import connect_to_snowflake
        conn = connect_to_snowflake(schema="SILVER", database="MINDMAP_DEV", warehouse="MINDMAP_WH")

    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]
    assert call_kwargs["schema"] == "SILVER"
    assert call_kwargs["database"] == "MINDMAP_DEV"
    assert call_kwargs["warehouse"] == "MINDMAP_WH"
    assert conn is mock_conn


def test_connect_to_snowflake_uses_env_vars(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "myaccount")
    monkeypatch.setenv("SNOWFLAKE_USER", "myuser")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "mypassword")

    mock_conn = MagicMock()
    with patch("app.utils.snowflake.connector.connect", return_value=mock_conn) as mock_connect:
        from app.utils import connect_to_snowflake
        connect_to_snowflake(schema="GOLD")

    call_kwargs = mock_connect.call_args[1]
    assert call_kwargs["account"] == "myaccount"
    assert call_kwargs["user"] == "myuser"
    assert call_kwargs["password"] == "mypassword"
