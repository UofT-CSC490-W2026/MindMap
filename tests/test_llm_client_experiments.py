"""Tests for experiment-oriented LLM client behavior."""

from unittest.mock import MagicMock, patch

from app.services.llm_client import LLMClient


def test_generate_text_returns_usage_and_attempts():
    with patch("app.services.llm_client.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        client = LLMClient(api_key="key", model="gpt-4.1-mini")
        with patch.object(client, "_call_openai", return_value=("plain answer", {"tokens": 12})):
            result = client.generate_text("prompt", retry_count=0)
    assert result["text"] == "plain answer"
    assert result["attempts"] == 1
    assert result["usage"] == {"tokens": 12}

