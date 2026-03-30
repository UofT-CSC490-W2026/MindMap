"""Tests for app/services/llm_client.py"""
import json
import pytest
from unittest.mock import MagicMock, patch
from app.services.llm_client import LLMClient


def make_client():
    return LLMClient(api_key="test-key")


# --- __init__ ---

def test_init_with_explicit_key():
    client = LLMClient(api_key="sk-test")
    assert client.api_key == "sk-test"


def test_init_reads_env_var(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-from-env")
    client = LLMClient()
    assert client.api_key == "sk-from-env"


def test_init_raises_without_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OpenAI API key"):
        LLMClient()


# --- _parse_json_response ---

def test_parse_json_direct():
    client = make_client()
    result = client._parse_json_response('{"key": "value"}')
    assert result == {"key": "value"}


def test_parse_json_with_markdown_json_block():
    client = make_client()
    text = '```json\n{"key": "value"}\n```'
    result = client._parse_json_response(text)
    assert result == {"key": "value"}


def test_parse_json_with_plain_code_block():
    client = make_client()
    text = '```\n{"key": "value"}\n```'
    result = client._parse_json_response(text)
    assert result == {"key": "value"}


def test_parse_json_extracts_from_surrounding_text():
    client = make_client()
    text = 'Here is the result: {"key": "value"} done.'
    result = client._parse_json_response(text)
    assert result == {"key": "value"}


def test_parse_json_raises_on_invalid():
    client = make_client()
    with pytest.raises(json.JSONDecodeError):
        client._parse_json_response("not json at all")


# --- _call_openai ---

def test_call_openai_returns_content():
    client = make_client()
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "choices": [{"message": {"content": '{"answer": "yes"}'}}],
        "usage": {"total_tokens": 10},
    }
    mock_response.raise_for_status = MagicMock()

    with patch.object(client.client, "post", return_value=mock_response):
        content, usage = client._call_openai("some prompt")

    assert content == '{"answer": "yes"}'
    assert usage["total_tokens"] == 10


# --- generate_structured_summary ---

def test_generate_structured_summary_success():
    client = make_client()
    summary_json = json.dumps({
        "research_question": "What is X?",
        "methods": ["method A"],
        "main_claims": ["claim 1"],
        "key_findings": ["finding 1"],
        "limitations": [],
        "conclusion": "X works.",
    })
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "choices": [{"message": {"content": summary_json}}],
        "usage": {},
    }
    mock_response.raise_for_status = MagicMock()

    with patch.object(client.client, "post", return_value=mock_response):
        result = client.generate_structured_summary("paper context")

    assert result["result"].research_question == "What is X?"
    assert result["attempts"] == 1


def test_generate_structured_summary_retries_on_bad_json():
    client = make_client()
    bad_response = MagicMock()
    bad_response.json.return_value = {
        "choices": [{"message": {"content": "not json"}}],
        "usage": {},
    }
    bad_response.raise_for_status = MagicMock()

    with patch.object(client.client, "post", return_value=bad_response):
        with patch("time.sleep"):
            with pytest.raises(ValueError, match="Cannot validate LLM response"):
                client.generate_structured_summary("ctx", retry_count=1)


# --- rewrite_followup_question ---

def test_rewrite_followup_question():
    client = make_client()
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "choices": [{"message": {"content": "  What is the proposed method?  "}}],
        "usage": {},
    }
    mock_response.raise_for_status = MagicMock()

    with patch.object(client.client, "post", return_value=mock_response):
        result = client.rewrite_followup_question("User: tell me about it", "what method?")

    assert result == "What is the proposed method?"
