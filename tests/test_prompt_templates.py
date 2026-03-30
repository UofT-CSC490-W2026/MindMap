"""Tests for app/services/prompt_templates.py"""
import json
import pytest
from app.services.prompt_templates import (
    build_summary_extraction_prompt,
    build_grounded_qa_prompt,
    build_query_rewrite_prompt,
    format_chunk_context,
    build_summary_refinement_prompt,
)


# --- build_summary_extraction_prompt ---

def test_summary_prompt_v1_contains_context():
    prompt = build_summary_extraction_prompt("some paper text", prompt_version="v1")
    assert "some paper text" in prompt


def test_summary_prompt_v1_contains_json_schema():
    prompt = build_summary_extraction_prompt("ctx")
    assert "research_question" in prompt
    assert "key_findings" in prompt


def test_summary_prompt_unknown_version_raises():
    with pytest.raises(ValueError, match="Unknown prompt version"):
        build_summary_extraction_prompt("ctx", prompt_version="v99")


# --- build_grounded_qa_prompt ---

def test_grounded_qa_prompt_contains_question():
    prompt = build_grounded_qa_prompt("What is BERT?", "some context", [1, 2, 3])
    assert "What is BERT?" in prompt


def test_grounded_qa_prompt_contains_chunk_ids():
    prompt = build_grounded_qa_prompt("Q?", "ctx", [10, 20])
    assert "10" in prompt
    assert "20" in prompt


def test_grounded_qa_prompt_includes_history():
    prompt = build_grounded_qa_prompt("Q?", "ctx", [1], history="User: hi\nAssistant: hello")
    assert "RECENT CONVERSATION" in prompt
    assert "User: hi" in prompt


def test_grounded_qa_prompt_no_history_block_when_empty():
    prompt = build_grounded_qa_prompt("Q?", "ctx", [1], history="")
    assert "RECENT CONVERSATION" not in prompt


def test_grounded_qa_prompt_unknown_version_raises():
    with pytest.raises(ValueError, match="Unknown prompt version"):
        build_grounded_qa_prompt("Q?", "ctx", [1], prompt_version="v2")


# --- build_query_rewrite_prompt ---

def test_query_rewrite_prompt_contains_question():
    prompt = build_query_rewrite_prompt("User: what is it?", "what about the results?")
    assert "what about the results?" in prompt


def test_query_rewrite_prompt_contains_history():
    prompt = build_query_rewrite_prompt("User: hello", "follow up?")
    assert "User: hello" in prompt


def test_query_rewrite_prompt_unknown_version_raises():
    with pytest.raises(ValueError, match="Unknown prompt version"):
        build_query_rewrite_prompt("h", "q", prompt_version="v9")


# --- format_chunk_context ---

def test_format_chunk_context_basic():
    chunks = [
        {"chunk_id": 1, "chunk_type": "abstract", "chunk_text": "This paper proposes X."},
        {"chunk_id": 2, "chunk_type": "body", "chunk_text": "We evaluate on Y."},
    ]
    result = format_chunk_context(chunks)
    assert "Chunk ID: 1" in result
    assert "This paper proposes X." in result
    assert "Chunk ID: 2" in result


def test_format_chunk_context_skips_empty_text():
    chunks = [
        {"chunk_id": 1, "chunk_type": "body", "chunk_text": ""},
        {"chunk_id": 2, "chunk_type": "body", "chunk_text": "Real content."},
    ]
    result = format_chunk_context(chunks)
    assert "Chunk ID: 1" not in result
    assert "Real content." in result


def test_format_chunk_context_respects_max_chars():
    chunks = [
        {"chunk_id": i, "chunk_type": "body", "chunk_text": "x" * 100}
        for i in range(10)
    ]
    result = format_chunk_context(chunks, max_chars=150)
    # Only first chunk should fit
    assert result.count("Chunk ID:") == 1


def test_format_chunk_context_uses_section_name_fallback():
    chunks = [{"chunk_id": 1, "section_name": "intro", "chunk_text": "Hello."}]
    result = format_chunk_context(chunks)
    assert "intro" in result


def test_format_chunk_context_empty_input():
    assert format_chunk_context([]) == ""


# --- build_summary_refinement_prompt ---

def test_summary_refinement_raises_not_implemented():
    with pytest.raises(NotImplementedError):
        build_summary_refinement_prompt({}, "ctx")
