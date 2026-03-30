"""Tests for app/services/summary_schema.py"""
import pytest
from app.services.summary_schema import PaperSummary, SummaryContext


# --- PaperSummary validators ---

def test_paper_summary_basic():
    s = PaperSummary(
        research_question="How does X work?",
        methods=["method A", "method B"],
        main_claims=["claim 1"],
        key_findings=["finding 1"],
        limitations=["limitation 1"],
        conclusion="X works well.",
    )
    assert s.research_question == "How does X work?"
    assert len(s.methods) == 2


def test_paper_summary_none_list_fields_become_empty():
    s = PaperSummary(methods=None, main_claims=None, key_findings=None, limitations=None)
    assert s.methods == []
    assert s.main_claims == []
    assert s.key_findings == []
    assert s.limitations == []


def test_paper_summary_string_wrapped_in_list():
    s = PaperSummary(methods="single method")
    assert s.methods == ["single method"]


def test_paper_summary_empty_string_not_added_to_list():
    s = PaperSummary(methods=["", "  ", "real method"])
    assert s.methods == ["real method"]


def test_paper_summary_none_string_fields_become_empty_string():
    s = PaperSummary(research_question=None, conclusion=None)
    assert s.research_question == ""
    assert s.conclusion == ""


def test_paper_summary_string_fields_stripped():
    s = PaperSummary(research_question="  trimmed  ", conclusion="  also trimmed  ")
    assert s.research_question == "trimmed"
    assert s.conclusion == "also trimmed"


def test_paper_summary_is_empty_true():
    s = PaperSummary()
    assert s.is_empty() is True


def test_paper_summary_is_empty_false_with_question():
    s = PaperSummary(research_question="Something")
    assert s.is_empty() is False


def test_paper_summary_is_empty_false_with_findings():
    s = PaperSummary(key_findings=["finding"])
    assert s.is_empty() is False


def test_paper_summary_to_dict():
    s = PaperSummary(research_question="Q?", methods=["m1"], conclusion="C.")
    d = s.to_dict()
    assert d["research_question"] == "Q?"
    assert d["methods"] == ["m1"]
    assert d["conclusion"] == "C."
    assert "key_findings" in d


# --- SummaryContext ---

def test_summary_context_build_context_string_with_title():
    ctx = SummaryContext(
        paper_title="My Paper",
        chunks=["chunk text 1"],
        chunk_ids=[1],
        chunk_types=["abstract"],
    )
    result = ctx.build_context_string()
    assert "My Paper" in result
    assert "chunk text 1" in result
    assert "abstract" in result


def test_summary_context_build_context_string_no_title():
    ctx = SummaryContext(chunks=["some text"], chunk_ids=[1], chunk_types=["body"])
    result = ctx.build_context_string()
    assert "TITLE" not in result
    assert "some text" in result


def test_summary_context_build_context_string_empty():
    ctx = SummaryContext()
    result = ctx.build_context_string()
    assert result == ""


def test_summary_context_chunk_id_fallback():
    # More chunks than chunk_ids — should fall back to index+1
    ctx = SummaryContext(chunks=["a", "b"], chunk_ids=[99], chunk_types=["body", "body"])
    result = ctx.build_context_string()
    assert "99" in result
    assert "2" in result  # fallback index for second chunk
