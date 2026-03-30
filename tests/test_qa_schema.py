"""Tests for app/services/qa_schema.py"""
import pytest
from app.services.qa_schema import GroundedAnswer, ConversationTurn


# --- GroundedAnswer ---

def test_grounded_answer_basic():
    ga = GroundedAnswer(answer="The method is X.", cited_chunk_ids=[1, 2])
    assert ga.answer == "The method is X."
    assert ga.cited_chunk_ids == [1, 2]


def test_grounded_answer_none_answer_becomes_empty_string():
    ga = GroundedAnswer(answer=None, cited_chunk_ids=[])
    assert ga.answer == ""


def test_grounded_answer_strips_whitespace():
    ga = GroundedAnswer(answer="  hello  ", cited_chunk_ids=[])
    assert ga.answer == "hello"


def test_grounded_answer_none_chunk_ids_becomes_empty_list():
    ga = GroundedAnswer(answer="ok", cited_chunk_ids=None)
    assert ga.cited_chunk_ids == []


def test_grounded_answer_filters_invalid_chunk_ids():
    ga = GroundedAnswer(answer="ok", cited_chunk_ids=[1, "bad", None, 3])
    assert ga.cited_chunk_ids == [1, 3]


def test_grounded_answer_coerces_string_chunk_ids():
    ga = GroundedAnswer(answer="ok", cited_chunk_ids=["1", "2"])
    assert ga.cited_chunk_ids == [1, 2]


def test_grounded_answer_non_list_chunk_ids_becomes_empty():
    ga = GroundedAnswer(answer="ok", cited_chunk_ids="not-a-list")
    assert ga.cited_chunk_ids == []


def test_grounded_answer_defaults():
    ga = GroundedAnswer()
    assert ga.answer == ""
    assert ga.cited_chunk_ids == []


# --- ConversationTurn ---

def test_conversation_turn_basic():
    turn = ConversationTurn(role="user", message="What is this?")
    assert turn.role == "user"
    assert turn.message == "What is this?"
    assert turn.rewritten_query is None
    assert turn.cited_chunk_ids == []


def test_conversation_turn_with_all_fields():
    turn = ConversationTurn(
        role="assistant",
        message="The answer is X.",
        rewritten_query="What is X in this paper?",
        cited_chunk_ids=[5, 6],
    )
    assert turn.rewritten_query == "What is X in this paper?"
    assert turn.cited_chunk_ids == [5, 6]
