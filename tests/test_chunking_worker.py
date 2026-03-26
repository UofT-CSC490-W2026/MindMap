"""
Deep unit tests for workers/chunking_worker.py.

Each test covers a specific edge case or use case in the chunking pipeline.
Comments above each test explain the rationale for the test case.
"""

import pytest
from unittest.mock import MagicMock, patch

from workers.chunking_worker import (
    _estimate_word_count,
    _truncate_words,
    _normalize_text,
    _canonical_section_name,
    _split_full_text_into_sections,
    _split_into_chunks,
    _build_sections_for_paper,
    chunk_papers,
)


# --- 3.1 ---
# Edge case: None and empty string are both "no text" inputs.
# The function must handle None gracefully (no AttributeError) and return 0
# for both, since downstream logic uses this count to decide whether to skip.
def test_estimate_word_count_none_and_empty():
    assert _estimate_word_count(None) == 0
    assert _estimate_word_count("") == 0


# --- 3.2 ---
# Happy path: when the text is already within the word limit, the function
# should return the text unchanged (stripped). No truncation should occur.
def test_truncate_words_within_limit():
    text = "one two three"
    assert _truncate_words(text, 5) == "one two three"


# --- 3.3 ---
# Truncation case: when the text exceeds max_words, the result must contain
# exactly max_words words. This verifies the hard cap is enforced correctly.
def test_truncate_words_exceeds_limit():
    text = "one two three four five six"
    result = _truncate_words(text, 3)
    assert len(result.split()) == 3
    assert result == "one two three"


# --- 3.5 ---
# Normalization must strip null bytes (which break Snowflake inserts),
# collapse multiple spaces/tabs to a single space, and collapse 3+ consecutive
# newlines to at most 2. This keeps text clean for embedding and storage.
def test_normalize_text():
    text = "hello\x00world  foo\n\n\nbar"
    result = _normalize_text(text)
    assert "\x00" not in result
    assert "  " not in result
    assert "\n\n\n" not in result


# --- 3.7 ---
# Known aliases must map to their canonical section names so that sections
# from different papers are grouped consistently in the RAG index.
def test_canonical_section_name_known_aliases():
    assert _canonical_section_name("methods") == "methods"
    assert _canonical_section_name("methodology") == "methods"
    assert _canonical_section_name("experimental setup") == "methods"
    assert _canonical_section_name("results") == "results"
    assert _canonical_section_name("evaluation") == "results"
    assert _canonical_section_name("conclusion") == "conclusion"
    assert _canonical_section_name("concluding remarks") == "conclusion"


# --- 3.8 ---
# Unknown section names (and empty string) must fall back to "body" so that
# unrecognized sections are still stored rather than silently dropped.
def test_canonical_section_name_unknown():
    assert _canonical_section_name("random section xyz") == "body"
    assert _canonical_section_name("") == "body"


# --- 3.10 ---
# When the full text contains recognizable section headers, the function must
# return a non-empty list of dicts, each with "section_name" and "content" keys
# and non-empty content. This is the primary happy path for structured papers.
def test_split_full_text_into_sections_with_headers():
    text = (
        "Introduction\nThis is the intro.\n\n"
        "Methods\nThis is the methods section.\n\n"
        "Conclusion\nThis is the conclusion."
    )
    sections = _split_full_text_into_sections(text)
    assert len(sections) > 0
    for s in sections:
        assert "section_name" in s
        assert "content" in s
        assert s["content"]  # non-empty


# --- 3.12 ---
# When no recognizable headers are present, the entire text should be returned
# as a single "body" section. This ensures unstructured papers are still chunked.
def test_split_full_text_into_sections_no_headers():
    text = "This is just a plain body of text with no section headers at all."
    sections = _split_full_text_into_sections(text)
    assert len(sections) == 1
    assert sections[0]["section_name"] == "body"


# --- 3.13 ---
# Short text (fewer words than target_words) must be returned as a single chunk
# without modification. Splitting short text would create unnecessary overhead.
def test_split_into_chunks_short_text():
    text = "short text"
    chunks = _split_into_chunks(text, target_words=500)
    assert len(chunks) == 1
    assert chunks[0] == text


# --- 3.14 ---
# Long text (more words than target_words) must be split into at least 2 chunks,
# and every chunk must be non-empty. This validates the core chunking logic.
def test_split_into_chunks_long_text():
    # Generate text with 1200 words
    text = " ".join(["word"] * 1200)
    chunks = _split_into_chunks(text, target_words=500)
    assert len(chunks) >= 2
    for chunk in chunks:
        assert chunk.strip()  # non-empty


# --- 3.16 ---
# When a paper has a non-empty abstract, it must appear as the first section.
# The abstract is the most important section for retrieval and must be indexed first.
def test_build_sections_abstract_first():
    paper = {"abstract": "This is the abstract.", "full_text": "", "conclusion": ""}
    sections = _build_sections_for_paper(paper)
    assert len(sections) > 0
    assert sections[0]["section_name"] == "abstract"


# --- 3.18 ---
# When only the conclusion is non-empty, the function should still produce at
# least one section. The conclusion may be stored directly or as a body fallback.
def test_build_sections_fallback_to_body():
    paper = {"abstract": "", "full_text": "", "conclusion": "This is the conclusion."}
    sections = _build_sections_for_paper(paper)
    assert len(sections) > 0
    # Should have a conclusion section (not body fallback since conclusion is added directly)
    section_names = [s["section_name"] for s in sections]
    assert "conclusion" in section_names or "body" in section_names


# --- 3.19 ---
# When all text fields are None or empty, there is nothing to chunk.
# The function must return an empty list so the caller can skip the paper.
def test_build_sections_all_empty():
    paper = {"abstract": None, "full_text": None, "conclusion": None}
    sections = _build_sections_for_paper(paper)
    assert sections == []


# --- 3.20 ---
# Happy-path integration test for chunk_papers. Verifies that the full pipeline
# (fetch papers → build sections → insert sections and chunks) runs without error
# and returns the expected status dict keys when given a single valid paper.
def test_chunk_papers_happy_path():
    # DESC TABLE column shapes
    desc_silver = [
        ("ID",), ("ARXIV_ID",), ("TITLE",), ("ABSTRACT",), ("CONCLUSION",), ("FULL_TEXT",),
    ]
    desc_sections = [
        ("SECTION_ID",), ("PAPER_ID",), ("SECTION_NAME",), ("SECTION_ORDER",),
        ("CONTENT",), ("TOKEN_ESTIMATE",),
    ]
    desc_chunks = [
        ("CHUNK_ID",), ("PAPER_ID",), ("SECTION_ID",), ("CHUNK_INDEX",),
        ("CHUNK_TEXT",), ("TOKEN_ESTIMATE",), ("CHUNK_TYPE",), ("EMBEDDING",),
    ]

    # Build a function-scoped cursor so side_effect lists are fresh for this test
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        desc_silver,    # DESC TABLE SILVER_PAPERS (for _fetch_unchunked_papers)
        desc_sections,  # DESC TABLE SILVER_PAPER_SECTIONS (for _fetch_unchunked_papers)
        # actual SELECT query — returns one paper row
        [(1, "2301.00001", "Test Paper", "Abstract text here.", "Conclusion text.", "")],
        desc_sections,  # DESC TABLE SILVER_PAPER_SECTIONS (for _insert_section_and_chunks)
        desc_chunks,    # DESC TABLE SILVER_PAPER_CHUNKS (for _insert_section_and_chunks)
    ]
    # fetchone returns the section_id after INSERT
    cursor.fetchone.return_value = (42,)
    # description is used to map column names after the data SELECT
    cursor.description = [
        ("id",), ("arxiv_id",), ("title",), ("abstract",), ("conclusion",), ("full_text",),
    ]

    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("workers.chunking_worker.connect_to_snowflake", return_value=conn):
        result = chunk_papers(limit=1)

    assert result["status"] == "ok"
    assert "papers_chunked" in result
    assert "chunks_created" in result


# ---------------------------------------------------------------------------
# Additional coverage tests
# ---------------------------------------------------------------------------

def test_require_columns_raises_on_missing():
    # Line 47: RuntimeError when required columns are missing
    from workers.chunking_worker import _require_columns
    with pytest.raises(RuntimeError, match="Missing required columns"):
        _require_columns({"id": '"ID"'}, ["id", "missing_col"], "MY_TABLE")


def test_normalize_text_empty_string():
    # Line 77: early return for empty/falsy input
    assert _normalize_text("") == ""
    assert _normalize_text(None) == ""


def test_truncate_words_empty_input():
    # Line 87: (text or "").split() on empty/None
    assert _truncate_words("", 5) == ""
    assert _truncate_words(None, 5) == ""


def test_canonical_section_name_abstract_prefix():
    # Line 94: startswith("abstract") branch
    assert _canonical_section_name("abstract") == "abstract"
    assert _canonical_section_name("Abstract: Overview") == "abstract"


def test_canonical_section_name_limitations():
    # Line 110: startswith("limitation") branch
    assert _canonical_section_name("limitations") == "limitations"
    assert _canonical_section_name("limitation of this work") == "limitations"


def test_canonical_section_name_passthrough_names():
    # Lines 126-131: introduction/background/related work returned as-is
    assert _canonical_section_name("introduction") == "introduction"
    assert _canonical_section_name("background") == "background"
    assert _canonical_section_name("related work") == "related work"


def test_split_full_text_empty():
    # Line 147: empty text returns []
    assert _split_full_text_into_sections("") == []
    assert _split_full_text_into_sections(None) == []


def test_split_full_text_skips_empty_section_content():
    # Lines 149/152: sections with no content after header are skipped
    text = "Introduction\n\nMethods\nActual methods content here."
    sections = _split_full_text_into_sections(text)
    names = [s["section_name"] for s in sections]
    assert "methods" in names


def test_split_into_chunks_empty_text():
    # Line 168: empty/whitespace-only text returns []
    assert _split_into_chunks("") == []
    assert _split_into_chunks("   ") == []


def test_build_sections_with_full_text_and_abstract():
    # Lines 183-188: dedup logic — abstract already in full_text sections shouldn't be duplicated
    paper = {
        "full_text": "Abstract\nThis is the abstract.\n\nMethods\nThis is the methods.",
        "abstract": "This is the abstract.",
        "conclusion": "",
    }
    sections = _build_sections_for_paper(paper)
    abstract_sections = [s for s in sections if s["section_name"] == "abstract"]
    # Should not have duplicate abstract entries
    assert len(abstract_sections) <= 1


def test_chunk_papers_no_papers():
    # Lines 351: early return when no papers to chunk
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("TITLE",), ("ABSTRACT",), ("CONCLUSION",), ("FULL_TEXT",)],
        [("SECTION_ID",), ("PAPER_ID",)],
        [],  # no unchunked papers
    ]
    cursor.description = []
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("workers.chunking_worker.connect_to_snowflake", return_value=conn):
        result = chunk_papers(limit=10)

    assert result["status"] == "ok"
    assert result["papers_chunked"] == 0
    assert "no new papers" in result["note"].lower()


def test_chunk_papers_skips_paper_with_no_sections():
    # Lines 376-378: paper with no usable text is skipped
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("TITLE",), ("ABSTRACT",), ("CONCLUSION",), ("FULL_TEXT",)],
        [("SECTION_ID",), ("PAPER_ID",)],
        [(1, "2301.00001", "Title", None, None, None)],  # all text fields None
    ]
    cursor.description = [("id",), ("arxiv_id",), ("title",), ("abstract",), ("conclusion",), ("full_text",)]
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("workers.chunking_worker.connect_to_snowflake", return_value=conn):
        result = chunk_papers(limit=1)

    assert result["papers_skipped"] == 1


def test_chunk_papers_skips_oversized_section():
    # Lines 389-393: section exceeding MAX_SECTION_WORDS is skipped
    from workers.chunking_worker import MAX_SECTION_WORDS
    big_content = " ".join(["word"] * (MAX_SECTION_WORDS + 1))

    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [("ID",), ("ARXIV_ID",), ("TITLE",), ("ABSTRACT",), ("CONCLUSION",), ("FULL_TEXT",)],
        [("SECTION_ID",), ("PAPER_ID",)],
        [(1, "2301.00001", "Title", "short abstract", None, None)],
    ]
    cursor.description = [("id",), ("arxiv_id",), ("title",), ("abstract",), ("conclusion",), ("full_text",)]
    conn = MagicMock()
    conn.cursor.return_value = cursor

    # Patch _build_sections_for_paper to return an oversized section
    oversized_section = [{"section_name": "body", "content": big_content}]
    with patch("workers.chunking_worker.connect_to_snowflake", return_value=conn):
        with patch("workers.chunking_worker._build_sections_for_paper", return_value=oversized_section):
            result = chunk_papers(limit=1)

    assert result["sections_skipped"] >= 1
