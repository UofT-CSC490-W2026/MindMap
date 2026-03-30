"""Tests for KG experiment helpers."""

from pathlib import Path
from typing import List, Optional

import pytest

from experiments.kg.kg_eval import (
    PaperRecord,
    _source_text_for_paper,
    compute_anchor_metrics,
    select_anchor_papers,
)
from experiments.kg.summarize_bridge import summarize_bridge_annotations


def make_paper(
    paper_id: int,
    *,
    title: str = "Title",
    abstract: str = "Abstract",
    tldr: str = "",
    conclusion: str = "",
    resolved_citation_ids: Optional[List[int]] = None,
) -> PaperRecord:
    return PaperRecord(
        paper_id=paper_id,
        title=title,
        abstract=abstract,
        tldr=tldr,
        conclusion=conclusion,
        citation_payload=[],
        resolved_citation_ids=resolved_citation_ids or [],
    )


def test_source_text_handles_missing_optional_fields():
    paper = make_paper(1, tldr="", conclusion="")
    assert "Title:" in _source_text_for_paper(paper, "title_abstract_tldr")
    assert "TLDR:" not in _source_text_for_paper(paper, "title_abstract_tldr")
    assert _source_text_for_paper(paper, "tldr_only") == ""


def test_select_anchor_papers_filters_to_resolvable_citations():
    papers = [
        make_paper(1, resolved_citation_ids=[2], tldr="short"),
        make_paper(2, resolved_citation_ids=[]),
        make_paper(3, title="", resolved_citation_ids=[1]),
    ]
    anchors = select_anchor_papers(papers, anchor_count=5, seed=7)
    assert [paper.paper_id for paper in anchors] == [1]


def test_compute_anchor_metrics_expected_values():
    metrics = compute_anchor_metrics([9, 4, 2, 8], [2, 7], k=3)
    assert metrics["hits"] == 1.0
    assert metrics["recall"] == 0.5
    assert metrics["mrr"] == pytest.approx(1 / 3)


def test_summarize_bridge_annotations_writes_summary(tmp_path: Path):
    input_path = tmp_path / "annotated.csv"
    input_path.write_text(
        "\n".join(
            [
                "source_type,embedding_model,anchor_paper_id,anchor_title,candidate_paper_id,candidate_title,similarity_score,rank,is_cited,score",
                "title_abstract,all-MiniLM-L12-v2,1,A,2,B,0.8,1,true,2",
                "title_abstract,all-MiniLM-L12-v2,1,A,3,C,0.7,2,false,1",
            ]
        ),
        encoding="utf-8",
    )
    output_path = tmp_path / "summary.csv"
    result = summarize_bridge_annotations(input_path, output_path)
    assert result["rows"] == 1
    assert output_path.exists()
