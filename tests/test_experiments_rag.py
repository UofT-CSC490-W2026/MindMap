"""Tests for RAG experiment helpers."""

from pathlib import Path
from unittest.mock import patch

import pytest

from experiments.rag.rag_eval import compute_similarity_metrics, compute_stability, run_rag_evaluation
from experiments.rag.summarize_human import summarize_human_annotations


def fake_embed(texts, batch_size=16):
    mapping = {
        "Answer one.": [1.0, 0.0],
        "Chunk one": [1.0, 0.0],
        "Chunk two": [0.0, 1.0],
        "Sentence A.": [1.0, 0.0],
        "Sentence B.": [0.0, 1.0],
        "Stable A": [1.0, 0.0],
        "Stable B": [1.0, 0.0],
        "Stable C": [1.0, 0.0],
        "Unstable A": [1.0, 0.0],
        "Unstable B": [0.0, 1.0],
        "Unstable C": [1.0, 0.0],
        "The abstract.": [1.0, 0.0],
        "Chunk-supported answer.": [1.0, 0.0],
    }
    return [mapping.get(text, [1.0, 0.0]) for text in texts]


def test_compute_similarity_metrics_support_and_similarity():
    with patch("experiments.rag.rag_eval._embed_texts", side_effect=fake_embed):
        metrics = compute_similarity_metrics("Answer one.", ["Chunk one", "Chunk two"], support_threshold=0.5)
    assert metrics["similarity_score"] == pytest.approx(1.0)
    assert metrics["support_rate"] == pytest.approx(1.0)


def test_compute_stability_pairwise_average():
    with patch("experiments.rag.rag_eval._embed_texts", side_effect=fake_embed):
        assert compute_stability(["Stable A", "Stable B", "Stable C"]) == pytest.approx(1.0)
        unstable = compute_stability(["Unstable A", "Unstable B", "Unstable C"])
    assert unstable < 1.0


def test_run_rag_evaluation_uses_abstract_for_llm_only(tmp_path: Path):
    questions_path = tmp_path / "questions.csv"
    questions_path.write_text("paper_id,question_id,question_text\n1,q1,What is the contribution?\n", encoding="utf-8")

    prompts = []

    def fake_generate(prompt: str, model_name: str, max_tokens: int = 600):
        prompts.append(prompt)
        return {"text": "Chunk-supported answer.", "usage": {}}

    with patch("experiments.rag.rag_eval._fetch_paper_lookup", return_value={1: {"title": "T", "abstract": "The abstract."}}):
        with patch("experiments.rag.rag_eval.retrieve_similar_chunks_local", return_value=[{"chunk_id": 1, "chunk_text": "Chunk one", "chunk_type": "methods"}]):
            with patch("experiments.rag.rag_eval._generate_llm_answer", side_effect=fake_generate):
                with patch("experiments.rag.rag_eval._embed_texts", side_effect=fake_embed):
                    run_rag_evaluation(
                        questions_file=questions_path,
                        output_dir=tmp_path / "out",
                        stage="stage1",
                        methods=["llm_only", "rag_llm"],
                        runs_per_question=1,
                    )

    assert any("ABSTRACT:\nThe abstract." in prompt for prompt in prompts)
    assert any("RETRIEVED CHUNKS:" in prompt for prompt in prompts)


def test_run_rag_evaluation_respects_max_questions(tmp_path: Path):
    questions_path = tmp_path / "questions.csv"
    questions_path.write_text(
        "paper_id,question_id,question_text\n1,q1,What is the contribution?\n1,q2,What is the method?\n",
        encoding="utf-8",
    )

    with patch("experiments.rag.rag_eval._fetch_paper_lookup", return_value={1: {"title": "T", "abstract": "The abstract."}}):
        with patch("experiments.rag.rag_eval._embed_texts", side_effect=fake_embed):
            result = run_rag_evaluation(
                questions_file=questions_path,
                output_dir=tmp_path / "out",
                stage="stage1",
                methods=["abstract_only"],
                runs_per_question=1,
                max_questions=1,
            )

    assert result["records"] == 1


def test_summarize_human_annotations(tmp_path: Path):
    input_path = tmp_path / "human.csv"
    input_path.write_text(
        "\n".join(
            [
                "paper_id,question,method_model,generated_answer,score",
                "1,Q,m1,a,2",
                "1,Q,m2,b,1",
                "2,Q2,m1,a,0",
                "2,Q2,m2,b,2",
            ]
        ),
        encoding="utf-8",
    )
    output_path = tmp_path / "summary.csv"
    result = summarize_human_annotations(input_path, output_path)
    assert result["rows"] == 2
    assert output_path.exists()
