"""Summarize annotated human evaluation scores for RAG outputs."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any, Union


def summarize_human_annotations(
    input_csv: Union[str, Path],
    output_csv: Union[str, Path] = "experiments/results/rag_human_summary.csv",
) -> dict[str, Any]:
    input_path = Path(input_csv)
    with input_path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    if not rows:
        raise ValueError("Annotated human evaluation CSV is empty.")

    required = {"paper_id", "question", "method_model", "generated_answer", "score"}
    missing = required - set(rows[0].keys())
    if missing:
        raise ValueError(f"Annotated human evaluation CSV missing required columns: {sorted(missing)}")

    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    question_groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["method_model"]].append(row)
        question_groups[(row["paper_id"], row["question"])].append(row)

    win_counts: defaultdict[str, float] = defaultdict(float)
    win_denominators: defaultdict[str, int] = defaultdict(int)
    for items in question_groups.values():
        scores = [(row["method_model"], int(row["score"])) for row in items]
        max_score = max(score for _, score in scores)
        winners = [method_model for method_model, score in scores if score == max_score]
        winner_share = 1.0 / len(winners)
        present_methods = {method_model for method_model, _ in scores}
        for method_model in present_methods:
            win_denominators[method_model] += 1
        for method_model in winners:
            win_counts[method_model] += winner_share

    output_rows = []
    for method_model, items in sorted(grouped.items()):
        scores = [int(item["score"]) for item in items]
        output_rows.append(
            {
                "method_model": method_model,
                "annotation_count": len(items),
                "avg_score": sum(scores) / len(scores),
                "percent_correct": sum(1 for score in scores if score >= 1) / len(scores),
                "win_rate": (win_counts[method_model] / win_denominators[method_model])
                if win_denominators[method_model]
                else 0.0,
            }
        )

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["method_model", "annotation_count", "avg_score", "percent_correct", "win_rate"],
        )
        writer.writeheader()
        writer.writerows(output_rows)
    return {"output_path": str(output_path), "rows": len(output_rows)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize annotated RAG human evaluation scores.")
    parser.add_argument("input_csv")
    parser.add_argument("--output-csv", default="experiments/results/rag_human_summary.csv")
    args = parser.parse_args()
    summarize_human_annotations(args.input_csv, args.output_csv)


if __name__ == "__main__":
    main()
