"""Summarize annotated bridge-candidate evaluations."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any, Union


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def summarize_bridge_annotations(
    input_csv: Union[str, Path],
    output_csv: Union[str, Path] = "experiments/results/kg_bridge_summary.csv",
) -> dict[str, Any]:
    rows = _read_rows(Path(input_csv))
    if not rows:
        raise ValueError("Annotated bridge CSV is empty.")

    required = {"source_type", "embedding_model", "is_cited"}
    missing = required - set(rows[0].keys())
    if missing:
        raise ValueError(f"Annotated bridge CSV is missing required columns: {sorted(missing)}")

    has_score = "score" in rows[0]
    has_label = "label" in rows[0]
    if not has_score and not has_label:
        raise ValueError("Annotated bridge CSV must include either a 'score' or 'label' column.")

    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(row["source_type"], row["embedding_model"])].append(row)

    output_rows: list[dict[str, Any]] = []
    for (source_type, embedding_model), items in sorted(grouped.items()):
        score_values = [float(item["score"]) for item in items if has_score and item.get("score", "").strip()]
        positive_count = 0
        if has_label:
            positive_count = sum(
                1
                for item in items
                if str(item.get("label", "")).strip().lower() in {"1", "true", "yes", "positive"}
            )
        cited_count = sum(1 for item in items if str(item.get("is_cited", "")).strip().lower() in {"1", "true", "yes"})
        output_rows.append(
            {
                "source_type": source_type,
                "embedding_model": embedding_model,
                "annotation_count": len(items),
                "avg_score": (sum(score_values) / len(score_values)) if score_values else "",
                "positive_rate": (positive_count / len(items)) if has_label else "",
                "cited_count": cited_count,
                "noncited_count": len(items) - cited_count,
            }
        )

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "source_type",
                "embedding_model",
                "annotation_count",
                "avg_score",
                "positive_rate",
                "cited_count",
                "noncited_count",
            ],
        )
        writer.writeheader()
        writer.writerows(output_rows)
    return {"output_path": str(output_path), "rows": len(output_rows)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize annotated KG bridge candidates.")
    parser.add_argument("input_csv")
    parser.add_argument("--output-csv", default="experiments/results/kg_bridge_summary.csv")
    args = parser.parse_args()
    summarize_bridge_annotations(args.input_csv, args.output_csv)


if __name__ == "__main__":
    main()
