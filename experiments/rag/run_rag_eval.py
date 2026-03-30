"""CLI entrypoint for RAG evaluation."""

from __future__ import annotations

import argparse
import json
import logging

from .rag_eval import DEFAULT_METHODS_STAGE1, DEFAULT_MODELS_STAGE2, DEFAULT_OUTPUT_DIR, run_rag_evaluation


def _parse_csv_list(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run RAG and LLM experiments.")
    parser.add_argument("--database", default=None)
    parser.add_argument("--questions-file", default="experiments/config/rag_questions.csv")
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--support-threshold", type=float, default=0.6)
    parser.add_argument("--runs-per-question", type=int, default=3)
    parser.add_argument("--stage", choices=["all", "stage1", "stage2"], default="all")
    parser.add_argument("--models", default=",".join(DEFAULT_MODELS_STAGE2))
    parser.add_argument("--methods", default=",".join(DEFAULT_METHODS_STAGE1))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--reuse-cache", action="store_true")
    parser.add_argument("--max-questions", type=int, default=None)
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args()
    result = run_rag_evaluation(
        database=args.database,
        questions_file=args.questions_file,
        seed=args.seed,
        top_k=args.top_k,
        support_threshold=args.support_threshold,
        runs_per_question=args.runs_per_question,
        stage=args.stage,
        models=_parse_csv_list(args.models),
        methods=_parse_csv_list(args.methods),
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        reuse_cache=args.reuse_cache,
        max_questions=args.max_questions,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
