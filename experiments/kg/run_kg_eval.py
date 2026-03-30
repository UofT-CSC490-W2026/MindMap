"""CLI entrypoint for KG evaluation."""

from __future__ import annotations

import argparse
import json
import logging

from .kg_eval import (
    DEFAULT_EMBEDDING_MODELS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SOURCE_TYPES,
    DEFAULT_TOP_K_VALUES,
    run_kg_evaluation,
)


def _parse_csv_list(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _parse_int_list(raw: str) -> list[int]:
    return [int(item.strip()) for item in raw.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run knowledge-graph retrieval experiments.")
    parser.add_argument("--database", default=None)
    parser.add_argument("--anchor-count", type=int, default=24)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--top-k-values", default=",".join(str(k) for k in DEFAULT_TOP_K_VALUES))
    parser.add_argument("--source-types", default=",".join(DEFAULT_SOURCE_TYPES))
    parser.add_argument("--embedding-models", default=",".join(DEFAULT_EMBEDDING_MODELS))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--reuse-cache", action="store_true")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args()
    result = run_kg_evaluation(
        database=args.database,
        anchor_count=args.anchor_count,
        seed=args.seed,
        top_k_values=_parse_int_list(args.top_k_values),
        source_types=_parse_csv_list(args.source_types),
        embedding_models=_parse_csv_list(args.embedding_models),
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        reuse_cache=args.reuse_cache,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

