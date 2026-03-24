import argparse
import json
import sys
from pathlib import Path
from typing import Any


# Allow running with either:
# 1) python -m app.main
# 2) python app/main.py
if __package__ in (None, ""):
	repo_root = Path(__file__).resolve().parents[1]
	sys.path.insert(0, str(repo_root))

from app.workers.citation_aware_embedding_worker import run_citation_aware_embedding_batch
from app.workers.embedding_worker import run_embedding_batch
from app.workers.ingestion import ingest_from_arxiv
from app.workers.semantic_search import get_related_papers
from app.workers.transformation import transform_to_silver

def _print_result(result: Any) -> None:
	if result is None:
		print("Done")
		return

	if isinstance(result, (dict, list)):
		print(json.dumps(result, indent=2, default=str))
		return

	print(result)


def _run_worker(fn: Any, mode: str, **kwargs: Any) -> Any:
	if mode == "local":
		return fn.local(**kwargs)
	return fn.remote(**kwargs)


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(
		description="MindMap backend runner (local-first Modal worker orchestrator)."
	)
	parser.add_argument(
		"--mode",
		choices=["local", "remote"],
		default="local",
		help="Execution mode for Modal functions (default: local).",
	)

	subparsers = parser.add_subparsers(dest="command", required=True)

	ingest = subparsers.add_parser("ingest", help="Ingest papers from arXiv into Bronze.")
	ingest.add_argument("--query", required=True, help="arXiv search query")
	ingest.add_argument("--max-results", type=int, default=10, help="Max arXiv results")

	subparsers.add_parser("transform", help="Transform Bronze data into Silver table.")

	embed = subparsers.add_parser("embed", help="Generate baseline embeddings.")
	embed.add_argument("--limit", type=int, default=200, help="Rows to embed")
	embed.add_argument(
		"--model-name",
		default="sentence-transformers/all-MiniLM-L6-v2",
		help="Sentence-transformers model name",
	)

	ca_embed = subparsers.add_parser(
		"citation-embed",
		help="Generate citation-aware embeddings.",
	)
	ca_embed.add_argument("--limit", type=int, default=50, help="Rows to process")
	ca_embed.add_argument("--alpha", type=float, default=0.8, help="Blend weight for self embedding")
	ca_embed.add_argument("--max-refs", type=int, default=80, help="Max parsed references per paper")
	ca_embed.add_argument(
		"--base-model",
		default="sentence-transformers/all-MiniLM-L6-v2",
		help="Base model name",
	)

	related = subparsers.add_parser("related", help="Get top-k related papers for one paper_id.")
	related.add_argument("--paper-id", required=True, help="Source paper_id")
	related.add_argument("--k", type=int, default=10, help="Top-k results")

	pipeline = subparsers.add_parser(
		"pipeline",
		help="Run ingest -> transform -> embed in sequence.",
	)
	pipeline.add_argument("--query", required=True, help="arXiv search query")
	pipeline.add_argument("--max-results", type=int, default=10, help="Max arXiv results")
	pipeline.add_argument("--embed-limit", type=int, default=200, help="Rows to embed")
	pipeline.add_argument(
		"--model-name",
		default="sentence-transformers/all-MiniLM-L6-v2",
		help="Sentence-transformers model name",
	)

	return parser


def main() -> None:
	parser = build_parser()
	args = parser.parse_args()

	if args.command == "ingest":
		result = _run_worker(
			ingest_from_arxiv,
			args.mode,
			query=args.query,
			max_results=args.max_results,
		)
		_print_result(result)
		return

	if args.command == "transform":
		result = _run_worker(transform_to_silver, args.mode)
		_print_result(result)
		return

	if args.command == "embed":
		result = _run_worker(
			run_embedding_batch,
			args.mode,
			limit=args.limit,
			model_name=args.model_name,
		)
		_print_result(result)
		return

	if args.command == "citation-embed":
		result = _run_worker(
			run_citation_aware_embedding_batch,
			args.mode,
			limit=args.limit,
			alpha=args.alpha,
			base_model=args.base_model,
			max_refs=args.max_refs,
		)
		_print_result(result)
		return

	if args.command == "related":
		result = _run_worker(
			get_related_papers,
			args.mode,
			paper_id=args.paper_id,
			k=args.k,
		)
		_print_result(result)
		return

	if args.command == "pipeline":
		print(f"Step 1/3: ingest ({args.mode})")
		_run_worker(
			ingest_from_arxiv,
			args.mode,
			query=args.query,
			max_results=args.max_results,
		)

		print(f"Step 2/3: transform ({args.mode})")
		_run_worker(transform_to_silver, args.mode)

		print(f"Step 3/3: embed ({args.mode})")
		result = _run_worker(
			run_embedding_batch,
			args.mode,
			limit=args.embed_limit,
			model_name=args.model_name,
		)
		_print_result(result)
		return

	parser.error("Unknown command")


if __name__ == "__main__":
	main()
