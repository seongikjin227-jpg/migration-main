"""Manually trigger synchronization of the feedback RAG vector index."""

from __future__ import annotations

import argparse

from _bootstrap import ROOT_DIR  # noqa: F401
from app.features.rag.feedback_rag_service import feedback_rag_service


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for manual index synchronization."""
    parser = argparse.ArgumentParser(description="Manual sync for feedback RAG vector index.")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum corpus rows to scan. 0 means use `RAG_CORPUS_LIMIT` from `.env`.",
    )
    parser.add_argument(
        "--kind",
        action="append",
        choices=["TOBE", "BIND", "TEST"],
        help="Restrict sync to one or more correct SQL kinds. Repeatable.",
    )
    return parser


def main() -> None:
    """CLI entry point that runs the sync and prints a short summary."""
    args = _build_parser().parse_args()
    limit = args.limit if args.limit > 0 else None
    result = feedback_rag_service.sync_index(limit=limit, correct_kinds=args.kind)

    print("[RAG_SYNC] completed")
    print(f"source_rows={result['source_rows']}")
    print(f"upserted={result['upserted']}")
    print(f"skipped_unchanged={result['skipped_unchanged']}")
    print(f"skipped_no_correct_sql={result['skipped_no_correct_sql']}")
    print(f"deleted={result['deleted']}")


if __name__ == "__main__":
    main()
