"""Manually trigger synchronization of the TOBE RAG index."""

from __future__ import annotations

import argparse

from _bootstrap import ROOT_DIR  # noqa: F401
from app.features.rag.tobe_rag_service import tobe_rag_service


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual sync for TOBE RAG index.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum corpus rows to scan. 0 means env default.")
    parser.add_argument("--rebuild", action="store_true", help="Drop existing TOBE RAG rows and rebuild from scratch.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    limit = args.limit if args.limit > 0 else None
    result = tobe_rag_service.sync_index(limit=limit, rebuild=args.rebuild)
    print("[TOBE_RAG_SYNC] completed")
    for key in ("source_rows", "upserted", "skipped_unchanged", "skipped_no_correct_sql", "skipped_parser_failed", "deleted"):
        print(f"{key}={result.get(key, 0)}")


if __name__ == "__main__":
    main()
