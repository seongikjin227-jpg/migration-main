"""Manually trigger synchronization of the BIND RAG index."""

from __future__ import annotations

import argparse

from _bootstrap import ROOT_DIR  # noqa: F401
from app.features.rag.bind_rag_service import bind_rag_service


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual sync for BIND RAG index.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum corpus rows to scan. 0 means env default.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    limit = args.limit if args.limit > 0 else None
    result = bind_rag_service.sync_index(limit=limit)
    print("[BIND_RAG_SYNC] completed")
    for key in ("source_rows", "upserted", "skipped_unchanged", "skipped_no_correct_sql", "deleted"):
        print(f"{key}={result.get(key, 0)}")


if __name__ == "__main__":
    main()
