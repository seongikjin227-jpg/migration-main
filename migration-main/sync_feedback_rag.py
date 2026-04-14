"""피드백 RAG 벡터 인덱스 수동 동기화 CLI."""

from __future__ import annotations

import argparse

from app.services.feedback_rag_service import feedback_rag_service


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual sync for feedback RAG vector index.")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="코퍼스 최대 처리 건수. 0이면 .env의 RAG_CORPUS_LIMIT 사용",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    limit = args.limit if args.limit > 0 else None
    result = feedback_rag_service.sync_index(limit=limit)

    print("[RAG_SYNC] completed")
    print(f"source_rows={result['source_rows']}")
    print(f"upserted={result['upserted']}")
    print(f"skipped_unchanged={result['skipped_unchanged']}")
    print(f"skipped_no_correct_sql={result['skipped_no_correct_sql']}")
    print(f"deleted={result['deleted']}")


if __name__ == "__main__":
    main()

