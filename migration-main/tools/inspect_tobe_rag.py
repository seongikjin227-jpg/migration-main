"""Inspect the local SQLite TOBE RAG index from the command line."""

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path

from _bootstrap import ROOT_DIR  # noqa: F401


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect TOBE RAG SQLite metadata.")
    parser.add_argument("--db-path", default=os.getenv("TOBE_RAG_DB_PATH", str(ROOT_DIR / "migration.db")))
    parser.add_argument("--case-table", default=os.getenv("TOBE_RAG_TABLE_CASE_MASTER", "rag_case_master"))
    parser.add_argument("--doc-table", default=os.getenv("TOBE_RAG_TABLE_CASE_DOC", "rag_case_doc"))
    parser.add_argument("--limit", type=int, default=10)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[ERROR] DB file not found: {db_path}")
        return
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        case_total = int(conn.execute(f"SELECT COUNT(*) FROM {args.case_table}").fetchone()[0] or 0)
        doc_total = int(conn.execute(f"SELECT COUNT(*) FROM {args.doc_table}").fetchone()[0] or 0)
        print(f"[OK] db={db_path}")
        print(f"case_total={case_total}")
        print(f"doc_total={doc_total}")
        if case_total <= 0:
            return
        print("")
        print("doc counts by type:")
        for row in conn.execute(
            f"SELECT doc_type, COUNT(*) AS cnt FROM {args.doc_table} GROUP BY doc_type ORDER BY doc_type"
        ).fetchall():
            print(f"- {row['doc_type']}: {row['cnt']}")
        print("")
        print("recent cases:")
        for idx, row in enumerate(
            conn.execute(
                f"""
                SELECT case_id, space_nm, sql_id, tag_kind, change_summary
                FROM {args.case_table}
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(1, args.limit),),
            ).fetchall(),
            start=1,
        ):
            print(f"{idx}. case_id={row['case_id']} space_nm={row['space_nm']} sql_id={row['sql_id']} tag_kind={row['tag_kind']}")
            print(f"   change_summary={row['change_summary']}")


if __name__ == "__main__":
    main()
