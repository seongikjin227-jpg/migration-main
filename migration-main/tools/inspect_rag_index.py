"""Inspect the local SQLite feedback RAG index from the command line."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path

from _bootstrap import ROOT_DIR  # noqa: F401


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for the index-inspection utility."""
    parser = argparse.ArgumentParser(description="Inspect feedback RAG vector index in SQLite.")
    parser.add_argument("--db-path", default=os.getenv("RAG_VECTOR_DB_PATH", str(ROOT_DIR / "migration.db")))
    parser.add_argument("--table", default=os.getenv("RAG_VECTOR_TABLE", "feedback_rag_index"))
    parser.add_argument("--limit", type=int, default=10, help="Sample row count to print.")
    parser.add_argument("--show-vector", action="store_true", help="Print first 8 vector values for each row.")
    return parser


def _safe_count(conn: sqlite3.Connection, table: str) -> int:
    """Return the number of rows currently stored in the target table."""
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0] or 0)


def _parse_dim(embedding_json: str) -> int:
    """Read the vector dimension from a JSON-encoded embedding."""
    try:
        vec = json.loads(embedding_json)
        if isinstance(vec, list):
            return len(vec)
    except Exception:
        pass
    return 0


def _parse_head(embedding_json: str, n: int = 8) -> str:
    """Return a compact preview of the first `n` vector values."""
    try:
        vec = json.loads(embedding_json)
        if isinstance(vec, list):
            return json.dumps(vec[:n], ensure_ascii=False)
    except Exception:
        pass
    return "[]"


def main() -> None:
    """CLI entry point that prints summary rows from the vector index."""
    args = _build_parser().parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[ERROR] DB file not found: {db_path}")
        return

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if args.table not in tables:
            print(f"[ERROR] Table not found: {args.table}")
            print(f"Existing tables: {tables}")
            return

        total = _safe_count(conn, args.table)
        print(f"[OK] db={db_path} table={args.table} total_rows={total}")
        if total == 0:
            return

        rows = conn.execute(
            f"""
            SELECT doc_id, correct_kind, space_nm, sql_id, edited_yn, upd_ts, embedding_json
            FROM {args.table}
            ORDER BY upd_ts DESC
            LIMIT ?
            """,
            (max(1, args.limit),),
        ).fetchall()

        print("")
        print("Sample rows:")
        for idx, row in enumerate(rows, start=1):
            dim = _parse_dim(row["embedding_json"])
            print(
                f"{idx}. doc_id={row['doc_id']} kind={row['correct_kind']} "
                f"space_nm={row['space_nm']} sql_id={row['sql_id']} "
                f"edited_yn={row['edited_yn']} upd_ts={row['upd_ts']} dim={dim}"
            )
            if args.show_vector:
                print(f"   vector_head={_parse_head(row['embedding_json'])}")


if __name__ == "__main__":
    main()
