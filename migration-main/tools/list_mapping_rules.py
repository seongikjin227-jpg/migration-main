"""CLI utility that lists mapping rules from the configured Oracle tables."""

import argparse
import csv
import json
import sys
from dataclasses import asdict
from pathlib import Path

from _bootstrap import ROOT_DIR  # noqa: F401
from app.repositories.mapper_repository import get_all_mapping_rules


def _normalize(value: str) -> str:
    """Normalize strings for case-insensitive exact matching."""
    return (value or "").strip().upper()


def _matches_filter(value: str, expected: str | None) -> bool:
    """Return True when the value matches the optional filter."""
    if not expected:
        return True
    return _normalize(value) == _normalize(expected)


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for listing mapping rules."""
    parser = argparse.ArgumentParser(description="List mapping rules currently loaded by this project.")
    parser.add_argument("--fr-table", help="Filter by FR_TABLE (exact, case-insensitive).")
    parser.add_argument("--to-table", help="Filter by TO_TABLE (exact, case-insensitive).")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of rows to print. 0 means no limit.")
    parser.add_argument("--format", choices=["table", "json", "csv"], default="table", help="Output format.")
    parser.add_argument("--out", help="Output file path for json/csv. If omitted, print to stdout.")
    return parser


def _filter_rules(rules, fr_table: str | None, to_table: str | None):
    """Apply the requested FR/TO table filters."""
    filtered = []
    for rule in rules:
        if not _matches_filter(rule.fr_table, fr_table):
            continue
        if not _matches_filter(rule.to_table, to_table):
            continue
        filtered.append(rule)
    return filtered


def _summarize(rules):
    """Return counts for rows, unique FR tables, and unique TO tables."""
    fr_tables = {_normalize(rule.fr_table) for rule in rules if (rule.fr_table or "").strip()}
    to_tables = {_normalize(rule.to_table) for rule in rules if (rule.to_table or "").strip()}
    return len(rules), len(fr_tables), len(to_tables)


def _format_table(rules):
    """Format rules into an aligned plain-text table."""
    headers = ["NO", "FR_TABLE", "FR_COL", "TO_TABLE", "TO_COL"]
    rows = []
    for idx, rule in enumerate(rules, start=1):
        rows.append([str(idx), rule.fr_table, rule.fr_col, rule.to_table, rule.to_col])

    widths = [len(header) for header in headers]
    for row in rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value or ""))

    def format_row(values):
        """Render one row using the precomputed widths."""
        return " | ".join((values[i] or "").ljust(widths[i]) for i in range(len(values)))

    separator = "-+-".join("-" * width for width in widths)
    output = [format_row(headers), separator]
    for row in rows:
        output.append(format_row(row))
    return "\n".join(output)


def main():
    """CLI entry point for listing mapping rules."""
    parser = _build_parser()
    args = parser.parse_args()

    rules = get_all_mapping_rules()
    filtered = _filter_rules(rules, fr_table=args.fr_table, to_table=args.to_table)

    if args.limit and args.limit > 0:
        filtered = filtered[: args.limit]

    total, fr_table_count, to_table_count = _summarize(filtered)

    if args.format == "json":
        payload = [asdict(rule) for rule in filtered]
        body = json.dumps(payload, ensure_ascii=False, indent=2)
        if args.out:
            Path(args.out).write_text(body, encoding="utf-8")
            print(f"Wrote {len(payload)} mapping rules to {args.out} (FR_TABLE={fr_table_count}, TO_TABLE={to_table_count})")
        else:
            print(body)
        return

    if args.format == "csv":
        fieldnames = ["map_type", "fr_table", "fr_col", "to_table", "to_col"]
        if args.out:
            with open(args.out, "w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                for rule in filtered:
                    writer.writerow(asdict(rule))
            print(f"Wrote {len(filtered)} mapping rules to {args.out} (FR_TABLE={fr_table_count}, TO_TABLE={to_table_count})")
        else:
            writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            for rule in filtered:
                writer.writerow(asdict(rule))
        return

    print(f"Loaded mapping rules: {total} rows (FR_TABLE={fr_table_count}, TO_TABLE={to_table_count})")
    if not filtered:
        print("No mapping rules matched your filters.")
        return
    print(_format_table(filtered))


if __name__ == "__main__":
    main()
