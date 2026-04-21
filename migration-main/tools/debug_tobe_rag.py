"""Debug helper that prints TOBE RAG preprocessing, parsing, and document outputs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from _bootstrap import ROOT_DIR  # noqa: F401

sys.path.insert(0, str(ROOT_DIR))

from app.features.rag.tobe_rag_indexer import (  # noqa: E402
    build_case_documents,
    build_change_summary,
    build_rule_snippets,
    extract_case_features,
    extract_rule_tags,
    extract_sql_blocks,
    normalize_sql,
    preprocess_mybatis_sql,
)
from app.features.rag.tobe_rag_models import TobeRagCase  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Debug TOBE RAG preprocessing and document generation.")
    parser.add_argument("--from-file", help="Path to source SQL text file.")
    parser.add_argument("--correct-file", help="Path to correct SQL text file.")
    parser.add_argument("--dialect", default="oracle")
    return parser


def _read_text(path: str | None) -> str:
    if not path:
        return ""
    return Path(path).read_text(encoding="utf-8")


def main() -> None:
    args = _build_parser().parse_args()
    source_sql = _read_text(args.from_file)
    correct_sql = _read_text(args.correct_file)
    if not source_sql.strip():
        print("[ERROR] --from-file is required and must contain SQL text.")
        return

    print("=== SOURCE SQL ===")
    print(source_sql.strip())
    print("")

    preprocessed = preprocess_mybatis_sql(source_sql)
    print("=== PREPROCESSED SQL ===")
    print(preprocessed.strip())
    print("")

    try:
        normalized = normalize_sql(preprocessed, dialect=args.dialect)
        print("=== NORMALIZED SQL ===")
        print(normalized)
        print("")

        features = extract_case_features(preprocessed, normalized, dialect=args.dialect)
        print("=== FEATURES ===")
        print(json.dumps(features, ensure_ascii=False, indent=2))
        print("")
    except Exception as exc:
        print("=== PARSE ERROR ===")
        print(repr(exc))
        return

    rule_tags = extract_rule_tags(source_sql, correct_sql)
    print("=== RULE TAGS ===")
    print(json.dumps(rule_tags, ensure_ascii=False, indent=2))
    print("")

    change_summary = build_change_summary(rule_tags, features)
    print("=== CHANGE SUMMARY ===")
    print(change_summary or "(none)")
    print("")

    blocks = extract_sql_blocks(normalized, correct_sql)
    print("=== BLOCKS ===")
    print(json.dumps(blocks, ensure_ascii=False, indent=2))
    print("")

    snippets = build_rule_snippets(normalized, correct_sql, rule_tags)
    print("=== RULE SNIPPETS ===")
    print(json.dumps(snippets, ensure_ascii=False, indent=2))
    print("")

    case = TobeRagCase(
        case_id="DEBUG_CASE",
        row_id="DEBUG_ROW",
        space_nm="DEBUG",
        sql_id="DEBUG_SQL",
        tag_kind="SELECT",
        source_db="Oracle",
        target_db="PostgreSQL",
        asis_sql_raw=source_sql,
        asis_sql_preprocessed=preprocessed,
        asis_sql_normalized=normalized,
        tobe_sql_generated="",
        correct_sql=correct_sql,
        target_tables=[],
        table_names=features["table_names"],
        column_names=features["column_names"],
        rule_tags=rule_tags,
        feature_tags=features["feature_tags"],
        functions=features["functions"],
        change_summary=change_summary,
    )
    docs = build_case_documents(case)
    print("=== GENERATED DOCS ===")
    for doc in docs:
        print(f"[{doc.doc_type}] {doc.doc_id}")
        print(doc.doc_text)
        print(f"tokens={json.dumps(doc.tokenized_text, ensure_ascii=False)}")
        print("")


if __name__ == "__main__":
    main()
