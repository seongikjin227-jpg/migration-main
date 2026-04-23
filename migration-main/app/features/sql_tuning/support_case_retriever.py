"""Select one compact support case for rule-first tuning prompts."""

from __future__ import annotations

import json
from pathlib import Path

from app.features.sql_tuning.tuning_models import DetectedRule, SupportCase


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
SAMPLE_CASE_PATH = ROOT_DIR / "data" / "rag" / "DATA" / "tobe_rag_samples.json"


def select_support_case(top_rules: list[DetectedRule]) -> SupportCase | None:
    rule_ids = [item.rule.rule_id for item in top_rules if item.rule.rule_id]
    if not rule_ids or not SAMPLE_CASE_PATH.exists():
        return None

    payload = json.loads(SAMPLE_CASE_PATH.read_text(encoding="utf-8"))
    rows = payload.get("rows", []) if isinstance(payload, dict) else []
    best_row: dict[str, object] | None = None
    best_score = -1

    for row in rows:
        if not isinstance(row, dict):
            continue
        expected_rules = [str(item).strip() for item in row.get("expected_rules", []) if str(item).strip()]
        overlap = len(set(rule_ids).intersection(expected_rules))
        if overlap <= 0:
            continue
        style_bonus = 1 if "JOIN" in str(row.get("style_goal") or "").upper() and any(rule_id.startswith("RULE_J") for rule_id in rule_ids) else 0
        score = overlap * 10 + style_bonus
        if score > best_score:
            best_score = score
            best_row = row

    if not best_row:
        return None

    matched_rules = [rule_id for rule_id in rule_ids if rule_id in set(str(item).strip() for item in best_row.get("expected_rules", []))]
    return SupportCase(
        case_id=str(best_row.get("sql_id") or best_row.get("row_id") or "SUPPORT_CASE"),
        why_selected=f"selected because it overlaps with rules: {', '.join(matched_rules)}",
        bad_sql=str(best_row.get("fr_sql_text") or best_row.get("to_sql_text") or "").strip(),
        tuned_sql=str(best_row.get("correct_sql") or "").strip(),
        applied_rules=matched_rules,
    )
