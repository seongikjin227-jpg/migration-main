"""Build evidence/context packs for rule-first tuning prompts."""

from __future__ import annotations

import json

from app.features.sql_tuning.tuning_models import DetectedRule, SupportCase


def serialize_detected_rules(detected_rules: list[DetectedRule]) -> str:
    """Render only the top rule JSON payload needed by the prompt."""
    payload = []
    for item in detected_rules[:3]:
        payload.append(
            {
                "rule_id": item.rule.rule_id,
                "guidance": list(item.rule.guidance),
                "example_bad_sql": item.rule.example_bad_sql,
                "example_tuned_sql": item.rule.example_tuned_sql,
                "match_reason": item.detection_reason,
                "detected_fragment": item.detected_fragment,
                "score": round(item.score, 4),
            }
        )
    return json.dumps(payload, ensure_ascii=False, indent=2)


def serialize_support_case(support_case: SupportCase | None) -> str:
    """Render a compact single support case JSON object."""
    if not support_case:
        return "{}"
    return json.dumps(
        {
            "case_id": support_case.case_id,
            "why_selected": support_case.why_selected,
            "bad_sql": support_case.bad_sql,
            "tuned_sql": support_case.tuned_sql,
            "applied_rules": list(support_case.applied_rules),
        },
        ensure_ascii=False,
        indent=2,
    )


def build_tuning_context(sql_text: str, detected_rules: list[DetectedRule], tag_kind: str) -> str:
    """Build one compact tuning context packet for the LLM proposal stage."""
    lines = [
        f"tag_kind={tag_kind}",
        f"sql_length={len(sql_text or '')}",
        "constraints:",
        "- preserve business semantics",
        "- preserve output row-count semantics",
        "- prefer minimal safe structural improvement",
        "- top_rules_json is the primary guidance",
        "- support_case_json is only a secondary example",
        "selected_rule_ids:",
        ", ".join(item.rule.rule_id for item in detected_rules[:3]) or "(none)",
    ]
    return "\n".join(lines)
