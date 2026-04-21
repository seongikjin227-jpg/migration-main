"""Build evidence/context packs for tuning proposals."""

from app.features.sql_tuning.tuning_models import DetectedRule


def serialize_detected_rules(detected_rules: list[DetectedRule]) -> str:
    """Render detected rules into a compact prompt-friendly text block."""
    if not detected_rules:
        return "- no_detected_rules"
    lines = []
    for item in detected_rules:
        lines.append(
            f"- {item.rule.rule_id} | {item.rule.rule_name} | severity={item.rule.severity} | "
            f"reason={item.detection_reason} | fragment={item.detected_fragment}"
        )
    return "\n".join(lines)


def build_tuning_context(sql_text: str, detected_rules: list[DetectedRule], tag_kind: str) -> str:
    """Build one compact tuning context packet for the LLM proposal stage."""
    lines = [
        f"tag_kind={tag_kind}",
        f"sql_length={len(sql_text or '')}",
        "constraints:",
        "- preserve business semantics",
        "- preserve output row-count semantics",
        "- do not aggressively rewrite joins without justification",
        "- prefer minimal safe structural improvement",
        "- if risk is high, return the most conservative candidate SQL",
        "detected_rules:",
        serialize_detected_rules(detected_rules),
    ]
    return "\n".join(lines)
