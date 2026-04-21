"""Rule detection for SQL tuning review."""

from __future__ import annotations

import re

from app.features.sql_tuning.rule_catalog import RULE_CATALOG
from app.features.sql_tuning.tuning_models import DetectedRule


def _contains_pagination_without_order_by(sql_text: str) -> bool:
    lowered = sql_text.lower()
    has_pagination = any(token in lowered for token in (" limit ", " fetch first ", " offset ", " rownum "))
    return has_pagination and " order by " not in lowered


def _contains_distinct_join_pattern(sql_text: str) -> bool:
    lowered = sql_text.lower()
    return "select distinct" in lowered and " join " in lowered


def _contains_not_in_pattern(sql_text: str) -> bool:
    return bool(re.search(r"\bNOT\s+IN\s*\(", sql_text, re.IGNORECASE))


def _contains_select_star(sql_text: str) -> bool:
    return bool(re.search(r"\bSELECT\s+\*", sql_text, re.IGNORECASE))


def _contains_correlated_subquery(sql_text: str) -> bool:
    try:
        import sqlglot  # type: ignore
        from sqlglot import exp  # type: ignore

        parsed = sqlglot.parse_one(sql_text, read="oracle")
        aliases = {
            table.alias_or_name.upper()
            for table in parsed.find_all(exp.Table)
            if getattr(table, "alias_or_name", None)
        }
        for subquery in parsed.find_all(exp.Subquery):
            sub_sql = subquery.sql(dialect="oracle").upper()
            if any(f"{alias}." in sub_sql for alias in aliases):
                return True
        return False
    except Exception:
        # Conservative fallback: detect EXISTS/IN subquery with outer alias-like references.
        return bool(re.search(r"\b(EXISTS|IN)\s*\(\s*SELECT\b", sql_text, re.IGNORECASE))


def detect_tuning_rules(sql_text: str) -> list[DetectedRule]:
    """Detect v1 tuning anti-patterns from normalized SQL."""
    detected: list[DetectedRule] = []

    if _contains_pagination_without_order_by(sql_text):
        detected.append(
            DetectedRule(RULE_CATALOG["RULE_P001"], "pagination without ORDER BY", "pagination exists but ORDER BY is missing")
        )
    if _contains_distinct_join_pattern(sql_text):
        detected.append(
            DetectedRule(RULE_CATALOG["RULE_J003"], "SELECT DISTINCT with JOIN", "DISTINCT may be hiding duplicate-producing joins")
        )
    if _contains_not_in_pattern(sql_text):
        detected.append(
            DetectedRule(RULE_CATALOG["RULE_F004"], "NOT IN (...)", "NOT IN may behave unexpectedly when NULL values are present")
        )
    if _contains_correlated_subquery(sql_text):
        detected.append(
            DetectedRule(RULE_CATALOG["RULE_S003"], "correlated subquery candidate", "subquery may depend on outer query aliases")
        )
    if _contains_select_star(sql_text):
        detected.append(
            DetectedRule(RULE_CATALOG["RULE_R001"], "SELECT *", "projection is implicit and may hide unnecessary columns")
        )
    return detected
