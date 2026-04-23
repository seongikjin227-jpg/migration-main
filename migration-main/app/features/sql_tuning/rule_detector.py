"""Rule-first tuning retrieval for TOBE SQL review."""

from __future__ import annotations

import re

from app.features.sql_tuning.rule_catalog import RULE_CATALOG
from app.features.sql_tuning.tuning_models import DetectedRule


def _contains_pagination_without_order_by(sql_text: str) -> bool:
    lowered = f" {sql_text.lower()} "
    has_pagination = any(token in lowered for token in (" fetch first ", " offset ", " rownum ", " row_number("))
    return has_pagination and " order by " not in lowered


def _contains_non_unique_pagination(sql_text: str) -> bool:
    lowered = f" {sql_text.lower()} "
    return " order by " in lowered and " fetch first " in lowered and "," not in lowered.split(" order by ", 1)[1]


def _contains_old_style_join(sql_text: str) -> bool:
    return "(+)" in sql_text


def _contains_mixed_join_filter(sql_text: str) -> bool:
    upper = sql_text.upper()
    return bool("," in upper and re.search(r"\bWHERE\b.+\b[A-Z_][A-Z0-9_]*\.[A-Z0-9_]+\s*=\s*[A-Z_][A-Z0-9_]*\.[A-Z0-9_]+", upper))


def _contains_distinct_join_pattern(sql_text: str) -> bool:
    lowered = sql_text.lower()
    return "select distinct" in lowered and " join " in lowered


def _contains_cartesian_risk(sql_text: str) -> bool:
    upper = sql_text.upper()
    if " JOIN " in upper:
        return False
    from_match = re.search(r"\bFROM\b(.*?)(?:\bWHERE\b|$)", upper, re.DOTALL)
    if not from_match:
        return False
    from_block = from_match.group(1)
    return "," in from_block and not re.search(r"\b[A-Z_][A-Z0-9_]*\.[A-Z0-9_]+\s*=\s*[A-Z_][A-Z0-9_]*\.[A-Z0-9_]+", upper)


def _contains_unused_join_candidate(sql_text: str) -> bool:
    upper = sql_text.upper()
    return " JOIN " in upper and bool(re.search(r"\bSELECT\s+[A-Z_][A-Z0-9_]*\.", upper))


def _contains_not_in_pattern(sql_text: str) -> bool:
    return bool(re.search(r"\bNOT\s+IN\s*\(", sql_text, re.IGNORECASE))


def _contains_in_subquery(sql_text: str) -> bool:
    return bool(re.search(r"\bIN\s*\(\s*SELECT\b", sql_text, re.IGNORECASE))


def _contains_select_star(sql_text: str) -> bool:
    return bool(re.search(r"\bSELECT\s+\*", sql_text, re.IGNORECASE))


def _contains_duplicate_predicate(sql_text: str) -> bool:
    upper = sql_text.upper()
    where_match = re.search(r"\bWHERE\b(.*)", upper, re.DOTALL)
    if not where_match:
        return False
    terms = [re.sub(r"\s+", " ", term.strip()) for term in re.split(r"\bAND\b", where_match.group(1)) if term.strip()]
    return len(terms) != len(set(terms))


def _contains_constant_predicate(sql_text: str) -> bool:
    return bool(re.search(r"\b1\s*=\s*[01]\b", sql_text, re.IGNORECASE))


def _contains_function_wrapped_filter(sql_text: str) -> bool:
    return bool(re.search(r"\bWHERE\b.*\b(UPPER|LOWER|TRIM|TO_CHAR)\s*\(", sql_text, re.IGNORECASE | re.DOTALL))


def _contains_concat_in_where(sql_text: str) -> bool:
    return bool(re.search(r"\bWHERE\b.*(\|\||CONCAT\s*\()", sql_text, re.IGNORECASE | re.DOTALL))


def _contains_null_unsafe_concat(sql_text: str) -> bool:
    upper = sql_text.upper()
    return "||" in upper and "COALESCE(" not in upper


def _contains_concat_usage(sql_text: str) -> bool:
    upper = sql_text.upper()
    return "||" in upper or "CONCAT(" in upper


def _contains_to_char_compare(sql_text: str) -> bool:
    return bool(re.search(r"\bTO_CHAR\s*\(", sql_text, re.IGNORECASE))


def _contains_trivial_subquery(sql_text: str) -> bool:
    return bool(re.search(r"\bFROM\s*\(\s*SELECT\b", sql_text, re.IGNORECASE))


def _contains_deep_nested_subquery(sql_text: str) -> bool:
    return len(re.findall(r"\bFROM\s*\(\s*SELECT\b", sql_text, re.IGNORECASE)) >= 2


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
        return bool(re.search(r"\b(EXISTS|IN)\s*\(\s*SELECT\b", sql_text, re.IGNORECASE))


def _contains_trivial_cte(sql_text: str) -> bool:
    return sql_text.upper().count("WITH ") > 0 and sql_text.upper().count(" AS (") >= 2


def _contains_distinct_group_by(sql_text: str) -> bool:
    upper = sql_text.upper()
    return "SELECT DISTINCT" in upper and "GROUP BY" in upper


def _contains_having_non_aggregate(sql_text: str) -> bool:
    upper = sql_text.upper()
    return "HAVING" in upper and not re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", upper)


def _contains_agg_join_explosion(sql_text: str) -> bool:
    upper = sql_text.upper()
    return upper.count(" JOIN ") >= 2 and "GROUP BY" in upper


def _contains_unclear_alias(sql_text: str) -> bool:
    return bool(re.search(r"\bSELECT\b.*\b[A-Z_][A-Z0-9_]*\.[A-Z0-9_]+\s+[A-Z_][A-Z0-9_]*\b", sql_text, re.IGNORECASE))


def _contains_redundant_inner_order(sql_text: str) -> bool:
    return bool(re.search(r"\bFROM\s*\(\s*SELECT\b.*\bORDER BY\b", sql_text, re.IGNORECASE | re.DOTALL))


def _contains_repeat_cast(sql_text: str) -> bool:
    return "CAST(CAST(" in sql_text.upper()


def _build_detected_rule(rule_id: str, fragment: str, reason: str, score: float) -> DetectedRule:
    return DetectedRule(
        rule=RULE_CATALOG[rule_id],
        detected_fragment=fragment,
        detection_reason=reason,
        score=score,
    )


def detect_tuning_rules(sql_text: str) -> list[DetectedRule]:
    """Return the top rule candidates for one TOBE SQL."""
    detected: list[DetectedRule] = []

    if _contains_old_style_join(sql_text):
        detected.append(_build_detected_rule("RULE_J001", "(+)", "old-style outer join syntax detected", 1.0))
    if _contains_mixed_join_filter(sql_text):
        detected.append(_build_detected_rule("RULE_J002", "WHERE join predicate", "join and filter predicates are mixed", 0.95))
    if _contains_distinct_join_pattern(sql_text):
        detected.append(_build_detected_rule("RULE_J003", "SELECT DISTINCT with JOIN", "DISTINCT may be masking duplicate-producing joins", 0.90))
    if _contains_cartesian_risk(sql_text):
        detected.append(_build_detected_rule("RULE_J004", "multi-table without join predicate", "Cartesian join risk detected", 0.98))
    if _contains_unused_join_candidate(sql_text):
        detected.append(_build_detected_rule("RULE_J005", "JOIN alias usage review", "joined table usage may be unnecessary", 0.45))
    if _contains_pagination_without_order_by(sql_text):
        detected.append(_build_detected_rule("RULE_P001", "pagination without ORDER BY", "pagination exists but ORDER BY is missing", 0.96))
    if _contains_non_unique_pagination(sql_text):
        detected.append(_build_detected_rule("RULE_P002", "non-unique ORDER BY with pagination", "page drift risk detected", 0.70))
    if "ROWNUM" in sql_text.upper():
        detected.append(_build_detected_rule("RULE_P003", "ROWNUM", "rownum-based paging detected", 0.88))
    if _contains_concat_usage(sql_text):
        detected.append(_build_detected_rule("RULE_C001", "concat style", "string concatenation style detected", 0.35))
    if _contains_null_unsafe_concat(sql_text):
        detected.append(_build_detected_rule("RULE_C002", "null-unsafe concat", "concat may require COALESCE protection", 0.68))
    if _contains_concat_in_where(sql_text):
        detected.append(_build_detected_rule("RULE_C003", "concat in WHERE", "concat usage in predicate may hurt filtering", 0.72))
    if _contains_to_char_compare(sql_text):
        detected.append(_build_detected_rule("RULE_C004", "TO_CHAR compare", "string conversion for comparison detected", 0.66))
    if _contains_function_wrapped_filter(sql_text):
        detected.append(_build_detected_rule("RULE_F001", "function-wrapped predicate", "function on filter column detected", 0.74))
    if _contains_duplicate_predicate(sql_text):
        detected.append(_build_detected_rule("RULE_F002", "duplicate predicate", "same predicate appears multiple times", 0.83))
    if _contains_constant_predicate(sql_text):
        detected.append(_build_detected_rule("RULE_F003", "constant predicate", "always true/false predicate detected", 0.82))
    if _contains_not_in_pattern(sql_text):
        detected.append(_build_detected_rule("RULE_F004", "NOT IN", "NOT IN may behave unexpectedly when NULL values are present", 0.91))
    elif _contains_in_subquery(sql_text):
        detected.append(_build_detected_rule("RULE_F005", "IN-subquery", "IN-subquery candidate for EXISTS/JOIN review", 0.64))
    if _contains_trivial_subquery(sql_text):
        detected.append(_build_detected_rule("RULE_S001", "simple nested subquery", "trivial subquery flattening candidate", 0.75))
    if _contains_deep_nested_subquery(sql_text):
        detected.append(_build_detected_rule("RULE_S002", "deep nested subquery", "nested subquery depth is high", 0.70))
    if _contains_correlated_subquery(sql_text):
        detected.append(_build_detected_rule("RULE_S003", "correlated subquery", "subquery may depend on outer query aliases", 0.89))
    if _contains_trivial_cte(sql_text):
        detected.append(_build_detected_rule("RULE_S004", "trivial CTEs", "multiple trivial CTEs detected", 0.58))
    if _contains_distinct_group_by(sql_text):
        detected.append(_build_detected_rule("RULE_A001", "DISTINCT + GROUP BY", "DISTINCT and GROUP BY are used together", 0.86))
    if _contains_having_non_aggregate(sql_text):
        detected.append(_build_detected_rule("RULE_A002", "HAVING non-aggregate predicate", "HAVING clause contains non-aggregate condition", 0.78))
    if _contains_agg_join_explosion(sql_text):
        detected.append(_build_detected_rule("RULE_A003", "aggregation join explosion", "multiple joins before GROUP BY may increase cardinality", 0.71))
    if _contains_select_star(sql_text):
        detected.append(_build_detected_rule("RULE_R001", "SELECT *", "projection is implicit and may hide unnecessary columns", 0.80))
    if _contains_unclear_alias(sql_text):
        detected.append(_build_detected_rule("RULE_R002", "unclear alias", "projection alias formatting can be improved", 0.42))
    if _contains_redundant_inner_order(sql_text):
        detected.append(_build_detected_rule("RULE_R003", "inner ORDER BY", "redundant inner ORDER BY detected", 0.65))
    if _contains_repeat_cast(sql_text):
        detected.append(_build_detected_rule("RULE_R004", "repeated CAST", "nested CAST repetition detected", 0.67))

    deduped: dict[str, DetectedRule] = {}
    for item in detected:
        current = deduped.get(item.rule.rule_id)
        if current is None or item.score > current.score:
            deduped[item.rule.rule_id] = item
    return sorted(deduped.values(), key=lambda item: item.score, reverse=True)
