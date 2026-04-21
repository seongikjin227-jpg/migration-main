"""Static rule catalog for SQL tuning review."""

from app.features.sql_tuning.tuning_models import TuningRule


RULE_CATALOG = {
    "RULE_P001": TuningRule("RULE_P001", "ORDER BY 없는 pagination", "pagination", "high", "high", True),
    "RULE_J003": TuningRule("RULE_J003", "DISTINCT로 join 중복 은폐 의심", "join", "high", "high", True),
    "RULE_F004": TuningRule("RULE_F004", "NOT IN + NULL 위험", "filter", "high", "high", True),
    "RULE_S003": TuningRule("RULE_S003", "correlated subquery 남용", "subquery", "high", "high", True),
    "RULE_R001": TuningRule("RULE_R001", "SELECT * 사용", "projection", "medium", "medium", False),
}


def list_rule_catalog() -> list[TuningRule]:
    """Return all configured tuning rules."""
    return list(RULE_CATALOG.values())
