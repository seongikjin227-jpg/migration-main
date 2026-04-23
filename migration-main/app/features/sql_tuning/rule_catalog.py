"""Load the SQL tuning rule catalog from JSON."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from app.features.sql_tuning.tuning_models import TuningRule


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
RULE_CATALOG_PATH = ROOT_DIR / "data" / "rag" / "DATA" / "tobe_rule_catalog.json"


@lru_cache(maxsize=1)
def _load_catalog() -> dict[str, TuningRule]:
    payload = json.loads(RULE_CATALOG_PATH.read_text(encoding="utf-8"))
    result: dict[str, TuningRule] = {}
    for item in payload.get("rules", []):
        rule = TuningRule(
            rule_id=str(item.get("rule_id") or "").strip(),
            guidance=[str(text).strip() for text in item.get("guidance", []) if str(text).strip()],
            example_bad_sql=str(item.get("example_bad_sql") or "").strip(),
            example_tuned_sql=str(item.get("example_tuned_sql") or "").strip(),
        )
        if rule.rule_id:
            result[rule.rule_id] = rule
    return result


RULE_CATALOG = _load_catalog()


def list_rule_catalog() -> list[TuningRule]:
    """Return all configured tuning rules."""
    return list(RULE_CATALOG.values())
