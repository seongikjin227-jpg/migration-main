"""Verification helpers for TUNED_SQL proposals."""

from __future__ import annotations

from app.common import DBSqlError
from app.features.validation.validation_feature import execute_validation_test_sql, evaluate_validation_status


def _syntax_check_sql(sql_text: str) -> None:
    """Validate SQL syntax using sqlglot when available."""
    if not sql_text.strip():
        raise DBSqlError("TUNED_SQL is empty.")
    try:
        import sqlglot  # type: ignore

        sqlglot.parse_one(sql_text, read="oracle")
    except ImportError:
        return
    except Exception as exc:
        raise DBSqlError(f"TUNED_SQL syntax check failed: {exc}") from exc


def verify_tuned_sql(tuned_sql: str, tuned_test_sql: str, tag_kind: str) -> tuple[str, str | None]:
    """Verify one TUNED_SQL proposal."""
    _syntax_check_sql(tuned_sql)
    if tag_kind.strip().upper() != "SELECT":
        return "PROPOSAL_GENERATED", "non-select job uses proposal-only tuning review"
    test_rows = execute_validation_test_sql(tuned_test_sql)
    status = evaluate_validation_status(test_rows)
    if status == "PASS":
        return "AUTO_TUNED_VERIFIED", None
    return "TUNING_FAILED", "TUNED_SQL validation failed against TO_SQL_TEXT"
