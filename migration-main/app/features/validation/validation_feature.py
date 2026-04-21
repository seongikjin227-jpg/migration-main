"""Reusable helpers for validation TEST SQL generation and execution."""

from app.services.llm_service import generate_test_sql, generate_test_sql_no_bind
from app.services.validation_service import evaluate_status_from_test_rows, execute_test_query


def generate_validation_test_sql(job, tobe_sql: str, bind_param_names: list[str], bind_set_json_for_test: str) -> str:
    """Build the TEST SQL used to compare FROM and TO result counts."""
    if not bind_param_names:
        return generate_test_sql_no_bind(job=job, tobe_sql=tobe_sql)
    return generate_test_sql(job=job, tobe_sql=tobe_sql, bind_set_json=bind_set_json_for_test)


def execute_validation_test_sql(test_sql: str) -> list[dict]:
    """Execute generated validation SQL."""
    return execute_test_query(test_sql)


def evaluate_validation_status(test_rows: list[dict]) -> str:
    """Convert TEST rows into PASS/FAIL."""
    return evaluate_status_from_test_rows(test_rows)
