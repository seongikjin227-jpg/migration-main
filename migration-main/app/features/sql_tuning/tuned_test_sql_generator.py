"""Generate TUNED_TEST_SQL for TO_SQL vs TUNED_SQL validation."""

from app.services.llm_service import generate_comparison_test_sql


def generate_tuned_test_sql(tobe_sql: str, tuned_sql: str, bind_set_json: str | None) -> str:
    """Reuse deterministic comparison SQL generation for tuning validation."""
    return generate_comparison_test_sql(
        left_sql=tobe_sql,
        right_sql=tuned_sql,
        bind_set_json=bind_set_json,
    )
