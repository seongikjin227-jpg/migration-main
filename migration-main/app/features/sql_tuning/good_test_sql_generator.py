"""Generate GOOD_TEST_SQL for TO_SQL vs GOOD_SQL validation."""

from app.services.llm_service import generate_comparison_test_sql


def generate_good_test_sql(tobe_sql: str, good_sql: str, bind_set_json: str | None) -> str:
    """Reuse deterministic comparison SQL generation for tuning validation."""
    return generate_comparison_test_sql(
        left_sql=tobe_sql,
        right_sql=good_sql,
        bind_set_json=bind_set_json,
    )
