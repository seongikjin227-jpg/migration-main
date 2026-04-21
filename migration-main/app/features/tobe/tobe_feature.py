"""Reusable helpers for the TOBE generation phase."""

from app.common import logger
from app.services.llm_service import generate_tobe_sql
from app.features.rag.tobe_rag_service import tobe_rag_service
from app.services.validation_service import collect_tobe_sql_column_coverage_issues


def load_tobe_feedback_examples(job, mapping_rules, last_error: str | None = None) -> list[dict[str, str]]:
    """Retrieve TOBE-stage RAG examples for the current job."""
    return tobe_rag_service.retrieve_reference_cases(
        job=job,
        mapping_rules=mapping_rules or [],
        last_error=last_error,
    )


def generate_tobe_sql_with_soft_validation(
    job,
    mapping_rules,
    feedback_examples: list[dict[str, str]] | None = None,
    last_error: str | None = None,
) -> tuple[str, str | None]:
    """Generate TOBE SQL and retry once when soft coverage warnings are detected."""
    selected_rules = mapping_rules or []
    generated_sql = generate_tobe_sql(
        job=job,
        mapping_rules=selected_rules,
        last_error=last_error,
        feedback_examples=feedback_examples or [],
    )
    coverage_issues = collect_tobe_sql_column_coverage_issues(
        tobe_sql=generated_sql,
        mapping_rules=selected_rules,
    )
    if not coverage_issues:
        return generated_sql, None

    coverage_hint = "TOBE_SQL_COLUMN_COVERAGE_HINT: " + " | ".join(coverage_issues)
    retry_hint = f"{last_error}\n{coverage_hint}" if last_error else coverage_hint
    logger.warning(
        f"[TOBE Pipeline] ({job.space_nm}.{job.sql_id}) soft_validation={coverage_hint}"
    )
    regenerated_sql = generate_tobe_sql(
        job=job,
        mapping_rules=selected_rules,
        last_error=retry_hint,
        feedback_examples=feedback_examples or [],
    )
    remaining_issues = collect_tobe_sql_column_coverage_issues(
        tobe_sql=regenerated_sql,
        mapping_rules=selected_rules,
    )
    warning_message = None
    if remaining_issues:
        warning_message = "TOBE_SQL_COLUMN_COVERAGE_HINT: " + " | ".join(remaining_issues)
        logger.warning(
            f"[TOBE Pipeline] ({job.space_nm}.{job.sql_id}) soft_validation_remaining={warning_message}"
        )
    return regenerated_sql, warning_message
