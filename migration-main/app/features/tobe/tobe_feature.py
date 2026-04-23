"""Reusable helpers for the TOBE generation phase."""

from app.common import logger
from app.features.tobe.tobe_block_rag_flow import build_tobe_block_rag_context
from app.services.llm_service import generate_tobe_sql
from app.services.validation_service import collect_tobe_sql_column_coverage_issues


def generate_tobe_sql_with_soft_validation(
    job,
    mapping_rules,
    last_error: str | None = None,
) -> tuple[str, str, str | None]:
    """Generate TOBE SQL and retry once when soft coverage warnings are detected."""
    selected_rules = mapping_rules or []
    block_rag_context = build_tobe_block_rag_context(job.source_sql)
    generated_sql = generate_tobe_sql(
        job=job,
        mapping_rules=selected_rules,
        last_error=last_error,
    )
    coverage_issues = collect_tobe_sql_column_coverage_issues(
        tobe_sql=generated_sql,
        mapping_rules=selected_rules,
    )
    if not coverage_issues:
        return generated_sql, block_rag_context, None

    coverage_hint = "TOBE_SQL_COLUMN_COVERAGE_HINT: " + " | ".join(coverage_issues)
    retry_hint = f"{last_error}\n{coverage_hint}" if last_error else coverage_hint
    logger.warning(
        f"[TOBE Pipeline] ({job.space_nm}.{job.sql_id}) soft_validation={coverage_hint}"
    )
    regenerated_sql = generate_tobe_sql(
        job=job,
        mapping_rules=selected_rules,
        last_error=retry_hint,
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
    return regenerated_sql, block_rag_context, warning_message
