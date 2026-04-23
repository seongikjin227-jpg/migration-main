"""TOBE generation-stage wrapper used by the job flow."""

from __future__ import annotations

from dataclasses import dataclass

from app.features.tobe.tobe_feature import generate_tobe_sql_with_soft_validation


@dataclass
class TobeGenerationStageResult:
    """TOBE SQL generation result plus optional warning text."""

    tobe_sql: str
    block_rag_content: str
    warning_message: str | None = None


def run_tobe_generation_stage(
    job,
    mapping_rules,
    last_error: str | None = None,
) -> TobeGenerationStageResult:
    """Generate TOBE SQL and return any soft-validation warning."""

    tobe_sql, block_rag_content, warning_message = generate_tobe_sql_with_soft_validation(
        job=job,
        mapping_rules=mapping_rules,
        last_error=last_error,
    )
    return TobeGenerationStageResult(
        tobe_sql=tobe_sql,
        block_rag_content=block_rag_content,
        warning_message=warning_message,
    )
