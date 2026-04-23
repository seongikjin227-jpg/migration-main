"""Reusable helpers for bind discovery and bind-set preparation."""

from app.services.binding_service import bind_sets_to_json, build_bind_sets, extract_bind_param_names
from app.services.llm_service import generate_bind_sql
from app.features.rag.bind_rag_service import bind_rag_service
from app.services.validation_service import execute_binding_query


def detect_bind_param_names(tobe_sql: str, source_sql: str) -> list[str]:
    """Detect bind parameters from TOBE SQL, falling back to source SQL."""
    return extract_bind_param_names(tobe_sql) or extract_bind_param_names(source_sql)


def load_bind_feedback_examples(job, tobe_sql: str, last_error: str | None, current_stage: str) -> list[dict[str, str]]:
    """Retrieve bind-stage RAG examples."""
    return bind_rag_service.retrieve_bind_examples(
        job=job,
        last_error=last_error,
        tobe_sql=tobe_sql,
        current_stage=current_stage,
    )


def generate_bind_sql_text(
    job,
    tobe_sql: str,
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> str:
    """Generate bind discovery SQL."""
    return generate_bind_sql(
        job=job,
        tobe_sql=tobe_sql,
        last_error=last_error,
        feedback_examples=feedback_examples or [],
    )


def execute_bind_sql_text(bind_sql: str, max_rows: int = 50) -> list[dict]:
    """Execute bind discovery SQL."""
    return execute_binding_query(bind_sql, max_rows=max_rows)


def build_bind_payloads(tobe_sql: str, source_sql: str, bind_query_rows: list[dict], max_cases: int = 3) -> tuple[str, str | None]:
    """Build bind-set payloads for test and persistence layers."""
    bind_sets = build_bind_sets(
        tobe_sql=tobe_sql,
        source_sql=source_sql,
        bind_query_rows=bind_query_rows or [],
        max_cases=max_cases,
    )
    bind_set_json = bind_sets_to_json(bind_sets)
    return bind_set_json, bind_set_json or None
