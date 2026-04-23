"""LLM-backed TUNED_SQL proposal generator."""

from app.services.llm_service import generate_tuned_sql


def propose_tuned_sql(job, tobe_sql: str, top_rules_json: str, support_case_json: str, tuning_context_text: str) -> str:
    """Generate one TUNED_SQL proposal using the configured LLM."""
    return generate_tuned_sql(
        job=job,
        tobe_sql=tobe_sql,
        top_rules_json=top_rules_json,
        support_case_json=support_case_json,
        tuning_context_text=tuning_context_text,
    )
