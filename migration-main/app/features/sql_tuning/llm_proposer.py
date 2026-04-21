"""LLM-backed GOOD_SQL proposal generator."""

from app.services.llm_service import generate_good_sql


def propose_good_sql(job, tobe_sql: str, normalized_sql: str, detected_rules_text: str, tuning_context_text: str) -> str:
    """Generate one GOOD_SQL proposal using the configured LLM."""
    return generate_good_sql(
        job=job,
        tobe_sql=tobe_sql,
        normalized_sql=normalized_sql,
        detected_rules_text=detected_rules_text,
        tuning_context_text=tuning_context_text,
    )
