"""Second-pass SQL tuning pipeline for TUNED_SQL proposal + verification."""

from __future__ import annotations

from app.common import LLMRateLimitError
from app.features.sql_tuning.tuned_test_sql_generator import generate_tuned_test_sql
from app.features.sql_tuning.llm_proposer import propose_tuned_sql
from app.features.sql_tuning.rule_detector import detect_tuning_rules
from app.features.sql_tuning.sql_normalizer import normalize_sql_for_tuning
from app.features.sql_tuning.support_case_retriever import select_support_case
from app.features.sql_tuning.tuning_context_builder import build_tuning_context, serialize_detected_rules, serialize_support_case
from app.features.sql_tuning.tuning_models import TuningPipelineResult, TuningStatus
from app.features.sql_tuning.tuning_verifier import verify_tuned_sql


def run_tuning_pipeline(job, tobe_sql: str, bind_set_json: str | None) -> TuningPipelineResult:
    """Run the full TUNED_SQL proposal and verification flow."""
    if not (tobe_sql or "").strip():
        return TuningPipelineResult(
            tuning_status=TuningStatus.TUNING_SKIPPED,
            error_message="TO_SQL_TEXT is empty; tuning skipped",
        )

    normalized_sql, normalization_notes = normalize_sql_for_tuning(tobe_sql)
    detected_rules = detect_tuning_rules(normalized_sql)
    top_rules = detected_rules[:3]
    top_rules_json = serialize_detected_rules(top_rules)
    support_case = select_support_case(top_rules)
    support_case_json = serialize_support_case(support_case)
    tuning_context_text = build_tuning_context(normalized_sql, detected_rules, job.tag_kind)

    try:
        tuned_sql = propose_tuned_sql(
            job=job,
            tobe_sql=tobe_sql,
            top_rules_json=top_rules_json,
            support_case_json=support_case_json,
            tuning_context_text=tuning_context_text,
        )
    except LLMRateLimitError as exc:
        return TuningPipelineResult(
            tuning_status=TuningStatus.TUNING_FAILED,
            normalized_sql=normalized_sql,
            normalization_notes=normalization_notes,
            detected_rules=detected_rules,
            top_rules_json=top_rules_json,
            support_case_json=support_case_json,
            tuning_context_text=tuning_context_text,
            llm_used_yn="Y",
            applied_rule_ids=[item.rule.rule_id for item in detected_rules],
            error_message=str(exc),
        )
    except Exception as exc:
        return TuningPipelineResult(
            tuning_status=TuningStatus.PROPOSAL_GENERATED,
            normalized_sql=normalized_sql,
            normalization_notes=normalization_notes,
            detected_rules=detected_rules,
            top_rules_json=top_rules_json,
            support_case_json=support_case_json,
            tuning_context_text=tuning_context_text,
            llm_used_yn="Y",
            applied_rule_ids=[item.rule.rule_id for item in detected_rules],
            error_message=str(exc),
        )

    tuned_test_sql = ""
    if (job.tag_kind or "").strip().upper() == "SELECT":
        tuned_test_sql = generate_tuned_test_sql(tobe_sql=tobe_sql, tuned_sql=tuned_sql, bind_set_json=bind_set_json)
    verification_status, verification_error = verify_tuned_sql(
        tuned_sql=tuned_sql,
        tuned_test_sql=tuned_test_sql,
        tag_kind=job.tag_kind,
    )
    final_tuned_sql = tuned_sql if verification_status == TuningStatus.AUTO_TUNED_VERIFIED else None
    final_tuned_test_sql = tuned_test_sql if verification_status == TuningStatus.AUTO_TUNED_VERIFIED else None
    diff_summary = (
        "Detected rules: " + ", ".join(item.rule.rule_id for item in detected_rules)
        if detected_rules
        else "No major tuning rules detected; proposal generated conservatively"
    )
    return TuningPipelineResult(
        tuning_status=verification_status,
        normalized_sql=normalized_sql,
        normalization_notes=normalization_notes,
        detected_rules=detected_rules,
        top_rules_json=top_rules_json,
        support_case_json=support_case_json,
        tuning_context_text=tuning_context_text,
        tuned_sql=final_tuned_sql,
        tuned_test_sql=final_tuned_test_sql,
        llm_used_yn="Y",
        applied_rule_ids=[item.rule.rule_id for item in detected_rules],
        diff_summary=diff_summary,
        error_message=verification_error,
    )
