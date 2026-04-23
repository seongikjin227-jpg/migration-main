"""Persistence adapters for the tuning pipeline."""

import json

from app.repositories.result_repository import insert_tuning_log, update_tuning_result


def persist_tuning_result(row_id: str, tuned_sql: str | None, tuned_test_sql: str | None, tuning_status: str) -> None:
    """Persist tuning-stage final fields back to NEXT_SQL_INFO."""
    update_tuning_result(
        row_id=row_id,
        tuned_sql=tuned_sql,
        tuned_test_sql=tuned_test_sql,
        tuning_status=tuning_status,
    )


def persist_tuning_log(
    execution_id: str | None,
    row_id: str | None,
    space_nm: str,
    sql_id: str,
    tag_kind: str | None,
    tuning_status: str,
    job_status: str | None,
    final_stage: str | None,
    retry_count: int | None,
    llm_used_yn: str,
    applied_rule_ids: list[str] | None,
    diff_summary: str | None,
    error_message: str | None,
    tobe_sql: str | None = None,
    tobe_rag_debug: dict[str, object] | None = None,
    tobe_feedback_examples: list[dict[str, str]] | None = None,
) -> None:
    """Insert one summary tuning log row when available."""
    rag_debug = tobe_rag_debug or {}
    query_case = rag_debug.get("query_case") if isinstance(rag_debug, dict) else {}
    retrieved_rule_ids = rag_debug.get("retrieved_rule_ids") if isinstance(rag_debug, dict) else []
    retrieved_cases = rag_debug.get("retrieved_cases") if isinstance(rag_debug, dict) else []
    if not isinstance(query_case, dict):
        query_case = {}
    if not isinstance(retrieved_rule_ids, list):
        retrieved_rule_ids = []
    if not isinstance(retrieved_cases, list):
        retrieved_cases = []
    insert_tuning_log(
        execution_id=execution_id,
        row_id=row_id,
        space_nm=space_nm,
        sql_id=sql_id,
        tag_kind=tag_kind,
        tuning_status=tuning_status,
        job_status=job_status,
        final_stage=final_stage,
        retry_count=retry_count,
        llm_used_yn=llm_used_yn,
        applied_rule_ids=",".join(applied_rule_ids or []),
        diff_summary=diff_summary,
        error_message=error_message,
        tobe_sql=tobe_sql,
        tobe_rag_debug_json=json.dumps(tobe_rag_debug or {}, ensure_ascii=False),
        tobe_feedback_examples_json=json.dumps(tobe_feedback_examples or [], ensure_ascii=False),
        retrieved_rule_ids_json=json.dumps(retrieved_rule_ids or [], ensure_ascii=False),
        retrieved_case_ids_json=json.dumps([item.get("case_id") for item in retrieved_cases if item.get("case_id")], ensure_ascii=False),
        source_sql_raw=str(query_case.get("source_sql_raw") or ""),
        source_sql_preprocessed=str(query_case.get("source_sql_preprocessed") or ""),
        source_sql_normalized=str(query_case.get("source_sql_normalized") or ""),
        retrieval_query_text=str(query_case.get("retrieval_query_text") or ""),
    )
