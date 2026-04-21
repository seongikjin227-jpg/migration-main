"""Persistence adapters for the tuning pipeline."""

from app.repositories.result_repository import insert_tuning_log, update_tuning_result


def persist_tuning_result(row_id: str, good_sql: str | None, good_test_sql: str | None, tuning_status: str) -> None:
    """Persist tuning-stage final fields back to NEXT_SQL_INFO."""
    update_tuning_result(
        row_id=row_id,
        good_sql=good_sql,
        good_test_sql=good_test_sql,
        tuning_status=tuning_status,
    )


def persist_tuning_log(
    space_nm: str,
    sql_id: str,
    tuning_status: str,
    llm_used_yn: str,
    applied_rule_ids: list[str] | None,
    diff_summary: str | None,
    error_message: str | None,
) -> None:
    """Insert one summary tuning log row when available."""
    insert_tuning_log(
        space_nm=space_nm,
        sql_id=sql_id,
        tuning_status=tuning_status,
        llm_used_yn=llm_used_yn,
        applied_rule_ids=",".join(applied_rule_ids or []),
        diff_summary=diff_summary,
        error_message=error_message,
    )
