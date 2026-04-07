import json
import time

from app.exceptions import LLMRateLimitError
from app.logger import logger
from app.repositories.mapper_repository import get_all_mapping_rules
from app.repositories.result_repository import (
    finalize_failed_job,
    get_feedback_examples,
    update_cycle_result,
)
from app.runtime import is_stop_requested
from app.services.binding_service import bind_sets_to_json, build_bind_sets
from app.services.llm_service import generate_bind_sql, generate_test_sql, generate_tobe_sql
from app.services.validation_service import evaluate_status_from_test_rows, execute_binding_query, execute_test_query


class MigrationOrchestrator:
    def process_job(self, job):
        logger.info("\n==========================================")
        logger.info(f"[Orchestrator] Starting job ({job.space_nm}.{job.sql_id})")
        job_key = f"{job.space_nm}.{job.sql_id}"

        def log_stage(stage_name: str, event: str, detail: str | None = None):
            suffix = f" {detail}" if detail else ""
            logger.info(f"[Orchestrator] ({job_key}) stage={stage_name} {event}{suffix}")

        if is_stop_requested():
            logger.info(f"[Orchestrator] Stop requested before start ({job_key}). Skipping.")
            return

        retry_count = 0
        max_retries = 3
        last_error = None

        while retry_count <= max_retries:
            if is_stop_requested():
                logger.info(f"[Orchestrator] Stop requested ({job_key}). Aborting job.")
                return
            stage = "INIT"
            bind_set_json = "[]"
            try:
                stage = "LOAD_RULES"
                mapping_rules = get_all_mapping_rules()
                feedback_examples = get_feedback_examples(job)
                log_stage(
                    stage,
                    "completed",
                    f"(mapping_rules={len(mapping_rules)}, feedback_examples={len(feedback_examples)})",
                )

                stage = "GENERATE_TOBE_SQL"
                tobe_sql = generate_tobe_sql(
                    job=job,
                    mapping_rules=mapping_rules,
                    last_error=last_error,
                    feedback_examples=feedback_examples,
                )
                log_stage(stage, "completed", f"(sql_length={len(tobe_sql)})")

                tag_kind = (job.tag_kind or "").strip().upper()
                if tag_kind != "SELECT":
                    stage = "SKIP_TEST_FOR_NON_SELECT"
                    status = "SKIP(NON_SELECT)"
                    final_log = (
                        f"FINAL SUCCESS stage=COMPLETED status={status} "
                        f"job={job.space_nm}.{job.sql_id} reason=TAG_KIND:{tag_kind or 'UNKNOWN'}"
                    )
                    update_cycle_result(
                        row_id=job.row_id,
                        tobe_sql=tobe_sql,
                        bind_sql="",
                        bind_set="[]",
                        test_sql="",
                        status=status,
                        final_log=final_log,
                    )
                    log_stage(stage, "completed", f"(tag_kind={tag_kind or 'UNKNOWN'})")
                    logger.info(
                        f"[Orchestrator] ({job.space_nm}.{job.sql_id}) non-SELECT tag; only TO_SQL_TEXT updated"
                    )
                    return

                stage = "GENERATE_BIND_SQL"
                bind_sql = generate_bind_sql(
                    job=job,
                    tobe_sql=tobe_sql,
                    mapping_rules=mapping_rules,
                    last_error=last_error,
                )
                log_stage(stage, "completed", f"(sql_length={len(bind_sql)})")
                stage = "EXECUTE_BIND_SQL"
                bind_query_rows = execute_binding_query(bind_sql, max_rows=50)
                log_stage(stage, "completed", f"(rows={len(bind_query_rows)})")
                stage = "BUILD_BIND_SET"
                bind_sets = build_bind_sets(
                    tobe_sql=tobe_sql,
                    source_sql=job.source_sql,
                    bind_query_rows=bind_query_rows,
                    max_cases=3,
                )
                bind_set_json = bind_sets_to_json(bind_sets)
                log_stage(stage, "completed", f"(cases={len(bind_sets)})")
                logger.info(
                    f"[Orchestrator] ({job.space_nm}.{job.sql_id}) bind cases prepared: {bind_set_json}"
                )

                stage = "GENERATE_TEST_SQL"
                test_sql = generate_test_sql(
                    job=job,
                    tobe_sql=tobe_sql,
                    bind_set_json=bind_set_json,
                    last_error=last_error,
                )
                log_stage(stage, "completed", f"(sql_length={len(test_sql)})")
                stage = "EXECUTE_TEST_SQL"
                test_rows = execute_test_query(test_sql)
                log_stage(stage, "completed", f"(rows={len(test_rows)})")
                logger.info(
                    f"[Orchestrator] ({job.space_nm}.{job.sql_id}) test rows: {json.dumps(test_rows, ensure_ascii=False)}"
                )
                stage = "EVALUATE_STATUS"
                status = evaluate_status_from_test_rows(test_rows)
                log_stage(stage, "completed", f"(status={status})")
                final_log = (
                    f"FINAL SUCCESS stage=COMPLETED status={status} "
                    f"job={job.space_nm}.{job.sql_id}"
                )

                stage = "UPDATE_DB"
                update_cycle_result(
                    row_id=job.row_id,
                    tobe_sql=tobe_sql,
                    bind_sql=bind_sql,
                    bind_set=bind_set_json,
                    test_sql=test_sql,
                    status=status,
                    final_log=final_log,
                )
                log_stage(stage, "completed")
                logger.info(
                    f"[Orchestrator] ({job.space_nm}.{job.sql_id}) TO_SQL_TEXT/BIND_SQL/BIND_SET/TEST_SQL/STATUS updated"
                )
                return

            except LLMRateLimitError as exc:
                retry_count += 1
                last_error = str(exc)
                logger.warning(
                    f"[Orchestrator] ({job.space_nm}.{job.sql_id}) stage={stage} LLM rate limit "
                    f"(retry={retry_count}): {last_error}"
                )
                time.sleep(1)

            except Exception as exc:
                retry_count += 1
                last_error = str(exc)
                logger.error(
                    f"[Orchestrator] ({job.space_nm}.{job.sql_id}) stage={stage} error "
                    f"(retry={retry_count}): {last_error}"
                )
                if stage in {"GENERATE_TEST_SQL", "EXECUTE_TEST_SQL", "EVALUATE_STATUS"}:
                    logger.error(
                        f"[Orchestrator] ({job.space_nm}.{job.sql_id}) bind cases at failure: {bind_set_json}"
                    )
                time.sleep(1)

        failed_status = "FAIL(from count : NULL, to count : NULL)"
        final_log = (
            f"FINAL FAIL stage={stage} retry_count={retry_count} "
            f"job={job.space_nm}.{job.sql_id} error={last_error or 'UNKNOWN'}"
        )
        finalize_failed_job(job.row_id, failed_status, final_log)
        logger.error(f"[Orchestrator] ({job.space_nm}.{job.sql_id}) failed after retries: {last_error}")
