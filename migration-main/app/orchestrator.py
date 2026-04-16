"""Oracle migration 작업의 생성/검증 파이프라인을 실행하는 오케스트레이터."""

import json
import random
import time
from dataclasses import dataclass

from app.common import LLMRateLimitError
from app.common import logger
from app.repositories.mapper_repository import get_all_mapping_rules
from app.repositories.result_repository import update_cycle_result
from app.common import is_stop_requested
from app.services.binding_service import bind_sets_to_json, build_bind_sets, extract_bind_param_names
from app.services.feedback_rag_service import feedback_rag_service
from app.services.llm_service import (
    generate_bind_sql,
    generate_test_sql,
    generate_test_sql_no_bind,
    generate_tobe_sql,
)
from app.services.validation_service import (
    evaluate_status_from_test_rows,
    execute_binding_query,
    execute_test_query,
)


@dataclass
class _AttemptArtifacts:
    tobe_sql: str = ""
    bind_sql: str = ""
    bind_set_for_db: str | None = None
    test_sql: str = ""
    bind_set_json_for_test: str = "[]"


class MigrationOrchestrator:
    def process_job(self, job):
        logger.info("\n==========================================")
        logger.info(f"[Orchestrator] Starting job ({job.space_nm}.{job.sql_id})")
        job_key = f"{job.space_nm}.{job.sql_id}"

        if is_stop_requested():
            logger.info(f"[Orchestrator] Stop requested before start ({job_key}). Skipping.")
            return

        retry_count = 0
        max_retries = 3
        last_error = None
        stage = "INIT"
        resume_from_stage: str | None = None
        mapping_rules = None
        artifacts = _AttemptArtifacts()

        while retry_count <= max_retries:
            if is_stop_requested():
                logger.info(f"[Orchestrator] Stop requested ({job_key}). Aborting job.")
                return
            try:
                if mapping_rules is None:
                    stage = "LOAD_RULES"
                    mapping_rules = get_all_mapping_rules()

                if self._should_run_stage("LOAD_RAG_FEEDBACK", resume_from_stage):
                    stage = "LOAD_RAG_FEEDBACK"
                    feedback_examples = feedback_rag_service.retrieve_feedback_examples(
                        job=job,
                        last_error=last_error,
                    )
                    self._log_stage(job_key, stage, "completed", f"(rag_examples={len(feedback_examples)})")
                    if resume_from_stage == "LOAD_RAG_FEEDBACK":
                        resume_from_stage = None
                else:
                    feedback_examples = []

                if self._should_run_stage("GENERATE_TOBE_SQL", resume_from_stage):
                    stage = "GENERATE_TOBE_SQL"
                    artifacts.tobe_sql = generate_tobe_sql(
                        job=job,
                        mapping_rules=mapping_rules,
                        last_error=last_error,
                        feedback_examples=feedback_examples,
                    )
                    self._log_stage(job_key, stage, "completed", f"(sql_length={len(artifacts.tobe_sql)})")
                    if resume_from_stage == "GENERATE_TOBE_SQL":
                        resume_from_stage = None

                tag_kind = (job.tag_kind or "").strip().upper()
                if tag_kind != "SELECT":
                    self._complete_non_select_job(job=job, job_key=job_key, tobe_sql=artifacts.tobe_sql, tag_kind=tag_kind)
                    return

                bind_param_names = extract_bind_param_names(artifacts.tobe_sql) or extract_bind_param_names(job.source_sql)
                if self._should_run_stage("PREPARE_BIND_ARTIFACTS", resume_from_stage):
                    stage = "PREPARE_BIND_ARTIFACTS"
                    bind_param_names = self._prepare_bind_artifacts(
                        job=job,
                        job_key=job_key,
                        tobe_sql=artifacts.tobe_sql,
                        last_error=last_error,
                        feedback_examples=feedback_examples,
                        artifacts=artifacts,
                    )
                    if resume_from_stage == "PREPARE_BIND_ARTIFACTS":
                        resume_from_stage = None

                if self._should_run_stage("GENERATE_TEST_SQL", resume_from_stage):
                    stage = "GENERATE_TEST_SQL"
                    if not bind_param_names:
                        artifacts.test_sql = generate_test_sql_no_bind(
                            job=job,
                            tobe_sql=artifacts.tobe_sql,
                            last_error=last_error,
                            feedback_examples=feedback_examples,
                        )
                    else:
                        artifacts.test_sql = generate_test_sql(
                            job=job,
                            tobe_sql=artifacts.tobe_sql,
                            bind_set_json=artifacts.bind_set_json_for_test,
                            last_error=last_error,
                            feedback_examples=feedback_examples,
                        )
                    self._log_stage(job_key, stage, "completed", f"(sql_length={len(artifacts.test_sql)})")
                    if resume_from_stage == "GENERATE_TEST_SQL":
                        resume_from_stage = None

                stage = "EXECUTE_TEST_SQL"
                test_rows = execute_test_query(artifacts.test_sql)
                self._log_stage(job_key, stage, "completed", f"(rows={len(test_rows)})")
                logger.info(f"[Orchestrator] ({job.space_nm}.{job.sql_id}) test rows: {json.dumps(test_rows, ensure_ascii=False)}")

                stage = "EVALUATE_STATUS"
                status = evaluate_status_from_test_rows(test_rows)
                self._log_stage(job_key, stage, "completed", f"(status={status})")
                if status != "PASS":
                    retry_count += 1
                    last_error = "TEST_VALIDATION_FAIL: " + self._summarize_test_rows_for_retry(test_rows)
                    logger.warning(
                        f"[Orchestrator] ({job.space_nm}.{job.sql_id}) stage={stage} "
                        f"status=FAIL (retry={retry_count}/{max_retries}): {last_error}"
                    )
                    if retry_count <= max_retries:
                        resume_from_stage = None
                        self._sleep_with_backoff(retry_count)
                        continue
                    break

                final_log = f"FINAL SUCCESS stage=COMPLETED status={status} job={job.space_nm}.{job.sql_id}"

                stage = "UPDATE_DB"
                update_cycle_result(
                    row_id=job.row_id,
                    tobe_sql=artifacts.tobe_sql,
                    bind_sql=artifacts.bind_sql,
                    bind_set=artifacts.bind_set_for_db,
                    test_sql=artifacts.test_sql,
                    status=status,
                    final_log=final_log,
                )
                self._log_stage(job_key, stage, "completed")
                logger.info(f"[Orchestrator] ({job.space_nm}.{job.sql_id}) TO_SQL_TEXT/BIND_SQL/BIND_SET/TEST_SQL/STATUS updated")
                return

            except LLMRateLimitError as exc:
                retry_count += 1
                last_error = str(exc)
                logger.warning(
                    f"[Orchestrator] ({job.space_nm}.{job.sql_id}) stage={stage} LLM rate limit "
                    f"(retry={retry_count}): {last_error}"
                )
                resume_from_stage = stage if self._is_overloaded_error(last_error) else None
                self._sleep_with_backoff(retry_count)

            except Exception as exc:
                retry_count += 1
                last_error = str(exc)
                logger.error(
                    f"[Orchestrator] ({job.space_nm}.{job.sql_id}) stage={stage} error "
                    f"(retry={retry_count}): {last_error}"
                )
                if stage in {"GENERATE_TEST_SQL", "EXECUTE_TEST_SQL", "EVALUATE_STATUS"}:
                    logger.error(
                        f"[Orchestrator] ({job.space_nm}.{job.sql_id}) bind cases at failure: {artifacts.bind_set_json_for_test}"
                    )
                resume_from_stage = stage if self._is_overloaded_error(last_error) else None
                self._sleep_with_backoff(retry_count)

        failed_status = "FAIL"
        final_log = (
            f"FINAL FAIL stage={stage} retry_count={retry_count} "
            f"job={job.space_nm}.{job.sql_id} error={last_error or 'UNKNOWN'}"
        )
        update_cycle_result(
            row_id=job.row_id,
            tobe_sql=artifacts.tobe_sql,
            bind_sql=artifacts.bind_sql,
            bind_set=artifacts.bind_set_for_db,
            test_sql=artifacts.test_sql,
            status=failed_status,
            final_log=final_log,
        )
        logger.error(f"[Orchestrator] ({job.space_nm}.{job.sql_id}) failed after retries: {last_error}")

    @staticmethod
    def _is_overloaded_error(message: str) -> bool:
        lower = (message or "").lower()
        return ("overloaded_error" in lower) or ("error code: 529" in lower) or (" http 529" in lower)

    @staticmethod
    def _sleep_with_backoff(retry_count: int) -> None:
        base = min(8, 2 ** max(0, retry_count - 1))
        jitter = random.uniform(0.0, 0.7)
        time.sleep(base + jitter)

    @staticmethod
    def _should_run_stage(stage_name: str, resume_from_stage: str | None) -> bool:
        if not resume_from_stage:
            return True
        stage_order = ["LOAD_RAG_FEEDBACK", "GENERATE_TOBE_SQL", "PREPARE_BIND_ARTIFACTS", "GENERATE_TEST_SQL"]
        if resume_from_stage not in stage_order or stage_name not in stage_order:
            return True
        return stage_order.index(stage_name) >= stage_order.index(resume_from_stage)

    @staticmethod
    def _log_stage(job_key: str, stage_name: str, event: str, detail: str | None = None) -> None:
        if stage_name == "LOAD_RULES" and event == "completed":
            return
        suffix = f" {detail}" if detail else ""
        logger.info(f"[Orchestrator] ({job_key}) stage={stage_name} {event}{suffix}")

    def _complete_non_select_job(self, job, job_key: str, tobe_sql: str, tag_kind: str) -> None:
        stage = "SKIP_TEST_FOR_NON_SELECT"
        status = "PASS"
        final_log = (
            f"FINAL SUCCESS stage=COMPLETED status={status} "
            f"job={job.space_nm}.{job.sql_id} reason=TAG_KIND:{tag_kind or 'UNKNOWN'}"
        )
        update_cycle_result(
            row_id=job.row_id,
            tobe_sql=tobe_sql,
            bind_sql="",
            bind_set=None,
            test_sql="",
            status=status,
            final_log=final_log,
        )
        self._log_stage(job_key, stage, "completed", f"(tag_kind={tag_kind or 'UNKNOWN'})")
        logger.info(f"[Orchestrator] ({job.space_nm}.{job.sql_id}) non-SELECT tag; only TO_SQL_TEXT updated")

    def _prepare_bind_artifacts(
        self,
        job,
        job_key: str,
        tobe_sql: str,
        last_error: str | None,
        feedback_examples: list[dict[str, str]],
        artifacts: _AttemptArtifacts,
    ) -> list[str]:
        bind_param_names = extract_bind_param_names(tobe_sql)
        if not bind_param_names:
            bind_param_names = extract_bind_param_names(job.source_sql)

        if not bind_param_names:
            stage = "SKIP_BIND_FOR_NO_PARAMS"
            artifacts.bind_sql = ""
            artifacts.bind_set_json_for_test = "[]"
            artifacts.bind_set_for_db = None
            self._log_stage(job_key, stage, "completed", "(reason=no_bind_params)")
            return bind_param_names

        stage = "GENERATE_BIND_SQL"
        artifacts.bind_sql = generate_bind_sql(
            job=job,
            tobe_sql=tobe_sql,
            last_error=last_error,
            feedback_examples=feedback_examples,
        )
        self._log_stage(job_key, stage, "completed", f"(sql_length={len(artifacts.bind_sql)})")

        stage = "EXECUTE_BIND_SQL"
        bind_query_rows = execute_binding_query(artifacts.bind_sql, max_rows=50)
        self._log_stage(job_key, stage, "completed", f"(rows={len(bind_query_rows)})")

        stage = "BUILD_BIND_SET"
        bind_sets = build_bind_sets(
            tobe_sql=tobe_sql,
            source_sql=job.source_sql,
            bind_query_rows=bind_query_rows,
            max_cases=3,
        )
        artifacts.bind_set_json_for_test = bind_sets_to_json(bind_sets)
        artifacts.bind_set_for_db = artifacts.bind_set_json_for_test
        self._log_stage(job_key, stage, "completed", f"(cases={len(bind_sets)})")
        logger.info(f"[Orchestrator] ({job.space_nm}.{job.sql_id}) bind cases prepared: {artifacts.bind_set_json_for_test}")
        return bind_param_names

    @staticmethod
    def _get_case_insensitive_value(row: dict, key: str):
        lowered = key.lower()
        for existing_key, value in row.items():
            if str(existing_key).lower() == lowered:
                return value
        return None

    @classmethod
    def _summarize_test_rows_for_retry(cls, rows: list[dict]) -> str:
        if not rows:
            return "no_rows_returned"

        samples: list[str] = []
        for row in rows[:5]:
            case_no = cls._get_case_insensitive_value(row, "case_no")
            from_count = cls._get_case_insensitive_value(row, "from_count")
            to_count = cls._get_case_insensitive_value(row, "to_count")
            samples.append(f"CASE_NO={case_no},FROM_COUNT={from_count},TO_COUNT={to_count}")

        return " ; ".join(samples)
