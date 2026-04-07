import time

from app.exceptions import LLMRateLimitError
from app.logger import logger
from app.repositories.mapper_repository import get_all_mapping_rules
from app.repositories.result_repository import (
    get_feedback_examples,
    update_target_flag,
    update_tobe_sql_text,
)
from app.services.llm_service import generate_tobe_sql


class MigrationOrchestrator:
    def process_job(self, job):
        logger.info("\n==========================================")
        logger.info(f"[Orchestrator] Starting job ({job.space_nm}.{job.sql_id})")

        retry_count = 0
        max_retries = 3
        last_error = None

        while retry_count <= max_retries:
            try:
                mapping_rules = get_all_mapping_rules()
                feedback_examples = get_feedback_examples(job)

                tobe_sql = generate_tobe_sql(
                    job=job,
                    mapping_rules=mapping_rules,
                    last_error=last_error,
                    feedback_examples=feedback_examples,
                )

                update_tobe_sql_text(job.row_id, tobe_sql)
                update_target_flag(job.row_id, "N")
                logger.info(f"[Orchestrator] ({job.space_nm}.{job.sql_id}) TO_SQL_TEXT updated")
                return

            except LLMRateLimitError as exc:
                retry_count += 1
                last_error = str(exc)
                logger.warning(f"[Orchestrator] LLM rate limit (retry={retry_count}): {last_error}")
                time.sleep(1)

            except Exception as exc:
                retry_count += 1
                last_error = str(exc)
                logger.error(f"[Orchestrator] Generation error (retry={retry_count}): {last_error}")
                time.sleep(1)

        update_target_flag(job.row_id, "N")
        logger.error(f"[Orchestrator] ({job.space_nm}.{job.sql_id}) failed after retries: {last_error}")
