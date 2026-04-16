"""스케줄러 콜백 모듈.

장수명 오케스트레이터 인스턴스를 유지하며
폴링 주기마다 대기 작업을 순차 처리한다.
"""

import traceback

from app.orchestrator import MigrationOrchestrator
from app.common import logger
from app.repositories.result_repository import get_pending_jobs, increment_batch_count
from app.common import is_stop_requested
from app.services.feedback_rag_service import feedback_rag_service


orchestrator = MigrationOrchestrator()


def poll_database():
    """NEXT_SQL_INFO 대기 작업을 조회하고 처리한다.

    한 사이클의 예외가 전체 스케줄러 중단으로 번지지 않도록 방어적으로 동작한다.
    """
    try:
        if is_stop_requested():
            logger.info("[Scheduler] Stop requested. Skipping polling cycle.")
            return

        logger.info("\n--- [Scheduler] Polling NEXT_SQL_INFO for pending jobs ---")

        jobs = get_pending_jobs()
        if not jobs:
            logger.info("[Scheduler] No pending jobs found.")
            return

        logger.info(f"[Scheduler] Found {len(jobs)} pending job(s).")
        try:
            result = feedback_rag_service.sync_index(limit=None)
            logger.info(
                "[Scheduler] RAG sync completed "
                f"(source_rows={result['source_rows']}, "
                f"upserted={result['upserted']}, "
                f"skipped_unchanged={result['skipped_unchanged']}, "
                f"skipped_no_correct_sql={result['skipped_no_correct_sql']}, "
                f"deleted={result['deleted']})"
            )
        except Exception as exc:
            logger.warning(f"[Scheduler] RAG sync skipped due to error: {exc}")

        for job in jobs:
            if is_stop_requested():
                logger.info("[Scheduler] Stop requested. Aborting remaining jobs in this cycle.")
                break
            increment_batch_count(job.row_id)
            orchestrator.process_job(job)

    except Exception as exc:
        logger.error(f"[Scheduler] Unexpected error while polling database: {exc}")
        logger.error(traceback.format_exc())
