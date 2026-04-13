"""스케줄러 콜백 모듈.

장수명 오케스트레이터 인스턴스를 유지하며
폴링 주기마다 대기 작업을 순차 처리한다.
"""

import traceback

from app.agent.mapper_sql_agent import MigrationOrchestrator
from app.logger import logger
from app.repositories.result_repository import get_pending_jobs
from app.runtime import is_stop_requested


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
        for job in jobs:
            if is_stop_requested():
                logger.info("[Scheduler] Stop requested. Aborting remaining jobs in this cycle.")
                break
            orchestrator.process_job(job)

    except Exception as exc:
        logger.error(f"[Scheduler] Unexpected error while polling database: {exc}")
        logger.error(traceback.format_exc())
