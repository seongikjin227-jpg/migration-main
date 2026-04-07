import traceback

from app.agent.mapper_sql_agent import MigrationOrchestrator
from app.logger import logger
from app.repositories.result_repository import get_pending_jobs


orchestrator = MigrationOrchestrator()


def poll_database():
    """Poll NEXT_SQL_INFO for pending jobs and process them."""
    try:
        logger.info("\n--- [Scheduler] Polling NEXT_SQL_INFO for pending jobs ---")

        jobs = get_pending_jobs()
        if not jobs:
            logger.info("[Scheduler] No pending jobs found.")
            return

        logger.info(f"[Scheduler] Found {len(jobs)} pending job(s).")
        for job in jobs:
            orchestrator.process_job(job)

    except Exception as exc:
        logger.error(f"[Scheduler] Unexpected error while polling database: {exc}")
        logger.error(traceback.format_exc())
