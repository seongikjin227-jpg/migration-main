"""배치 프로세스 엔트리포인트.

1분 주기로 DB 폴링 스케줄러를 실행하며,
시그널(SIGINT/SIGTERM) 기반으로 안전 종료를 처리한다.
"""

import logging
import os
import signal
import sys
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.batch.runner import poll_database
from app.logger import logger
from app.runtime import clear_stop, request_stop
from app.services.feedback_rag_service import feedback_rag_service


class _SkipMaxInstancesLogFilter(logging.Filter):
    """APScheduler의 max_instances 경고 로그를 필터링한다."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "maximum number of running instances reached" not in record.getMessage().lower()


def _attach_filter_to_logger_and_handlers(logger_name: str, log_filter: logging.Filter) -> None:
    """지정 logger와 하위 handler에 동일 필터를 등록한다."""
    target = logging.getLogger(logger_name)
    target.addFilter(log_filter)
    for handler in target.handlers:
        handler.addFilter(log_filter)


def _sync_feedback_rag_on_startup() -> None:
    """배치 시작 전에 CORRECT_SQL 기반 RAG 인덱스를 자동 동기화한다."""
    logger.info("[Startup] RAG sync started (source=NEXT_SQL_INFO.CORRECT_SQL)")
    try:
        result = feedback_rag_service.sync_index(limit=None)
        logger.info(
            "[Startup] RAG sync completed "
            f"(source_rows={result['source_rows']}, "
            f"upserted={result['upserted']}, "
            f"skipped_unchanged={result['skipped_unchanged']}, "
            f"skipped_no_correct_sql={result['skipped_no_correct_sql']}, "
            f"deleted={result['deleted']})"
        )
        if result["source_rows"] <= 0:
            logger.warning(
                "[Startup] RAG source rows are empty. "
                "No CORRECT_SQL corpus found; retrieval examples may be empty."
            )
    except Exception as exc:
        logger.error(f"[Startup] RAG sync failed: {exc}")
        logger.warning("[Startup] Continuing without startup sync; scheduler will still start.")


if __name__ == "__main__":
    logger.info("====================================")
    logger.info(" Oracle SQL Migration Agent Started ")
    logger.info("====================================")

    skip_max_instances_filter = _SkipMaxInstancesLogFilter()
    _attach_filter_to_logger_and_handlers("apscheduler", skip_max_instances_filter)
    _attach_filter_to_logger_and_handlers("apscheduler.scheduler", skip_max_instances_filter)
    _attach_filter_to_logger_and_handlers("apscheduler.executors.default", skip_max_instances_filter)
    for handler in logging.getLogger().handlers:
        handler.addFilter(skip_max_instances_filter)

    _sync_feedback_rag_on_startup()

    clear_stop()
    scheduler = BlockingScheduler()
    scheduler.add_job(
        poll_database,
        "interval",
        minutes=1,
        next_run_time=datetime.now(),
        id="poll_database",
        max_instances=1,
        coalesce=True,
    )

    logger.info("APScheduler started. Polling Oracle every 1 minute.")
    logger.info("Press Ctrl+C to stop.")

    signal_count = {"count": 0}

    def _handle_stop_signal(signum, _frame):
        # 첫 번째 시그널은 안전 종료, 두 번째 시그널은 강제 종료로 처리한다.
        signal_count["count"] += 1
        request_stop()
        if signal_count["count"] == 1:
            logger.warning(f"Received signal {signum}. Stopping scheduler (wait=False).")
            logger.warning("If shutdown hangs due to an external call, press Ctrl+C again to force exit.")
            try:
                scheduler.shutdown(wait=False)
            except Exception:
                # Scheduler may not be running yet.
                pass
            return
        logger.error("Forced termination requested.")
        os._exit(130)

    signal.signal(signal.SIGINT, _handle_stop_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_stop_signal)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Migration agent stopped gracefully.")
    finally:
        request_stop()
