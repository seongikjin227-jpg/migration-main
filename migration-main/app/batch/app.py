import logging
import os
import signal
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from app.batch.poller import poll_database
from app.common import clear_stop, logger, request_stop
from app.services.feedback_rag_service import feedback_rag_service


class _SkipMaxInstancesLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "maximum number of running instances reached" not in record.getMessage().lower()


def _attach_filter_to_logger_and_handlers(logger_name: str, log_filter: logging.Filter) -> None:
    target = logging.getLogger(logger_name)
    target.addFilter(log_filter)
    for handler in target.handlers:
        handler.addFilter(log_filter)


def _sync_feedback_rag_on_startup() -> None:
    logger.info("[Startup] RAG sync started (source=NEXT_SQL_INFO.TOBE/BIND/TEST_CORRECT_SQL)")
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
                "No staged correct SQL corpus found; retrieval examples may be empty."
            )
    except Exception as exc:
        logger.error(f"[Startup] RAG sync failed: {exc}")
        logger.warning("[Startup] Continuing without startup sync; scheduler will still start.")


def run_batch() -> None:
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

    def _safe_signal_print(message: str) -> None:
        try:
            os.write(2, (message + "\n").encode("utf-8", errors="ignore"))
        except Exception:
            pass

    def _handle_stop_signal(signum, _frame):
        signal_count["count"] += 1
        request_stop()
        if signal_count["count"] == 1:
            _safe_signal_print(f"Received signal {signum}. Stopping scheduler (wait=False).")
            _safe_signal_print("If shutdown hangs due to an external call, press Ctrl+C again to force exit.")
            try:
                scheduler.shutdown(wait=False)
            except Exception:
                pass
            return
        _safe_signal_print("Forced termination requested.")
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
