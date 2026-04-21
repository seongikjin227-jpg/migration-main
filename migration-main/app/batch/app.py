"""Batch scheduler bootstrap and runtime loop helpers."""

import logging
import os
import signal
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from app.batch.poller import poll_database
from app.common import clear_stop, logger, request_stop
from app.flows.runtime_flow import BatchRuntimeGraphRunner


POLL_JOB_ID = "poll_database"
POLL_INTERVAL_MINUTES = 1


class _SkipMaxInstancesLogFilter(logging.Filter):
    """Hide repetitive APScheduler max-instance warnings from the console."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "maximum number of running instances reached" not in record.getMessage().lower()


def _attach_filter_to_logger_and_handlers(logger_name: str, log_filter: logging.Filter) -> None:
    """Attach the same logging filter to a logger and all of its handlers."""
    target = logging.getLogger(logger_name)
    target.addFilter(log_filter)
    for handler in target.handlers:
        handler.addFilter(log_filter)


def _configure_scheduler_logging() -> None:
    """Apply scheduler log filtering so batch output stays readable."""
    skip_max_instances_filter = _SkipMaxInstancesLogFilter()
    _attach_filter_to_logger_and_handlers("apscheduler", skip_max_instances_filter)
    _attach_filter_to_logger_and_handlers("apscheduler.scheduler", skip_max_instances_filter)
    _attach_filter_to_logger_and_handlers("apscheduler.executors.default", skip_max_instances_filter)
    for handler in logging.getLogger().handlers:
        handler.addFilter(skip_max_instances_filter)


def run_startup_sync_cycle() -> None:
    """Run the startup-only outer cycle that performs the initial RAG sync."""
    BatchRuntimeGraphRunner().run_cycle(sync_rag=True, load_jobs=False)


def _build_scheduler() -> BlockingScheduler:
    """Create the APScheduler instance and register the polling job."""
    clear_stop()
    scheduler = BlockingScheduler()
    scheduler.add_job(
        poll_database,
        "interval",
        minutes=POLL_INTERVAL_MINUTES,
        next_run_time=datetime.now(),
        id=POLL_JOB_ID,
        max_instances=1,
        coalesce=True,
    )
    return scheduler


def _register_stop_signal_handlers(scheduler: BlockingScheduler) -> None:
    """Register SIGINT/SIGTERM handlers for graceful scheduler shutdown."""
    signal_count = {"count": 0}

    def _safe_signal_print(message: str) -> None:
        """Write a last-resort message directly to stderr inside signal handling."""
        try:
            os.write(2, (message + "\n").encode("utf-8", errors="ignore"))
        except Exception:
            pass

    def _handle_stop_signal(signum, _frame):
        """Request graceful stop on first signal and force exit on the second."""
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


def configure_runtime_bootstrap() -> None:
    """Prepare logging and banner output for the batch runtime."""
    logger.info("====================================")
    logger.info(" Oracle SQL Migration Agent Started ")
    logger.info("====================================")
    _configure_scheduler_logging()


def start_batch_scheduler() -> None:
    """Start APScheduler after bootstrap and startup sync already completed."""

    scheduler = _build_scheduler()
    _register_stop_signal_handlers(scheduler)

    logger.info(f"APScheduler started. Polling Oracle every {POLL_INTERVAL_MINUTES} minute.")
    logger.info("Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Migration agent stopped gracefully.")
    finally:
        request_stop()


def run_batch() -> None:
    """Backward-compatible one-shot bootstrap that runs the full batch runtime."""
    configure_runtime_bootstrap()
    run_startup_sync_cycle()
    start_batch_scheduler()
