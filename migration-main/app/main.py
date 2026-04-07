import logging
import os
import signal
import sys

from apscheduler.schedulers.blocking import BlockingScheduler


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.batch.runner import poll_database
from app.logger import logger
from app.runtime import clear_stop, request_stop


class _SkipMaxInstancesLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "maximum number of running instances reached" not in record.getMessage().lower()


def _attach_filter_to_logger_and_handlers(logger_name: str, log_filter: logging.Filter) -> None:
    target = logging.getLogger(logger_name)
    target.addFilter(log_filter)
    for handler in target.handlers:
        handler.addFilter(log_filter)


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

    clear_stop()
    scheduler = BlockingScheduler()
    scheduler.add_job(
        poll_database,
        "interval",
        minutes=1,
        id="poll_database",
        max_instances=1,
        coalesce=True,
    )

    logger.info("APScheduler started. Polling Oracle every 1 minute.")
    logger.info("Press Ctrl+C to stop.")

    signal_count = {"count": 0}

    def _handle_stop_signal(signum, _frame):
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
