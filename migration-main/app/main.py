import logging
import os
import sys

from apscheduler.schedulers.blocking import BlockingScheduler


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.batch.runner import poll_database
from app.logger import logger


class _SkipMaxInstancesLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "maximum number of running instances reached" not in record.getMessage().lower()


if __name__ == "__main__":
    logger.info("====================================")
    logger.info(" Oracle SQL Migration Agent Started ")
    logger.info("====================================")

    logging.getLogger("apscheduler.executors.default").addFilter(_SkipMaxInstancesLogFilter())

    scheduler = BlockingScheduler()
    scheduler.add_job(
        poll_database,
        "interval",
        minutes=10,
        id="poll_database",
        max_instances=1,
        coalesce=True,
    )

    logger.info("APScheduler started. Polling Oracle every 10 minutes.")
    logger.info("Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Migration agent stopped gracefully.")
