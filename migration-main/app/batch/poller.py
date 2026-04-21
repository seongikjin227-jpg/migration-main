"""Scheduler polling entrypoint."""

from app.runtime.batch_runtime import run_poll_cycle


def poll_database():
    """Run one scheduler-driven polling cycle without startup sync."""
    run_poll_cycle()
