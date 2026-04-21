"""Readable wrappers around the outer batch runtime cycle."""

from app.flows.runtime_flow import BatchRuntimeGraphRunner


def run_startup_sync_only() -> None:
    """Run the startup-only cycle that performs initial sync work."""
    BatchRuntimeGraphRunner().run_cycle(sync_rag=True, load_jobs=False)


def run_poll_cycle() -> None:
    """Run one normal polling cycle that loads and dispatches pending jobs."""
    BatchRuntimeGraphRunner().run_cycle(sync_rag=False, load_jobs=True)
