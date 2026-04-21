"""Unit tests for outer batch graph routing decisions."""

import unittest

from tests.runtime_import_stubs import install_runtime_import_stubs

install_runtime_import_stubs()

from app.flows.runtime_flow import (
    NODE_ABORT_ON_STOP,
    NODE_FINISH_CYCLE,
    NODE_LOAD_PENDING_JOBS,
    NODE_PROCESS_JOBS,
    NODE_STARTUP_RAG_SYNC,
    BatchRuntimeGraphRunner,
)


class BatchRuntimeGraphRunnerRouteTests(unittest.TestCase):
    """Covers route methods that choose the next outer-graph node."""

    @classmethod
    def setUpClass(cls) -> None:
        """Create one runner instance shared by these routing tests."""
        cls.runner = BatchRuntimeGraphRunner()

    def test_route_after_init_uses_startup_sync_for_startup_cycle(self) -> None:
        next_node = self.runner._route_after_init({"sync_rag": True, "stop_requested": False})
        self.assertEqual(next_node, NODE_STARTUP_RAG_SYNC)

    def test_route_after_init_uses_job_loading_for_polling_cycle(self) -> None:
        next_node = self.runner._route_after_init({"sync_rag": False, "stop_requested": False})
        self.assertEqual(next_node, NODE_LOAD_PENDING_JOBS)

    def test_route_after_job_load_finishes_when_no_jobs_exist(self) -> None:
        next_node = self.runner._route_after_job_load({"jobs": [], "stop_requested": False})
        self.assertEqual(next_node, NODE_FINISH_CYCLE)

    def test_route_after_job_load_processes_jobs_when_present(self) -> None:
        next_node = self.runner._route_after_job_load({"jobs": [object()], "stop_requested": False})
        self.assertEqual(next_node, NODE_PROCESS_JOBS)

    def test_route_after_init_aborts_when_stop_requested(self) -> None:
        next_node = self.runner._route_after_init({"sync_rag": True, "stop_requested": True})
        self.assertEqual(next_node, NODE_ABORT_ON_STOP)

    def test_route_after_job_processing_aborts_when_stop_requested(self) -> None:
        next_node = self.runner._route_after_job_processing({"stop_requested": True})
        self.assertEqual(next_node, NODE_ABORT_ON_STOP)


if __name__ == "__main__":
    unittest.main()
