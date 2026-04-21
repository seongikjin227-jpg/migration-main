"""Unit tests for `MigrationOrchestrator` routing decisions."""

import unittest

from tests.runtime_import_stubs import install_runtime_import_stubs

install_runtime_import_stubs()

from app.flows.job_flow import (
    NODE_ABORT_ON_STOP,
    NODE_DETECT_BIND_PARAMS,
    NODE_GENERATE_BIND_SQL,
    NODE_GENERATE_TEST_SQL,
    NODE_LOAD_TUNING_CONTEXT,
    NODE_LOAD_BIND_FEEDBACK,
    NODE_LOAD_MAPPING_RULES,
    NODE_LOAD_TOBE_FEEDBACK,
    NODE_PERSIST_FAILURE,
    NODE_PERSIST_TUNING_RESULT,
    NODE_PREPARE_RETRY,
    STAGE_GENERATE_BIND_SQL,
    MigrationOrchestrator,
)


class MigrationOrchestratorRouteTests(unittest.TestCase):
    """Covers the route methods that choose the next inner-graph node."""

    @classmethod
    def setUpClass(cls) -> None:
        """Create one orchestrator instance shared by these fast tests."""
        cls.orchestrator = MigrationOrchestrator()

    def test_route_after_init_enters_mapping_rules(self) -> None:
        next_node = self.orchestrator._route_after_init({"stop_requested": False})
        self.assertEqual(next_node, NODE_LOAD_MAPPING_RULES)

    def test_standard_route_uses_retry_when_stage_failed(self) -> None:
        next_node = self.orchestrator._route_after_standard_stage(
            {"stop_requested": False, "stage_error": True},
            success_node=NODE_LOAD_TOBE_FEEDBACK,
        )
        self.assertEqual(next_node, NODE_PREPARE_RETRY)

    def test_route_after_tobe_generation_short_circuits_non_select_jobs(self) -> None:
        next_node = self.orchestrator._route_after_tobe_generation(
            {"stop_requested": False, "stage_error": False, "tag_kind": "UPDATE"}
        )
        self.assertEqual(next_node, NODE_LOAD_TUNING_CONTEXT)

    def test_route_after_tobe_generation_continues_to_bind_detection_for_select(self) -> None:
        next_node = self.orchestrator._route_after_tobe_generation(
            {"stop_requested": False, "stage_error": False, "tag_kind": "SELECT"}
        )
        self.assertEqual(next_node, NODE_DETECT_BIND_PARAMS)

    def test_route_after_bind_param_detection_uses_bind_branch_when_params_exist(self) -> None:
        next_node = self.orchestrator._route_after_bind_param_detection(
            {"stop_requested": False, "bind_param_names": ["P_ID"]}
        )
        self.assertEqual(next_node, NODE_LOAD_BIND_FEEDBACK)

    def test_route_after_bind_param_detection_skips_to_test_when_no_params_exist(self) -> None:
        next_node = self.orchestrator._route_after_bind_param_detection(
            {"stop_requested": False, "bind_param_names": []}
        )
        self.assertEqual(next_node, NODE_GENERATE_TEST_SQL)

    def test_route_after_test_evaluation_persists_success_when_no_stage_error(self) -> None:
        next_node = self.orchestrator._route_after_test_evaluation(
            {"stop_requested": False, "stage_error": False}
        )
        self.assertEqual(next_node, NODE_LOAD_TUNING_CONTEXT)

    def test_route_after_tuning_evaluation_persists_tuning_result(self) -> None:
        next_node = self.orchestrator._route_after_tuning_evaluation(
            {"stop_requested": False, "stage_error": False}
        )
        self.assertEqual(next_node, NODE_PERSIST_TUNING_RESULT)

    def test_route_after_retry_prepare_fails_when_retry_limit_is_exceeded(self) -> None:
        next_node = self.orchestrator._route_after_retry_prepare(
            {"stop_requested": False, "retry_count": 4, "max_retries": 3}
        )
        self.assertEqual(next_node, NODE_PERSIST_FAILURE)

    def test_route_after_retry_prepare_resumes_from_recorded_stage(self) -> None:
        next_node = self.orchestrator._route_after_retry_prepare(
            {
                "stop_requested": False,
                "retry_count": 1,
                "max_retries": 3,
                "resume_from_stage": STAGE_GENERATE_BIND_SQL,
            }
        )
        self.assertEqual(next_node, NODE_GENERATE_BIND_SQL)

    def test_route_methods_abort_when_stop_requested(self) -> None:
        self.assertEqual(
            self.orchestrator._route_after_tobe_generation({"stop_requested": True, "stage_error": False}),
            NODE_ABORT_ON_STOP,
        )
        self.assertEqual(
            self.orchestrator._route_after_retry_prepare({"stop_requested": True}),
            NODE_ABORT_ON_STOP,
        )


if __name__ == "__main__":
    unittest.main()
