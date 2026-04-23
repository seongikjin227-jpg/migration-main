"""Lightweight import stubs used by route tests.

The route tests only need the graph modules to import successfully. They do not
exercise real LangGraph, database, or LLM integrations, so this module injects
minimal stand-ins into `sys.modules`.
"""

import sys
import types


def install_runtime_import_stubs() -> None:
    """Install minimal runtime dependencies required by the unit tests."""
    if "langgraph.graph" not in sys.modules:
        langgraph_module = types.ModuleType("langgraph")
        graph_module = types.ModuleType("langgraph.graph")

        class _CompiledGraph:
            """Minimal compiled-graph stub that just echoes the provided state."""

            def invoke(self, state):
                return state

        class _DummyStateGraph:
            """Small `StateGraph` stub that exposes the methods used in tests."""

            def __init__(self, *_args, **_kwargs) -> None:
                pass

            def add_node(self, *_args, **_kwargs) -> None:
                return None

            def add_edge(self, *_args, **_kwargs) -> None:
                return None

            def add_conditional_edges(self, *_args, **_kwargs) -> None:
                return None

            def compile(self):
                return _CompiledGraph()

        graph_module.END = "__end__"
        graph_module.START = "__start__"
        graph_module.StateGraph = _DummyStateGraph
        langgraph_module.graph = graph_module
        sys.modules["langgraph"] = langgraph_module
        sys.modules["langgraph.graph"] = graph_module

    if "app.repositories.mapper_repository" not in sys.modules:
        mapper_repository = types.ModuleType("app.repositories.mapper_repository")
        mapper_repository.get_all_mapping_rules = lambda: []
        sys.modules["app.repositories.mapper_repository"] = mapper_repository

    if "app.repositories.result_repository" not in sys.modules:
        result_repository = types.ModuleType("app.repositories.result_repository")
        result_repository.get_pending_jobs = lambda: []
        result_repository.increment_batch_count = lambda _row_id: None
        result_repository.update_cycle_result = lambda **_kwargs: None
        result_repository.update_tuning_result = lambda **_kwargs: None
        result_repository.insert_tuning_log = lambda **_kwargs: None
        result_repository.get_feedback_corpus_rows = lambda limit=2000, correct_kinds=None: []
        sys.modules["app.repositories.result_repository"] = result_repository

    if "app.repositories.migration_log_repository" not in sys.modules:
        migration_log_repository = types.ModuleType("app.repositories.migration_log_repository")
        migration_log_repository.insert_migration_logs = lambda **_kwargs: None
        sys.modules["app.repositories.migration_log_repository"] = migration_log_repository

    if "app.services.binding_service" not in sys.modules:
        binding_service = types.ModuleType("app.services.binding_service")
        binding_service.bind_sets_to_json = lambda bind_sets: "[]"
        binding_service.build_bind_sets = lambda **_kwargs: []
        binding_service.extract_bind_param_names = lambda _sql: []
        sys.modules["app.services.binding_service"] = binding_service

    if "app.features.rag.bind_rag_service" not in sys.modules:
        bind_rag_module = types.ModuleType("app.features.rag.bind_rag_service")

        class _BindRagService:
            """BIND RAG stub that returns empty retrieval results."""

            def retrieve_bind_examples(self, **_kwargs):
                return []

            def sync_index(self, **_kwargs):
                return {
                    "source_rows": 0,
                    "upserted": 0,
                    "skipped_unchanged": 0,
                    "skipped_no_correct_sql": 0,
                    "deleted": 0,
                }

        bind_rag_module.bind_rag_service = _BindRagService()
        sys.modules["app.features.rag.bind_rag_service"] = bind_rag_module

    if "app.services.llm_service" not in sys.modules:
        llm_service = types.ModuleType("app.services.llm_service")
        llm_service.generate_tobe_sql = lambda **_kwargs: ""
        llm_service.generate_bind_sql = lambda **_kwargs: ""
        llm_service.generate_test_sql = lambda **_kwargs: ""
        llm_service.generate_test_sql_no_bind = lambda **_kwargs: ""
        llm_service.generate_comparison_test_sql = lambda *args, **_kwargs: ""
        llm_service.generate_tuned_sql = lambda **_kwargs: ""
        llm_service.select_mapping_rules_for_job = lambda job, mapping_rules, fallback_to_all=True: mapping_rules
        sys.modules["app.services.llm_service"] = llm_service

    if "app.services.validation_service" not in sys.modules:
        validation_service = types.ModuleType("app.services.validation_service")
        validation_service.collect_tobe_sql_column_coverage_issues = lambda _sql, _mapping_rules: []
        validation_service.evaluate_status_from_test_rows = lambda _rows: "PASS"
        validation_service.execute_binding_query = lambda _sql, max_rows=50: []
        validation_service.execute_test_query = lambda _sql: []
        sys.modules["app.services.validation_service"] = validation_service
