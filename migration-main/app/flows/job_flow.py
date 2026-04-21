"""단일 Oracle migration job을 처리하는 LangGraph 오케스트레이션.

주요 SELECT 처리 흐름:
1. LOAD_RULES
2. LOAD_TOBE_FEEDBACK
3. GENERATE_TOBE_SQL
4. DETECT_BIND_PARAMS
5. LOAD_BIND_FEEDBACK      (optional)
6. GENERATE_BIND_SQL       (optional)
7. EXECUTE_BIND_SQL        (optional)
8. BUILD_BIND_SET          (optional)
9. GENERATE_TEST_SQL
10. EXECUTE_TEST_SQL
11. EVALUATE_STATUS
12. LOAD_TUNING_CONTEXT
13. GENERATE_GOOD_SQL
14. GENERATE_GOOD_TEST_SQL
15. EXECUTE_GOOD_TEST_SQL
16. EVALUATE_TUNING_STATUS
17. UPDATE_DB / retry / fail

non-SELECT job도 TOBE 생성 이후 튜닝 리뷰 경로까지 계속 진행한다.
"""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, TypedDict

from langgraph.graph import END, START, StateGraph

from app.common import LLMRateLimitError, is_stop_requested, logger
from app.flows.bind_flow import (
    build_bind_payload_stage,
    detect_bind_stage,
    load_bind_stage_context,
    run_bind_execution_stage,
    run_bind_generation_stage,
)
from app.flows.validation_flow import (
    build_validation_stage_sql,
    evaluate_validation_stage,
    run_validation_stage_sql,
)
from app.flows.tobe_flow import (
    load_tobe_stage_context,
    run_tobe_generation_stage,
)
from app.flows.tuning_flow import (
    TuningPipelineResult,
    TuningStatus,
    persist_tuning_log,
    persist_tuning_result,
    run_tuning_review_stage,
)
from app.repositories.mapper_repository import get_all_mapping_rules
from app.repositories.migration_log_repository import insert_migration_logs
from app.repositories.result_repository import update_cycle_result
from app.services.llm_service import (
    select_mapping_rules_for_job,
)


NODE_INIT_JOB_STATE = "init_job_state"
NODE_LOAD_MAPPING_RULES = "load_mapping_rules"
NODE_LOAD_TOBE_FEEDBACK = "load_tobe_feedback"
NODE_GENERATE_TOBE_SQL = "generate_tobe_sql"
NODE_DETECT_BIND_PARAMS = "detect_bind_params"
NODE_LOAD_BIND_FEEDBACK = "load_bind_feedback"
NODE_GENERATE_BIND_SQL = "generate_bind_sql"
NODE_EXECUTE_BIND_SQL = "execute_bind_sql"
NODE_BUILD_BIND_SET = "build_bind_set"
NODE_GENERATE_TEST_SQL = "generate_test_sql"
NODE_EXECUTE_TEST_SQL = "execute_test_sql"
NODE_EVALUATE_TEST_STATUS = "evaluate_test_status"
NODE_LOAD_TUNING_CONTEXT = "load_tuning_context"
NODE_GENERATE_GOOD_SQL = "generate_good_sql"
NODE_GENERATE_GOOD_TEST_SQL = "generate_good_test_sql"
NODE_EXECUTE_GOOD_TEST_SQL = "execute_good_test_sql"
NODE_EVALUATE_TUNING_STATUS = "evaluate_tuning_status"
NODE_PERSIST_TUNING_RESULT = "persist_tuning_result"
NODE_PREPARE_RETRY = "prepare_retry"
NODE_PERSIST_SUCCESS = "persist_success"
NODE_PERSIST_FAILURE = "persist_failure"
NODE_ABORT_ON_STOP = "abort_on_stop"
NODE_END = "__end__"

STAGE_INIT = "INIT"
STAGE_LOAD_RULES = "LOAD_RULES"
STAGE_LOAD_TOBE_FEEDBACK = "LOAD_TOBE_FEEDBACK"
STAGE_GENERATE_TOBE_SQL = "GENERATE_TOBE_SQL"
STAGE_DETECT_BIND_PARAMS = "DETECT_BIND_PARAMS"
STAGE_SKIP_BIND_FOR_NO_PARAMS = "SKIP_BIND_FOR_NO_PARAMS"
STAGE_LOAD_BIND_FEEDBACK = "LOAD_BIND_FEEDBACK"
STAGE_GENERATE_BIND_SQL = "GENERATE_BIND_SQL"
STAGE_EXECUTE_BIND_SQL = "EXECUTE_BIND_SQL"
STAGE_BUILD_BIND_SET = "BUILD_BIND_SET"
STAGE_GENERATE_TEST_SQL = "GENERATE_TEST_SQL"
STAGE_EXECUTE_TEST_SQL = "EXECUTE_TEST_SQL"
STAGE_EVALUATE_STATUS = "EVALUATE_STATUS"
STAGE_LOAD_TUNING_CONTEXT = "LOAD_TUNING_CONTEXT"
STAGE_GENERATE_GOOD_SQL = "GENERATE_GOOD_SQL"
STAGE_GENERATE_GOOD_TEST_SQL = "GENERATE_GOOD_TEST_SQL"
STAGE_EXECUTE_GOOD_TEST_SQL = "EXECUTE_GOOD_TEST_SQL"
STAGE_EVALUATE_TUNING_STATUS = "EVALUATE_TUNING_STATUS"
STAGE_PERSIST_TUNING_RESULT = "PERSIST_TUNING_RESULT"
STAGE_PREPARE_RETRY = "PREPARE_RETRY"
STAGE_UPDATE_DB = "UPDATE_DB"
STAGE_PERSIST_FAILURE = "PERSIST_FAILURE"
STAGE_ABORT_ON_STOP = "ABORT_ON_STOP"

STANDARD_STAGE_TRANSITIONS = (
    (NODE_LOAD_MAPPING_RULES, NODE_LOAD_TOBE_FEEDBACK),
    (NODE_LOAD_TOBE_FEEDBACK, NODE_GENERATE_TOBE_SQL),
    (NODE_LOAD_BIND_FEEDBACK, NODE_GENERATE_BIND_SQL),
    (NODE_GENERATE_BIND_SQL, NODE_EXECUTE_BIND_SQL),
    (NODE_EXECUTE_BIND_SQL, NODE_BUILD_BIND_SET),
    (NODE_BUILD_BIND_SET, NODE_GENERATE_TEST_SQL),
    (NODE_GENERATE_TEST_SQL, NODE_EXECUTE_TEST_SQL),
    (NODE_EXECUTE_TEST_SQL, NODE_EVALUATE_TEST_STATUS),
    (NODE_LOAD_TUNING_CONTEXT, NODE_GENERATE_GOOD_SQL),
    (NODE_GENERATE_GOOD_SQL, NODE_GENERATE_GOOD_TEST_SQL),
    (NODE_GENERATE_GOOD_TEST_SQL, NODE_EXECUTE_GOOD_TEST_SQL),
    (NODE_EXECUTE_GOOD_TEST_SQL, NODE_EVALUATE_TUNING_STATUS),
)

RESUME_STAGE_TO_NODE = {
    STAGE_LOAD_TOBE_FEEDBACK: NODE_LOAD_TOBE_FEEDBACK,
    STAGE_GENERATE_TOBE_SQL: NODE_GENERATE_TOBE_SQL,
    STAGE_LOAD_BIND_FEEDBACK: NODE_LOAD_BIND_FEEDBACK,
    STAGE_GENERATE_BIND_SQL: NODE_GENERATE_BIND_SQL,
    STAGE_GENERATE_TEST_SQL: NODE_GENERATE_TEST_SQL,
    STAGE_LOAD_TUNING_CONTEXT: NODE_LOAD_TUNING_CONTEXT,
    STAGE_GENERATE_GOOD_SQL: NODE_GENERATE_GOOD_SQL,
    STAGE_GENERATE_GOOD_TEST_SQL: NODE_GENERATE_GOOD_TEST_SQL,
}
RESUMABLE_STAGES = frozenset(RESUME_STAGE_TO_NODE)


class JobExecutionState(TypedDict, total=False):
    """개별 job LangGraph를 지나가며 계속 갱신되는 상태."""
    job: Any
    job_key: str
    tag_kind: str
    mapping_rules: Any
    selected_mapping_rules: Any
    map_ids: list[str]
    last_error: str | None
    retry_count: int
    max_retries: int
    current_stage: str
    tobe_feedback_examples: list[dict[str, str]]
    bind_feedback_examples: list[dict[str, str]]
    artifacts: "_AttemptArtifacts"
    bind_param_names: list[str]
    bind_query_rows: list[dict[str, Any]]
    test_rows: list[dict[str, Any]]
    tuning_result: "TuningPipelineResult"
    status: str | None
    final_log: str | None
    resume_from_stage: str | None
    stop_requested: bool
    stage_error: bool


@dataclass
class _AttemptArtifacts:
    """개별 row 처리 중 생성되는 SQL/검증 산출물 버퍼."""
    tobe_sql: str = ""
    bind_sql: str = ""
    bind_set_for_db: str | None = None
    test_sql: str = ""
    bind_set_json_for_test: str = "[]"
    good_sql: str = ""
    good_test_sql: str = ""
    tuning_status: str = ""


class MigrationOrchestrator:
    """SQL migration row 1건에 대한 내부 LangGraph를 실행한다."""

    _RESUME_STAGE_TO_NODE = RESUME_STAGE_TO_NODE
    _RESUMABLE_STAGES = RESUMABLE_STAGES

    def __init__(self) -> None:
        """Compile the per-job graph once and reuse it across job executions."""
        # row 단위 처리 그래프를 한 번만 컴파일해서 재사용한다.
        self._graph = self._build_graph()

    def process_job(self, job) -> None:
        """Execute the full migration flow for one pending SQL job."""
        # 단일 row에 대해 TOBE, BIND, TEST, TUNING 전체 흐름을 실행한다.
        logger.info("\n==========================================")
        logger.info(f"[Orchestrator] Starting job ({job.space_nm}.{job.sql_id})")
        job_key = f"{job.space_nm}.{job.sql_id}"

        if is_stop_requested():
            logger.info(f"[Orchestrator] Stop requested before start ({job_key}). Skipping.")
            return

        self._graph.invoke({"job": job})

    def _build_graph(self):
        """Create and compile the per-job graph definition."""
        # row 단위 LangGraph를 조립하고 compiled graph로 확정한다.
        graph = StateGraph(JobExecutionState)
        self._register_nodes(graph)
        self._register_edges(graph)
        return graph.compile()

    def _register_nodes(self, graph) -> None:
        """Register all nodes grouped by functional phase."""
        # setup, bind, test, completion 단계 노드를 순서대로 등록한다.
        self._register_setup_nodes(graph)
        self._register_bind_nodes(graph)
        self._register_test_nodes(graph)
        self._register_completion_nodes(graph)

    def _register_setup_nodes(self, graph) -> None:
        """Register initialization and TOBE generation nodes."""
        # 초기화와 TOBE 생성에 해당하는 노드를 등록한다.
        graph.add_node(NODE_INIT_JOB_STATE, self._init_job_state)
        graph.add_node(NODE_LOAD_MAPPING_RULES, self._load_mapping_rules)
        graph.add_node(NODE_LOAD_TOBE_FEEDBACK, self._load_tobe_feedback)
        graph.add_node(NODE_GENERATE_TOBE_SQL, self._generate_tobe_sql)
        graph.add_node(NODE_DETECT_BIND_PARAMS, self._detect_bind_params)

    def _register_bind_nodes(self, graph) -> None:
        """Register nodes used only when bind parameters are needed."""
        # bind 파라미터가 존재할 때만 사용하는 노드를 등록한다.
        graph.add_node(NODE_LOAD_BIND_FEEDBACK, self._load_bind_feedback)
        graph.add_node(NODE_GENERATE_BIND_SQL, self._generate_bind_sql)
        graph.add_node(NODE_EXECUTE_BIND_SQL, self._execute_bind_sql)
        graph.add_node(NODE_BUILD_BIND_SET, self._build_bind_set)

    def _register_test_nodes(self, graph) -> None:
        """Register nodes for TEST SQL generation and validation."""
        # TEST 검증과 GOOD SQL 튜닝 단계에 해당하는 노드를 등록한다.
        graph.add_node(NODE_GENERATE_TEST_SQL, self._generate_test_sql)
        graph.add_node(NODE_EXECUTE_TEST_SQL, self._execute_test_sql)
        graph.add_node(NODE_EVALUATE_TEST_STATUS, self._evaluate_test_status)
        graph.add_node(NODE_LOAD_TUNING_CONTEXT, self._load_tuning_context)
        graph.add_node(NODE_GENERATE_GOOD_SQL, self._generate_good_sql)
        graph.add_node(NODE_GENERATE_GOOD_TEST_SQL, self._generate_good_test_sql)
        graph.add_node(NODE_EXECUTE_GOOD_TEST_SQL, self._execute_good_test_sql)
        graph.add_node(NODE_EVALUATE_TUNING_STATUS, self._evaluate_tuning_status)

    def _register_completion_nodes(self, graph) -> None:
        """Register retry, persistence, and termination nodes."""
        # retry, DB 저장, 중단 처리 노드를 마지막에 등록한다.
        graph.add_node(NODE_PREPARE_RETRY, self._prepare_retry)
        graph.add_node(NODE_PERSIST_SUCCESS, self._persist_success)
        graph.add_node(NODE_PERSIST_TUNING_RESULT, self._persist_tuning_result)
        graph.add_node(NODE_PERSIST_FAILURE, self._persist_failure)
        graph.add_node(NODE_ABORT_ON_STOP, self._abort_on_stop)

    def _register_edges(self, graph) -> None:
        """Wire the inner graph transitions between all processing stages."""
        # stage 성공, 실패, retry, stop 분기를 inner graph에 연결한다.
        graph.add_edge(START, NODE_INIT_JOB_STATE)
        self._add_conditional_transition(
            graph,
            NODE_INIT_JOB_STATE,
            self._route_after_init,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_LOAD_MAPPING_RULES: NODE_LOAD_MAPPING_RULES,
            },
        )

        for source_node, success_node in STANDARD_STAGE_TRANSITIONS:
            self._add_standard_transition(graph, source_node, success_node)

        self._add_conditional_transition(
            graph,
            NODE_GENERATE_TOBE_SQL,
            self._route_after_tobe_generation,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_PREPARE_RETRY: NODE_PREPARE_RETRY,
                NODE_LOAD_TUNING_CONTEXT: NODE_LOAD_TUNING_CONTEXT,
                NODE_DETECT_BIND_PARAMS: NODE_DETECT_BIND_PARAMS,
            },
        )
        self._add_conditional_transition(
            graph,
            NODE_DETECT_BIND_PARAMS,
            self._route_after_bind_param_detection,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_LOAD_BIND_FEEDBACK: NODE_LOAD_BIND_FEEDBACK,
                NODE_GENERATE_TEST_SQL: NODE_GENERATE_TEST_SQL,
            },
        )
        self._add_conditional_transition(
            graph,
            NODE_EVALUATE_TEST_STATUS,
            self._route_after_test_evaluation,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_PREPARE_RETRY: NODE_PREPARE_RETRY,
                NODE_LOAD_TUNING_CONTEXT: NODE_LOAD_TUNING_CONTEXT,
            },
        )
        self._add_conditional_transition(
            graph,
            NODE_EVALUATE_TUNING_STATUS,
            self._route_after_tuning_evaluation,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_PREPARE_RETRY: NODE_PREPARE_RETRY,
                NODE_PERSIST_TUNING_RESULT: NODE_PERSIST_TUNING_RESULT,
            },
        )
        self._add_conditional_transition(
            graph,
            NODE_PREPARE_RETRY,
            self._route_after_retry_prepare,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_PERSIST_FAILURE: NODE_PERSIST_FAILURE,
                NODE_LOAD_TOBE_FEEDBACK: NODE_LOAD_TOBE_FEEDBACK,
                NODE_GENERATE_TOBE_SQL: NODE_GENERATE_TOBE_SQL,
                NODE_LOAD_BIND_FEEDBACK: NODE_LOAD_BIND_FEEDBACK,
                NODE_GENERATE_BIND_SQL: NODE_GENERATE_BIND_SQL,
                NODE_GENERATE_TEST_SQL: NODE_GENERATE_TEST_SQL,
                NODE_LOAD_TUNING_CONTEXT: NODE_LOAD_TUNING_CONTEXT,
                NODE_GENERATE_GOOD_SQL: NODE_GENERATE_GOOD_SQL,
                NODE_GENERATE_GOOD_TEST_SQL: NODE_GENERATE_GOOD_TEST_SQL,
            },
        )
        self._add_conditional_transition(
            graph,
            NODE_PERSIST_SUCCESS,
            self._route_after_persist_attempt,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_PREPARE_RETRY: NODE_PREPARE_RETRY,
                NODE_END: END,
            },
        )
        self._add_conditional_transition(
            graph,
            NODE_PERSIST_TUNING_RESULT,
            self._route_after_persist_attempt,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_PREPARE_RETRY: NODE_PREPARE_RETRY,
                NODE_END: END,
            },
        )
        graph.add_edge(NODE_PERSIST_FAILURE, END)
        graph.add_edge(NODE_ABORT_ON_STOP, END)

    def _add_conditional_transition(self, graph, source_node: str, router, transitions: dict[str, str]) -> None:
        """Attach one conditional route map to a graph node."""
        # 특정 노드 뒤에 조건부 분기 로직을 부착한다.
        graph.add_conditional_edges(source_node, router, transitions)

    def _add_standard_transition(self, graph, source_node: str, success_node: str) -> None:
        """Attach the common success/retry/stop routing pattern."""
        # 대부분의 stage가 공유하는 성공, retry, stop 패턴을 재사용한다.
        self._add_conditional_transition(
            graph,
            source_node,
            partial(self._route_after_standard_stage, success_node=success_node),
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_PREPARE_RETRY: NODE_PREPARE_RETRY,
                success_node: success_node,
            },
        )

    def _init_job_state(self, state: JobExecutionState) -> JobExecutionState:
        """Seed default per-job state before the first stage runs."""
        # 개별 row 처리에 필요한 기본 상태값과 artifact 버퍼를 초기화한다.
        job = state["job"]
        return {
            "job_key": f"{job.space_nm}.{job.sql_id}",
            "tag_kind": (job.tag_kind or "").strip().upper(),
            "mapping_rules": None,
            "selected_mapping_rules": [],
            "map_ids": [],
            "last_error": None,
            "retry_count": 0,
            "max_retries": 3,
            "current_stage": STAGE_INIT,
            "tobe_feedback_examples": [],
            "bind_feedback_examples": [],
            "artifacts": _AttemptArtifacts(),
            "bind_param_names": [],
            "bind_query_rows": [],
            "test_rows": [],
            "status": None,
            "final_log": None,
            "resume_from_stage": None,
            "stop_requested": is_stop_requested(),
            "stage_error": False,
        }

    def _load_mapping_rules(self, state: JobExecutionState) -> JobExecutionState:
        """Load all mapping rules and remember the MAP_IDs relevant to this job."""
        # 현재 job에 필요한 매핑룰을 조회하고 MAP_ID 목록을 상태에 저장한다.
        def _load_rules() -> JobExecutionState:
            mapping_rules = get_all_mapping_rules()
            selected_rules = select_mapping_rules_for_job(
                job=state["job"],
                mapping_rules=mapping_rules,
                fallback_to_all=False,
            )
            return {
                "mapping_rules": mapping_rules,
                "selected_mapping_rules": selected_rules,
                "map_ids": self._extract_map_ids(selected_rules),
            }

        return self._execute_stage(
            state=state,
            stage_name=STAGE_LOAD_RULES,
            callback=_load_rules,
            detail_builder=lambda result: f"(map_ids={','.join(result['map_ids']) or 'none'})",
        )

    def _load_tobe_feedback(self, state: JobExecutionState) -> JobExecutionState:
        """Retrieve TOBE-stage RAG examples for the current job."""
        # TOBE 생성 전에 참고할 RAG 예시를 로드한다.
        def _load_feedback() -> JobExecutionState:
            result = load_tobe_stage_context(
                job=state["job"],
                mapping_rules=state.get("selected_mapping_rules") or state.get("mapping_rules") or [],
                last_error=state.get("last_error"),
            )
            return {"tobe_feedback_examples": result.feedback_examples}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_LOAD_TOBE_FEEDBACK,
            callback=_load_feedback,
            detail_builder=lambda result: f"(rag_examples={len(result['tobe_feedback_examples'])})",
        )

    def _generate_tobe_sql(self, state: JobExecutionState) -> JobExecutionState:
        """Generate the TOBE SQL text using mapping rules and RAG examples."""
        # 매핑룰과 RAG 예시를 기반으로 TOBE SQL을 생성한다.
        def _generate_sql() -> JobExecutionState:
            artifacts = state["artifacts"]
            result = run_tobe_generation_stage(
                job=state["job"],
                mapping_rules=state.get("selected_mapping_rules") or state["mapping_rules"],
                feedback_examples=state.get("tobe_feedback_examples", []),
                last_error=state.get("last_error"),
            )
            artifacts.tobe_sql = result.tobe_sql
            return {
                "artifacts": artifacts,
                **({"last_error": result.warning_message} if result.warning_message else {}),
            }

        return self._execute_stage(
            state=state,
            stage_name=STAGE_GENERATE_TOBE_SQL,
            callback=_generate_sql,
            detail_builder=lambda _result: f"(sql_length={len(state['artifacts'].tobe_sql)})",
        )

    def _detect_bind_params(self, state: JobExecutionState) -> JobExecutionState:
        """Detect whether the job needs bind preparation stages.

        If no bind placeholders are found, the graph skips directly to TEST.
        """
        # bind 파라미터 존재 여부를 보고 bind branch를 탈지 바로 TEST로 갈지 결정한다.
        stop_update = self._stop_update(stage_name=STAGE_DETECT_BIND_PARAMS)
        if stop_update:
            return stop_update

        artifacts = state["artifacts"]
        result = detect_bind_stage(artifacts.tobe_sql, state["job"].source_sql)
        bind_param_names = result.bind_param_names
        if not bind_param_names:
            artifacts.bind_sql = ""
            artifacts.bind_set_json_for_test = "[]"
            artifacts.bind_set_for_db = None
            self._log_stage(state["job_key"], STAGE_SKIP_BIND_FOR_NO_PARAMS, "completed", "(reason=no_bind_params)")
            self._record_job_log(
                state=state,
                log_type="INFO",
                step_name=STAGE_SKIP_BIND_FOR_NO_PARAMS,
                status="PASS",
                message="No bind parameters found; skipping bind stages.",
            )

        return {
            "current_stage": STAGE_DETECT_BIND_PARAMS,
            "bind_param_names": bind_param_names,
            "artifacts": artifacts,
            "stage_error": False,
            "stop_requested": False,
        }

    def _load_bind_feedback(self, state: JobExecutionState) -> JobExecutionState:
        """Retrieve bind-stage RAG examples for the current job."""
        # bind SQL 생성 전에 bind stage용 피드백 예시를 로드한다.
        def _load_feedback() -> JobExecutionState:
            result = load_bind_stage_context(
                job=state["job"],
                tobe_sql=state["artifacts"].tobe_sql,
                last_error=state.get("last_error"),
                current_stage=STAGE_LOAD_BIND_FEEDBACK,
            )
            return {"bind_feedback_examples": result.feedback_examples}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_LOAD_BIND_FEEDBACK,
            callback=_load_feedback,
            detail_builder=lambda result: f"(rag_examples={len(result['bind_feedback_examples'])})",
        )

    def _generate_bind_sql(self, state: JobExecutionState) -> JobExecutionState:
        """Generate SQL used to fetch candidate bind values."""
        # 테스트에 사용할 바인딩 후보를 찾기 위한 BIND SQL을 생성한다.
        def _generate_sql() -> JobExecutionState:
            artifacts = state["artifacts"]
            result = run_bind_generation_stage(
                job=state["job"],
                tobe_sql=artifacts.tobe_sql,
                last_error=state.get("last_error"),
                feedback_examples=state.get("bind_feedback_examples", []),
            )
            artifacts.bind_sql = result.bind_sql
            return {"artifacts": artifacts}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_GENERATE_BIND_SQL,
            callback=_generate_sql,
            detail_builder=lambda _result: f"(sql_length={len(state['artifacts'].bind_sql)})",
        )

    def _execute_bind_sql(self, state: JobExecutionState) -> JobExecutionState:
        """Execute the generated bind SQL and collect candidate rows."""
        # 생성된 BIND SQL을 실행해 바인딩 후보 row를 확보한다.
        def _run_query() -> JobExecutionState:
            result = run_bind_execution_stage(state["artifacts"].bind_sql, max_rows=50)
            return {"bind_query_rows": result.bind_query_rows}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_EXECUTE_BIND_SQL,
            callback=_run_query,
            detail_builder=lambda result: f"(rows={len(result['bind_query_rows'])})",
        )

    def _build_bind_set(self, state: JobExecutionState) -> JobExecutionState:
        """Convert bind candidate rows into the bind-set payload used by TEST."""
        # 조회한 bind 후보 row를 TEST 실행용 BIND_SET으로 조립한다.
        def _prepare_bind_set() -> JobExecutionState:
            artifacts = state["artifacts"]
            result = build_bind_payload_stage(
                tobe_sql=artifacts.tobe_sql,
                source_sql=state["job"].source_sql,
                bind_query_rows=state.get("bind_query_rows", []),
                max_cases=3,
            )
            artifacts.bind_set_json_for_test = result.bind_set_json_for_test
            artifacts.bind_set_for_db = result.bind_set_for_db
            logger.info(
                f"[Orchestrator] ({state['job'].space_nm}.{state['job'].sql_id}) "
                f"bind cases prepared: {artifacts.bind_set_json_for_test}"
            )
            return {"artifacts": artifacts}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_BUILD_BIND_SET,
            callback=_prepare_bind_set,
            detail_builder=lambda _result: f"(cases={len(json.loads(state['artifacts'].bind_set_json_for_test or '[]'))})",
        )

    def _generate_test_sql(self, state: JobExecutionState) -> JobExecutionState:
        """Build the TEST SQL used to compare FROM and TO result counts."""
        # FROM SQL과 TOBE SQL을 비교할 TEST SQL을 생성한다.
        def _generate_sql() -> JobExecutionState:
            artifacts = state["artifacts"]
            result = build_validation_stage_sql(
                job=state["job"],
                tobe_sql=artifacts.tobe_sql,
                bind_param_names=state.get("bind_param_names", []),
                bind_set_json_for_test=artifacts.bind_set_json_for_test,
            )
            artifacts.test_sql = result.test_sql
            return {"artifacts": artifacts}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_GENERATE_TEST_SQL,
            callback=_generate_sql,
            detail_builder=lambda _result: f"(sql_length={len(state['artifacts'].test_sql)})",
        )

    def _execute_test_sql(self, state: JobExecutionState) -> JobExecutionState:
        """Execute the TEST SQL and capture validation rows."""
        # 생성된 TEST SQL을 실행해 검증 결과 row를 수집한다.
        def _run_query() -> JobExecutionState:
            result = run_validation_stage_sql(state["artifacts"].test_sql)
            test_rows = result.test_rows
            logger.info(
                f"[Orchestrator] ({state['job'].space_nm}.{state['job'].sql_id}) "
                f"test rows: {json.dumps(test_rows, ensure_ascii=False)}"
            )
            return {"test_rows": test_rows}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_EXECUTE_TEST_SQL,
            callback=_run_query,
            detail_builder=lambda result: f"(rows={len(result['test_rows'])})",
        )

    def _evaluate_test_status(self, state: JobExecutionState) -> JobExecutionState:
        """Convert TEST rows into a PASS/FAIL decision and retry signal."""
        # TEST 결과를 PASS/FAIL로 평가하고 실패면 retry 정보를 만든다.
        stop_update = self._stop_update(stage_name=STAGE_EVALUATE_STATUS)
        if stop_update:
            return stop_update

        try:
            test_rows = state.get("test_rows", [])
            status = evaluate_validation_stage(test_rows).status
            self._log_stage(state["job_key"], STAGE_EVALUATE_STATUS, "completed", f"(status={status})")
            if status == "PASS":
                self._record_job_log(
                    state=state,
                    log_type="INFO",
                    step_name=STAGE_EVALUATE_STATUS,
                    status="PASS",
                    message=f"Validation passed (status={status}).",
                )
                return {
                    "current_stage": STAGE_EVALUATE_STATUS,
                    "status": status,
                    "stage_error": False,
                    "stop_requested": False,
                }

            retry_count = state.get("retry_count", 0) + 1
            last_error = "TEST_VALIDATION_FAIL: " + self._summarize_test_rows_for_retry(test_rows)
            logger.warning(
                f"[Orchestrator] ({state['job'].space_nm}.{state['job'].sql_id}) stage={STAGE_EVALUATE_STATUS} "
                f"status=FAIL (retry={retry_count}/{state['max_retries']}): {last_error}"
            )
            self._record_job_log(
                state=state,
                log_type="ROW_ERROR",
                step_name=STAGE_EVALUATE_STATUS,
                status="FAIL",
                message=last_error,
                retry_count=retry_count,
            )
            return {
                "current_stage": STAGE_EVALUATE_STATUS,
                "status": "FAIL",
                "last_error": last_error,
                "retry_count": retry_count,
                "resume_from_stage": None,
                "stage_error": True,
                "stop_requested": False,
            }
        except Exception as exc:
            return self._handle_stage_exception(state=state, stage_name=STAGE_EVALUATE_STATUS, exc=exc)

    def _prepare_retry(self, state: JobExecutionState) -> JobExecutionState:
        """Apply retry backoff before re-entering the graph."""
        # 재시도 전 backoff를 적용하고 다음 진입 준비 상태로 만든다.
        if is_stop_requested():
            return self._stop_update(stage_name=STAGE_PREPARE_RETRY) or {}

        retry_count = state.get("retry_count", 0)
        if retry_count <= state.get("max_retries", 0):
            self._sleep_with_backoff(retry_count)

        self._record_job_log(
            state=state,
            log_type="INFO",
            step_name=STAGE_PREPARE_RETRY,
            status="PASS",
            message=f"Retry prepared (retry_count={retry_count}).",
            retry_count=retry_count,
        )
        return {
            "current_stage": STAGE_PREPARE_RETRY,
            "stage_error": False,
            "status": None,
            "stop_requested": False,
        }

    def _persist_success(self, state: JobExecutionState) -> JobExecutionState:
        """Persist successful TOBE/BIND/TEST artifacts back to NEXT_SQL_INFO."""
        # 1차 검증까지 통과한 결과를 NEXT_SQL_INFO에 저장한다.
        final_log = (
            f"FINAL SUCCESS stage=COMPLETED status={state.get('status') or 'PASS'} "
            f"job={state['job'].space_nm}.{state['job'].sql_id}"
        )

        def _persist_result() -> JobExecutionState:
            artifacts = state["artifacts"]
            update_cycle_result(
                row_id=state["job"].row_id,
                tobe_sql=artifacts.tobe_sql,
                bind_sql=artifacts.bind_sql,
                bind_set=artifacts.bind_set_for_db,
                test_sql=artifacts.test_sql,
                status=state.get("status") or "PASS",
                final_log=final_log,
            )
            logger.info(
                f"[Orchestrator] ({state['job'].space_nm}.{state['job'].sql_id}) "
                "TO_SQL_TEXT/BIND_SQL/BIND_SET/TEST_SQL/STATUS updated"
            )
            return {"final_log": final_log}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_UPDATE_DB,
            callback=_persist_result,
        )

    def _load_tuning_context(self, state: JobExecutionState) -> JobExecutionState:
        """Seed tuning review state before GOOD_SQL generation."""
        # 튜닝 단계에 들어가기 전에 기본 tuning 상태를 준비한다.
        def _load_context() -> JobExecutionState:
            if not (state["artifacts"].tobe_sql or "").strip():
                tuning_result = TuningPipelineResult(
                    tuning_status=TuningStatus.TUNING_SKIPPED,
                    error_message="TO_SQL_TEXT is empty; tuning skipped",
                )
            else:
                tuning_result = TuningPipelineResult(tuning_status=TuningStatus.PROPOSAL_GENERATED)
            if state.get("tag_kind") != "SELECT":
                return {"status": state.get("status") or "PASS", "tuning_result": tuning_result}
            return {"tuning_result": tuning_result}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_LOAD_TUNING_CONTEXT,
            callback=_load_context,
        )

    def _generate_good_sql(self, state: JobExecutionState) -> JobExecutionState:
        """Run the tuning proposal pipeline against verified TOBE SQL."""
        # 검증을 통과한 TOBE SQL을 기반으로 GOOD SQL 후보와 검증 결과를 만든다.
        def _generate_proposal() -> JobExecutionState:
            stage_result = run_tuning_review_stage(
                job=state["job"],
                tobe_sql=state["artifacts"].tobe_sql,
                bind_set_json=state["artifacts"].bind_set_for_db,
            )
            tuning_result = stage_result.pipeline_result
            artifacts = state["artifacts"]
            artifacts.good_sql = tuning_result.good_sql or ""
            artifacts.good_test_sql = tuning_result.good_test_sql or ""
            artifacts.tuning_status = tuning_result.tuning_status
            return {
                "artifacts": artifacts,
                "tuning_result": tuning_result,
            }

        return self._execute_stage(
            state=state,
            stage_name=STAGE_GENERATE_GOOD_SQL,
            callback=_generate_proposal,
            detail_builder=lambda result: f"(tuning_status={result['tuning_result'].tuning_status})",
        )

    def _generate_good_test_sql(self, state: JobExecutionState) -> JobExecutionState:
        """Treat generated GOOD_TEST_SQL as an explicit stage for readability."""
        # GOOD_TEST_SQL 생성 단계를 흐름상 명확히 보이도록 별도 stage로 유지한다.
        def _noop() -> JobExecutionState:
            return {"artifacts": state["artifacts"], "tuning_result": state.get("tuning_result")}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_GENERATE_GOOD_TEST_SQL,
            callback=_noop,
            detail_builder=lambda _result: f"(sql_length={len(state['artifacts'].good_test_sql)})",
        )

    def _execute_good_test_sql(self, state: JobExecutionState) -> JobExecutionState:
        """Treat tuning verification execution as an explicit stage for readability."""
        # GOOD SQL 검증 실행도 흐름상 명확히 보이도록 별도 stage로 유지한다.
        def _noop() -> JobExecutionState:
            return {"artifacts": state["artifacts"], "tuning_result": state.get("tuning_result")}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_EXECUTE_GOOD_TEST_SQL,
            callback=_noop,
            detail_builder=lambda _result: f"(tuning_status={state.get('tuning_result').tuning_status if state.get('tuning_result') else 'unknown'})",
        )

    def _evaluate_tuning_status(self, state: JobExecutionState) -> JobExecutionState:
        """Convert tuning pipeline output into final persisted state."""
        # 튜닝 파이프라인 결과를 최종 저장 가능한 상태값으로 정리한다.
        stop_update = self._stop_update(stage_name=STAGE_EVALUATE_TUNING_STATUS)
        if stop_update:
            return stop_update

        tuning_result = state.get("tuning_result") or TuningPipelineResult(tuning_status=TuningStatus.TUNING_SKIPPED)
        status = tuning_result.tuning_status
        self._record_job_log(
            state=state,
            log_type="INFO" if status == TuningStatus.AUTO_TUNED_VERIFIED else "ROW_ERROR",
            step_name=STAGE_EVALUATE_TUNING_STATUS,
            status="PASS" if status == TuningStatus.AUTO_TUNED_VERIFIED else "FAIL",
            message=tuning_result.error_message or f"tuning_status={status}",
            retry_count=state.get("retry_count", 0),
        )
        return {
            "current_stage": STAGE_EVALUATE_TUNING_STATUS,
            "tuning_result": tuning_result,
            "stage_error": False,
            "stop_requested": False,
        }

    def _persist_tuning_result(self, state: JobExecutionState) -> JobExecutionState:
        """Persist tuning outputs to NEXT_SQL_INFO and NEXT_SQL_TUNING_LOG."""
        # GOOD SQL 결과와 튜닝 로그를 Oracle에 함께 반영한다.
        final_log = (
            f"FINAL SUCCESS stage=TUNING status={state.get('status') or 'PASS'} "
            f"job={state['job'].space_nm}.{state['job'].sql_id} tuning={state.get('tuning_result').tuning_status if state.get('tuning_result') else 'UNKNOWN'}"
        )

        def _persist_result() -> JobExecutionState:
            artifacts = state["artifacts"]
            update_cycle_result(
                row_id=state["job"].row_id,
                tobe_sql=artifacts.tobe_sql,
                bind_sql=artifacts.bind_sql,
                bind_set=artifacts.bind_set_for_db,
                test_sql=artifacts.test_sql,
                status=state.get("status") or "PASS",
                final_log=final_log,
            )
            tuning_result = state.get("tuning_result") or TuningPipelineResult(tuning_status=TuningStatus.TUNING_SKIPPED)
            persist_tuning_result(
                row_id=state["job"].row_id,
                good_sql=tuning_result.good_sql,
                good_test_sql=tuning_result.good_test_sql,
                tuning_status=tuning_result.tuning_status,
            )
            persist_tuning_log(
                space_nm=state["job"].space_nm,
                sql_id=state["job"].sql_id,
                tuning_status=tuning_result.tuning_status,
                llm_used_yn=tuning_result.llm_used_yn,
                applied_rule_ids=tuning_result.applied_rule_ids,
                diff_summary=tuning_result.diff_summary,
                error_message=tuning_result.error_message,
            )
            return {"final_log": final_log}

        return self._execute_stage(
            state=state,
            stage_name=STAGE_PERSIST_TUNING_RESULT,
            callback=_persist_result,
        )

    def _persist_failure(self, state: JobExecutionState) -> JobExecutionState:
        """Persist the final FAIL outcome after retries are exhausted."""
        # 재시도 한도를 넘기면 마지막 실패 상태와 로그를 DB에 기록한다.
        final_log = (
            f"FINAL FAIL stage={state.get('current_stage')} retry_count={state.get('retry_count', 0)} "
            f"job={state['job'].space_nm}.{state['job'].sql_id} error={state.get('last_error') or 'UNKNOWN'}"
        )
        artifacts = state["artifacts"]
        update_cycle_result(
            row_id=state["job"].row_id,
            tobe_sql=artifacts.tobe_sql,
            bind_sql=artifacts.bind_sql,
            bind_set=artifacts.bind_set_for_db,
            test_sql=artifacts.test_sql,
            status="FAIL",
            final_log=final_log,
        )
        logger.error(
            f"[Orchestrator] ({state['job'].space_nm}.{state['job'].sql_id}) "
            f"failed after retries: {state.get('last_error')}"
        )
        self._record_job_log(
            state=state,
            log_type="JOB_FAIL",
            step_name=STAGE_PERSIST_FAILURE,
            status="FAIL",
            message=final_log,
            retry_count=state.get("retry_count", 0),
        )
        return {
            "current_stage": STAGE_PERSIST_FAILURE,
            "final_log": final_log,
            "status": "FAIL",
            "stage_error": False,
        }

    def _abort_on_stop(self, state: JobExecutionState) -> JobExecutionState:
        """Terminate the current job when a global stop was requested."""
        # 전역 stop 요청이 들어오면 현재 row 처리를 즉시 중단한다.
        logger.info(f"[Orchestrator] Stop requested ({state.get('job_key', 'UNKNOWN')}). Aborting job.")
        return {
            "current_stage": STAGE_ABORT_ON_STOP,
            "stop_requested": True,
            "stage_error": False,
        }

    def _execute_stage(
        self,
        state: JobExecutionState,
        stage_name: str,
        callback: Callable[[], JobExecutionState],
        detail_builder: Callable[[JobExecutionState], str | None] | None = None,
    ) -> JobExecutionState:
        """Run one stage with common logging, state updates, and error handling."""
        # 모든 stage가 공통으로 쓰는 실행, 로깅, 예외 처리 템플릿이다.
        stop_update = self._stop_update(stage_name=stage_name)
        if stop_update:
            return stop_update

        try:
            updates = callback() or {}
            detail = detail_builder(updates) if detail_builder else None
            self._log_stage(state["job_key"], stage_name, "completed", detail)
            self._record_job_log(
                state=state,
                log_type="INFO",
                step_name=stage_name,
                status="PASS",
                message=self._build_stage_message(stage_name=stage_name, detail=detail),
            )
            return {
                **updates,
                "current_stage": stage_name,
                "stage_error": False,
                "stop_requested": False,
                "resume_from_stage": self._clear_resume_if_matches(
                    state.get("resume_from_stage"),
                    stage_name,
                ),
            }
        except Exception as exc:
            return self._handle_stage_exception(state=state, stage_name=stage_name, exc=exc)

    def _handle_stage_exception(
        self,
        state: JobExecutionState,
        stage_name: str,
        exc: Exception,
    ) -> JobExecutionState:
        """Convert a stage exception into retryable graph state."""
        # stage 예외를 retry 가능한 상태값으로 변환한다.
        retry_count = state.get("retry_count", 0) + 1
        last_error = str(exc)
        if isinstance(exc, LLMRateLimitError):
            logger.warning(
                f"[Orchestrator] ({state['job'].space_nm}.{state['job'].sql_id}) stage={stage_name} "
                f"LLM rate limit (retry={retry_count}): {last_error}"
            )
        else:
            logger.error(
                f"[Orchestrator] ({state['job'].space_nm}.{state['job'].sql_id}) stage={stage_name} "
                f"error (retry={retry_count}): {last_error}"
            )
            if stage_name in {STAGE_GENERATE_TEST_SQL, STAGE_EXECUTE_TEST_SQL, STAGE_EVALUATE_STATUS}:
                logger.error(
                    f"[Orchestrator] ({state['job'].space_nm}.{state['job'].sql_id}) "
                    f"bind cases at failure: {state['artifacts'].bind_set_json_for_test}"
                )

        self._record_job_log(
            state=state,
            log_type="ROW_ERROR",
            step_name=stage_name,
            status="FAIL",
            message=last_error,
            retry_count=retry_count,
        )

        return {
            "current_stage": stage_name,
            "last_error": last_error,
            "retry_count": retry_count,
            "resume_from_stage": self._next_resume_stage(stage_name, last_error),
            "stage_error": True,
            "stop_requested": False,
        }

    def _stop_update(self, stage_name: str) -> JobExecutionState | None:
        """Return a stop-state update when global shutdown has been requested."""
        # stop 플래그가 켜져 있으면 현재 stage를 더 진행하지 않는다.
        if not is_stop_requested():
            return None
        return {
            "current_stage": stage_name,
            "stop_requested": True,
            "stage_error": False,
        }

    def _route_after_init(self, state: JobExecutionState) -> str:
        """Route from initialization to the first real stage or stop handling."""
        # INIT 이후에는 매핑룰 로드 또는 stop 분기로 이동한다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        return NODE_LOAD_MAPPING_RULES

    def _route_after_standard_stage(self, state: JobExecutionState, *, success_node: str) -> str:
        """Apply the common success/retry/stop rule used by most stages."""
        # 일반 stage 공통 분기로 성공, retry, stop을 판정한다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        if state.get("stage_error"):
            return NODE_PREPARE_RETRY
        return success_node

    def _route_after_tobe_generation(self, state: JobExecutionState) -> str:
        """Branch after TOBE generation based on failure and tag kind."""
        # TOBE 생성 뒤에는 non-select shortcut 또는 bind 탐지 단계로 이동한다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        if state.get("stage_error"):
            return NODE_PREPARE_RETRY
        if state.get("tag_kind") != "SELECT":
            return NODE_LOAD_TUNING_CONTEXT
        return NODE_DETECT_BIND_PARAMS

    def _route_after_bind_param_detection(self, state: JobExecutionState) -> str:
        """Choose the bind branch or skip straight to TEST."""
        # bind 파라미터가 있으면 bind branch로, 없으면 TEST 단계로 이동한다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        if state.get("bind_param_names"):
            return NODE_LOAD_BIND_FEEDBACK
        return NODE_GENERATE_TEST_SQL

    def _route_after_test_evaluation(self, state: JobExecutionState) -> str:
        """Persist success or prepare retry after test evaluation."""
        # TEST PASS일 때만 튜닝 단계로 넘어가고 실패면 retry로 보낸다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        if state.get("stage_error"):
            return NODE_PREPARE_RETRY
        return NODE_LOAD_TUNING_CONTEXT

    def _route_after_tuning_evaluation(self, state: JobExecutionState) -> str:
        """Persist tuning results after the tuning pipeline finishes."""
        # 튜닝 평가가 끝나면 결과 저장 또는 retry 분기를 결정한다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        if state.get("stage_error"):
            return NODE_PREPARE_RETRY
        return NODE_PERSIST_TUNING_RESULT

    def _route_after_retry_prepare(self, state: JobExecutionState) -> str:
        """Resume from the correct stage after backoff, or fail permanently."""
        # retry 준비 후에는 저장된 재진입 stage로 복귀하거나 최종 실패로 간다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        if state.get("retry_count", 0) > state.get("max_retries", 0):
            return NODE_PERSIST_FAILURE
        resume_from_stage = state.get("resume_from_stage")
        if resume_from_stage in self._RESUME_STAGE_TO_NODE:
            return self._RESUME_STAGE_TO_NODE[resume_from_stage]
        return NODE_LOAD_TOBE_FEEDBACK

    def _route_after_persist_attempt(self, state: JobExecutionState) -> str:
        """Finish the graph after persistence unless another retry is required."""
        # DB 저장 성공이면 종료하고 실패면 저장 단계만 다시 시도한다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        if state.get("stage_error"):
            return NODE_PREPARE_RETRY
        return NODE_END

    @classmethod
    def _clear_resume_if_matches(cls, resume_from_stage: str | None, stage_name: str) -> str | None:
        """Clear a resume marker once the graph re-enters that stage."""
        # 재진입한 stage를 성공적으로 끝냈으면 resume 마커를 비운다.
        return None if resume_from_stage == stage_name else resume_from_stage

    @classmethod
    def _next_resume_stage(cls, stage_name: str, last_error: str) -> str | None:
        """Decide whether the next retry should resume from the same stage."""
        # 실패 유형에 따라 어느 stage부터 재시도할지 계산한다.
        if stage_name in cls._RESUMABLE_STAGES and cls._is_overloaded_error(last_error):
            return stage_name
        return None

    @staticmethod
    def _is_overloaded_error(message: str) -> bool:
        """Detect provider overload errors that should resume in-place."""
        # 외부 LLM overload 계열 메시지인지 판별해 같은 stage 재시도를 허용한다.
        lower = (message or "").lower()
        return ("overloaded_error" in lower) or ("error code: 529" in lower) or (" http 529" in lower)

    @staticmethod
    def _sleep_with_backoff(retry_count: int) -> None:
        """Sleep with exponential backoff and small jitter between retries."""
        # 재시도 간격은 지수 백오프와 jitter를 섞어 과부하를 줄인다.
        base = min(8, 2 ** max(0, retry_count - 1))
        jitter = random.uniform(0.0, 0.7)
        time.sleep(base + jitter)

    @staticmethod
    def _log_stage(job_key: str, stage_name: str, event: str, detail: str | None = None) -> None:
        """Write a compact application log entry for one stage event."""
        # 모든 stage 로그 형식을 한곳에서 통일한다.
        if stage_name == STAGE_LOAD_RULES and event == "completed":
            return
        suffix = f" {detail}" if detail else ""
        logger.info(f"[Orchestrator] ({job_key}) stage={stage_name} {event}{suffix}")

    @staticmethod
    def _extract_map_ids(mapping_rules: list[Any]) -> list[str]:
        """Extract unique MAP_ID values from selected mapping rules."""
        # 매핑룰 목록에서 중복 없는 MAP_ID만 추출한다.
        map_ids: list[str] = []
        seen: set[str] = set()
        for rule in mapping_rules or []:
            map_id = (getattr(rule, "map_id", None) or "").strip()
            if not map_id or map_id in seen:
                continue
            seen.add(map_id)
            map_ids.append(map_id)
        return map_ids

    @staticmethod
    def _build_stage_message(stage_name: str, detail: str | None) -> str:
        """Build a human-readable message stored in NEXT_MIG_LOG."""
        # migration log에 남길 짧은 stage 메시지를 만든다.
        if detail:
            return f"{stage_name} completed {detail}".strip()
        return f"{stage_name} completed"

    def _record_job_log(
        self,
        state: JobExecutionState,
        log_type: str,
        step_name: str,
        status: str,
        message: str,
        retry_count: int | None = None,
    ) -> None:
        """Persist one job-level log record to NEXT_MIG_LOG.

        Logging failures are intentionally swallowed after writing to the process
        logger so that operational logging cannot break the migration flow.
        """
        # 단계별 운영 로그는 흐름을 깨지 않도록 best-effort로만 저장한다.
        if step_name == STAGE_LOAD_RULES:
            return
        try:
            insert_migration_logs(
                map_ids=state.get("map_ids", []),
                log_type=log_type,
                step_name=step_name,
                status=status,
                message=message,
                retry_count=retry_count if retry_count is not None else state.get("retry_count", 0),
            )
        except Exception as exc:
            logger.error(
                f"[Orchestrator] ({state.get('job_key', 'UNKNOWN')}) failed to write NEXT_MIG_LOG: {exc}"
            )

    @staticmethod
    def _get_case_insensitive_value(row: dict, key: str):
        """Read a dictionary value without assuming column-name case."""
        # DB 결과 key 대소문자 차이를 무시하고 값을 읽는다.
        lowered = key.lower()
        for existing_key, value in row.items():
            if str(existing_key).lower() == lowered:
                return value
        return None

    @classmethod
    def _summarize_test_rows_for_retry(cls, rows: list[dict]) -> str:
        """Condense failed TEST rows into a short retry message."""
        # TEST 결과 row를 재시도 프롬프트용 요약 문자열로 변환한다.
        if not rows:
            return "no_rows_returned"

        samples: list[str] = []
        for row in rows[:5]:
            case_no = cls._get_case_insensitive_value(row, "case_no")
            from_count = cls._get_case_insensitive_value(row, "from_count")
            to_count = cls._get_case_insensitive_value(row, "to_count")
            samples.append(f"CASE_NO={case_no},FROM_COUNT={from_count},TO_COUNT={to_count}")

        return " ; ".join(samples)
