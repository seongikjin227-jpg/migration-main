"""LangGraph 기반 배치 런타임 오케스트레이션.

배치 1사이클 흐름:
1. init_cycle
2. startup_rag_sync        (startup 시에만)
3. load_pending_jobs
4. process_jobs            (row별 처리)
5. finish_cycle

이 그래프는 바깥쪽 스케줄러 사이클만 담당한다.
개별 row 처리는 내부 `MigrationOrchestrator`가 맡는다.
"""

from __future__ import annotations

import traceback
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from app.common import is_stop_requested, logger
from app.flows.job_flow import MigrationOrchestrator
from app.repositories.result_repository import get_pending_jobs, increment_batch_count
from app.features.rag.bind_rag_service import bind_rag_service


NODE_INIT_CYCLE = "init_cycle"
NODE_STARTUP_RAG_SYNC = "startup_rag_sync"
NODE_LOAD_PENDING_JOBS = "load_pending_jobs"
NODE_PROCESS_JOBS = "process_jobs"
NODE_FINISH_CYCLE = "finish_cycle"
NODE_ABORT_ON_STOP = "abort_on_stop"


class BatchRuntimeState(TypedDict, total=False):
    """바깥쪽 배치 LangGraph를 지나가며 갱신되는 상태."""
    sync_rag: bool
    load_jobs: bool
    startup_synced: bool
    startup_sync_error: str | None
    jobs: list
    processed_count: int
    stop_requested: bool
    cycle_log: list[str]


class BatchRuntimeGraphRunner:
    """개별 job 오케스트레이터 바깥의 배치 1사이클을 실행한다."""

    def __init__(self) -> None:
        """재사용할 내부 오케스트레이터와 바깥 그래프를 준비한다."""
        # 배치 1사이클 그래프와 내부 job 오케스트레이터를 한 번만 준비한다.
        self._orchestrator = MigrationOrchestrator()
        self._graph = self._build_graph()

    def run_cycle(self, sync_rag: bool = False, load_jobs: bool = True) -> BatchRuntimeState:
        """배치 바깥 그래프 기준 1사이클을 실행한다."""
        # startup sync 여부와 job 로드 여부를 받아 배치 1사이클을 실행한다.
        return self._graph.invoke({"sync_rag": sync_rag, "load_jobs": load_jobs})

    def _build_graph(self):
        """바깥쪽 배치 그래프를 생성하고 컴파일한다."""
        # 바깥쪽 배치 그래프를 구성하고 재사용 가능한 compiled graph로 만든다.
        graph = StateGraph(BatchRuntimeState)
        self._register_nodes(graph)
        self._register_edges(graph)
        return graph.compile()

    def _register_nodes(self, graph) -> None:
        """런타임 단계별 노드를 등록한다."""
        # startup, polling, completion 단계 노드를 한곳에서 등록한다.
        self._register_startup_nodes(graph)
        self._register_polling_nodes(graph)
        self._register_completion_nodes(graph)

    def _register_startup_nodes(self, graph) -> None:
        """startup 전용 경로에 속한 노드를 등록한다."""
        # 프로세스 시작 시에만 사용하는 startup 노드를 등록한다.
        graph.add_node(NODE_INIT_CYCLE, self._init_cycle)
        graph.add_node(NODE_STARTUP_RAG_SYNC, self._startup_rag_sync)

    def _register_polling_nodes(self, graph) -> None:
        """일반 polling cycle에서 쓰는 노드를 등록한다."""
        # 주기적 polling cycle에서 쓰는 노드를 등록한다.
        graph.add_node(NODE_LOAD_PENDING_JOBS, self._load_pending_jobs)
        graph.add_node(NODE_PROCESS_JOBS, self._process_jobs)

    def _register_completion_nodes(self, graph) -> None:
        """현재 사이클을 종료시키는 노드를 등록한다."""
        # 현재 배치 사이클을 종료시키는 노드를 등록한다.
        graph.add_node(NODE_FINISH_CYCLE, self._finish_cycle)
        graph.add_node(NODE_ABORT_ON_STOP, self._abort_on_stop)

    def _register_edges(self, graph) -> None:
        """startup, polling, 종료 경로를 그래프에 연결한다."""
        # startup 여부와 stop 상태에 따라 바깥 그래프의 라우팅을 연결한다.
        graph.add_edge(START, NODE_INIT_CYCLE)
        self._add_transition(
            graph,
            NODE_INIT_CYCLE,
            self._route_after_init,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_STARTUP_RAG_SYNC: NODE_STARTUP_RAG_SYNC,
                NODE_LOAD_PENDING_JOBS: NODE_LOAD_PENDING_JOBS,
            },
        )
        self._add_transition(
            graph,
            NODE_STARTUP_RAG_SYNC,
            self._route_after_startup_sync,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_LOAD_PENDING_JOBS: NODE_LOAD_PENDING_JOBS,
            },
        )
        self._add_transition(
            graph,
            NODE_LOAD_PENDING_JOBS,
            self._route_after_job_load,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_PROCESS_JOBS: NODE_PROCESS_JOBS,
                NODE_FINISH_CYCLE: NODE_FINISH_CYCLE,
            },
        )
        self._add_transition(
            graph,
            NODE_PROCESS_JOBS,
            self._route_after_job_processing,
            {
                NODE_ABORT_ON_STOP: NODE_ABORT_ON_STOP,
                NODE_FINISH_CYCLE: NODE_FINISH_CYCLE,
            },
        )
        graph.add_edge(NODE_FINISH_CYCLE, END)
        graph.add_edge(NODE_ABORT_ON_STOP, END)

    def _add_transition(self, graph, source_node: str, router, transitions: dict[str, str]) -> None:
        """특정 노드 뒤에 조건부 라우팅 함수를 연결한다."""
        # 특정 노드 뒤에 조건부 라우팅 함수를 연결한다.
        graph.add_conditional_edges(source_node, router, transitions)

    def _init_cycle(self, state: BatchRuntimeState) -> BatchRuntimeState:
        """새 배치 사이클의 기본 상태를 초기화한다."""
        # 배치 1사이클 시작 전에 기본 상태값을 초기화한다.
        return {
            "startup_synced": False,
            "startup_sync_error": None,
            "jobs": [],
            "processed_count": 0,
            "stop_requested": is_stop_requested(),
            "cycle_log": [],
        }

    def _startup_rag_sync(self, state: BatchRuntimeState) -> BatchRuntimeState:
        """일반 polling 전에 startup 전용 RAG 동기화를 수행한다."""
        # startup 시점에 TOBE/BIND RAG 인덱스를 동기화하고 실패해도 프로세스는 계속 진행한다.
        if is_stop_requested():
            return {"stop_requested": True}

        logger.info("[Startup] RAG sync started (BIND=bind-only index)")
        try:
            result = bind_rag_service.sync_index(limit=None)
            logger.info(
                "[Startup] BIND RAG sync completed "
                f"(source_rows={result['source_rows']}, "
                f"upserted={result['upserted']}, "
                f"skipped_unchanged={result['skipped_unchanged']}, "
                f"skipped_no_correct_sql={result['skipped_no_correct_sql']}, "
                f"deleted={result['deleted']})"
            )
            return {"startup_synced": True, "startup_sync_error": None, "stop_requested": False}
        except Exception as exc:
            logger.error(f"[Startup] RAG sync failed: {exc}")
            logger.warning("[Startup] Continuing without startup sync; scheduler will still start.")
            return {"startup_synced": False, "startup_sync_error": str(exc), "stop_requested": False}

    def _load_pending_jobs(self, state: BatchRuntimeState) -> BatchRuntimeState:
        """현재 polling cycle에서 처리할 pending job을 로드한다."""
        # 이번 polling cycle에서 처리할 FAIL row 목록을 Oracle에서 읽어온다.
        if is_stop_requested():
            return {"stop_requested": True}

        if not state.get("load_jobs", True):
            return {"jobs": [], "stop_requested": False}

        logger.info("\n--- [Scheduler] Polling NEXT_SQL_INFO for pending jobs ---")
        jobs = get_pending_jobs()
        if not jobs:
            logger.info("[Scheduler] No pending jobs found.")
        else:
            logger.info(f"[Scheduler] Found {len(jobs)} pending job(s).")
        return {"jobs": jobs, "stop_requested": False}

    def _process_jobs(self, state: BatchRuntimeState) -> BatchRuntimeState:
        """조회한 각 row를 내부 job 오케스트레이터로 전달한다."""
        # 조회한 각 row를 내부 job flow로 넘겨 TOBE/BIND/TEST/TUNING을 처리한다.
        processed_count = 0
        for job in state.get("jobs", []):
            if is_stop_requested():
                logger.info("[Scheduler] Stop requested. Aborting remaining jobs in this cycle.")
                return {"processed_count": processed_count, "stop_requested": True}

            # The outer graph owns the polling cycle, but each job is delegated
            # to the inner per-job LangGraph for TOBE/BIND/TEST processing.
            increment_batch_count(job.row_id)
            self._orchestrator.process_job(job)
            processed_count += 1
        return {"processed_count": processed_count, "stop_requested": False}

    def _finish_cycle(self, state: BatchRuntimeState) -> BatchRuntimeState:
        """완료된 배치 사이클의 요약 상태를 반환한다."""
        # 배치 1사이클 종료 시 처리 건수와 stop 상태만 요약해 반환한다.
        return {
            "stop_requested": state.get("stop_requested", False),
            "processed_count": state.get("processed_count", 0),
        }

    def _abort_on_stop(self, state: BatchRuntimeState) -> BatchRuntimeState:
        """stop 요청이 들어오면 현재 사이클을 중단한다."""
        # 전역 stop 요청이 들어오면 현재 배치 사이클을 더 진행하지 않는다.
        if state.get("load_jobs", True):
            logger.info("[Scheduler] Stop requested. Skipping polling cycle.")
        return {"stop_requested": True}

    @staticmethod
    def _route_after_init(state: BatchRuntimeState) -> str:
        """초기화 뒤에 startup sync 또는 일반 polling 분기를 고른다."""
        # init 이후에는 startup sync 또는 일반 polling 분기로 이동한다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        if state.get("sync_rag"):
            return NODE_STARTUP_RAG_SYNC
        return NODE_LOAD_PENDING_JOBS

    @staticmethod
    def _route_after_startup_sync(state: BatchRuntimeState) -> str:
        """startup sync 후 다음 노드를 결정한다."""
        # startup sync가 끝나면 다음 단계는 항상 job 로드다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        return NODE_LOAD_PENDING_JOBS

    @staticmethod
    def _route_after_job_load(state: BatchRuntimeState) -> str:
        """job 처리로 갈지 빈 사이클 종료로 갈지 결정한다."""
        # 로드된 job이 있으면 처리로, 없으면 사이클 종료로 이동한다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        if not state.get("jobs"):
            return NODE_FINISH_CYCLE
        return NODE_PROCESS_JOBS

    @staticmethod
    def _route_after_job_processing(state: BatchRuntimeState) -> str:
        """job 처리 이후 종료 또는 stop 분기를 결정한다."""
        # job 처리가 끝난 뒤에는 stop 여부만 보고 종료 노드를 결정한다.
        if state.get("stop_requested"):
            return NODE_ABORT_ON_STOP
        return NODE_FINISH_CYCLE


def run_batch_cycle(sync_rag: bool = False, load_jobs: bool = True) -> BatchRuntimeState:
    """클래스를 직접 다루지 않고 배치 1사이클만 실행하는 래퍼다."""
    # 외부에서 배치 1사이클만 직접 실행할 수 있도록 래핑한 진입점이다.
    runner = BatchRuntimeGraphRunner()
    try:
        return runner.run_cycle(sync_rag=sync_rag, load_jobs=load_jobs)
    except Exception as exc:
        logger.error(f"[Scheduler] Unexpected error while running batch graph: {exc}")
        logger.error(traceback.format_exc())
        raise
