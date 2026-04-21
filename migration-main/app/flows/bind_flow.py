"""BIND 단계 흐름을 담당하는 오케스트레이션 모듈."""

from __future__ import annotations

from dataclasses import dataclass

from app.features.bind.bind_feature import (
    build_bind_payloads,
    detect_bind_param_names,
    execute_bind_sql_text,
    generate_bind_sql_text,
    load_bind_feedback_examples,
)


@dataclass
class BindDetectionStageResult:
    """bind 탐지 결과를 담는 단계 결과."""
    bind_param_names: list[str]
    bind_required: bool


@dataclass
class BindFeedbackStageResult:
    """bind 단계 RAG 조회 결과를 담는 단계 결과."""
    feedback_examples: list[dict[str, str]]


@dataclass
class BindGenerationStageResult:
    """BIND SQL 생성 결과를 담는 단계 결과."""
    bind_sql: str


@dataclass
class BindExecutionStageResult:
    """BIND SQL 실행 결과 row를 담는 단계 결과."""
    bind_query_rows: list[dict]


@dataclass
class BindPayloadStageResult:
    """TEST와 DB 저장에 사용할 bind payload를 담는 단계 결과."""
    bind_set_json_for_test: str
    bind_set_for_db: str | None


def detect_bind_stage(tobe_sql: str, source_sql: str) -> BindDetectionStageResult:
    """bind placeholder 존재 여부와 bind 단계 필요 여부를 판별한다."""
    # TOBE SQL과 원본 SQL을 함께 보고 bind branch 진입 여부를 정한다.
    bind_param_names = detect_bind_param_names(tobe_sql, source_sql)
    return BindDetectionStageResult(
        bind_param_names=bind_param_names,
        bind_required=bool(bind_param_names),
    )


def load_bind_stage_context(job, tobe_sql: str, last_error: str | None, current_stage: str) -> BindFeedbackStageResult:
    """bind 단계에서 사용할 retrieval 문맥을 조회한다."""
    # BIND SQL 생성 전에 bind stage용 피드백 예시를 불러온다.
    return BindFeedbackStageResult(
        feedback_examples=load_bind_feedback_examples(
            job=job,
            tobe_sql=tobe_sql,
            last_error=last_error,
            current_stage=current_stage,
        )
    )


def run_bind_generation_stage(
    job,
    tobe_sql: str,
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> BindGenerationStageResult:
    """bind 후보 값을 찾기 위한 SQL을 생성한다."""
    # 이후 TEST에서 쓸 바인딩 후보를 찾도록 BIND SQL을 생성한다.
    return BindGenerationStageResult(
        bind_sql=generate_bind_sql_text(
            job=job,
            tobe_sql=tobe_sql,
            last_error=last_error,
            feedback_examples=feedback_examples or [],
        )
    )


def run_bind_execution_stage(bind_sql: str, max_rows: int = 50) -> BindExecutionStageResult:
    """bind 탐색 SQL을 실행한다."""
    # 생성된 BIND SQL을 실행해 실제 바인딩 후보 row를 수집한다.
    return BindExecutionStageResult(bind_query_rows=execute_bind_sql_text(bind_sql, max_rows=max_rows))


def build_bind_payload_stage(
    tobe_sql: str,
    source_sql: str,
    bind_query_rows: list[dict],
    max_cases: int = 3,
) -> BindPayloadStageResult:
    """TEST 실행과 DB 저장에 사용할 bind payload를 만든다."""
    # 수집한 후보 row를 TEST 실행용 BIND_SET 형태로 변환한다.
    bind_set_json_for_test, bind_set_for_db = build_bind_payloads(
        tobe_sql=tobe_sql,
        source_sql=source_sql,
        bind_query_rows=bind_query_rows,
        max_cases=max_cases,
    )
    return BindPayloadStageResult(
        bind_set_json_for_test=bind_set_json_for_test,
        bind_set_for_db=bind_set_for_db,
    )
