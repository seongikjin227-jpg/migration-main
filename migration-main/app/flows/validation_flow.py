"""검증 단계 흐름을 담당하는 오케스트레이션 모듈."""

from __future__ import annotations

from dataclasses import dataclass

from app.features.validation.validation_feature import (
    evaluate_validation_status,
    execute_validation_test_sql,
    generate_validation_test_sql,
)


@dataclass
class ValidationSqlStageResult:
    """검증용 TEST SQL 생성 결과를 담는 단계 결과."""
    test_sql: str


@dataclass
class ValidationExecutionStageResult:
    """검증용 TEST SQL 실행 결과를 담는 단계 결과."""
    test_rows: list[dict]


@dataclass
class ValidationEvaluationStageResult:
    """검증 결과 상태를 담는 단계 결과."""
    status: str


def build_validation_stage_sql(job, tobe_sql: str, bind_param_names: list[str], bind_set_json_for_test: str) -> ValidationSqlStageResult:
    """FROM SQL과 TOBE SQL 비교용 TEST SQL을 생성한다."""
    # FROM SQL과 TOBE SQL의 결과를 비교할 TEST SQL을 deterministic하게 만든다.
    return ValidationSqlStageResult(
        test_sql=generate_validation_test_sql(
            job=job,
            tobe_sql=tobe_sql,
            bind_param_names=bind_param_names,
            bind_set_json_for_test=bind_set_json_for_test,
        )
    )


def run_validation_stage_sql(test_sql: str) -> ValidationExecutionStageResult:
    """검증용 TEST SQL을 실행한다."""
    # 생성된 TEST SQL을 실행해 비교 결과 row를 얻는다.
    return ValidationExecutionStageResult(test_rows=execute_validation_test_sql(test_sql))


def evaluate_validation_stage(test_rows: list[dict]) -> ValidationEvaluationStageResult:
    """검증 결과 row를 PASS/FAIL 상태로 평가한다."""
    # TEST 실행 결과를 PASS 또는 FAIL 상태로 판정한다.
    return ValidationEvaluationStageResult(status=evaluate_validation_status(test_rows))
