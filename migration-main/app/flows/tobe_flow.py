"""TOBE 단계 흐름을 담당하는 오케스트레이션 모듈.

이 모듈은 TOBE 단계에서 필요한 문맥 조회와 SQL 생성 결과를
job flow가 바로 사용할 수 있는 형태로 묶어서 반환한다.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.features.tobe.tobe_feature import (
    generate_tobe_sql_with_soft_validation,
    load_tobe_feedback_examples,
)


@dataclass
class TobeFeedbackStageResult:
    """TOBE RAG 조회 결과를 담는 단계 결과."""
    feedback_examples: list[dict[str, str]]


@dataclass
class TobeGenerationStageResult:
    """TOBE SQL 생성 결과와 경고를 담는 단계 결과."""
    tobe_sql: str
    warning_message: str | None = None


def load_tobe_stage_context(job, mapping_rules, last_error: str | None = None) -> TobeFeedbackStageResult:
    """현재 job에 필요한 TOBE RAG 문맥을 조회한다."""
    # TOBE SQL 생성 전에 유사 사례와 보조 문맥을 먼저 불러온다.
    return TobeFeedbackStageResult(
        feedback_examples=load_tobe_feedback_examples(
            job=job,
            mapping_rules=mapping_rules or [],
            last_error=last_error,
        )
    )


def run_tobe_generation_stage(
    job,
    mapping_rules,
    feedback_examples: list[dict[str, str]] | None = None,
    last_error: str | None = None,
) -> TobeGenerationStageResult:
    """TOBE SQL과 soft-validation 경고를 함께 생성한다."""
    # 매핑룰과 RAG 예시를 사용해 TOBE SQL을 만들고 경고를 같이 돌려준다.
    tobe_sql, warning_message = generate_tobe_sql_with_soft_validation(
        job=job,
        mapping_rules=mapping_rules,
        feedback_examples=feedback_examples or [],
        last_error=last_error,
    )
    return TobeGenerationStageResult(tobe_sql=tobe_sql, warning_message=warning_message)
