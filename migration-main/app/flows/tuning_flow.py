"""튜닝 단계 흐름을 담당하는 오케스트레이션 모듈."""

from __future__ import annotations

from dataclasses import dataclass

from app.features.sql_tuning.tuning_models import TuningPipelineResult, TuningStatus
from app.features.sql_tuning.tuning_pipeline import run_tuning_pipeline
from app.features.sql_tuning.tuning_repository import persist_tuning_log, persist_tuning_result


@dataclass
class TuningStageResult:
    """튜닝 파이프라인 실행 결과를 담는 단계 결과."""
    pipeline_result: TuningPipelineResult


def run_tuning_review_stage(job, tobe_sql: str, bind_set_json: str | None) -> TuningStageResult:
    """검증된 TOBE SQL에 대해 전체 튜닝 리뷰 단계를 실행한다."""
    # 검증을 통과한 TOBE SQL에 대해 GOOD SQL 후보 생성과 재검증을 수행한다.
    return TuningStageResult(
        pipeline_result=run_tuning_pipeline(
            job=job,
            tobe_sql=tobe_sql,
            bind_set_json=bind_set_json,
        )
    )


__all__ = [
    "TuningPipelineResult",
    "TuningStageResult",
    "TuningStatus",
    "persist_tuning_log",
    "persist_tuning_result",
    "run_tuning_review_stage",
]
