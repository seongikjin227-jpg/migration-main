"""Data models used by the SQL tuning pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field


class TuningStatus:
    AUTO_TUNED_VERIFIED = "AUTO_TUNED_VERIFIED"
    PROPOSAL_GENERATED = "PROPOSAL_GENERATED"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    TUNING_FAILED = "TUNING_FAILED"
    TUNING_SKIPPED = "TUNING_SKIPPED"


@dataclass
class TuningRule:
    rule_id: str
    guidance: list[str] = field(default_factory=list)
    example_bad_sql: str = ""
    example_tuned_sql: str = ""


@dataclass
class DetectedRule:
    rule: TuningRule
    detected_fragment: str
    detection_reason: str
    score: float = 0.0


@dataclass
class SupportCase:
    case_id: str
    why_selected: str
    bad_sql: str
    tuned_sql: str
    applied_rules: list[str] = field(default_factory=list)


@dataclass
class TuningPipelineResult:
    tuning_status: str
    normalized_sql: str = ""
    normalization_notes: list[str] = field(default_factory=list)
    detected_rules: list[DetectedRule] = field(default_factory=list)
    top_rules_json: str = "[]"
    support_case_json: str = "{}"
    tuning_context_text: str = ""
    tuned_sql: str | None = None
    tuned_test_sql: str | None = None
    llm_used_yn: str = "N"
    applied_rule_ids: list[str] = field(default_factory=list)
    diff_summary: str | None = None
    error_message: str | None = None
