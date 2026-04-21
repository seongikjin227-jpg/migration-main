"""Data models used by the SQL tuning pipeline."""

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
    rule_name: str
    category: str
    severity: str
    semantic_risk: str
    llm_allowed: bool
    verification_required: bool = True


@dataclass
class DetectedRule:
    rule: TuningRule
    detected_fragment: str
    detection_reason: str


@dataclass
class TuningPipelineResult:
    tuning_status: str
    normalized_sql: str = ""
    normalization_notes: list[str] = field(default_factory=list)
    detected_rules: list[DetectedRule] = field(default_factory=list)
    detected_rules_text: str = ""
    tuning_context_text: str = ""
    good_sql: str | None = None
    good_test_sql: str | None = None
    llm_used_yn: str = "N"
    applied_rule_ids: list[str] = field(default_factory=list)
    diff_summary: str | None = None
    error_message: str | None = None
