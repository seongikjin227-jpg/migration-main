from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TobeRagCase:
    case_id: str
    row_id: str
    space_nm: str
    sql_id: str
    tag_kind: str
    source_db: str
    target_db: str
    asis_sql_raw: str
    asis_sql_preprocessed: str
    asis_sql_normalized: str
    tobe_sql_generated: str
    correct_sql: str
    target_tables: list[str] = field(default_factory=list)
    table_names: list[str] = field(default_factory=list)
    column_names: list[str] = field(default_factory=list)
    rule_tags: list[str] = field(default_factory=list)
    feature_tags: list[str] = field(default_factory=list)
    functions: list[str] = field(default_factory=list)
    change_summary: str = ""
    parser_version: str = "sqlglot-oracle-v1"
    normalizer_version: str = "v1"
    source_hash: str = ""


@dataclass
class TobeRagDoc:
    doc_id: str
    case_id: str
    doc_type: str
    doc_text: str
    tokenized_text: list[str]
    embedding: list[float] = field(default_factory=list)
    faiss_row_id: int = -1
    source_priority: float = 1.0


@dataclass
class RetrievalCandidate:
    case_id: str
    dense_score: float = 0.0
    sparse_score: float = 0.0
    rule_overlap_score: float = 0.0
    target_table_match_score: float = 0.0
    pre_rerank_score: float = 0.0
    matched_doc_types: list[str] = field(default_factory=list)
    why_matched: str = ""
    llm_reason: str = ""
