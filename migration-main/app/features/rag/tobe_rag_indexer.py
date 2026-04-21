"""TOBE-stage case-based RAG indexer."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from app.common import logger
from app.features.rag.tobe_rag_models import TobeRagCase, TobeRagDoc
from app.repositories.result_repository import get_tobe_rag_corpus_rows


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
load_dotenv(ROOT_DIR / ".env")
DOC_TYPE_SQL_SHAPE = "sql_shape"
DOC_TYPE_RULE_SUMMARY = "rule_summary"
DOC_TYPE_DIFF_SUMMARY = "diff_summary"
DOC_TYPE_SELECT_BLOCK = "select_block"
DOC_TYPE_JOIN_BLOCK = "join_block"
DOC_TYPE_WHERE_BLOCK = "where_block"
DOC_TYPE_PAGING_BLOCK = "paging_block"
DOC_TYPE_RULE_SNIPPET = "rule_snippet"


def _utc_now_text() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _sha256(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _normalize_identifier(token: str) -> str:
    value = (token or "").strip().strip('"').strip("'")
    if not value:
        return ""
    if "." in value:
        value = value.split(".")[-1]
    return value.upper()


def _parse_target_tables(raw: str) -> list[str]:
    text = (raw or "").strip()
    if not text:
        return []
    tokens: list[str] = []
    if text.startswith("[") or text.startswith("{"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                tokens = [str(item) for item in parsed]
            elif isinstance(parsed, str):
                tokens = [parsed]
        except Exception:
            tokens = []
    if not tokens:
        tokens = re.split(r"[,\s;|]+", text)
    return sorted({_normalize_identifier(token) for token in tokens if _normalize_identifier(token)})


def _tokenize_sparse(text: str) -> list[str]:
    return [token.upper() for token in re.findall(r"[A-Z0-9_]+", (text or "").upper()) if token]


def preprocess_mybatis_sql(sql_text: str) -> str:
    text = sql_text or ""
    text = re.sub(r"<if\b[^>]*>", " /*MB_IF_START*/ ", text, flags=re.IGNORECASE)
    text = re.sub(r"</if>", " /*MB_IF_END*/ ", text, flags=re.IGNORECASE)
    text = re.sub(r"<choose\b[^>]*>", " /*MB_CHOOSE_START*/ ", text, flags=re.IGNORECASE)
    text = re.sub(r"</choose>", " /*MB_CHOOSE_END*/ ", text, flags=re.IGNORECASE)
    text = re.sub(r"<when\b[^>]*>", " /*MB_WHEN_START*/ ", text, flags=re.IGNORECASE)
    text = re.sub(r"</when>", " /*MB_WHEN_END*/ ", text, flags=re.IGNORECASE)
    text = re.sub(r"<otherwise\b[^>]*>", " /*MB_OTHERWISE_START*/ ", text, flags=re.IGNORECASE)
    text = re.sub(r"</otherwise>", " /*MB_OTHERWISE_END*/ ", text, flags=re.IGNORECASE)
    text = re.sub(r"<foreach\b[^>]*>", " /*MB_FOREACH_START*/ ", text, flags=re.IGNORECASE)
    text = re.sub(r"</foreach>", " /*MB_FOREACH_END*/ ", text, flags=re.IGNORECASE)
    text = re.sub(r"#\{\s*([^}]+)\s*\}", lambda m: f":BIND_{_normalize_identifier(m.group(1)) or 'PARAM'}", text)
    text = re.sub(r"\$\{\s*([^}]+)\s*\}", lambda m: f"MB_SUBST_{_normalize_identifier(m.group(1)) or 'PARAM'}", text)
    text = re.sub(r"<[^>]+>", " ", text)
    return text


def try_parse_sql(sql_text: str, dialect: str = "oracle"):
    import sqlglot

    return sqlglot.parse_one(sql_text, read=dialect)


def normalize_sql(sql_text: str, dialect: str = "oracle") -> str:
    expression = try_parse_sql(sql_text, dialect=dialect)
    normalized = expression.sql(dialect=dialect, pretty=False)
    normalized = re.sub(r"'(?:''|[^'])*'", "<STR>", normalized)
    normalized = re.sub(r"\b\d+(?:\.\d+)?\b", "<NUM>", normalized)
    normalized = re.sub(r":BIND_[A-Z0-9_]+", ":BIND", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def extract_case_features(preprocessed_sql: str, normalized_sql: str, dialect: str = "oracle") -> dict[str, Any]:
    from sqlglot import exp

    expression = try_parse_sql(preprocessed_sql, dialect=dialect)
    table_names = sorted({_normalize_identifier(table.sql()) for table in expression.find_all(exp.Table) if table.sql()})
    column_names = sorted({_normalize_identifier(column.sql()) for column in expression.find_all(exp.Column) if column.sql()})
    query_type = expression.key.upper() if getattr(expression, "key", "") else "UNKNOWN"
    subquery_count = sum(1 for _ in expression.find_all(exp.Subquery))
    group_by_flag = any(True for _ in expression.find_all(exp.Group))
    order_by_flag = any(True for _ in expression.find_all(exp.Order))
    having_flag = any(True for _ in expression.find_all(exp.Having))
    window_function_flag = any(True for _ in expression.find_all(exp.Window))
    aggregate_flag = bool(re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", normalized_sql, flags=re.IGNORECASE))
    union_flag = any(True for _ in expression.find_all(exp.Union))
    exists_flag = " EXISTS " in f" {normalized_sql.upper()} " or " NOT EXISTS " in f" {normalized_sql.upper()} "
    pagination_flag = any(token in normalized_sql.upper() for token in ("ROWNUM", "ROW_NUMBER(", "FETCH FIRST"))
    feature_tags: set[str] = set()
    if query_type:
        feature_tags.add(query_type)
    if subquery_count > 0:
        feature_tags.add("SUBQUERY")
    if group_by_flag:
        feature_tags.add("GROUP_BY")
    if order_by_flag:
        feature_tags.add("ORDER_BY")
    if having_flag:
        feature_tags.add("HAVING")
    if window_function_flag:
        feature_tags.add("WINDOW_FUNCTION")
    if aggregate_flag:
        feature_tags.add("AGGREGATE")
    if union_flag:
        feature_tags.add("UNION")
    if exists_flag:
        feature_tags.add("EXISTS")
    if pagination_flag:
        feature_tags.add("PAGING")
    if "MB_IF" in preprocessed_sql:
        feature_tags.add("MYBATIS_IF")
    if "MB_CHOOSE" in preprocessed_sql:
        feature_tags.add("MYBATIS_CHOOSE")
    if "MB_FOREACH" in preprocessed_sql:
        feature_tags.add("MYBATIS_FOREACH")

    functions = []
    for fn_name in ("NVL", "DECODE", "ROWNUM", "SYSDATE", "SYSTIMESTAMP", "LISTAGG", "CONNECT BY", "MERGE", "(+)"):
        if fn_name in normalized_sql.upper():
            functions.append(fn_name)

    return {
        "table_names": table_names,
        "column_names": column_names,
        "feature_tags": sorted(feature_tags),
        "functions": functions,
        "dynamic_sql_flag": any(tag.startswith("MYBATIS_") for tag in feature_tags),
        "order_by_flag": order_by_flag,
        "pagination_flag": pagination_flag,
    }


def extract_rule_tags(source_sql: str, correct_sql: str) -> list[str]:
    source_upper = (source_sql or "").upper()
    correct_upper = (correct_sql or "").upper()
    tags: set[str] = set()
    if "NVL(" in source_upper:
        tags.add("NVL_TO_COALESCE")
    if "DECODE(" in source_upper:
        tags.add("DECODE_TO_CASE")
    if "ROWNUM" in source_upper:
        tags.add("ROWNUM_REWRITE")
    if "(+)" in source_upper:
        tags.add("OUTER_JOIN_PLUS_TO_ANSI_JOIN")
    if "SYSDATE" in source_upper or "SYSTIMESTAMP" in source_upper:
        tags.add("SYSDATE_TO_CURRENT_TIMESTAMP")
    if "CONNECT BY" in source_upper or "START WITH" in source_upper:
        tags.add("CONNECT_BY_REWRITE")
    if "LISTAGG(" in source_upper:
        tags.add("LISTAGG_REWRITE")
    if re.search(r"\bMERGE\b", source_upper):
        tags.add("MERGE_REWRITE")
    if "COALESCE(" in correct_upper and "NVL(" in source_upper:
        tags.add("NVL_TO_COALESCE")
    if "CASE" in correct_upper and "DECODE(" in source_upper:
        tags.add("DECODE_TO_CASE")
    if "JOIN" in correct_upper and "(+)" in source_upper:
        tags.add("OUTER_JOIN_PLUS_TO_ANSI_JOIN")
    return sorted(tags)


def build_change_summary(rule_tags: list[str], features: dict[str, Any]) -> str:
    rule_to_summary = {
        "NVL_TO_COALESCE": "Oracle null handling was rewritten.",
        "DECODE_TO_CASE": "Oracle conditional expressions were rewritten.",
        "ROWNUM_REWRITE": "Oracle pagination logic was rewritten.",
        "OUTER_JOIN_PLUS_TO_ANSI_JOIN": "Old Oracle outer join syntax was normalized.",
        "SYSDATE_TO_CURRENT_TIMESTAMP": "Oracle date or timestamp functions were standardized.",
        "CONNECT_BY_REWRITE": "Hierarchical query syntax requires rewrite handling.",
        "LISTAGG_REWRITE": "Oracle aggregation syntax requires rewrite handling.",
        "MERGE_REWRITE": "MERGE syntax requires rewrite handling.",
    }
    changes = [rule_to_summary[tag] for tag in rule_tags if tag in rule_to_summary]
    if features.get("dynamic_sql_flag"):
        changes.append("Dynamic SQL conditions must preserve original branching semantics.")
    if features.get("order_by_flag") and features.get("pagination_flag"):
        changes.append("Ordering and paging behavior must be preserved together.")
    return " ".join(changes).strip()


def _extract_clause_block(sql_text: str, clause_name: str) -> str:
    upper_text = f" {sql_text or ''} "
    clause = clause_name.upper()
    boundaries = {
        "SELECT": [" FROM "],
        "FROM": [" WHERE ", " GROUP BY ", " ORDER BY ", " HAVING ", " UNION ", " FETCH FIRST "],
        "WHERE": [" GROUP BY ", " ORDER BY ", " HAVING ", " UNION ", " FETCH FIRST "],
        "ORDER BY": [" FETCH FIRST "],
    }
    marker = f" {clause} "
    start = upper_text.upper().find(marker)
    if start < 0:
        return ""
    start += 1
    end = len(upper_text) - 1
    for boundary in boundaries.get(clause, []):
        pos = upper_text.upper().find(boundary, start + len(marker))
        if pos >= 0:
            end = min(end, pos)
    return upper_text[start:end].strip()


def extract_sql_blocks(source_sql: str, target_sql: str = "") -> dict[str, dict[str, str]]:
    source_from_block = _extract_clause_block(source_sql, "FROM")
    target_from_block = _extract_clause_block(target_sql, "FROM")
    source_where_block = _extract_clause_block(source_sql, "WHERE")
    target_where_block = _extract_clause_block(target_sql, "WHERE")
    source_order_block = _extract_clause_block(source_sql, "ORDER BY")
    target_order_block = _extract_clause_block(target_sql, "ORDER BY")
    source_select_block = _extract_clause_block(source_sql, "SELECT")
    target_select_block = _extract_clause_block(target_sql, "SELECT")

    return {
        "select_block": {
            "source": source_select_block,
            "target": target_select_block,
        },
        "join_block": {
            "source": source_from_block,
            "target": target_from_block,
        },
        "where_block": {
            "source": source_where_block,
            "target": target_where_block,
        },
        "paging_block": {
            "source": " ".join(
                part for part in [source_where_block if "ROWNUM" in source_where_block.upper() else "", source_order_block] if part
            ).strip(),
            "target": " ".join(
                part
                for part in [
                    target_where_block if "ROWNUM" in target_where_block.upper() else "",
                    target_order_block,
                    target_sql if "FETCH FIRST" in target_sql.upper() else "",
                ]
                if part
            ).strip(),
        },
    }


def build_rule_snippets(source_sql: str, target_sql: str, rule_tags: list[str]) -> list[dict[str, str]]:
    source_upper = (source_sql or "").upper()
    target_upper = (target_sql or "").upper()
    snippets: list[dict[str, str]] = []
    if "NVL_TO_COALESCE" in rule_tags:
        snippets.append(
            {
                "rule_tag": "NVL_TO_COALESCE",
                "source": "NVL(...) detected in source SQL.",
                "target": "COALESCE(...) expected in correct SQL." if "COALESCE(" in target_upper else "COALESCE-style rewrite expected.",
            }
        )
    if "DECODE_TO_CASE" in rule_tags:
        snippets.append(
            {
                "rule_tag": "DECODE_TO_CASE",
                "source": "DECODE(...) detected in source SQL.",
                "target": "CASE expression expected in correct SQL." if "CASE" in target_upper else "CASE-style rewrite expected.",
            }
        )
    if "OUTER_JOIN_PLUS_TO_ANSI_JOIN" in rule_tags:
        snippets.append(
            {
                "rule_tag": "OUTER_JOIN_PLUS_TO_ANSI_JOIN",
                "source": "(+) outer join syntax detected.",
                "target": "ANSI JOIN expected in correct SQL." if "JOIN" in target_upper else "ANSI JOIN-style rewrite expected.",
            }
        )
    if "ROWNUM_REWRITE" in rule_tags:
        snippets.append(
            {
                "rule_tag": "ROWNUM_REWRITE",
                "source": "ROWNUM-based paging detected.",
                "target": "FETCH FIRST / ROW_NUMBER style rewrite expected."
                if ("FETCH FIRST" in target_upper or "ROW_NUMBER(" in target_upper)
                else "Paging rewrite expected.",
            }
        )
    if not snippets:
        for tag in rule_tags:
            snippets.append({"rule_tag": tag, "source": f"{tag} detected.", "target": f"{tag} rewrite expected."})
    return snippets


def build_case_documents(case: TobeRagCase) -> list[TobeRagDoc]:
    shape_lines = [
        "SQL shape summary.",
        f"Tag kind: {case.tag_kind}",
        f"Target tables: {', '.join(case.target_tables) if case.target_tables else '(none)'}",
        f"Main tables: {', '.join(case.table_names) if case.table_names else '(none)'}",
        f"Features: {', '.join(case.feature_tags) if case.feature_tags else '(none)'}",
        f"Functions: {', '.join(case.functions) if case.functions else '(none)'}",
    ]
    rule_lines = [
        "Migration rule summary.",
        "Likely applied rules:",
        *([f"- {tag}" for tag in case.rule_tags] or ["- (none)"]),
    ]
    diff_lines = [
        "Transformation diff summary.",
        case.change_summary or "No rule-driven change summary was detected.",
    ]
    docs = [
        TobeRagDoc(
            doc_id=f"{case.case_id}::{DOC_TYPE_SQL_SHAPE}",
            case_id=case.case_id,
            doc_type=DOC_TYPE_SQL_SHAPE,
            doc_text="\n".join(shape_lines),
            tokenized_text=_tokenize_sparse(" ".join(case.rule_tags + case.feature_tags + case.target_tables + case.functions + case.table_names)),
        ),
        TobeRagDoc(
            doc_id=f"{case.case_id}::{DOC_TYPE_RULE_SUMMARY}",
            case_id=case.case_id,
            doc_type=DOC_TYPE_RULE_SUMMARY,
            doc_text="\n".join(rule_lines),
            tokenized_text=_tokenize_sparse(" ".join(case.rule_tags + case.functions)),
        ),
        TobeRagDoc(
            doc_id=f"{case.case_id}::{DOC_TYPE_DIFF_SUMMARY}",
            case_id=case.case_id,
            doc_type=DOC_TYPE_DIFF_SUMMARY,
            doc_text="\n".join(diff_lines),
            tokenized_text=_tokenize_sparse(" ".join(case.rule_tags + case.feature_tags + case.functions)),
        ),
    ]

    blocks = extract_sql_blocks(case.asis_sql_normalized, case.correct_sql)
    block_type_to_doc_type = {
        "select_block": DOC_TYPE_SELECT_BLOCK,
        "join_block": DOC_TYPE_JOIN_BLOCK,
        "where_block": DOC_TYPE_WHERE_BLOCK,
        "paging_block": DOC_TYPE_PAGING_BLOCK,
    }
    for block_type, payload in blocks.items():
        if not (payload.get("source") or payload.get("target")):
            continue
        docs.append(
            TobeRagDoc(
                doc_id=f"{case.case_id}::{block_type}",
                case_id=case.case_id,
                doc_type=block_type_to_doc_type[block_type],
                doc_text="\n".join(
                    [
                        f"{block_type} transformation summary.",
                        f"Source block: {payload.get('source') or '(none)'}",
                        f"Target block: {payload.get('target') or '(unknown)'}",
                    ]
                ),
                tokenized_text=_tokenize_sparse(
                    " ".join(
                        [
                            block_type,
                            payload.get("source", ""),
                            payload.get("target", ""),
                            " ".join(case.rule_tags),
                        ]
                    )
                ),
                source_priority=0.9,
            )
        )

    for idx, snippet in enumerate(build_rule_snippets(case.asis_sql_normalized, case.correct_sql, case.rule_tags), start=1):
        docs.append(
            TobeRagDoc(
                doc_id=f"{case.case_id}::{DOC_TYPE_RULE_SNIPPET}::{idx}",
                case_id=case.case_id,
                doc_type=DOC_TYPE_RULE_SNIPPET,
                doc_text="\n".join(
                    [
                        "Rule snippet summary.",
                        f"Rule tag: {snippet['rule_tag']}",
                        f"Source hint: {snippet['source']}",
                        f"Target hint: {snippet['target']}",
                    ]
                ),
                tokenized_text=_tokenize_sparse(
                    " ".join([snippet["rule_tag"], snippet["source"], snippet["target"]])
                ),
                source_priority=0.8,
            )
        )
    return docs


class TobeRagIndexer:
    def __init__(self) -> None:
        self.db_path = os.getenv("TOBE_RAG_DB_PATH", str(ROOT_DIR / "migration.db"))
        self.faiss_index_path = os.getenv("TOBE_RAG_FAISS_INDEX_PATH", str(ROOT_DIR / "migration_tobe_rag.faiss"))
        self.case_table = os.getenv("TOBE_RAG_TABLE_CASE_MASTER", "rag_case_master")
        self.doc_table = os.getenv("TOBE_RAG_TABLE_CASE_DOC", "rag_case_doc")
        self.corpus_limit = int(os.getenv("TOBE_RAG_CORPUS_LIMIT", "2000"))
        self.embed_timeout_sec = int(os.getenv("RAG_EMBED_TIMEOUT_SEC", "30"))
        self.parser_dialect = os.getenv("TOBE_RAG_PARSER_DIALECT", "oracle")
        self.allow_legacy_fallback = os.getenv("TOBE_RAG_ENABLE_LEGACY_CORRECT_FALLBACK", "Y").strip().upper() == "Y"
        self._ensure_schema()

    def sync_index(self, limit: int | None = None, rebuild: bool = False) -> dict[str, int]:
        source_rows = get_tobe_rag_corpus_rows(
            limit=limit or self.corpus_limit,
            allow_legacy_fallback=self.allow_legacy_fallback,
        )
        if rebuild:
            self._reset_storage()
        upserted = 0
        skipped_unchanged = 0
        skipped_no_correct_sql = 0
        skipped_parser_failed = 0
        now_text = _utc_now_text()
        for row in source_rows:
            if not (row.get("correct_sql") or "").strip():
                skipped_no_correct_sql += 1
                continue
            case = self.build_case_from_row(row)
            if case is None:
                skipped_parser_failed += 1
                continue
            existing_hash = self._load_case_hash(case.case_id)
            if existing_hash == case.source_hash:
                skipped_unchanged += 1
                continue
            docs = self.build_docs_for_case(case)
            embeddings = self.embed_docs(docs)
            for doc, embedding in zip(docs, embeddings):
                doc.embedding = embedding
            self._upsert_case(case, docs, now_text)
            upserted += 1
        self._rebuild_faiss_index()
        return {
            "source_rows": len(source_rows),
            "upserted": upserted,
            "skipped_unchanged": skipped_unchanged,
            "skipped_no_correct_sql": skipped_no_correct_sql,
            "skipped_parser_failed": skipped_parser_failed,
            "deleted": 0,
        }

    def build_case_from_row(self, row: dict[str, str]) -> TobeRagCase | None:
        asis_sql_raw = (row.get("edit_fr_sql") or "").strip() or (row.get("fr_sql_text") or "")
        correct_sql = row.get("correct_sql") or ""
        preprocessed = preprocess_mybatis_sql(asis_sql_raw)
        try:
            normalized = normalize_sql(preprocessed, dialect=self.parser_dialect)
            features = extract_case_features(preprocessed, normalized, dialect=self.parser_dialect)
        except Exception:
            return None
        rule_tags = extract_rule_tags(asis_sql_raw, correct_sql)
        change_summary = build_change_summary(rule_tags, features)
        source_hash = _sha256("||".join([asis_sql_raw, correct_sql, json.dumps(features, sort_keys=True), ",".join(rule_tags), "v1"]))
        return TobeRagCase(
            case_id=_sha256(f"{row.get('row_id','')}::{row.get('space_nm','')}::{row.get('sql_id','')}"),
            row_id=row.get("row_id", ""),
            space_nm=row.get("space_nm", ""),
            sql_id=row.get("sql_id", ""),
            tag_kind=(row.get("tag_kind", "") or "SELECT").strip().upper(),
            source_db="Oracle",
            target_db="PostgreSQL",
            asis_sql_raw=asis_sql_raw,
            asis_sql_preprocessed=preprocessed,
            asis_sql_normalized=normalized,
            tobe_sql_generated=row.get("to_sql_text", "") or "",
            correct_sql=correct_sql,
            target_tables=_parse_target_tables(row.get("target_table", "")),
            table_names=features["table_names"],
            column_names=features["column_names"],
            rule_tags=rule_tags,
            feature_tags=features["feature_tags"],
            functions=features["functions"],
            change_summary=change_summary,
            source_hash=source_hash,
        )

    def build_docs_for_case(self, case: TobeRagCase) -> list[TobeRagDoc]:
        return build_case_documents(case)

    def embed_docs(self, docs: list[TobeRagDoc]) -> list[list[float]]:
        endpoint = os.getenv("RAG_EMBED_BASE_URL", "").strip()
        if not endpoint:
            raise ValueError("Required environment variable 'RAG_EMBED_BASE_URL' is not set.")
        model = os.getenv("RAG_EMBED_MODEL", "BAAI/bge-m3")
        api_key = os.getenv("RAG_EMBED_API_KEY", "").strip()
        payload = {"model": model, "input": [doc.doc_text for doc in docs]}
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        response = requests.post(endpoint, headers=headers, json=payload, timeout=self.embed_timeout_sec)
        response.raise_for_status()
        body = response.json()
        vectors = self._parse_embeddings_from_response(body)
        if len(vectors) != len(docs):
            raise ValueError(f"Embedding result size mismatch. expected={len(docs)} actual={len(vectors)}")
        return vectors

    def _parse_embeddings_from_response(self, body: Any) -> list[list[float]]:
        if isinstance(body, dict):
            data = body.get("data")
            if isinstance(data, list):
                vectors = []
                for item in data:
                    if isinstance(item, dict) and isinstance(item.get("embedding"), list):
                        vectors.append([float(v) for v in item["embedding"]])
                if vectors:
                    return vectors
            embeddings = body.get("embeddings")
            if isinstance(embeddings, list):
                vectors = []
                for item in embeddings:
                    if isinstance(item, list):
                        vectors.append([float(v) for v in item])
                if vectors:
                    return vectors
            embedding = body.get("embedding")
            if isinstance(embedding, list):
                return [[float(v) for v in embedding]]
        raise ValueError(f"Unsupported embedding response format: {str(body)[:500]}")

    def _ensure_schema(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.case_table} (
                    case_id TEXT PRIMARY KEY,
                    row_id TEXT NOT NULL,
                    space_nm TEXT NOT NULL,
                    sql_id TEXT NOT NULL,
                    tag_kind TEXT NOT NULL,
                    source_db TEXT NOT NULL,
                    target_db TEXT NOT NULL,
                    asis_sql_raw TEXT NOT NULL,
                    asis_sql_preprocessed TEXT NOT NULL,
                    asis_sql_normalized TEXT NOT NULL,
                    tobe_sql_generated TEXT NOT NULL DEFAULT '',
                    correct_sql TEXT NOT NULL,
                    target_tables_json TEXT NOT NULL DEFAULT '[]',
                    table_names_json TEXT NOT NULL DEFAULT '[]',
                    column_names_json TEXT NOT NULL DEFAULT '[]',
                    rule_tags_json TEXT NOT NULL DEFAULT '[]',
                    feature_tags_json TEXT NOT NULL DEFAULT '[]',
                    functions_json TEXT NOT NULL DEFAULT '[]',
                    change_summary TEXT NOT NULL DEFAULT '',
                    parser_version TEXT NOT NULL,
                    normalizer_version TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    active_yn TEXT NOT NULL DEFAULT 'Y',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.doc_table} (
                    doc_id TEXT PRIMARY KEY,
                    case_id TEXT NOT NULL,
                    doc_type TEXT NOT NULL,
                    doc_text TEXT NOT NULL,
                    tokenized_text_json TEXT NOT NULL DEFAULT '[]',
                    embedding_json TEXT NOT NULL DEFAULT '[]',
                    faiss_row_id INTEGER NOT NULL DEFAULT -1,
                    source_priority REAL NOT NULL DEFAULT 1.0,
                    active_yn TEXT NOT NULL DEFAULT 'Y',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(case_id) REFERENCES {self.case_table}(case_id)
                )
                """
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.case_table}_space_sql ON {self.case_table}(space_nm, sql_id)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.case_table}_active ON {self.case_table}(active_yn, tag_kind)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.doc_table}_case ON {self.doc_table}(case_id, doc_type, active_yn)")
            conn.commit()

    def _reset_storage(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(f"DELETE FROM {self.doc_table}")
            conn.execute(f"DELETE FROM {self.case_table}")
            conn.commit()
        Path(self.faiss_index_path).unlink(missing_ok=True)

    def _load_case_hash(self, case_id: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(f"SELECT source_hash FROM {self.case_table} WHERE case_id = ?", (case_id,)).fetchone()
        return str(row[0]) if row else None

    def _upsert_case(self, case: TobeRagCase, docs: list[TobeRagDoc], now_text: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                INSERT INTO {self.case_table} (
                    case_id, row_id, space_nm, sql_id, tag_kind, source_db, target_db,
                    asis_sql_raw, asis_sql_preprocessed, asis_sql_normalized, tobe_sql_generated,
                    correct_sql, target_tables_json, table_names_json, column_names_json,
                    rule_tags_json, feature_tags_json, functions_json, change_summary,
                    parser_version, normalizer_version, source_hash, active_yn, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y', ?, ?)
                ON CONFLICT(case_id) DO UPDATE SET
                    row_id=excluded.row_id,
                    space_nm=excluded.space_nm,
                    sql_id=excluded.sql_id,
                    tag_kind=excluded.tag_kind,
                    source_db=excluded.source_db,
                    target_db=excluded.target_db,
                    asis_sql_raw=excluded.asis_sql_raw,
                    asis_sql_preprocessed=excluded.asis_sql_preprocessed,
                    asis_sql_normalized=excluded.asis_sql_normalized,
                    tobe_sql_generated=excluded.tobe_sql_generated,
                    correct_sql=excluded.correct_sql,
                    target_tables_json=excluded.target_tables_json,
                    table_names_json=excluded.table_names_json,
                    column_names_json=excluded.column_names_json,
                    rule_tags_json=excluded.rule_tags_json,
                    feature_tags_json=excluded.feature_tags_json,
                    functions_json=excluded.functions_json,
                    change_summary=excluded.change_summary,
                    parser_version=excluded.parser_version,
                    normalizer_version=excluded.normalizer_version,
                    source_hash=excluded.source_hash,
                    active_yn='Y',
                    updated_at=excluded.updated_at
                """,
                (
                    case.case_id,
                    case.row_id,
                    case.space_nm,
                    case.sql_id,
                    case.tag_kind,
                    case.source_db,
                    case.target_db,
                    case.asis_sql_raw,
                    case.asis_sql_preprocessed,
                    case.asis_sql_normalized,
                    case.tobe_sql_generated,
                    case.correct_sql,
                    _json_dumps(case.target_tables),
                    _json_dumps(case.table_names),
                    _json_dumps(case.column_names),
                    _json_dumps(case.rule_tags),
                    _json_dumps(case.feature_tags),
                    _json_dumps(case.functions),
                    case.change_summary,
                    case.parser_version,
                    case.normalizer_version,
                    case.source_hash,
                    now_text,
                    now_text,
                ),
            )
            conn.execute(f"DELETE FROM {self.doc_table} WHERE case_id = ?", (case.case_id,))
            for doc in docs:
                conn.execute(
                    f"""
                    INSERT INTO {self.doc_table} (
                        doc_id, case_id, doc_type, doc_text, tokenized_text_json,
                        embedding_json, faiss_row_id, source_priority, active_yn, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, -1, ?, 'Y', ?, ?)
                    """,
                    (
                        doc.doc_id,
                        doc.case_id,
                        doc.doc_type,
                        doc.doc_text,
                        _json_dumps(doc.tokenized_text),
                        _json_dumps(doc.embedding),
                        doc.source_priority,
                        now_text,
                        now_text,
                    ),
                )
            conn.commit()

    def _rebuild_faiss_index(self) -> None:
        try:
            import faiss
            import numpy as np
        except Exception as exc:
            logger.warning(f"[TOBE_RAG] Faiss rebuild skipped: {exc}")
            return
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT doc_id, embedding_json FROM {self.doc_table} WHERE active_yn = 'Y' ORDER BY doc_id"
            ).fetchall()
            vectors: list[list[float]] = []
            doc_ids: list[str] = []
            for row in rows:
                embedding = json.loads(str(row["embedding_json"]))
                if isinstance(embedding, list) and embedding:
                    vectors.append([float(v) for v in embedding])
                    doc_ids.append(str(row["doc_id"]))
            if not vectors:
                Path(self.faiss_index_path).unlink(missing_ok=True)
                conn.execute(f"UPDATE {self.doc_table} SET faiss_row_id = -1")
                conn.commit()
                return
            array = np.array(vectors, dtype="float32")
            faiss.normalize_L2(array)
            index = faiss.IndexFlatIP(array.shape[1])
            index.add(array)
            faiss.write_index(index, str(self.faiss_index_path))
            for idx, doc_id in enumerate(doc_ids):
                conn.execute(f"UPDATE {self.doc_table} SET faiss_row_id = ? WHERE doc_id = ?", (idx, doc_id))
            conn.commit()


tobe_rag_indexer = TobeRagIndexer()
