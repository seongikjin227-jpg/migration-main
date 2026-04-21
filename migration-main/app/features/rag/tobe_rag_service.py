"""TOBE-stage retrieval service built on case-based hybrid RAG."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

from app.common import logger
from app.common import MappingRuleItem, SqlInfoJob
from app.features.rag.tobe_rag_indexer import (
    DOC_TYPE_DIFF_SUMMARY,
    DOC_TYPE_JOIN_BLOCK,
    DOC_TYPE_PAGING_BLOCK,
    DOC_TYPE_RULE_SUMMARY,
    DOC_TYPE_RULE_SNIPPET,
    DOC_TYPE_SQL_SHAPE,
    DOC_TYPE_SELECT_BLOCK,
    DOC_TYPE_WHERE_BLOCK,
    ROOT_DIR,
    TobeRagIndexer,
    build_case_documents,
    build_change_summary,
    extract_case_features,
    extract_rule_tags,
    normalize_sql,
    preprocess_mybatis_sql,
)
from app.features.rag.tobe_rag_models import RetrievalCandidate, TobeRagCase
from app.services.llm_service import call_llm_text_api
from app.services.prompt_service import render_prompt


load_dotenv(ROOT_DIR / ".env")


class TobeRagService:
    def __init__(self) -> None:
        self.db_path = os.getenv("TOBE_RAG_DB_PATH", str(ROOT_DIR / "migration.db"))
        self.faiss_index_path = os.getenv("TOBE_RAG_FAISS_INDEX_PATH", str(ROOT_DIR / "migration_tobe_rag.faiss"))
        self.case_table = os.getenv("TOBE_RAG_TABLE_CASE_MASTER", "rag_case_master")
        self.doc_table = os.getenv("TOBE_RAG_TABLE_CASE_DOC", "rag_case_doc")
        self.top_k_dense = int(os.getenv("TOBE_RAG_TOP_K_DENSE", "12"))
        self.top_k_sparse = int(os.getenv("TOBE_RAG_TOP_K_SPARSE", "12"))
        self.top_k_rerank = int(os.getenv("TOBE_RAG_TOP_K_RERANK", "8"))
        self.top_k_prompt = int(os.getenv("TOBE_RAG_TOP_K_PROMPT", "4"))
        self.parser_dialect = os.getenv("TOBE_RAG_PARSER_DIALECT", "oracle")
        self.indexer = TobeRagIndexer()

    def sync_index(self, limit: int | None = None, rebuild: bool = False) -> dict[str, int]:
        return self.indexer.sync_index(limit=limit, rebuild=rebuild)

    def retrieve_reference_cases(
        self,
        job: SqlInfoJob,
        mapping_rules: list[MappingRuleItem] | None = None,
        last_error: str | None = None,
    ) -> list[dict[str, str]]:
        query_case = self.build_query_case(job=job, mapping_rules=mapping_rules)
        if query_case is None:
            logger.info(f"[TOBE_RAG] parser failed for query ({job.space_nm}.{job.sql_id}); skip retrieval.")
            return []
        allowed_case_ids = self._load_allowed_case_ids(query_case)
        dense_candidates = self.search_dense(query_case, allowed_case_ids)
        sparse_candidates = self.search_sparse(query_case, allowed_case_ids)
        merged = self._merge_candidates(query_case, dense_candidates, sparse_candidates)
        if not merged:
            return []
        reranked = self.rerank_with_llm(query_case, merged[: self.top_k_rerank], last_error=last_error)
        return self.serialize_prompt_examples(query_case, reranked[: self.top_k_prompt])

    def build_query_case(
        self,
        job: SqlInfoJob,
        mapping_rules: list[MappingRuleItem] | None = None,
    ) -> TobeRagCase | None:
        preprocessed = preprocess_mybatis_sql(job.source_sql)
        try:
            normalized = normalize_sql(preprocessed, dialect=self.parser_dialect)
            features = extract_case_features(preprocessed, normalized, dialect=self.parser_dialect)
        except Exception:
            return None
        rule_tags = extract_rule_tags(job.source_sql, "")
        target_tables = self._extract_target_tables_from_mapping(mapping_rules or [], job.target_table or "")
        return TobeRagCase(
            case_id=f"QUERY::{job.space_nm}.{job.sql_id}",
            row_id=job.row_id,
            space_nm=job.space_nm,
            sql_id=job.sql_id,
            tag_kind=(job.tag_kind or "").strip().upper() or "SELECT",
            source_db="Oracle",
            target_db="PostgreSQL",
            asis_sql_raw=job.source_sql,
            asis_sql_preprocessed=preprocessed,
            asis_sql_normalized=normalized,
            tobe_sql_generated=job.to_sql_text or "",
            correct_sql="",
            target_tables=target_tables,
            table_names=features["table_names"],
            column_names=features["column_names"],
            rule_tags=rule_tags,
            feature_tags=features["feature_tags"],
            functions=features["functions"],
            change_summary=build_change_summary(rule_tags, features),
        )

    def search_dense(self, query_case: TobeRagCase, allowed_case_ids: set[str]) -> list[RetrievalCandidate]:
        if not allowed_case_ids:
            return []
        try:
            import faiss
            import numpy as np
        except Exception as exc:
            logger.warning(f"[TOBE_RAG] dense retrieval skipped because Faiss is unavailable: {exc}")
            return []
        index_path = Path(self.faiss_index_path)
        if not index_path.exists():
            return []
        index = faiss.read_index(str(index_path))
        query_docs = build_case_documents(query_case)
        embeddings = self.indexer.embed_docs(query_docs)
        candidates: dict[str, RetrievalCandidate] = {}
        scan_k = max(self.top_k_dense * 10, 50)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            doc_by_row_id = {
                int(row["faiss_row_id"]): row
                for row in conn.execute(
                    f"SELECT faiss_row_id, doc_type, case_id FROM {self.doc_table} WHERE active_yn = 'Y'"
                ).fetchall()
            }
        for doc, embedding in zip(query_docs, embeddings):
            query_array = np.array([embedding], dtype="float32")
            faiss.normalize_L2(query_array)
            scores, ids = index.search(query_array, min(scan_k, index.ntotal))
            for score, row_id in zip(scores[0], ids[0]):
                if int(row_id) < 0:
                    continue
                meta = doc_by_row_id.get(int(row_id))
                if not meta:
                    continue
                case_id = str(meta["case_id"])
                if case_id not in allowed_case_ids or str(meta["doc_type"]) != doc.doc_type:
                    continue
                candidate = candidates.setdefault(case_id, RetrievalCandidate(case_id=case_id))
                candidate.dense_score += float(score) * {
                    DOC_TYPE_SQL_SHAPE: 0.45,
                    DOC_TYPE_RULE_SUMMARY: 0.35,
                    DOC_TYPE_DIFF_SUMMARY: 0.20,
                    DOC_TYPE_SELECT_BLOCK: 0.18,
                    DOC_TYPE_JOIN_BLOCK: 0.20,
                    DOC_TYPE_WHERE_BLOCK: 0.18,
                    DOC_TYPE_PAGING_BLOCK: 0.20,
                    DOC_TYPE_RULE_SNIPPET: 0.22,
                }.get(doc.doc_type, 0.0)
                if doc.doc_type not in candidate.matched_doc_types:
                    candidate.matched_doc_types.append(doc.doc_type)
        return sorted(candidates.values(), key=lambda item: item.dense_score, reverse=True)[: self.top_k_dense]

    def search_sparse(self, query_case: TobeRagCase, allowed_case_ids: set[str]) -> list[RetrievalCandidate]:
        if not allowed_case_ids:
            return []
        try:
            from rank_bm25 import BM25Okapi
        except Exception as exc:
            logger.warning(f"[TOBE_RAG] sparse retrieval skipped because rank-bm25 is unavailable: {exc}")
            return []
        rows = self._load_sparse_docs(allowed_case_ids)
        if not rows:
            return []
        bm25 = BM25Okapi([row["tokens"] for row in rows])
        query_tokens = sorted({
            *[token.upper() for token in query_case.rule_tags],
            *[token.upper() for token in query_case.feature_tags],
            *[token.upper() for token in query_case.target_tables],
            *[token.upper() for token in query_case.functions],
        })
        scores = bm25.get_scores(query_tokens)
        raw_max = max(scores) if len(scores) > 0 else 0.0
        candidates: dict[str, RetrievalCandidate] = {}
        for row, score in zip(rows, scores):
            if score <= 0:
                continue
            candidate = candidates.setdefault(row["case_id"], RetrievalCandidate(case_id=row["case_id"]))
            candidate.sparse_score = max(candidate.sparse_score, float(score / raw_max) if raw_max > 0 else 0.0)
        return sorted(candidates.values(), key=lambda item: item.sparse_score, reverse=True)[: self.top_k_sparse]

    def rerank_with_llm(
        self,
        query_case: TobeRagCase,
        candidates: list[RetrievalCandidate],
        last_error: str | None = None,
    ) -> list[RetrievalCandidate]:
        if not candidates:
            return []
        candidate_payload = []
        for candidate in candidates:
            case_payload = self._load_case_payload(candidate.case_id)
            if case_payload:
                candidate_payload.append(
                    {
                        "case_id": candidate.case_id,
                        "pre_rerank_score": round(candidate.pre_rerank_score, 6),
                        "shape_summary": ", ".join(case_payload["feature_tags"]) or "(none)",
                        "applied_rules": case_payload["rule_tags"],
                        "change_summary": case_payload["change_summary"],
                        "target_tables": case_payload["target_tables"],
                    }
                )
        if not candidate_payload:
            return candidates
        messages = [
            {
                "role": "system",
                "content": render_prompt(
                    "tobe_rag_rerank_prompt.txt",
                    current_query_summary=json.dumps(
                        {
                            "tag_kind": query_case.tag_kind,
                            "target_tables": query_case.target_tables,
                            "feature_tags": query_case.feature_tags,
                            "rule_tags": query_case.rule_tags,
                            "last_error": last_error or "",
                        },
                        ensure_ascii=False,
                    ),
                    candidate_cases_json=json.dumps(candidate_payload, ensure_ascii=False),
                ),
            },
            {"role": "user", "content": "Return the reranked case ids in JSON only."},
        ]
        try:
            response_text = call_llm_text_api(api_key=None, model=None, base_url=None, messages=messages)
            result = self._parse_rerank_response(response_text)
            order_map = {case_id: idx for idx, case_id in enumerate(result.get("ranked_case_ids", []))}
            reasons = result.get("reasons", {})
            ordered = sorted(candidates, key=lambda item: (order_map.get(item.case_id, 10_000), -item.pre_rerank_score))
            for candidate in ordered:
                candidate.llm_reason = str(reasons.get(candidate.case_id, "")).strip()
            return ordered
        except Exception as exc:
            logger.warning(f"[TOBE_RAG] LLM rerank failed. Fallback to pre-rerank order: {exc}")
            return candidates

    def serialize_prompt_examples(self, query_case: TobeRagCase, ranked_candidates: list[RetrievalCandidate]) -> list[dict[str, str]]:
        examples: list[dict[str, str]] = []
        for candidate in ranked_candidates:
            case_payload = self._load_case_payload(candidate.case_id)
            if not case_payload:
                continue
            shared_rules = sorted(set(query_case.rule_tags).intersection(case_payload["rule_tags"]))
            shared_features = sorted(set(query_case.feature_tags).intersection(case_payload["feature_tags"]))
            why_parts = []
            if shared_rules:
                why_parts.append("shares rules: " + ", ".join(shared_rules))
            if shared_features:
                why_parts.append("shares shape: " + ", ".join(shared_features[:5]))
            if not why_parts and candidate.llm_reason:
                why_parts.append(candidate.llm_reason)
            examples.append(
                {
                    "case_id": candidate.case_id,
                    "why_matched": "; ".join(why_parts) or candidate.llm_reason or "structurally similar migration case",
                    "applied_rules": ", ".join(case_payload["rule_tags"]),
                    "change_summary": case_payload["change_summary"],
                    "shape_summary": ", ".join(case_payload["feature_tags"]),
                }
            )
        return examples

    def _load_allowed_case_ids(self, query_case: TobeRagCase) -> set[str]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT case_id, target_tables_json
                FROM {self.case_table}
                WHERE active_yn = 'Y'
                  AND tag_kind = ?
                  AND source_db = 'Oracle'
                  AND target_db = 'PostgreSQL'
                """,
                (query_case.tag_kind,),
            ).fetchall()
        if not query_case.target_tables:
            return {str(row["case_id"]) for row in rows}
        query_target_set = set(query_case.target_tables)
        allowed: set[str] = set()
        for row in rows:
            candidate_targets = set(json.loads(str(row["target_tables_json"])))
            if not candidate_targets or query_target_set.intersection(candidate_targets):
                allowed.add(str(row["case_id"]))
        return allowed

    def _load_sparse_docs(self, allowed_case_ids: set[str]) -> list[dict[str, object]]:
        placeholders = ", ".join("?" for _ in allowed_case_ids)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT case_id, tokenized_text_json FROM {self.doc_table} WHERE active_yn = 'Y' AND case_id IN ({placeholders})",
                tuple(sorted(allowed_case_ids)),
            ).fetchall()
        result = []
        for row in rows:
            tokens = json.loads(str(row["tokenized_text_json"]))
            if isinstance(tokens, list) and tokens:
                result.append({"case_id": str(row["case_id"]), "tokens": [str(token) for token in tokens]})
        return result

    def _merge_candidates(
        self,
        query_case: TobeRagCase,
        dense_candidates: list[RetrievalCandidate],
        sparse_candidates: list[RetrievalCandidate],
    ) -> list[RetrievalCandidate]:
        merged: dict[str, RetrievalCandidate] = {}
        for candidate in dense_candidates:
            merged[candidate.case_id] = RetrievalCandidate(
                case_id=candidate.case_id,
                dense_score=candidate.dense_score,
                sparse_score=candidate.sparse_score,
                matched_doc_types=list(candidate.matched_doc_types),
            )
        for candidate in sparse_candidates:
            current = merged.setdefault(candidate.case_id, RetrievalCandidate(case_id=candidate.case_id))
            current.sparse_score = max(current.sparse_score, candidate.sparse_score)
        for candidate in merged.values():
            case_payload = self._load_case_payload(candidate.case_id)
            if not case_payload:
                continue
            candidate.rule_overlap_score = self._compute_overlap_ratio(query_case.rule_tags, case_payload["rule_tags"])
            candidate.target_table_match_score = self._compute_overlap_ratio(query_case.target_tables, case_payload["target_tables"])
            candidate.pre_rerank_score = (
                0.50 * candidate.dense_score
                + 0.20 * candidate.sparse_score
                + 0.20 * candidate.rule_overlap_score
                + 0.10 * candidate.target_table_match_score
            )
        return sorted(merged.values(), key=lambda item: item.pre_rerank_score, reverse=True)

    def _load_case_payload(self, case_id: str) -> dict[str, object] | None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                f"SELECT target_tables_json, rule_tags_json, feature_tags_json, change_summary FROM {self.case_table} WHERE case_id = ?",
                (case_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "target_tables": json.loads(str(row["target_tables_json"])),
            "rule_tags": json.loads(str(row["rule_tags_json"])),
            "feature_tags": json.loads(str(row["feature_tags_json"])),
            "change_summary": str(row["change_summary"] or ""),
        }

    def _parse_rerank_response(self, raw_text: str) -> dict[str, object]:
        text = (raw_text or "").strip()
        code_block = re.search(r"```(?:json)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
        if code_block:
            text = code_block.group(1).strip()
        result = json.loads(text)
        if not isinstance(result, dict):
            raise ValueError("rerank response must be a JSON object")
        return result

    def _compute_overlap_ratio(self, left: list[str], right: list[str]) -> float:
        left_set = {str(item).upper() for item in left if str(item).strip()}
        right_set = {str(item).upper() for item in right if str(item).strip()}
        if not left_set or not right_set:
            return 0.0
        return len(left_set.intersection(right_set)) / float(len(left_set))

    def _extract_target_tables_from_mapping(self, mapping_rules: list[MappingRuleItem], raw_target_table: str) -> list[str]:
        target_tables = set()
        for rule in mapping_rules:
            value = (rule.to_table or "").strip()
            if value:
                target_tables.add(value.split(".")[-1].upper())
        if not target_tables:
            for token in re.split(r"[,\s;|]+", raw_target_table or ""):
                cleaned = token.strip().strip('"')
                if cleaned:
                    target_tables.add(cleaned.split(".")[-1].upper())
        return sorted(target_tables)


tobe_rag_service = TobeRagService()
