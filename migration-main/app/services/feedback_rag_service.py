"""Stage-aware correct SQL 기반 feedback RAG 서비스.

운영 원칙:
1) 벡터 인덱스 동기화는 시작 시점/수동 실행에서만 수행한다.
2) 배치 실행 중에는 조회만 수행한다.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from app.common import SqlInfoJob
from app.repositories.result_repository import get_feedback_corpus_rows


ROOT_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT_DIR / ".env")
_VALID_CORRECT_KINDS = ("TOBE", "BIND", "TEST")


@dataclass
class _VectorItem:
    doc_id: str
    correct_kind: str
    space_nm: str
    sql_id: str
    source_sql: str
    generated_sql: str
    correct_sql: str
    edited_yn: str
    upd_ts: str
    pattern_tags: list[str]
    embedding: list[float]


class FeedbackRagService:
    def __init__(self) -> None:
        self.db_path = os.getenv("RAG_VECTOR_DB_PATH", str(ROOT_DIR / "migration.db"))
        self.table_name = os.getenv("RAG_VECTOR_TABLE", "feedback_rag_index")
        self.top_k = int(os.getenv("RAG_TOP_K", "3"))
        self.corpus_limit = int(os.getenv("RAG_CORPUS_LIMIT", "2000"))
        self.embed_timeout_sec = int(os.getenv("RAG_EMBED_TIMEOUT_SEC", "30"))
        self._ensure_schema()

    def sync_index(self, limit: int | None = None) -> dict[str, int]:
        target_limit = limit if (limit and limit > 0) else self.corpus_limit
        source_rows: list[dict[str, str]] = []
        for correct_kind in _VALID_CORRECT_KINDS:
            source_rows.extend(get_feedback_corpus_rows(correct_kind=correct_kind, limit=target_limit))

        source_doc_ids: set[str] = set()
        existing_hash = self._load_existing_doc_hash()

        upserted = 0
        skipped_unchanged = 0
        skipped_no_correct_sql = 0

        for row in source_rows:
            source_sql = (row.get("edit_fr_sql") or "").strip() or (row.get("fr_sql_text") or "")
            generated_sql = row.get("to_sql_text") or ""
            correct_sql = row.get("correct_sql") or ""
            correct_kind = (row.get("correct_kind") or "").strip().upper()
            if not correct_sql.strip():
                skipped_no_correct_sql += 1
                continue

            doc_id = self._build_doc_id(row)
            source_doc_ids.add(doc_id)
            pattern_tags = self._extract_pattern_tags(source_sql, generated_sql, correct_sql)
            doc_text = self._build_doc_text(
                correct_kind=correct_kind,
                space_nm=row.get("space_nm", ""),
                sql_id=row.get("sql_id", ""),
                source_sql=source_sql,
                generated_sql=generated_sql,
                correct_sql=correct_sql,
                pattern_tags=pattern_tags,
            )
            text_hash = self._sha256(doc_text)

            if existing_hash.get(doc_id) == text_hash:
                skipped_unchanged += 1
                continue

            embedding = self._embed_texts([doc_text])[0]
            self._upsert_vector(
                item=_VectorItem(
                    doc_id=doc_id,
                    correct_kind=correct_kind,
                    space_nm=row.get("space_nm", ""),
                    sql_id=row.get("sql_id", ""),
                    source_sql=source_sql,
                    generated_sql=generated_sql,
                    correct_sql=correct_sql,
                    edited_yn=row.get("edited_yn", ""),
                    upd_ts=row.get("upd_ts", ""),
                    pattern_tags=pattern_tags,
                    embedding=embedding,
                ),
                text_hash=text_hash,
            )
            upserted += 1

        deleted = self._delete_removed_docs(source_doc_ids=source_doc_ids)
        return {
            "source_rows": len(source_rows),
            "upserted": upserted,
            "skipped_unchanged": skipped_unchanged,
            "skipped_no_correct_sql": skipped_no_correct_sql,
            "deleted": deleted,
        }

    def retrieve_feedback_examples(
        self,
        job: SqlInfoJob,
        last_error: str | None = None,
    ) -> list[dict[str, str]]:
        candidates = self._load_candidates()
        if not candidates:
            return []

        query_text = self._build_query_text(job=job, last_error=last_error)
        query_embedding = self._embed_texts([query_text])[0]
        query_tags = self._extract_pattern_tags(job.source_sql, last_error or "")
        ranked = self._rank_candidates(
            query_embedding=query_embedding,
            query_tags=query_tags,
            candidates=candidates,
        )

        examples: list[dict[str, str]] = []
        for item, score in ranked[: self.top_k]:
            examples.append(
                {
                    "correct_kind": item.correct_kind,
                    "edited_yn": item.edited_yn,
                    "correct_sql": item.correct_sql,
                    "generated_sql": item.generated_sql,
                    "source_sql": item.source_sql,
                    "similarity_score": f"{score:.6f}",
                    "pattern_tags_csv": ",".join(item.pattern_tags),
                }
            )
        return examples

    def _ensure_schema(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    doc_id TEXT PRIMARY KEY,
                    correct_kind TEXT NOT NULL,
                    space_nm TEXT NOT NULL,
                    sql_id TEXT NOT NULL,
                    source_sql TEXT NOT NULL,
                    generated_sql TEXT NOT NULL,
                    correct_sql TEXT NOT NULL,
                    edited_yn TEXT NOT NULL,
                    upd_ts TEXT NOT NULL,
                    pattern_tags_json TEXT NOT NULL DEFAULT '[]',
                    text_hash TEXT NOT NULL,
                    embedding_json TEXT NOT NULL
                )
                """
            )
            columns = {
                str(row[1]).lower() for row in conn.execute(f"PRAGMA table_info({self.table_name})").fetchall()
            }
            if "correct_kind" not in columns:
                conn.execute(
                    f"ALTER TABLE {self.table_name} ADD COLUMN correct_kind TEXT NOT NULL DEFAULT 'TOBE'"
                )
            if "pattern_tags_json" not in columns:
                conn.execute(
                    f"ALTER TABLE {self.table_name} ADD COLUMN pattern_tags_json TEXT NOT NULL DEFAULT '[]'"
                )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_kind_ns_id "
                f"ON {self.table_name}(correct_kind, space_nm, sql_id)"
            )
            conn.commit()

    def _load_existing_doc_hash(self) -> dict[str, str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(f"SELECT doc_id, text_hash FROM {self.table_name}").fetchall()
        return {str(row[0]): str(row[1]) for row in rows}

    def _build_doc_id(self, row: dict[str, str]) -> str:
        return (
            f"{row.get('correct_kind', 'TOBE')}::"
            f"{row.get('space_nm', '')}.{row.get('sql_id', '')}::{row.get('row_id', '')}"
        )

    def _build_doc_text(
        self,
        correct_kind: str,
        space_nm: str,
        sql_id: str,
        source_sql: str,
        generated_sql: str,
        correct_sql: str,
        pattern_tags: list[str],
    ) -> str:
        return (source_sql or "").strip()

    def _build_query_text(self, job: SqlInfoJob, last_error: str | None) -> str:
        return (job.source_sql or "").strip()

    def _upsert_vector(self, item: _VectorItem, text_hash: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    doc_id, correct_kind, space_nm, sql_id, source_sql, generated_sql,
                    correct_sql, edited_yn, upd_ts, pattern_tags_json, text_hash, embedding_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET
                    correct_kind=excluded.correct_kind,
                    space_nm=excluded.space_nm,
                    sql_id=excluded.sql_id,
                    source_sql=excluded.source_sql,
                    generated_sql=excluded.generated_sql,
                    correct_sql=excluded.correct_sql,
                    edited_yn=excluded.edited_yn,
                    upd_ts=excluded.upd_ts,
                    pattern_tags_json=excluded.pattern_tags_json,
                    text_hash=excluded.text_hash,
                    embedding_json=excluded.embedding_json
                """,
                (
                    item.doc_id,
                    item.correct_kind,
                    item.space_nm,
                    item.sql_id,
                    item.source_sql,
                    item.generated_sql,
                    item.correct_sql,
                    item.edited_yn,
                    item.upd_ts,
                    json.dumps(item.pattern_tags, ensure_ascii=False),
                    text_hash,
                    json.dumps(item.embedding, ensure_ascii=False),
                ),
            )
            conn.commit()

    def _delete_removed_docs(self, source_doc_ids: set[str]) -> int:
        if not source_doc_ids:
            return 0
        with sqlite3.connect(self.db_path) as conn:
            existing_ids = [row[0] for row in conn.execute(f"SELECT doc_id FROM {self.table_name}").fetchall()]
            to_delete = [doc_id for doc_id in existing_ids if doc_id not in source_doc_ids]
            if not to_delete:
                return 0
            conn.executemany(
                f"DELETE FROM {self.table_name} WHERE doc_id = ?",
                [(doc_id,) for doc_id in to_delete],
            )
            conn.commit()
            return len(to_delete)

    def _load_candidates(self) -> list[_VectorItem]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT doc_id, correct_kind, space_nm, sql_id, source_sql, generated_sql,
                       correct_sql, edited_yn, upd_ts, pattern_tags_json, embedding_json
                FROM {self.table_name}
                ORDER BY upd_ts DESC
                """,
            ).fetchall()

        result: list[_VectorItem] = []
        for row in rows:
            result.append(
                _VectorItem(
                    doc_id=str(row[0]),
                    correct_kind=str(row[1]),
                    space_nm=str(row[2]),
                    sql_id=str(row[3]),
                    source_sql=str(row[4]),
                    generated_sql=str(row[5]),
                    correct_sql=str(row[6]),
                    edited_yn=str(row[7]),
                    upd_ts=str(row[8]),
                    pattern_tags=self._parse_pattern_tags_json(row[9]),
                    embedding=self._parse_embedding_json(row[10]),
                )
            )
        return result

    def _rank_candidates(
        self,
        query_embedding: list[float],
        query_tags: list[str],
        candidates: list[_VectorItem],
    ) -> list[tuple[_VectorItem, float]]:
        query_tag_set = set(query_tags)
        scored: list[tuple[_VectorItem, float]] = []
        for item in candidates:
            cosine = self._cosine_similarity(query_embedding, item.embedding)
            overlap = len(query_tag_set.intersection(item.pattern_tags))
            bonus = min(0.20, overlap * 0.05)
            scored.append((item, cosine + bonus))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored

    def _extract_pattern_tags(self, *texts: str) -> list[str]:
        merged = " ".join((text or "") for text in texts)
        upper = merged.upper()
        tags: set[str] = set()

        if upper.count("SELECT") >= 3:
            tags.add("NESTED_SUBQUERY")
        if re.search(r"\(\s*SELECT", upper):
            tags.add("SUBQUERY")
        if re.search(r"SELECT[^;]*\(\s*SELECT", upper):
            tags.add("SCALAR_SUBQUERY")
        if upper.count(" JOIN ") >= 2:
            tags.add("MULTI_JOIN")
        if "ROW_NUMBER()" in upper or "FETCH FIRST" in upper or "ROWNUM" in upper:
            tags.add("PAGING")
        if "LIMIT " in upper or " OFFSET " in upper:
            tags.add("NON_ORACLE_PAGING")
        if upper.count(" FROM ") >= 2:
            tags.add("MULTI_FROM_CLAUSE")
        if upper.count(" WHERE ") >= 2:
            tags.add("MULTI_WHERE_CLAUSE")
        if "<IF" in upper or "<WHERE" in upper or "<CHOOSE" in upper:
            tags.add("MYBATIS_DYNAMIC_TAG")
        if "#{" in merged or "${" in merged:
            tags.add("MYBATIS_PLACEHOLDER")

        return sorted(tags)

    def _embed_texts(self, texts: list[str]) -> list[list[float]]:
        endpoint = self._require_env("RAG_EMBED_BASE_URL")
        model = os.getenv("RAG_EMBED_MODEL", "BAAI/bge-m3")
        api_key = os.getenv("RAG_EMBED_API_KEY", "").strip()

        payload = {"model": model, "input": texts}
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        response = requests.post(endpoint, headers=headers, json=payload, timeout=self.embed_timeout_sec)
        response.raise_for_status()
        body = response.json()
        vectors = self._parse_embeddings_from_response(body)
        if len(vectors) != len(texts):
            raise ValueError(
                f"Embedding result size mismatch. expected={len(texts)} actual={len(vectors)}"
            )
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

    def _parse_embedding_json(self, raw: Any) -> list[float]:
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, list):
                return [float(v) for v in parsed]
        except Exception:
            pass
        return []

    def _parse_pattern_tags_json(self, raw: Any) -> list[str]:
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, list):
                return sorted({str(item).strip().upper() for item in parsed if str(item).strip()})
        except Exception:
            pass
        return []

    def _cosine_similarity(self, a: list[float], b: list[float]) -> float:
        if not a or not b or len(a) != len(b):
            return -1.0
        dot = 0.0
        norm_a = 0.0
        norm_b = 0.0
        for x, y in zip(a, b):
            dot += x * y
            norm_a += x * x
            norm_b += y * y
        if norm_a <= 0.0 or norm_b <= 0.0:
            return -1.0
        return dot / (math.sqrt(norm_a) * math.sqrt(norm_b))

    def _sha256(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

    def _require_env(self, name: str) -> str:
        value = os.getenv(name, "").strip()
        if not value:
            raise ValueError(f"Required environment variable '{name}' is not set.")
        return value


feedback_rag_service = FeedbackRagService()
