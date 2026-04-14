"""CORRECT_SQL 기반 피드백 RAG 서비스.

운영 원칙:
1) 벡터 인덱스 저장(동기화)은 수동 명령에서만 수행
2) 배치 실행 중에는 저장 없이 조회만 수행
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

from app.models import SqlInfoJob
from app.repositories.result_repository import get_feedback_corpus_rows


ROOT_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT_DIR / ".env")


@dataclass
class _VectorItem:
    """벡터 인덱스 1건."""

    doc_id: str
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
    """정답 SQL 코퍼스를 벡터화하고 조회하는 서비스."""

    def __init__(self) -> None:
        self.db_path = os.getenv("RAG_VECTOR_DB_PATH", str(ROOT_DIR / "migration.db"))
        self.table_name = os.getenv("RAG_VECTOR_TABLE", "feedback_rag_index")
        self.top_k = int(os.getenv("RAG_TOP_K", "5"))
        self.corpus_limit = int(os.getenv("RAG_CORPUS_LIMIT", "2000"))
        self.embed_timeout_sec = int(os.getenv("RAG_EMBED_TIMEOUT_SEC", "30"))
        self._ensure_schema()

    def sync_index(self, limit: int | None = None) -> dict[str, int]:
        """Oracle 코퍼스를 SQLite 벡터 인덱스로 동기화한다.

        반환:
        - source_rows: 원본 코퍼스 조회 건수
        - upserted: 신규/변경으로 벡터 저장된 건수
        - skipped_unchanged: 변경 없음으로 건너뛴 건수
        - skipped_no_correct_sql: CORRECT_SQL 부재로 제외된 건수
        - deleted: 원본에서 사라져 인덱스에서 삭제된 건수
        """
        target_limit = limit if (limit and limit > 0) else self.corpus_limit
        source_rows = get_feedback_corpus_rows(limit=target_limit)
        source_doc_ids: set[str] = set()
        existing_hash = self._load_existing_doc_hash()

        upserted = 0
        skipped_unchanged = 0
        skipped_no_correct_sql = 0

        for row in source_rows:
            source_sql = (row.get("edit_fr_sql") or "").strip() or (row.get("fr_sql_text") or "")
            generated_sql = row.get("to_sql_text") or ""
            correct_sql = row.get("correct_sql") or ""
            if not correct_sql.strip():
                skipped_no_correct_sql += 1
                continue

            doc_id = self._build_doc_id(row)
            source_doc_ids.add(doc_id)
            pattern_tags = self._extract_pattern_tags(source_sql, generated_sql, correct_sql)
            doc_text = self._build_doc_text(
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

    def retrieve_feedback_examples(self, job: SqlInfoJob, last_error: str | None = None) -> list[dict[str, str]]:
        """현재 작업과 유사한 정답 SQL 예시를 조회한다.

        주의: 이 함수는 저장/동기화를 수행하지 않는다.
        """
        candidates = self._load_candidates(space_nm=job.space_nm, sql_id=job.sql_id)
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
        """SQLite 벡터 테이블 생성."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    doc_id TEXT PRIMARY KEY,
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
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_ns_id ON {self.table_name}(space_nm, sql_id)"
            )
            columns = {str(row[1]).lower() for row in conn.execute(f"PRAGMA table_info({self.table_name})").fetchall()}
            if "pattern_tags_json" not in columns:
                conn.execute(f"ALTER TABLE {self.table_name} ADD COLUMN pattern_tags_json TEXT NOT NULL DEFAULT '[]'")
            conn.commit()

    def _load_existing_doc_hash(self) -> dict[str, str]:
        """현재 인덱스의 doc_id -> text_hash 조회."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(f"SELECT doc_id, text_hash FROM {self.table_name}").fetchall()
        return {str(row[0]): str(row[1]) for row in rows}

    def _build_doc_id(self, row: dict[str, str]) -> str:
        """ROWID 기반 문서 ID."""
        return f"{row.get('space_nm','')}.{row.get('sql_id','')}::{row.get('row_id','')}"

    def _build_doc_text(
        self,
        space_nm: str,
        sql_id: str,
        source_sql: str,
        generated_sql: str,
        correct_sql: str,
        pattern_tags: list[str],
    ) -> str:
        """임베딩 입력 텍스트 구성."""
        return "\n".join(
            [
                f"[NAMESPACE] {space_nm}",
                f"[SQL_ID] {sql_id}",
                f"[PATTERN_TAGS] {','.join(pattern_tags)}",
                "[SOURCE_SQL]",
                source_sql.strip(),
                "[GENERATED_SQL]",
                generated_sql.strip(),
                "[CORRECT_SQL]",
                correct_sql.strip(),
            ]
        )

    def _build_query_text(self, job: SqlInfoJob, last_error: str | None) -> str:
        """조회 질의 임베딩용 텍스트 구성."""
        error_text = (last_error or "").strip()
        query_tags = self._extract_pattern_tags(job.source_sql, error_text)
        return "\n".join(
            [
                f"[NAMESPACE] {job.space_nm}",
                f"[SQL_ID] {job.sql_id}",
                f"[PATTERN_TAGS] {','.join(query_tags)}",
                "[SOURCE_SQL]",
                (job.source_sql or "").strip(),
                "[LAST_ERROR]",
                error_text or "NONE",
            ]
        )

    def _upsert_vector(self, item: _VectorItem, text_hash: str) -> None:
        """벡터 인덱스 upsert."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    doc_id, space_nm, sql_id, source_sql, generated_sql,
                    correct_sql, edited_yn, upd_ts, pattern_tags_json, text_hash, embedding_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET
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
        """원본 코퍼스에서 사라진 문서를 인덱스에서도 삭제."""
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

    def _load_candidates(self, space_nm: str, sql_id: str) -> list[_VectorItem]:
        """후보 벡터 로딩.

        - 1순위: 동일 namespace/sql_id
        - 2순위: 전체 인덱스
        """
        with sqlite3.connect(self.db_path) as conn:
            scoped_rows = conn.execute(
                f"""
                SELECT doc_id, space_nm, sql_id, source_sql, generated_sql,
                       correct_sql, edited_yn, upd_ts, pattern_tags_json, embedding_json
                FROM {self.table_name}
                WHERE UPPER(space_nm)=UPPER(?) AND UPPER(sql_id)=UPPER(?)
                """,
                (space_nm, sql_id),
            ).fetchall()

            rows = scoped_rows
            if not rows:
                rows = conn.execute(
                    f"""
                    SELECT doc_id, space_nm, sql_id, source_sql, generated_sql,
                           correct_sql, edited_yn, upd_ts, pattern_tags_json, embedding_json
                    FROM {self.table_name}
                    """
                ).fetchall()

        result: list[_VectorItem] = []
        for row in rows:
            result.append(
                _VectorItem(
                    doc_id=str(row[0]),
                    space_nm=str(row[1]),
                    sql_id=str(row[2]),
                    source_sql=str(row[3]),
                    generated_sql=str(row[4]),
                    correct_sql=str(row[5]),
                    edited_yn=str(row[6]),
                    upd_ts=str(row[7]),
                    pattern_tags=self._parse_pattern_tags_json(row[8]),
                    embedding=self._parse_embedding_json(row[9]),
                )
            )
        return result

    def _rank_candidates(
        self,
        query_embedding: list[float],
        query_tags: list[str],
        candidates: list[_VectorItem],
    ) -> list[tuple[_VectorItem, float]]:
        """코사인 유사도 + 패턴 태그 일치 보너스로 정렬한다."""
        query_tag_set = set(query_tags)
        scored: list[tuple[_VectorItem, float]] = []
        for item in candidates:
            cosine = self._cosine_similarity(query_embedding, item.embedding)
            overlap = len(query_tag_set.intersection(item.pattern_tags))
            bonus = min(0.20, overlap * 0.05)
            score = cosine + bonus
            scored.append((item, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored

    def _extract_pattern_tags(self, *texts: str) -> list[str]:
        """복합형 SQL 패턴 태그를 추출한다."""
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
        """임베딩 API 호출."""
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
        """OpenAI 호환/일반 임베딩 응답 파싱."""
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
        """DB embedding_json 복원."""
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, list):
                return [float(v) for v in parsed]
        except Exception:
            pass
        return []

    def _parse_pattern_tags_json(self, raw: Any) -> list[str]:
        """DB pattern_tags_json 복원."""
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, list):
                return sorted({str(item).strip().upper() for item in parsed if str(item).strip()})
        except Exception:
            pass
        return []

    def _cosine_similarity(self, a: list[float], b: list[float]) -> float:
        """코사인 유사도 계산."""
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
        """문서 해시."""
        return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

    def _require_env(self, name: str) -> str:
        """필수 환경변수 조회."""
        value = os.getenv(name, "").strip()
        if not value:
            raise ValueError(f"Required environment variable '{name}' is not set.")
        return value


feedback_rag_service = FeedbackRagService()
