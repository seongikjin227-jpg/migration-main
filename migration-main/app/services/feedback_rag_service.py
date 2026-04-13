"""CORRECT_SQL 기반 피드백 RAG 서비스.

동작 개요:
1) Oracle(NEXT_SQL_INFO)에서 정답 SQL 코퍼스 로드
2) BGE-M3 임베딩 API 호출
3) SQLite 벡터 인덱스에 저장
4) 현재 작업 SQL과 유사한 정답 예시를 검색해 프롬프트에 주입
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import time
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
    """벡터 인덱스 1건을 표현한다."""

    doc_id: str
    space_nm: str
    sql_id: str
    source_sql: str
    generated_sql: str
    correct_sql: str
    edited_yn: str
    upd_ts: str
    embedding: list[float]


class FeedbackRagService:
    """정답 SQL 코퍼스의 벡터 인덱싱/검색을 담당한다."""

    def __init__(self) -> None:
        self.db_path = os.getenv("RAG_VECTOR_DB_PATH", str(ROOT_DIR / "migration.db"))
        self.table_name = os.getenv("RAG_VECTOR_TABLE", "feedback_rag_index")
        self.top_k = int(os.getenv("RAG_TOP_K", "5"))
        self.sync_interval_sec = int(os.getenv("RAG_SYNC_INTERVAL_SEC", "300"))
        self.corpus_limit = int(os.getenv("RAG_CORPUS_LIMIT", "2000"))
        self.embed_timeout_sec = int(os.getenv("RAG_EMBED_TIMEOUT_SEC", "30"))
        self.last_sync_ts = 0.0
        self._ensure_schema()

    def retrieve_feedback_examples(self, job: SqlInfoJob, last_error: str | None = None) -> list[dict[str, str]]:
        """현재 작업과 유사한 정답 SQL 예시를 반환한다."""
        self._sync_if_needed()
        query_text = self._build_query_text(job=job, last_error=last_error)
        query_embedding = self._embed_texts([query_text])[0]
        candidates = self._load_candidates(space_nm=job.space_nm, sql_id=job.sql_id)
        ranked = self._rank_by_cosine(query_embedding=query_embedding, candidates=candidates)

        examples: list[dict[str, str]] = []
        for item, score in ranked[: self.top_k]:
            examples.append(
                {
                    "edited_yn": item.edited_yn,
                    "correct_sql": item.correct_sql,
                    "generated_sql": item.generated_sql,
                    "source_sql": item.source_sql,
                    "similarity_score": f"{score:.6f}",
                }
            )
        return examples

    def _sync_if_needed(self) -> None:
        """주기적으로 Oracle 코퍼스를 SQLite 벡터 인덱스에 동기화한다."""
        now = time.time()
        if (now - self.last_sync_ts) < self.sync_interval_sec:
            return

        source_rows = get_feedback_corpus_rows(limit=self.corpus_limit)
        source_doc_ids = set()
        existing_hash = self._load_existing_doc_hash()

        for row in source_rows:
            source_sql = (row.get("edit_fr_sql") or "").strip() or (row.get("fr_sql_text") or "")
            generated_sql = row.get("to_sql_text") or ""
            correct_sql = row.get("correct_sql") or ""
            if not correct_sql.strip():
                continue

            doc_id = self._build_doc_id(row)
            source_doc_ids.add(doc_id)
            doc_text = self._build_doc_text(
                space_nm=row.get("space_nm", ""),
                sql_id=row.get("sql_id", ""),
                source_sql=source_sql,
                generated_sql=generated_sql,
                correct_sql=correct_sql,
            )
            text_hash = self._sha256(doc_text)

            if existing_hash.get(doc_id) == text_hash:
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
                    embedding=embedding,
                ),
                text_hash=text_hash,
            )

        self._delete_removed_docs(source_doc_ids=source_doc_ids)
        self.last_sync_ts = now

    def _ensure_schema(self) -> None:
        """SQLite 벡터 테이블을 생성한다."""
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
                    text_hash TEXT NOT NULL,
                    embedding_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_ns_id ON {self.table_name}(space_nm, sql_id)"
            )
            conn.commit()

    def _load_existing_doc_hash(self) -> dict[str, str]:
        """인덱스에 저장된 doc_id -> text_hash 맵을 읽는다."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(f"SELECT doc_id, text_hash FROM {self.table_name}").fetchall()
        return {str(row[0]): str(row[1]) for row in rows}

    def _build_doc_id(self, row: dict[str, str]) -> str:
        """ROWID 기반 고유 문서 ID를 생성한다."""
        return f"{row.get('space_nm','')}.{row.get('sql_id','')}::{row.get('row_id','')}"

    def _build_doc_text(
        self,
        space_nm: str,
        sql_id: str,
        source_sql: str,
        generated_sql: str,
        correct_sql: str,
    ) -> str:
        """임베딩 입력용 텍스트를 구성한다."""
        return "\n".join(
            [
                f"[NAMESPACE] {space_nm}",
                f"[SQL_ID] {sql_id}",
                "[SOURCE_SQL]",
                source_sql.strip(),
                "[GENERATED_SQL]",
                generated_sql.strip(),
                "[CORRECT_SQL]",
                correct_sql.strip(),
            ]
        )

    def _build_query_text(self, job: SqlInfoJob, last_error: str | None) -> str:
        """현재 작업 문맥을 임베딩 질의 텍스트로 구성한다."""
        error_text = (last_error or "").strip()
        return "\n".join(
            [
                f"[NAMESPACE] {job.space_nm}",
                f"[SQL_ID] {job.sql_id}",
                "[SOURCE_SQL]",
                (job.source_sql or "").strip(),
                "[LAST_ERROR]",
                error_text or "NONE",
            ]
        )

    def _upsert_vector(self, item: _VectorItem, text_hash: str) -> None:
        """문서 벡터를 upsert 한다."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    doc_id, space_nm, sql_id, source_sql, generated_sql,
                    correct_sql, edited_yn, upd_ts, text_hash, embedding_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET
                    space_nm=excluded.space_nm,
                    sql_id=excluded.sql_id,
                    source_sql=excluded.source_sql,
                    generated_sql=excluded.generated_sql,
                    correct_sql=excluded.correct_sql,
                    edited_yn=excluded.edited_yn,
                    upd_ts=excluded.upd_ts,
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
                    text_hash,
                    json.dumps(item.embedding, ensure_ascii=False),
                ),
            )
            conn.commit()

    def _delete_removed_docs(self, source_doc_ids: set[str]) -> None:
        """원본 코퍼스에서 사라진 문서를 인덱스에서도 제거한다."""
        if not source_doc_ids:
            return
        with sqlite3.connect(self.db_path) as conn:
            existing_ids = [row[0] for row in conn.execute(f"SELECT doc_id FROM {self.table_name}").fetchall()]
            to_delete = [doc_id for doc_id in existing_ids if doc_id not in source_doc_ids]
            if not to_delete:
                return
            conn.executemany(
                f"DELETE FROM {self.table_name} WHERE doc_id = ?",
                [(doc_id,) for doc_id in to_delete],
            )
            conn.commit()

    def _load_candidates(self, space_nm: str, sql_id: str) -> list[_VectorItem]:
        """우선 동일 namespace/sql_id에서 검색하고, 없으면 전체 검색한다."""
        with sqlite3.connect(self.db_path) as conn:
            scoped_rows = conn.execute(
                f"""
                SELECT doc_id, space_nm, sql_id, source_sql, generated_sql,
                       correct_sql, edited_yn, upd_ts, embedding_json
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
                           correct_sql, edited_yn, upd_ts, embedding_json
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
                    embedding=self._parse_embedding_json(row[8]),
                )
            )
        return result

    def _rank_by_cosine(self, query_embedding: list[float], candidates: list[_VectorItem]) -> list[tuple[_VectorItem, float]]:
        """코사인 유사도로 후보를 정렬한다."""
        scored: list[tuple[_VectorItem, float]] = []
        for item in candidates:
            score = self._cosine_similarity(query_embedding, item.embedding)
            scored.append((item, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored

    def _embed_texts(self, texts: list[str]) -> list[list[float]]:
        """외부 임베딩 API를 호출해 벡터를 생성한다."""
        base_url = self._require_env("RAG_EMBED_BASE_URL")
        model = os.getenv("RAG_EMBED_MODEL", "BAAI/bge-m3")
        api_key = os.getenv("RAG_EMBED_API_KEY", "").strip()

        payload = {
            "model": model,
            "input": texts,
        }
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        response = requests.post(
            base_url,
            headers=headers,
            json=payload,
            timeout=self.embed_timeout_sec,
        )
        response.raise_for_status()
        body = response.json()
        vectors = self._parse_embeddings_from_response(body)
        if len(vectors) != len(texts):
            raise ValueError(
                f"Embedding result size mismatch. expected={len(texts)} actual={len(vectors)}"
            )
        return vectors

    def _parse_embeddings_from_response(self, body: Any) -> list[list[float]]:
        """OpenAI 호환/일반 embeddings 응답을 모두 파싱한다."""
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
        """DB 문자열 embedding_json을 float 리스트로 복원한다."""
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, list):
                return [float(v) for v in parsed]
        except Exception:
            pass
        return []

    def _cosine_similarity(self, a: list[float], b: list[float]) -> float:
        """코사인 유사도를 계산한다."""
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
        """문서 변경 감지를 위한 해시."""
        return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

    def _require_env(self, name: str) -> str:
        """필수 환경변수를 읽고 누락 시 예외를 발생시킨다."""
        value = os.getenv(name, "").strip()
        if not value:
            raise ValueError(f"Required environment variable '{name}' is not set.")
        return value


feedback_rag_service = FeedbackRagService()

