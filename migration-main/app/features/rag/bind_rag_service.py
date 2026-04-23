"""Dedicated BIND-stage RAG service backed by BIND_CORRECT_SQL examples."""

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


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
load_dotenv(ROOT_DIR / ".env")


@dataclass
class _BindVectorItem:
    doc_id: str
    space_nm: str
    sql_id: str
    source_sql: str
    tobe_sql: str
    correct_sql: str
    edited_yn: str
    upd_ts: str
    bind_params: list[str]
    pattern_tags: list[str]
    embedding: list[float]


class BindRagService:
    def __init__(self) -> None:
        self.db_path = os.getenv("BIND_RAG_DB_PATH", str(ROOT_DIR / "data" / "rag" / "rag.db"))
        self.table_name = os.getenv("BIND_RAG_TABLE", "bind_rag_index")
        self.top_k = int(os.getenv("BIND_RAG_TOP_K", "3"))
        self.corpus_limit = int(os.getenv("BIND_RAG_CORPUS_LIMIT", "2000"))
        self.embed_timeout_sec = int(os.getenv("RAG_EMBED_TIMEOUT_SEC", "30"))
        self._ensure_schema()

    def sync_index(self, limit: int | None = None) -> dict[str, int]:
        target_limit = limit if (limit and limit > 0) else self.corpus_limit
        source_rows = get_feedback_corpus_rows(correct_kind="BIND", limit=target_limit)
        existing_doc_ids = self._load_existing_doc_ids()
        upserted = 0
        skipped_unchanged = 0
        skipped_no_correct_sql = 0

        for row in source_rows:
            source_sql = (row.get("edit_fr_sql") or "").strip() or (row.get("fr_sql_text") or "")
            tobe_sql = row.get("to_sql_text") or ""
            correct_sql = row.get("correct_sql") or ""
            if not correct_sql.strip():
                skipped_no_correct_sql += 1
                continue

            bind_params = self._extract_bind_params(tobe_sql, correct_sql, source_sql)
            pattern_tags = self._extract_pattern_tags(source_sql, tobe_sql, correct_sql)
            doc_text = self._build_doc_text(
                space_nm=row.get("space_nm", ""),
                sql_id=row.get("sql_id", ""),
                source_sql=source_sql,
                tobe_sql=tobe_sql,
                correct_sql=correct_sql,
                bind_params=bind_params,
                pattern_tags=pattern_tags,
            )
            text_hash = self._sha256(doc_text)
            doc_id = f"BIND::{row.get('space_nm', '')}.{row.get('sql_id', '')}::{row.get('row_id', '')}::{text_hash[:16]}"
            if doc_id in existing_doc_ids:
                skipped_unchanged += 1
                continue

            embedding = self._embed_texts([doc_text])[0]
            self._upsert_vector(
                item=_BindVectorItem(
                    doc_id=doc_id,
                    space_nm=row.get("space_nm", ""),
                    sql_id=row.get("sql_id", ""),
                    source_sql=source_sql,
                    tobe_sql=tobe_sql,
                    correct_sql=correct_sql,
                    edited_yn=row.get("edited_yn", ""),
                    upd_ts=row.get("upd_ts", ""),
                    bind_params=bind_params,
                    pattern_tags=pattern_tags,
                    embedding=embedding,
                )
            )
            existing_doc_ids.add(doc_id)
            upserted += 1

        return {
            "source_rows": len(source_rows),
            "upserted": upserted,
            "skipped_unchanged": skipped_unchanged,
            "skipped_no_correct_sql": skipped_no_correct_sql,
            "deleted": 0,
        }

    def retrieve_bind_examples(
        self,
        job: SqlInfoJob,
        tobe_sql: str,
        last_error: str | None = None,
        current_stage: str | None = None,
    ) -> list[dict[str, str]]:
        candidates = self._load_candidates()
        if not candidates:
            return []

        query_bind_params = self._extract_bind_params(tobe_sql, "", job.source_sql)
        query_tags = self._extract_pattern_tags(job.source_sql, tobe_sql, last_error or "", current_stage or "")
        query_text = self._build_doc_text(
            space_nm=job.space_nm,
            sql_id=job.sql_id,
            source_sql=job.source_sql,
            tobe_sql=tobe_sql,
            correct_sql="",
            bind_params=query_bind_params,
            pattern_tags=query_tags,
        )
        query_embedding = self._embed_texts([query_text])[0]
        ranked = self._rank_candidates(query_embedding, query_tags, query_bind_params, candidates)
        examples: list[dict[str, str]] = []
        for item, score in ranked[: self.top_k]:
            shared_params = sorted(set(query_bind_params).intersection(item.bind_params))
            examples.append(
                {
                    "correct_kind": "BIND",
                    "edited_yn": item.edited_yn,
                    "correct_sql": item.correct_sql,
                    "bind_params_csv": ",".join(item.bind_params),
                    "shared_bind_params_csv": ",".join(shared_params),
                    "pattern_tags_csv": ",".join(item.pattern_tags),
                    "similarity_score": f"{score:.6f}",
                    "match_reason": self._build_match_reason(shared_params, query_tags, item.pattern_tags),
                }
            )
        return examples

    def _ensure_schema(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    doc_id TEXT PRIMARY KEY,
                    space_nm TEXT NOT NULL,
                    sql_id TEXT NOT NULL,
                    source_sql TEXT NOT NULL,
                    tobe_sql TEXT NOT NULL,
                    correct_sql TEXT NOT NULL,
                    edited_yn TEXT NOT NULL,
                    upd_ts TEXT NOT NULL,
                    bind_params_json TEXT NOT NULL DEFAULT '[]',
                    pattern_tags_json TEXT NOT NULL DEFAULT '[]',
                    embedding_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_ns_id ON {self.table_name}(space_nm, sql_id)"
            )
            conn.commit()

    def _load_existing_doc_ids(self) -> set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(f"SELECT doc_id FROM {self.table_name}").fetchall()
        return {str(row[0]) for row in rows}

    def _upsert_vector(self, item: _BindVectorItem) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    doc_id, space_nm, sql_id, source_sql, tobe_sql, correct_sql,
                    edited_yn, upd_ts, bind_params_json, pattern_tags_json, embedding_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET
                    space_nm=excluded.space_nm,
                    sql_id=excluded.sql_id,
                    source_sql=excluded.source_sql,
                    tobe_sql=excluded.tobe_sql,
                    correct_sql=excluded.correct_sql,
                    edited_yn=excluded.edited_yn,
                    upd_ts=excluded.upd_ts,
                    bind_params_json=excluded.bind_params_json,
                    pattern_tags_json=excluded.pattern_tags_json,
                    embedding_json=excluded.embedding_json
                """,
                (
                    item.doc_id,
                    item.space_nm,
                    item.sql_id,
                    item.source_sql,
                    item.tobe_sql,
                    item.correct_sql,
                    item.edited_yn,
                    item.upd_ts,
                    json.dumps(item.bind_params, ensure_ascii=False),
                    json.dumps(item.pattern_tags, ensure_ascii=False),
                    json.dumps(item.embedding, ensure_ascii=False),
                ),
            )
            conn.commit()

    def _load_candidates(self) -> list[_BindVectorItem]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT doc_id, space_nm, sql_id, source_sql, tobe_sql, correct_sql,
                       edited_yn, upd_ts, bind_params_json, pattern_tags_json, embedding_json
                FROM {self.table_name}
                ORDER BY upd_ts DESC
                """
            ).fetchall()
        result: list[_BindVectorItem] = []
        for row in rows:
            result.append(
                _BindVectorItem(
                    doc_id=str(row[0]),
                    space_nm=str(row[1]),
                    sql_id=str(row[2]),
                    source_sql=str(row[3]),
                    tobe_sql=str(row[4]),
                    correct_sql=str(row[5]),
                    edited_yn=str(row[6]),
                    upd_ts=str(row[7]),
                    bind_params=self._parse_json_list(row[8]),
                    pattern_tags=self._parse_json_list(row[9]),
                    embedding=self._parse_embedding_json(row[10]),
                )
            )
        return result

    def _build_doc_text(
        self,
        space_nm: str,
        sql_id: str,
        source_sql: str,
        tobe_sql: str,
        correct_sql: str,
        bind_params: list[str],
        pattern_tags: list[str],
    ) -> str:
        payload = {
            "space_nm": (space_nm or "").strip(),
            "sql_id": (sql_id or "").strip(),
            "source_sql": (source_sql or "").strip(),
            "tobe_sql": (tobe_sql or "").strip(),
            "correct_sql": (correct_sql or "").strip(),
            "bind_params": bind_params,
            "pattern_tags": pattern_tags,
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def _rank_candidates(
        self,
        query_embedding: list[float],
        query_tags: list[str],
        query_bind_params: list[str],
        candidates: list[_BindVectorItem],
    ) -> list[tuple[_BindVectorItem, float]]:
        query_tag_set = {tag.upper() for tag in query_tags}
        query_param_set = {param.upper() for param in query_bind_params}
        scored: list[tuple[_BindVectorItem, float]] = []
        for item in candidates:
            cosine = self._cosine_similarity(query_embedding, item.embedding)
            overlap_tags = len(query_tag_set.intersection({tag.upper() for tag in item.pattern_tags}))
            overlap_params = len(query_param_set.intersection({param.upper() for param in item.bind_params}))
            bonus = min(0.25, overlap_tags * 0.04 + overlap_params * 0.07)
            scored.append((item, cosine + bonus))
        scored.sort(key=lambda pair: pair[1], reverse=True)
        return scored

    def _extract_pattern_tags(self, *texts: str) -> list[str]:
        merged = " ".join((text or "") for text in texts)
        upper = merged.upper()
        tags: set[str] = set()
        if "ROWNUM" in upper or "FETCH FIRST" in upper or "ROW_NUMBER()" in upper:
            tags.add("PAGING")
        if upper.count(" JOIN ") >= 2:
            tags.add("MULTI_JOIN")
        if re.search(r"\(\s*SELECT", upper):
            tags.add("SUBQUERY")
        if "GROUP BY" in upper:
            tags.add("GROUP_BY")
        if "<IF" in upper or "<WHERE" in upper or "<CHOOSE" in upper:
            tags.add("MYBATIS_DYNAMIC_TAG")
        return sorted(tags)

    def _extract_bind_params(self, *texts: str) -> list[str]:
        params: set[str] = set()
        for text in texts:
            for match in re.finditer(r"[#$]\{\s*([^}]+?)\s*\}", text or ""):
                params.add(match.group(1).split(".")[-1].strip().upper())
            for match in re.finditer(r":([A-Za-z_][A-Za-z0-9_.]*)", text or ""):
                params.add(match.group(1).split(".")[-1].strip().upper())
        return sorted(param for param in params if param)

    def _build_match_reason(self, shared_params: list[str], query_tags: list[str], item_tags: list[str]) -> str:
        reasons: list[str] = []
        if shared_params:
            reasons.append("shared bind params: " + ", ".join(shared_params))
        shared_tags = sorted({tag.upper() for tag in query_tags}.intersection({tag.upper() for tag in item_tags}))
        if shared_tags:
            reasons.append("shared shape: " + ", ".join(shared_tags[:4]))
        return "; ".join(reasons) or "similar bind-discovery pattern"

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
            raise ValueError(f"Embedding result size mismatch. expected={len(texts)} actual={len(vectors)}")
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

    def _parse_json_list(self, raw: Any) -> list[str]:
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item).strip()]
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


bind_rag_service = BindRagService()
