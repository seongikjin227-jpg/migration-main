"""Vector index service for TOBE rule catalog retrieval."""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
from typing import Any

import requests
from dotenv import load_dotenv

from app.features.sql_tuning.rule_catalog import list_rule_catalog


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
load_dotenv(ROOT_DIR / ".env")


@dataclass
class RuleVectorItem:
    rule_id: str
    guidance: list[str]
    example_bad_sql: str
    example_tuned_sql: str
    doc_text: str
    text_hash: str
    embedding: list[float]


class TobeRuleVectorService:
    def __init__(self) -> None:
        self.db_path = os.getenv("TOBE_RULE_VECTOR_DB_PATH", str(ROOT_DIR / "data" / "rag" / "rule_catalog.db"))
        self.table_name = os.getenv("TOBE_RULE_VECTOR_TABLE", "tobe_rule_vector_index")
        self.top_k = int(os.getenv("TOBE_RULE_VECTOR_TOP_K", "3"))
        self.embed_timeout_sec = int(os.getenv("RAG_EMBED_TIMEOUT_SEC", "30"))
        self.embedding_dim = int(os.getenv("TOBE_RULE_VECTOR_FALLBACK_DIM", "256"))
        self._ensure_schema()

    def sync_index(self) -> dict[str, int]:
        rules = list_rule_catalog()
        existing = self._load_existing_hashes()
        active_ids = {rule.rule_id for rule in rules if rule.rule_id}
        upserted = 0
        skipped_unchanged = 0

        for rule in rules:
            if not rule.rule_id:
                continue
            doc_text = self._build_doc_text(
                rule_id=rule.rule_id,
                guidance=rule.guidance,
                example_bad_sql=rule.example_bad_sql,
                example_tuned_sql=rule.example_tuned_sql,
            )
            text_hash = self._sha256(doc_text)
            if existing.get(rule.rule_id) == text_hash:
                skipped_unchanged += 1
                continue
            embedding = self._embed_texts([doc_text])[0]
            self._upsert_vector(
                RuleVectorItem(
                    rule_id=rule.rule_id,
                    guidance=list(rule.guidance),
                    example_bad_sql=rule.example_bad_sql,
                    example_tuned_sql=rule.example_tuned_sql,
                    doc_text=doc_text,
                    text_hash=text_hash,
                    embedding=embedding,
                )
            )
            upserted += 1

        deleted = self._delete_stale_rules(active_ids)
        return {
            "source_rules": len(rules),
            "upserted": upserted,
            "skipped_unchanged": skipped_unchanged,
            "deleted": deleted,
        }

    def retrieve_similar_rules(self, query_text: str, top_k: int | None = None) -> list[tuple[RuleVectorItem, float]]:
        self.sync_index()
        candidates = self._load_candidates()
        if not candidates:
            return []
        query_embedding = self._embed_texts([query_text])[0]
        ranked = sorted(
            ((item, self._cosine_similarity(query_embedding, item.embedding)) for item in candidates),
            key=lambda pair: pair[1],
            reverse=True,
        )
        limit = top_k if (top_k and top_k > 0) else self.top_k
        return [(item, score) for item, score in ranked[:limit] if score > -1.0]

    def _ensure_schema(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    rule_id TEXT PRIMARY KEY,
                    guidance_json TEXT NOT NULL,
                    example_bad_sql TEXT NOT NULL,
                    example_tuned_sql TEXT NOT NULL,
                    doc_text TEXT NOT NULL,
                    text_hash TEXT NOT NULL,
                    embedding_json TEXT NOT NULL,
                    upd_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()

    def _load_existing_hashes(self) -> dict[str, str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(f"SELECT rule_id, text_hash FROM {self.table_name}").fetchall()
        return {str(rule_id): str(text_hash) for rule_id, text_hash in rows}

    def _delete_stale_rules(self, active_ids: set[str]) -> int:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(f"SELECT rule_id FROM {self.table_name}").fetchall()
            stale_ids = [str(row[0]) for row in rows if str(row[0]) not in active_ids]
            if stale_ids:
                conn.executemany(
                    f"DELETE FROM {self.table_name} WHERE rule_id = ?",
                    [(rule_id,) for rule_id in stale_ids],
                )
                conn.commit()
            return len(stale_ids)

    def _upsert_vector(self, item: RuleVectorItem) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    rule_id, guidance_json, example_bad_sql, example_tuned_sql,
                    doc_text, text_hash, embedding_json, upd_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(rule_id) DO UPDATE SET
                    guidance_json=excluded.guidance_json,
                    example_bad_sql=excluded.example_bad_sql,
                    example_tuned_sql=excluded.example_tuned_sql,
                    doc_text=excluded.doc_text,
                    text_hash=excluded.text_hash,
                    embedding_json=excluded.embedding_json,
                    upd_ts=CURRENT_TIMESTAMP
                """,
                (
                    item.rule_id,
                    json.dumps(item.guidance, ensure_ascii=False),
                    item.example_bad_sql,
                    item.example_tuned_sql,
                    item.doc_text,
                    item.text_hash,
                    json.dumps(item.embedding, ensure_ascii=False),
                ),
            )
            conn.commit()

    def _load_candidates(self) -> list[RuleVectorItem]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT rule_id, guidance_json, example_bad_sql, example_tuned_sql,
                       doc_text, text_hash, embedding_json
                FROM {self.table_name}
                ORDER BY rule_id
                """
            ).fetchall()
        result: list[RuleVectorItem] = []
        for row in rows:
            result.append(
                RuleVectorItem(
                    rule_id=str(row[0]),
                    guidance=self._parse_json_list(row[1]),
                    example_bad_sql=str(row[2]),
                    example_tuned_sql=str(row[3]),
                    doc_text=str(row[4]),
                    text_hash=str(row[5]),
                    embedding=self._parse_embedding_json(row[6]),
                )
            )
        return result

    def _build_doc_text(
        self,
        rule_id: str,
        guidance: list[str],
        example_bad_sql: str,
        example_tuned_sql: str,
    ) -> str:
        payload = {
            "rule_id": rule_id,
            "guidance": guidance,
            "example_bad_sql": (example_bad_sql or "").strip(),
            "example_tuned_sql": (example_tuned_sql or "").strip(),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def _embed_texts(self, texts: list[str]) -> list[list[float]]:
        endpoint = os.getenv("RAG_EMBED_BASE_URL", "").strip()
        if not endpoint:
            return [self._fallback_embed_text(text) for text in texts]

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

    def _fallback_embed_text(self, text: str) -> list[float]:
        vector = [0.0] * self.embedding_dim
        for token in self._tokenize(text):
            digest = hashlib.sha256(token.encode("utf-8", errors="ignore")).digest()
            index = int.from_bytes(digest[:4], "big") % self.embedding_dim
            sign = 1.0 if digest[4] % 2 == 0 else -1.0
            vector[index] += sign
        norm = math.sqrt(sum(value * value for value in vector))
        if norm <= 0.0:
            return vector
        return [value / norm for value in vector]

    def _tokenize(self, text: str) -> list[str]:
        return [token for token in re.findall(r"[A-Z_][A-Z0-9_]*", (text or "").upper()) if token]

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


tobe_rule_vector_service = TobeRuleVectorService()
