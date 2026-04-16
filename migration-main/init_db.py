"""?듯빀 ?곌껐 ?먭? ?ㅽ겕由쏀듃.

?먭? ???
1) Oracle DB ?곌껐/?꾩닔 ?뚯씠釉??묎렐
2) LLM API ?곌껐 (models ?붾뱶?ъ씤??湲곗?)
3) ?꾨쿋??API ?곌껐 (?섑뵆 ?꾨쿋??1嫄??앹꽦)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import requests

from app.config import (
    get_connection,
    get_mapping_rule_detail_table,
    get_mapping_rule_table,
    get_result_table,
)
from app.logger import logger


@dataclass
class HealthResult:
    """?곌껐 ?먭? 寃곌낵 1嫄?"""

    name: str
    ok: bool
    detail: str


def _join_url(base_url: str, suffix: str) -> str:
    """base URL ?ㅼ뿉 suffix瑜??덉쟾?섍쾶 遺숈씤??"""
    return f"{base_url.rstrip('/')}/{suffix.lstrip('/')}"


def _normalize_anthropic_base_url(raw_base_url: str) -> str:
    """Normalize Anthropic base URL to API root."""
    normalized = raw_base_url.strip().rstrip("/")
    if normalized.endswith("/v1/messages"):
        return normalized[: -len("/v1/messages")]
    if normalized.endswith("/v1"):
        return normalized[: -len("/v1")]
    return normalized

def _extract_embedding_vectors(body: Any) -> list[list[float]]:
    """?꾨쿋??API ?묐떟?먯꽌 踰≫꽣 紐⑸줉???뚯떛?쒕떎.

    吏???뺤떇:
    - OpenAI ?명솚: {"data":[{"embedding":[...]}]}
    - ?쇰컲 ?뺤떇: {"embeddings":[[...],[...]]}
    - ?⑥씪 踰≫꽣: {"embedding":[...]}
    """
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

    return []


def check_oracle_connection() -> HealthResult:
    """Oracle ?곌껐怨??꾩닔 ?뚯씠釉??묎렐 ?щ?瑜??먭??쒕떎."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            mapping_table = get_mapping_rule_table()
            mapping_detail_table = get_mapping_rule_detail_table()
            result_table = get_result_table()

            cursor.execute(f"SELECT COUNT(*) FROM {mapping_table}")
            mapping_count = int(cursor.fetchone()[0] or 0)

            cursor.execute(f"SELECT COUNT(*) FROM {mapping_detail_table}")
            mapping_detail_count = int(cursor.fetchone()[0] or 0)

            cursor.execute(f"SELECT COUNT(*) FROM {result_table}")
            result_count = int(cursor.fetchone()[0] or 0)

        return HealthResult(
            name="ORACLE",
            ok=True,
            detail=(
                f"connected, {mapping_table}={mapping_count}, "
                f"{mapping_detail_table}={mapping_detail_count}, {result_table}={result_count}"
            ),
        )
    except Exception as exc:
        return HealthResult(name="ORACLE", ok=False, detail=str(exc))


def check_llm_connection(timeout_sec: int = 15) -> HealthResult:
    """LLM API ?묎렐 媛???щ?瑜??먭??쒕떎.

    湲곕낯 ?먭? 寃쎈줈:
    - {LLM_BASE_URL}/models
    """
    base_url = os.getenv("LLM_BASE_URL", "").strip()
    api_key = os.getenv("LLM_API_KEY", "").strip()
    model = os.getenv("LLM_MODEL", "").strip()
    if not base_url:
        return HealthResult(name="LLM", ok=False, detail="LLM_BASE_URL is not set")
    if not api_key:
        return HealthResult(name="LLM", ok=False, detail="LLM_API_KEY is not set")

    is_anthropic = ("anthropic.com" in base_url.lower()) or model.lower().startswith("claude")
    if is_anthropic:
        normalized_base = _normalize_anthropic_base_url(base_url)
        endpoint = _join_url(normalized_base, "/v1/models")
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        }
    else:
        endpoint = _join_url(base_url, "models")
        headers = {"Authorization": f"Bearer {api_key}"}

    try:
        response = requests.get(endpoint, headers=headers, timeout=timeout_sec)
        if response.status_code >= 400:
            return HealthResult(
                name="LLM",
                ok=False,
                detail=f"HTTP {response.status_code} from {endpoint}: {response.text[:200]}",
            )
        body = response.json()
        data = body.get("data") if isinstance(body, dict) else None
        if isinstance(data, list):
            sample = ""
            if data and isinstance(data[0], dict):
                sample = str(data[0].get("id") or "")
            configured = f", configured_model={model}" if model else ""
            return HealthResult(
                name="LLM",
                ok=True,
                detail=f"connected ({endpoint}), models={len(data)}, sample_model={sample}{configured}",
            )
        configured = f", configured_model={model}" if model else ""
        return HealthResult(
            name="LLM",
            ok=True,
            detail=f"connected ({endpoint}){configured}",
        )
    except Exception as exc:
        return HealthResult(name="LLM", ok=False, detail=f"{type(exc).__name__}: {exc}")


def check_embedding_connection(timeout_sec: int = 20) -> HealthResult:
    """?꾨쿋??API ?묎렐 諛??섑뵆 踰≫꽣 ?앹꽦 ?щ?瑜??먭??쒕떎."""
    base_url = os.getenv("RAG_EMBED_BASE_URL", "").strip()
    api_key = os.getenv("RAG_EMBED_API_KEY", "").strip()
    model = os.getenv("RAG_EMBED_MODEL", "BAAI/bge-m3").strip()
    if not base_url:
        return HealthResult(name="EMBEDDING", ok=False, detail="RAG_EMBED_BASE_URL is not set")

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    endpoint = base_url
    payload = {"model": model, "input": ["health check text"]}
    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=timeout_sec)
        if response.status_code >= 400:
            return HealthResult(
                name="EMBEDDING",
                ok=False,
                detail=f"HTTP {response.status_code} from {endpoint}: {response.text[:200]}",
            )
        body = response.json()
        vectors = _extract_embedding_vectors(body)
        if not vectors:
            return HealthResult(
                name="EMBEDDING",
                ok=False,
                detail=f"connected but unsupported response format: {json.dumps(body, ensure_ascii=False)[:250]}",
            )
        dim = len(vectors[0]) if vectors and vectors[0] else 0
        return HealthResult(
            name="EMBEDDING",
            ok=True,
            detail=f"connected ({endpoint}), model={model}, vectors={len(vectors)}, dim={dim}",
        )
    except Exception as exc:
        return HealthResult(name="EMBEDDING", ok=False, detail=f"{type(exc).__name__}: {exc}")


def run_health_checks() -> list[HealthResult]:
    """?꾩껜 ?곌껐 ?먭????섑뻾?쒕떎."""
    results = [
        check_oracle_connection(),
        check_llm_connection(),
        check_embedding_connection(),
    ]
    return results


def init_db():
    """湲곗〈 ?ㅽ겕由쏀듃 ?명솚??吏꾩엯??

    - Oracle留뚯씠 ?꾨땲??LLM/Embedding ?곌껐 ?곹깭???④퍡 異쒕젰?쒕떎.
    """
    results = run_health_checks()
    logger.info("========== Connection Health Check ==========")
    all_ok = True
    for result in results:
        if result.ok:
            logger.info(f"[OK]   {result.name:<10} {result.detail}")
        else:
            all_ok = False
            logger.error(f"[FAIL] {result.name:<10} {result.detail}")
    logger.info("============================================")

    if all_ok:
        logger.info("All connection checks passed.")
    else:
        logger.error("Some connection checks failed. Please inspect .env and endpoint accessibility.")


if __name__ == "__main__":
    init_db()


