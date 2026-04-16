from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import requests

from _bootstrap import ROOT_DIR  # noqa: F401
from app.db import (
    get_connection,
    get_mapping_rule_detail_table,
    get_mapping_rule_table,
    get_result_table,
)
from app.common import logger


@dataclass
class HealthResult:
    name: str
    ok: bool
    detail: str


def _join_url(base_url: str, suffix: str) -> str:
    return f"{base_url.rstrip('/')}/{suffix.lstrip('/')}"


def _normalize_anthropic_base_url(raw_base_url: str) -> str:
    normalized = raw_base_url.strip().rstrip("/")
    if normalized.endswith("/v1/message"):
        return normalized[: -len("/v1/message")]
    if normalized.endswith("/v1/messages"):
        return normalized[: -len("/v1/messages")]
    if normalized.endswith("/v1"):
        return normalized[: -len("/v1")]
    return normalized


def _normalize_openai_base_url(raw_base_url: str) -> str:
    normalized = raw_base_url.strip().rstrip("/")
    for suffix in ("/chat/completions", "/responses", "/completions", "/models"):
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    return normalized


def _extract_embedding_vectors(body: Any) -> list[list[float]]:
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
    provider = os.getenv("LLM_PROVIDER", "").strip().lower()
    base_url = os.getenv("LLM_BASE_URL", "").strip()
    api_key = os.getenv("LLM_API_KEY", "").strip()
    model = os.getenv("LLM_MODEL", "").strip()
    if not base_url:
        return HealthResult(name="LLM", ok=False, detail="LLM_BASE_URL is not set")
    if not api_key:
        return HealthResult(name="LLM", ok=False, detail="LLM_API_KEY is not set")

    if provider and provider not in {"anthropic", "openai"}:
        return HealthResult(name="LLM", ok=False, detail="LLM_PROVIDER must be either 'anthropic' or 'openai'")

    is_anthropic = provider == "anthropic"
    if not provider:
        is_anthropic = ("anthropic.com" in base_url.lower()) or model.lower().startswith("claude")
    if is_anthropic:
        normalized_base = _normalize_anthropic_base_url(base_url)
        endpoint = _join_url(normalized_base, "/v1/models")
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        }
    else:
        endpoint = _join_url(_normalize_openai_base_url(base_url), "models")
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
        return HealthResult(name="LLM", ok=True, detail=f"connected ({endpoint}){configured}")
    except Exception as exc:
        return HealthResult(name="LLM", ok=False, detail=f"{type(exc).__name__}: {exc}")


def check_embedding_connection(timeout_sec: int = 20) -> HealthResult:
    base_url = os.getenv("RAG_EMBED_BASE_URL", "").strip()
    api_key = os.getenv("RAG_EMBED_API_KEY", "").strip()
    model = os.getenv("RAG_EMBED_MODEL", "BAAI/bge-m3").strip()
    if not base_url:
        return HealthResult(name="EMBEDDING", ok=False, detail="RAG_EMBED_BASE_URL is not set")

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {"model": model, "input": ["health check text"]}
    try:
        response = requests.post(base_url, headers=headers, json=payload, timeout=timeout_sec)
        if response.status_code >= 400:
            return HealthResult(
                name="EMBEDDING",
                ok=False,
                detail=f"HTTP {response.status_code} from {base_url}: {response.text[:200]}",
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
            detail=f"connected ({base_url}), model={model}, vectors={len(vectors)}, dim={dim}",
        )
    except Exception as exc:
        return HealthResult(name="EMBEDDING", ok=False, detail=f"{type(exc).__name__}: {exc}")


def run_health_checks() -> list[HealthResult]:
    return [
        check_oracle_connection(),
        check_llm_connection(),
        check_embedding_connection(),
    ]


def main() -> None:
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
    main()
