"""LLM 호출과 프롬프트 조립, 응답 SQL 정규화를 담당하는 서비스."""

import json
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_anthropic import ChatAnthropic
# from langchain_openai import ChatOpenAI  # OpenAI fallback path (kept commented)

from app.common import LLMRateLimitError
from app.common import MappingRuleItem, SqlInfoJob
from app.services.binding_service import build_bind_target_hints
from app.services.prompt_service import render_prompt


ROOT_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT_DIR / ".env")
_BIND_TOKEN_PATTERN = re.compile(r"[#$]\{\s*([^}]+?)\s*\}")


def _env_or_value(value: str | None, env_name: str) -> str:
    """함수 인자로 받은 값이 우선이며, 없으면 환경변수를 사용한다."""
    resolved = value or os.getenv(env_name)
    if not resolved:
        raise ValueError(f"Required environment variable '{env_name}' is not set.")
    return resolved


def _normalize_anthropic_base_url(raw_base_url: str) -> str:
    """Anthropic endpoint는 API root를 사용하도록 보정한다."""
    normalized = raw_base_url.strip().rstrip("/")
    if normalized.endswith("/v1/messages"):
        return normalized[: -len("/v1/messages")]
    if normalized.endswith("/v1"):
        return normalized[: -len("/v1")]
    return normalized


def _serialize_mapping_rules(mapping_rules: list[MappingRuleItem]) -> str:
    """프롬프트 주입용으로 매핑 룰을 사람이 읽기 쉬운 구조 텍스트로 직렬화한다."""
    if not mapping_rules:
        return "[TABLE_MAPPING]\n- (empty)\n\n[COLUMN_MAPPING_BY_TABLE]\n- (empty)"

    table_pairs: set[tuple[str, str]] = set()
    column_pairs_by_table: dict[tuple[str, str], set[tuple[str, str]]] = {}

    for rule in mapping_rules:
        fr_table = (rule.fr_table or "").strip()
        to_table = (rule.to_table or "").strip()
        fr_col = (rule.fr_col or "").strip()
        to_col = (rule.to_col or "").strip()
        if not fr_table or not to_table:
            continue

        table_key = (fr_table, to_table)
        table_pairs.add(table_key)
        if fr_col and to_col:
            if table_key not in column_pairs_by_table:
                column_pairs_by_table[table_key] = set()
            column_pairs_by_table[table_key].add((fr_col, to_col))

    lines: list[str] = []
    lines.append("[TABLE_MAPPING]")
    for fr_table, to_table in sorted(table_pairs):
        lines.append(f"- {fr_table} -> {to_table}")

    lines.append("")
    lines.append("[COLUMN_MAPPING_BY_TABLE]")
    for fr_table, to_table in sorted(table_pairs):
        lines.append(f"- {fr_table} -> {to_table}")
        for fr_col, to_col in sorted(column_pairs_by_table.get((fr_table, to_table), set())):
            lines.append(f"  - {fr_table}.{fr_col} -> {to_table}.{to_col}")

    return "\n".join(lines)


def _normalize_table_token(token: str) -> str:
    """테이블 토큰을 비교 가능한 형태(대문자, 스키마 제거)로 정규화한다."""
    value = (token or "").strip().strip('"').strip("'")
    if not value:
        return ""
    if "." in value:
        value = value.split(".")[-1]
    return value.upper()


def _load_target_tables(job: SqlInfoJob) -> set[str]:
    """job.target_table(JSON/CSV/공백구분)를 테이블 집합으로 복원한다."""
    raw = (job.target_table or "").strip()
    if not raw:
        return set()

    tokens: list[str] = []
    if raw.startswith("[") or raw.startswith("{"):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                tokens = [str(item) for item in parsed]
            elif isinstance(parsed, str):
                tokens = [parsed]
        except Exception:
            tokens = []
    if not tokens:
        tokens = re.split(r"[,\s;|]+", raw)

    result: set[str] = set()
    for token in tokens:
        normalized = _normalize_table_token(token)
        if normalized:
            result.add(normalized)
    return result


def _extract_referenced_fr_tables_from_source_sql(
    source_sql: str,
    candidate_fr_tables: set[str],
) -> set[str]:
    """source_sql에 실제 등장하는 FR_TABLE 후보를 단어 경계 기준으로 추출한다."""
    if not source_sql or not candidate_fr_tables:
        return set()

    text = source_sql
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    text = re.sub(r"--[^\n]*", " ", text)
    text = re.sub(r"'(?:''|[^'])*'", " ", text)
    scan = text.upper()

    matched: set[str] = set()
    for table in candidate_fr_tables:
        pattern = rf"(?<![A-Z0-9_$#]){re.escape(table)}(?![A-Z0-9_$#])"
        if re.search(pattern, scan):
            matched.add(table)
    return matched


def _select_mapping_rules_for_job(
    job: SqlInfoJob,
    mapping_rules: list[MappingRuleItem],
) -> list[MappingRuleItem]:
    """현재 SQL에 필요한 매핑룰만 선별한다.

    우선순위:
    1) TARGET_TABLE 기반 FR_TABLE 매칭
    2) TARGET_TABLE이 비어있으면 source_sql 등장 FR_TABLE 매칭
    """
    if not mapping_rules:
        return []

    rules_by_fr: dict[str, list[MappingRuleItem]] = {}
    for rule in mapping_rules:
        fr_norm = _normalize_table_token(rule.fr_table)
        if not fr_norm:
            continue
        rules_by_fr.setdefault(fr_norm, []).append(rule)

    target_tables = _load_target_tables(job)
    selected_fr_tables = {tbl for tbl in target_tables if tbl in rules_by_fr}

    if not selected_fr_tables:
        selected_fr_tables = _extract_referenced_fr_tables_from_source_sql(
            source_sql=job.source_sql,
            candidate_fr_tables=set(rules_by_fr.keys()),
        )

    if not selected_fr_tables:
        return mapping_rules

    filtered: list[MappingRuleItem] = []
    for fr_table in sorted(selected_fr_tables):
        filtered.extend(rules_by_fr.get(fr_table, []))
    return filtered


def _serialize_feedback_examples(feedback_examples: list[dict[str, str]]) -> str:
    """프롬프트 주입용으로 피드백 예시를 JSON 문자열로 직렬화한다."""
    if not feedback_examples:
        return "[]"
    return json.dumps(feedback_examples, ensure_ascii=False)


def build_tobe_sql_messages(
    job: SqlInfoJob,
    mapping_rules: list[MappingRuleItem],
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    """TO-BE SQL 생성용 시스템 프롬프트를 만든다."""
    scoped_rules = _select_mapping_rules_for_job(job=job, mapping_rules=mapping_rules)
    merged_prompt = render_prompt(
        "tobe_sql_prompt.txt",
        from_sql=job.source_sql,
        mapping_schema_text=_serialize_mapping_rules(scoped_rules),
        feedback_examples_json=_serialize_feedback_examples(feedback_examples or []),
        last_error=last_error or "None",
    )
    return [
        {"role": "system", "content": merged_prompt},
        {"role": "user", "content": "Generate one executable Oracle SQL statement only."},
    ]


def build_bind_sql_messages(
    job: SqlInfoJob,
    tobe_sql: str,
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    """bind 값 조회 SQL 생성용 시스템 프롬프트를 만든다."""
    bind_target_hints = build_bind_target_hints(tobe_sql=tobe_sql, source_sql=job.source_sql)
    merged_prompt = render_prompt(
        "bind_sql_prompt.txt",
        from_sql=job.source_sql,
        tobe_sql=tobe_sql,
        bind_target_hints_json=json.dumps(bind_target_hints, ensure_ascii=False),
        feedback_examples_json=_serialize_feedback_examples(feedback_examples or []),
        last_error=last_error or "None",
    )
    return [
        {"role": "system", "content": merged_prompt},
        {"role": "user", "content": "Generate one executable Oracle SQL statement only."},
    ]


def build_test_sql_messages(
    job: SqlInfoJob,
    tobe_sql: str,
    bind_set_json: str,
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    """bind-aware 테스트 SQL 생성용 시스템 프롬프트를 만든다."""
    merged_prompt = render_prompt(
        "test_sql_prompt.txt",
        from_sql=job.source_sql,
        tobe_sql=tobe_sql,
        bind_set_json=bind_set_json,
        feedback_examples_json=_serialize_feedback_examples(feedback_examples or []),
        last_error=last_error or "None",
    )
    return [
        {"role": "system", "content": merged_prompt},
        {"role": "user", "content": "Generate one executable Oracle SQL statement only."},
    ]


def build_test_sql_no_bind_messages(
    job: SqlInfoJob,
    tobe_sql: str,
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    """bind 파라미터가 없을 때 사용할 테스트 SQL 프롬프트를 만든다."""
    merged_prompt = render_prompt(
        "test_sql_no_bind_prompt.txt",
        from_sql=job.source_sql,
        tobe_sql=tobe_sql,
        feedback_examples_json=_serialize_feedback_examples(feedback_examples or []),
        last_error=last_error or "None",
    )
    return [
        {"role": "system", "content": merged_prompt},
        {"role": "user", "content": "Generate one executable Oracle SQL statement only."},
    ]


def _extract_sql_text(response_text: str) -> str:
    """LLM 원문에서 실행 가능한 단일 SQL 본문만 추출한다."""
    text = response_text.strip()
    code_block_match = re.search(r"```(?:sql)?\s*(.*?)```", text, re.IGNORECASE | re.DOTALL)
    if code_block_match:
        text = code_block_match.group(1).strip()
    if not text:
        raise ValueError("LLM returned an empty response.")
    first_sql_keyword = re.search(
        r"\b(SELECT|INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|WITH)\b",
        text,
        re.IGNORECASE,
    )
    if first_sql_keyword and first_sql_keyword.start() > 0:
        text = text[first_sql_keyword.start():].strip()
    if not re.match(r"^(SELECT|INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|WITH)\b", text, re.IGNORECASE):
        raise ValueError("LLM response does not start with executable SQL.")
    return _normalize_oracle_sql(text)


def _normalize_bind_name(token: str) -> str:
    cleaned = (token or "").strip()
    if not cleaned:
        return ""
    return cleaned.split(".")[-1].strip()


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return f"TO_DATE('{value.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')"
    if isinstance(value, date):
        return f"TO_DATE('{value.isoformat()}', 'YYYY-MM-DD')"

    text = str(value)
    # JSON 직렬화된 datetime/date 문자열 우선 처리
    iso_match = re.match(r"^(\d{4}-\d{2}-\d{2})(?:[T\s].*)?$", text)
    if iso_match:
        return f"TO_DATE('{iso_match.group(1)}', 'YYYY-MM-DD')"
    escaped = text.replace("'", "''")
    return f"'{escaped}'"


def _render_sql_with_bind_values(sql_text: str, bind_case: dict[str, object]) -> str:
    def _replace(match: re.Match[str]) -> str:
        param_name = _normalize_bind_name(match.group(1))
        return _sql_literal(bind_case.get(param_name))

    return _BIND_TOKEN_PATTERN.sub(_replace, sql_text or "")


def _build_deterministic_test_sql(
    from_sql: str,
    tobe_sql: str,
    bind_sets: list[dict[str, object]],
) -> str:
    if not bind_sets:
        bind_sets = [{}]

    selects: list[str] = []
    for idx, bind_case in enumerate(bind_sets, start=1):
        rendered_from = _render_sql_with_bind_values(from_sql, bind_case).strip()
        rendered_to = _render_sql_with_bind_values(tobe_sql, bind_case).strip()
        selects.append(
            "SELECT "
            f"{idx} AS CASE_NO, "
            f"(SELECT COUNT(*) FROM ({rendered_from}) f) AS FROM_COUNT, "
            f"(SELECT COUNT(*) FROM ({rendered_to}) t) AS TO_COUNT "
            "FROM DUAL"
        )
    return " UNION ALL ".join(selects)


def _strip_sqlplus_terminator_lines(lines: Iterable[str]) -> list[str]:
    """SQL*Plus 구분자(`/`) 라인을 제거한다."""
    cleaned = []
    for line in lines:
        if line.strip() == "/":
            continue
        cleaned.append(line)
    return cleaned


def _replace_limit_with_fetch_first(text: str) -> str:
    # LIMIT 절을 Oracle 친화적인 FETCH FIRST 절로 치환한다.
    return re.sub(
        r"\s+LIMIT\s+(\d+)\s*$",
        r" FETCH FIRST \1 ROWS ONLY",
        text,
        flags=re.IGNORECASE,
    )


def _normalize_oracle_sql(sql_text: str) -> str:
    """공백/종결자 정리 후 단일 문장 SQL 규칙을 강제한다."""
    text = sql_text.replace("\ufeff", "").replace("\u200b", "").replace("\u00a0", " ")
    text = "\n".join(_strip_sqlplus_terminator_lines(text.splitlines())).strip()
    text = _replace_limit_with_fetch_first(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s+\n", "\n", text)
    text = text.strip().rstrip(";").strip()

    # 세미콜론 다중문을 차단해 ORA-00933/00911류 오류를 사전에 방지한다.
    if _has_unquoted_semicolon(text):
        raise ValueError("LLM response must contain exactly one SQL statement.")
    if not text:
        raise ValueError("LLM returned an empty SQL statement after normalization.")
    return text


def _has_unquoted_semicolon(sql_text: str) -> bool:
    """문자열 리터럴 바깥의 세미콜론 존재 여부를 검사한다."""
    in_single_quote = False
    idx = 0
    length = len(sql_text)
    while idx < length:
        ch = sql_text[idx]
        if in_single_quote:
            if ch == "'":
                # Oracle 문자열 이스케이프: ''
                if idx + 1 < length and sql_text[idx + 1] == "'":
                    idx += 2
                    continue
                in_single_quote = False
            idx += 1
            continue
        if ch == "'":
            in_single_quote = True
            idx += 1
            continue
        if ch == ";":
            return True
        idx += 1
    return False


def _to_langchain_messages(messages: list[dict[str, str]]):
    """내부 메시지 포맷(dict)을 LangChain 메시지 객체로 변환한다."""
    converted = []
    for message in messages:
        if message.get("role") == "system":
            converted.append(SystemMessage(content=message.get("content", "")))
        else:
            converted.append(HumanMessage(content=message.get("content", "")))
    return converted


def _ensure_anthropic_message_requirements(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    """Anthropic 요구사항에 맞게 최소 1개의 user 메시지를 보장한다."""
    safe = list(messages or [])
    has_user_or_assistant = any((m.get("role") or "").lower() in {"user", "assistant"} for m in safe)
    if not has_user_or_assistant:
        safe.append({"role": "user", "content": "Generate one executable Oracle SQL statement only."})
    return safe


def call_llm_api(api_key: str | None, model: str | None, base_url: str | None, messages: list[dict[str, str]]) -> str:
    """LLM을 호출하고 일시적 장애를 재시도 가능 예외로 변환한다."""
    resolved_api_key = _env_or_value(api_key, "LLM_API_KEY")
    resolved_model = _env_or_value(model, "LLM_MODEL")
    resolved_base_url = _normalize_anthropic_base_url(_env_or_value(base_url, "LLM_BASE_URL"))
    try:
        # OpenAI fallback path (intentionally kept as comment):
        # llm = ChatOpenAI(
        #     api_key=resolved_api_key,
        #     model=resolved_model,
        #     base_url=resolved_base_url,
        #     temperature=0,
        # )
        llm = ChatAnthropic(
            anthropic_api_key=resolved_api_key,
            model=resolved_model,
            anthropic_api_url=resolved_base_url,
            temperature=0,
        )
        safe_messages = _ensure_anthropic_message_requirements(messages)
        response = llm.invoke(_to_langchain_messages(safe_messages))
        content = getattr(response, "content", response)
        if isinstance(content, list):
            text = "".join(item.get("text", "") if isinstance(item, dict) else str(item) for item in content)
        else:
            text = str(content)
        return _extract_sql_text(text)
    except Exception as exc:
        msg = str(exc)
        lowered = msg.lower()
        if (
            "429" in msg
            or "rate limit" in lowered
            or "504" in msg
            or "gateway timeout" in lowered
            or "timed out" in lowered
        ):
            raise LLMRateLimitError(msg) from exc
        raise


def generate_tobe_sql(
    job: SqlInfoJob,
    mapping_rules: list[MappingRuleItem],
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> str:
    """오케스트레이터가 사용하는 TO-BE SQL 생성 진입점."""
    return call_llm_api(
        api_key=None,
        model=None,
        base_url=None,
        messages=build_tobe_sql_messages(
            job=job,
            mapping_rules=mapping_rules,
            last_error=last_error,
            feedback_examples=feedback_examples,
        ),
    )


def generate_bind_sql(
    job: SqlInfoJob,
    tobe_sql: str,
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> str:
    """오케스트레이터가 사용하는 bind SQL 생성 진입점."""
    return call_llm_api(
        api_key=None,
        model=None,
        base_url=None,
        messages=build_bind_sql_messages(
            job=job,
            tobe_sql=tobe_sql,
            last_error=last_error,
            feedback_examples=feedback_examples,
        ),
    )


def generate_test_sql(
    job: SqlInfoJob,
    tobe_sql: str,
    bind_set_json: str,
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> str:
    """bind-aware 시나리오용 테스트 SQL 생성 진입점."""
    try:
        bind_sets = json.loads(bind_set_json or "[]")
    except Exception:
        bind_sets = []
    if not isinstance(bind_sets, list):
        bind_sets = []
    return _build_deterministic_test_sql(job.source_sql, tobe_sql, bind_sets)


def generate_test_sql_no_bind(
    job: SqlInfoJob,
    tobe_sql: str,
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> str:
    """no-bind 시나리오용 테스트 SQL 생성 진입점."""
    return _build_deterministic_test_sql(job.source_sql, tobe_sql, [{}])
