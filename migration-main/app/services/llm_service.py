"""LLM 호출과 프롬프트 조립, 응답 SQL 정규화를 담당하는 서비스."""

import json
import os
import re
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from app.exceptions import LLMRateLimitError
from app.models import MappingRuleItem, SqlInfoJob
from app.services.binding_service import build_bind_target_hints
from app.services.prompt_service import render_prompt


ROOT_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT_DIR / ".env")


def _env_or_value(value: str | None, env_name: str) -> str:
    """함수 인자로 받은 값이 우선이며, 없으면 환경변수를 사용한다."""
    resolved = value or os.getenv(env_name)
    if not resolved:
        raise ValueError(f"Required environment variable '{env_name}' is not set.")
    return resolved


def _serialize_mapping_rules(mapping_rules: list[MappingRuleItem]) -> str:
    """프롬프트 주입용으로 매핑 룰을 JSON 문자열로 직렬화한다."""
    if not mapping_rules:
        return "[]"
    payload = [
        {
            "MAP_TYPE": rule.map_type,
            "FR_TABLE": rule.fr_table,
            "FR_COL": rule.fr_col,
            "TO_TABLE": rule.to_table,
            "TO_COL": rule.to_col,
        }
        for rule in mapping_rules
    ]
    return json.dumps(payload, ensure_ascii=False)


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
    merged_prompt = render_prompt(
        "tobe_sql_prompt.txt",
        from_sql=job.source_sql,
        mapping_rules_json=_serialize_mapping_rules(mapping_rules),
        feedback_examples_json=_serialize_feedback_examples(feedback_examples or []),
        last_error=last_error or "None",
    )
    return [
        {"role": "system", "content": merged_prompt},
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


def call_llm_api(api_key: str | None, model: str | None, base_url: str | None, messages: list[dict[str, str]]) -> str:
    """LLM을 호출하고 일시적 장애를 재시도 가능 예외로 변환한다."""
    resolved_api_key = _env_or_value(api_key, "LLM_API_KEY")
    resolved_model = _env_or_value(model, "LLM_MODEL")
    resolved_base_url = _env_or_value(base_url, "LLM_BASE_URL")
    try:
        llm = ChatOpenAI(
            api_key=resolved_api_key,
            model=resolved_model,
            base_url=resolved_base_url,
            temperature=0,
        )
        response = llm.invoke(_to_langchain_messages(messages))
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
    return call_llm_api(
        api_key=None,
        model=None,
        base_url=None,
        messages=build_test_sql_messages(
            job=job,
            tobe_sql=tobe_sql,
            bind_set_json=bind_set_json,
            last_error=last_error,
            feedback_examples=feedback_examples,
        ),
    )


def generate_test_sql_no_bind(
    job: SqlInfoJob,
    tobe_sql: str,
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> str:
    """no-bind 시나리오용 테스트 SQL 생성 진입점."""
    return call_llm_api(
        api_key=None,
        model=None,
        base_url=None,
        messages=build_test_sql_no_bind_messages(
            job=job,
            tobe_sql=tobe_sql,
            last_error=last_error,
            feedback_examples=feedback_examples,
        ),
    )
