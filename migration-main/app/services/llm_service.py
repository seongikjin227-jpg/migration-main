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
    resolved = value or os.getenv(env_name)
    if not resolved:
        raise ValueError(f"Required environment variable '{env_name}' is not set.")
    return resolved


def _serialize_mapping_rules(mapping_rules: list[MappingRuleItem]) -> str:
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


def _normalize_mapping_token(token: str) -> str:
    return (token or "").strip().strip('"').strip("'")


def _token_occurs(sql_text: str, token: str) -> bool:
    normalized_token = _normalize_mapping_token(token)
    if not normalized_token:
        return False
    pattern = re.compile(
        rf"(?<![A-Za-z0-9_$#]){re.escape(normalized_token)}(?![A-Za-z0-9_$#])",
        flags=re.IGNORECASE,
    )
    return bool(pattern.search(sql_text or ""))


def _filter_relevant_mapping_rules(source_sql: str, mapping_rules: list[MappingRuleItem]) -> list[MappingRuleItem]:
    relevant: list[MappingRuleItem] = []
    seen: set[tuple[str, str, str, str]] = set()
    for rule in mapping_rules:
        fr_table = _normalize_mapping_token(rule.fr_table)
        fr_col = _normalize_mapping_token(rule.fr_col)
        if not fr_table and not fr_col:
            continue
        if not (_token_occurs(source_sql, fr_table) or _token_occurs(source_sql, fr_col)):
            continue
        key = (
            fr_table.upper(),
            fr_col.upper(),
            _normalize_mapping_token(rule.to_table).upper(),
            _normalize_mapping_token(rule.to_col).upper(),
        )
        if key in seen:
            continue
        seen.add(key)
        relevant.append(rule)
    return relevant


def _serialize_required_mapping_pairs(mapping_rules: list[MappingRuleItem]) -> str:
    if not mapping_rules:
        return "[]"
    pairs: list[dict[str, str]] = []
    for rule in mapping_rules:
        fr_table = _normalize_mapping_token(rule.fr_table)
        fr_col = _normalize_mapping_token(rule.fr_col)
        to_table = _normalize_mapping_token(rule.to_table)
        to_col = _normalize_mapping_token(rule.to_col)
        if not (fr_table or fr_col):
            continue
        pairs.append(
            {
                "FR_TABLE": fr_table,
                "FR_COL": fr_col,
                "TO_TABLE": to_table,
                "TO_COL": to_col,
            }
        )
    return json.dumps(pairs, ensure_ascii=False)


def _serialize_feedback_examples(feedback_examples: list[dict[str, str]]) -> str:
    if not feedback_examples:
        return "[]"
    return json.dumps(feedback_examples, ensure_ascii=False)


def build_tobe_sql_messages(
    job: SqlInfoJob,
    mapping_rules: list[MappingRuleItem],
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    relevant_rules = _filter_relevant_mapping_rules(job.source_sql, mapping_rules)
    merged_prompt = render_prompt(
        "tobe_sql_prompt.txt",
        tag_kind=job.tag_kind or "None",
        space_nm=job.space_nm,
        sql_id=job.sql_id,
        source_sql=job.source_sql,
        mapping_rules_json=_serialize_mapping_rules(relevant_rules),
        required_mappings_json=_serialize_required_mapping_pairs(relevant_rules),
        feedback_examples_json=_serialize_feedback_examples(feedback_examples or []),
        last_error=last_error or "None",
    )
    return [
        {"role": "system", "content": merged_prompt},
    ]


def build_bind_sql_messages(
    job: SqlInfoJob,
    tobe_sql: str,
    mapping_rules: list[MappingRuleItem],
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    relevant_rules = _filter_relevant_mapping_rules(job.source_sql, mapping_rules)
    bind_target_hints = build_bind_target_hints(tobe_sql=tobe_sql, source_sql=job.source_sql)
    merged_prompt = render_prompt(
        "bind_sql_prompt.txt",
        tag_kind=job.tag_kind or "None",
        space_nm=job.space_nm,
        sql_id=job.sql_id,
        source_sql=job.source_sql,
        tobe_sql=tobe_sql,
        bind_target_hints_json=json.dumps(bind_target_hints, ensure_ascii=False),
        mapping_rules_json=_serialize_mapping_rules(relevant_rules),
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
    merged_prompt = render_prompt(
        "test_sql_prompt.txt",
        tag_kind=job.tag_kind or "None",
        space_nm=job.space_nm,
        sql_id=job.sql_id,
        source_sql=job.source_sql,
        tobe_sql=tobe_sql,
        bind_set_json=bind_set_json,
        feedback_examples_json=_serialize_feedback_examples(feedback_examples or []),
        last_error=last_error or "None",
    )
    return [
        {"role": "system", "content": merged_prompt},
    ]


def _extract_sql_text(response_text: str) -> str:
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
    cleaned = []
    for line in lines:
        if line.strip() == "/":
            continue
        cleaned.append(line)
    return cleaned


def _replace_limit_with_fetch_first(text: str) -> str:
    # Convert trailing LIMIT to Oracle-friendly FETCH FIRST syntax.
    return re.sub(
        r"\s+LIMIT\s+(\d+)\s*$",
        r" FETCH FIRST \1 ROWS ONLY",
        text,
        flags=re.IGNORECASE,
    )


def _normalize_oracle_sql(sql_text: str) -> str:
    text = sql_text.replace("\ufeff", "").replace("\u200b", "").replace("\u00a0", " ")
    text = "\n".join(_strip_sqlplus_terminator_lines(text.splitlines())).strip()
    text = _replace_limit_with_fetch_first(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s+\n", "\n", text)
    text = text.strip().rstrip(";").strip()

    # Reject stacked statements to avoid ORA-00933/00911 caused by extra separators.
    if _has_unquoted_semicolon(text):
        raise ValueError("LLM response must contain exactly one SQL statement.")
    if not text:
        raise ValueError("LLM returned an empty SQL statement after normalization.")
    return text


def _has_unquoted_semicolon(sql_text: str) -> bool:
    in_single_quote = False
    idx = 0
    length = len(sql_text)
    while idx < length:
        ch = sql_text[idx]
        if in_single_quote:
            if ch == "'":
                # Oracle escaped quote: ''
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
    converted = []
    for message in messages:
        if message.get("role") == "system":
            converted.append(SystemMessage(content=message.get("content", "")))
        else:
            converted.append(HumanMessage(content=message.get("content", "")))
    return converted


def call_llm_api(api_key: str | None, model: str | None, base_url: str | None, messages: list[dict[str, str]]) -> str:
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
    mapping_rules: list[MappingRuleItem],
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> str:
    generated = call_llm_api(
        api_key=None,
        model=None,
        base_url=None,
        messages=build_bind_sql_messages(
            job=job,
            tobe_sql=tobe_sql,
            mapping_rules=mapping_rules,
            last_error=last_error,
            feedback_examples=feedback_examples,
        ),
    )
    _validate_bind_sql_quality(generated)
    return generated


def _validate_bind_sql_quality(sql_text: str) -> None:
    normalized = re.sub(r"\s+", " ", (sql_text or "")).strip()
    if not normalized:
        raise ValueError("BIND SQL is empty.")
    lowered = normalized.lower()
    if re.search(r"\bfrom\s+dual\b", lowered):
        raise ValueError("BIND SQL must read from real tables; FROM DUAL is not allowed.")
    if " from " not in f" {lowered} ":
        raise ValueError("BIND SQL must include FROM clause with real table access.")
    literal_projection_pattern = re.compile(
        r"^\s*select\s+(?:distinct\s+)?"
        r"(?:'[^']*'|-?\d+(?:\.\d+)?|null)(?:\s+as\s+[a-zA-Z_][\w$#]*)?"
        r"(?:\s*,\s*(?:'[^']*'|-?\d+(?:\.\d+)?|null)(?:\s+as\s+[a-zA-Z_][\w$#]*)?)*"
        r"\s+from\b",
        flags=re.IGNORECASE,
    )
    if literal_projection_pattern.match(normalized):
        raise ValueError(
            "BIND SQL projection cannot be literal-only. Select actual table-derived columns."
        )


def generate_test_sql(
    job: SqlInfoJob,
    tobe_sql: str,
    bind_set_json: str,
    last_error: str | None = None,
    feedback_examples: list[dict[str, str]] | None = None,
) -> str:
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
