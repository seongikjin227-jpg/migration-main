"""Database execution and validation helpers for generated SQL."""

import re
from typing import Any

from app.db import get_connection, split_table_owner_and_name
from app.common import DBSqlError
from app.common import MappingRuleItem


# Runtime SQL must not contain unresolved mapper tags or bind placeholders.
_FORBIDDEN_RUNTIME_TOKENS = (
    "<if",
    "<choose",
    "<when",
    "<otherwise",
    "<where",
    "<trim",
    "#{",
    "${",
)
_TABLE_ALIAS_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_$#\.]*|\"[^\"]+\")"
    r"(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_$#]*|\"[^\"]+\"))?",
    re.IGNORECASE,
)
_QUALIFIED_COLUMN_PATTERN = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_$#]*)\.([A-Za-z_][A-Za-z0-9_$#]*)\b"
)
_WITH_CTE_NAME_PATTERN = re.compile(
    r"\bWITH\s+([A-Za-z_][A-Za-z0-9_$#]*)\s+AS\s*\(",
    re.IGNORECASE,
)
_WITH_CTE_FOLLOWUP_PATTERN = re.compile(
    r"\)\s*,\s*([A-Za-z_][A-Za-z0-9_$#]*)\s+AS\s*\(",
    re.IGNORECASE,
)
_SQL_RESERVED_ALIAS_NAMES = {
    "SELECT",
    "WHERE",
    "GROUP",
    "ORDER",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "CROSS",
    "ON",
    "UNION",
    "FETCH",
    "CONNECT",
    "START",
}
_TABLE_COLUMNS_CACHE: dict[str, set[str]] = {}


def _shorten_sql_for_log(sql_text: str, max_len: int = 700) -> str:
    """Collapse SQL to one line and truncate it for error logging."""
    one_line = re.sub(r"\s+", " ", (sql_text or "")).strip()
    if len(one_line) <= max_len:
        return one_line
    return one_line[:max_len] + "...(truncated)"


def execute_binding_query(binding_query_sql: str, max_rows: int = 20) -> list[dict[str, Any]]:
    """Execute bind-discovery SQL and return rows as dictionaries."""
    clean_sql = _prepare_runtime_sql(binding_query_sql, stage="EXECUTE_BIND_SQL")
    if not clean_sql:
        raise DBSqlError("Binding query SQL is empty.")

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(clean_sql)
            columns = [column[0] for column in cursor.description] if cursor.description else []
            rows = cursor.fetchmany(max_rows)
    except Exception as exc:
        raise DBSqlError(
            f"EXECUTE_BIND_SQL failed: {exc} | SQL={_shorten_sql_for_log(clean_sql)}"
        ) from exc

    bind_sets: list[dict[str, Any]] = []
    for row in rows:
        bind_item: dict[str, Any] = {}
        for idx, column in enumerate(columns):
            bind_item[column] = row[idx]
        bind_sets.append(bind_item)
    return bind_sets


def execute_test_query(test_sql: str) -> list[dict[str, Any]]:
    """Execute validation SQL and return the full result set."""
    clean_sql = _prepare_runtime_sql(test_sql, stage="EXECUTE_TEST_SQL")
    if not clean_sql:
        raise DBSqlError("TEST SQL is empty.")

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(clean_sql)
            columns = [column[0] for column in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
    except Exception as exc:
        raise DBSqlError(
            f"EXECUTE_TEST_SQL failed: {exc} | SQL={_shorten_sql_for_log(clean_sql)}"
        ) from exc

    result = []
    for row in rows:
        item: dict[str, Any] = {}
        for idx, col in enumerate(columns):
            item[col] = row[idx]
        result.append(item)
    return result


def _to_int_or_none(value) -> int | None:
    """Convert a value to `int`, returning `None` when conversion fails."""
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _get_value_case_insensitive(row: dict[str, Any], key: str):
    """Read one row value without depending on Oracle column-name casing."""
    if key in row:
        return row[key]
    lowered = key.lower()
    for existing_key, value in row.items():
        if str(existing_key).lower() == lowered:
            return value
    return None


def evaluate_status_from_test_rows(rows: list[dict[str, Any]]) -> str:
    """Translate test rows into a final PASS or FAIL status."""
    if not rows:
        return "FAIL"

    required_cols = {"case_no", "from_count", "to_count"}
    sample_keys = {str(key).lower() for key in rows[0].keys()}
    if not required_cols.issubset(sample_keys):
        raise DBSqlError(
            "TEST SQL must return CASE_NO, FROM_COUNT, TO_COUNT columns. "
            f"Actual columns: {sorted(sample_keys)}"
        )

    all_match = True
    for row in rows:
        from_count = _to_int_or_none(_get_value_case_insensitive(row, "from_count"))
        to_count = _to_int_or_none(_get_value_case_insensitive(row, "to_count"))

        if from_count is None or to_count is None:
            all_match = False
            continue

        if from_count == 0 and to_count == 0:
            all_match = False
            continue

        if from_count != to_count:
            all_match = False

    return "PASS" if all_match else "FAIL"


def collect_tobe_sql_column_coverage_issues(
    tobe_sql: str,
    mapping_rules: list[MappingRuleItem],
) -> list[str]:
    """Collect invalid table/column references in generated TOBE SQL."""
    clean_sql = (tobe_sql or "").strip()
    if not clean_sql:
        return ["generated TOBE SQL is empty"]

    allowed_columns_by_table = _build_allowed_columns_by_table(mapping_rules)
    if not allowed_columns_by_table:
        return []

    alias_to_table = _extract_table_aliases(clean_sql)
    if not alias_to_table:
        return []

    invalid_references: list[str] = []
    seen_refs: set[tuple[str, str]] = set()
    candidate_tables_by_column = _build_candidate_tables_by_column(allowed_columns_by_table)

    for alias, column_name in _extract_qualified_columns(clean_sql):
        normalized_alias = _normalize_identifier(alias)
        normalized_column = _normalize_identifier(column_name)
        if not normalized_alias or not normalized_column:
            continue
        target_table = alias_to_table.get(normalized_alias)
        if not target_table:
            continue

        allowed_columns = allowed_columns_by_table.get(target_table)
        if not allowed_columns or normalized_column in allowed_columns:
            continue

        ref_key = (target_table, normalized_column)
        if ref_key in seen_refs:
            continue
        seen_refs.add(ref_key)

        candidate_tables = [
            table_name
            for table_name in candidate_tables_by_column.get(normalized_column, [])
            if table_name != target_table
        ]
        hint = (
            f"invalid_reference={normalized_alias}.{normalized_column}; "
            f"reason=column_not_in_table; table={target_table}; column={normalized_column}; "
            f"candidate_target_tables={','.join(candidate_tables) if candidate_tables else 'none'}"
        )
        invalid_references.append(hint)
        if len(invalid_references) >= 5:
            break

    return invalid_references


def validate_tobe_sql_column_coverage(
    tobe_sql: str,
    mapping_rules: list[MappingRuleItem],
) -> None:
    """Fail fast wrapper retained for callers that want hard validation."""
    invalid_references = collect_tobe_sql_column_coverage_issues(
        tobe_sql=tobe_sql,
        mapping_rules=mapping_rules,
    )
    if invalid_references:
        raise DBSqlError("TOBE_SQL_COLUMN_COVERAGE_FAIL: " + " | ".join(invalid_references))


def _prepare_runtime_sql(sql_text: str, stage: str) -> str:
    """Normalize and validate SQL before it is executed against Oracle."""
    clean_sql = (sql_text or "").replace("\ufeff", "").strip().rstrip(";").strip()
    if not clean_sql:
        return clean_sql

    # Rewrite row-limiting syntax into an Oracle 11g-friendly form.
    if stage in {"EXECUTE_BIND_SQL", "EXECUTE_TEST_SQL"}:
        clean_sql = _normalize_select_row_limit(clean_sql)

    lowered = clean_sql.lower()
    for token in _FORBIDDEN_RUNTIME_TOKENS:
        if token in lowered:
            raise DBSqlError(
                f"{stage} generated non-executable SQL containing '{token}'. "
                "MyBatis tags/placeholders must be fully resolved before execution."
            )

    if _has_unquoted_semicolon(clean_sql):
        raise DBSqlError(f"{stage} generated multiple SQL statements; only one statement is allowed.")
    return clean_sql


def _build_allowed_columns_by_table(mapping_rules: list[MappingRuleItem]) -> dict[str, set[str]]:
    """Merge mapped target columns with live Oracle metadata when available."""
    mapped_columns_by_table: dict[str, set[str]] = {}
    for rule in mapping_rules or []:
        table_name = _normalize_table_name(rule.to_table)
        column_name = _normalize_identifier(rule.to_col)
        if not table_name or not column_name:
            continue
        mapped_columns_by_table.setdefault(table_name, set()).add(column_name)

    if not mapped_columns_by_table:
        return {}

    merged: dict[str, set[str]] = {}
    for table_name, mapped_columns in mapped_columns_by_table.items():
        runtime_columns = _load_table_columns(table_name)
        merged[table_name] = runtime_columns if runtime_columns else set(mapped_columns)
    return merged


def _load_table_columns(table_name: str) -> set[str]:
    """Load Oracle column names for one table, falling back silently when unavailable."""
    normalized_table = _normalize_table_name(table_name)
    if not normalized_table:
        return set()
    if normalized_table in _TABLE_COLUMNS_CACHE:
        return _TABLE_COLUMNS_CACHE[normalized_table]

    owner, bare_table = split_table_owner_and_name(normalized_table)
    if owner:
        query = """
            SELECT COLUMN_NAME
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :1
              AND TABLE_NAME = :2
        """
        params = [owner, bare_table]
    else:
        query = """
            SELECT COLUMN_NAME
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = :1
        """
        params = [bare_table]

    columns: set[str] = set()
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            for (column_name,) in cursor.fetchall():
                normalized_column = _normalize_identifier(str(column_name))
                if normalized_column:
                    columns.add(normalized_column)
    except Exception:
        columns = set()

    _TABLE_COLUMNS_CACHE[normalized_table] = columns
    return columns


def _extract_table_aliases(sql_text: str) -> dict[str, str]:
    """Map SQL aliases back to the referenced target table names."""
    cte_names = _extract_cte_names(sql_text)
    alias_to_table: dict[str, str] = {}
    for match in _TABLE_ALIAS_PATTERN.finditer(sql_text):
        raw_table_name = match.group(1)
        raw_alias = match.group(2)
        table_name = _normalize_table_name(raw_table_name)
        if not table_name or table_name in cte_names:
            continue

        alias = _normalize_identifier(raw_alias) if raw_alias else table_name
        if not alias or alias in _SQL_RESERVED_ALIAS_NAMES:
            alias = table_name
        alias_to_table[alias] = table_name
        alias_to_table.setdefault(table_name, table_name)
    return alias_to_table


def _extract_cte_names(sql_text: str) -> set[str]:
    """Extract top-level CTE names so they are not mistaken for physical tables."""
    names = { _normalize_identifier(match.group(1)) for match in _WITH_CTE_NAME_PATTERN.finditer(sql_text) }
    for match in _WITH_CTE_FOLLOWUP_PATTERN.finditer(sql_text):
        normalized = _normalize_identifier(match.group(1))
        if normalized:
            names.add(normalized)
    return {name for name in names if name}


def _extract_qualified_columns(sql_text: str) -> list[tuple[str, str]]:
    """Return alias-qualified column references found in the SQL text."""
    references: list[tuple[str, str]] = []
    for match in _QUALIFIED_COLUMN_PATTERN.finditer(sql_text):
        references.append((match.group(1), match.group(2)))
    return references


def _build_candidate_tables_by_column(
    allowed_columns_by_table: dict[str, set[str]],
) -> dict[str, list[str]]:
    """Build reverse lookup from column name to candidate target tables."""
    candidate_tables: dict[str, list[str]] = {}
    for table_name, columns in allowed_columns_by_table.items():
        for column_name in columns:
            if column_name not in candidate_tables:
                candidate_tables[column_name] = []
            candidate_tables[column_name].append(table_name)
    for tables in candidate_tables.values():
        tables.sort()
    return candidate_tables


def _normalize_table_name(value: str) -> str:
    """Normalize Oracle table names for case-insensitive comparison."""
    normalized = _normalize_identifier(value)
    if not normalized:
        return ""
    if "." in normalized:
        return normalized.split(".")[-1]
    return normalized


def _normalize_identifier(value: str | None) -> str:
    """Normalize one SQL identifier by stripping quotes and upper-casing it."""
    clean = (value or "").strip()
    if not clean:
        return ""
    return clean.strip('"').strip().upper()


def _normalize_select_row_limit(sql_text: str) -> str:
    """Convert LIMIT/FETCH clauses into a ROWNUM wrapper for Oracle 11g."""
    text = sql_text.strip().rstrip(";")

    # LIMIT n -> SELECT * FROM (...) WHERE ROWNUM <= n
    limit_match = re.search(r"\s+LIMIT\s+(\d+)\s*$", text, flags=re.IGNORECASE)
    if limit_match:
        limit = int(limit_match.group(1))
        inner = re.sub(r"\s+LIMIT\s+\d+\s*$", "", text, flags=re.IGNORECASE).strip()
        return f"SELECT * FROM ({inner}) WHERE ROWNUM <= {limit}"

    # FETCH FIRST n ROWS ONLY -> SELECT * FROM (...) WHERE ROWNUM <= n
    fetch_match = re.search(r"\s+FETCH\s+FIRST\s+(\d+)\s+ROWS\s+ONLY\s*$", text, flags=re.IGNORECASE)
    if fetch_match:
        limit = int(fetch_match.group(1))
        inner = re.sub(
            r"\s+FETCH\s+FIRST\s+\d+\s+ROWS\s+ONLY\s*$",
            "",
            text,
            flags=re.IGNORECASE,
        ).strip()
        return f"SELECT * FROM ({inner}) WHERE ROWNUM <= {limit}"

    return text


def _has_unquoted_semicolon(sql_text: str) -> bool:
    """Return True when a semicolon exists outside string literals."""
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
