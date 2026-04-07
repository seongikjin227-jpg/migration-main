import json
import re
from typing import Any

from app.config import get_connection
from app.exceptions import DBSqlError


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _preview_applied_sql(sql: str, bind_values: dict[str, Any]) -> str:
    applied = sql
    for key, value in bind_values.items():
        applied = re.sub(rf":{re.escape(str(key))}\b", _literal(value), applied)
    return applied


def execute_binding_query(binding_query_sql: str, max_rows: int = 20) -> list[dict[str, Any]]:
    clean_sql = binding_query_sql.strip().rstrip(";")
    if not clean_sql:
        raise DBSqlError("Binding query SQL is empty.")
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(clean_sql)
        columns = [column[0] for column in cursor.description] if cursor.description else []
        rows = cursor.fetchmany(max_rows)
    bind_sets: list[dict[str, Any]] = []
    for row in rows:
        bind_item: dict[str, Any] = {}
        for idx, column in enumerate(columns):
            bind_item[column] = row[idx]
            lowered = column.lower()
            if lowered not in bind_item:
                bind_item[lowered] = row[idx]
        bind_sets.append(bind_item)
    return bind_sets


def verify_select_sql(tobe_sql: str, bind_sets: list[dict[str, Any]]) -> tuple[str, str | None, int | None, str]:
    clean_sql = tobe_sql.strip().rstrip(";")
    if not clean_sql:
        return "FAIL", "TO-BE SQL is empty.", None, ""

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            if bind_sets:
                total_row_count = 0
                applied_sql = _preview_applied_sql(clean_sql, bind_sets[0])
                for bind_set in bind_sets:
                    cursor.execute(clean_sql, bind_set)
                    rows = cursor.fetchall()
                    total_row_count += len(rows)
                row_count = total_row_count
            else:
                cursor.execute(clean_sql)
                rows = cursor.fetchall()
                row_count = len(rows)
                applied_sql = clean_sql
        return "SUCCESS", None, row_count, applied_sql
    except Exception as exc:
        return "FAIL", str(exc), None, ""


def to_json_text(bind_sets: list[dict[str, Any]]) -> str:
    return json.dumps(bind_sets, ensure_ascii=False)


def execute_test_query(test_sql: str) -> list[dict[str, Any]]:
    clean_sql = test_sql.strip().rstrip(";")
    if not clean_sql:
        raise DBSqlError("TEST SQL is empty.")

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(clean_sql)
        columns = [column[0] for column in cursor.description] if cursor.description else []
        rows = cursor.fetchall()

    result = []
    for row in rows:
        item: dict[str, Any] = {}
        for idx, col in enumerate(columns):
            item[col] = row[idx]
            item[col.lower()] = row[idx]
        result.append(item)
    return result


def _to_int_or_none(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def evaluate_status_from_test_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "FAIL(from count : NULL, to count : NULL)"

    required_cols = {"case_no", "from_count", "to_count"}
    sample_keys = {str(key).lower() for key in rows[0].keys()}
    if not required_cols.issubset(sample_keys):
        raise DBSqlError(
            "TEST SQL must return CASE_NO, FROM_COUNT, TO_COUNT columns. "
            f"Actual columns: {sorted(sample_keys)}"
        )

    all_match = True
    first_match_value = None
    first_mismatch: tuple[int | None, int | None] | None = None

    for row in rows:
        from_count = _to_int_or_none(row.get("from_count", row.get("FROM_COUNT")))
        to_count = _to_int_or_none(row.get("to_count", row.get("TO_COUNT")))

        if from_count is None or to_count is None:
            all_match = False
            if first_mismatch is None:
                first_mismatch = (from_count, to_count)
            continue

        if from_count != to_count:
            all_match = False
            if first_mismatch is None:
                first_mismatch = (from_count, to_count)
        elif first_match_value is None:
            first_match_value = from_count

    if all_match:
        display = first_match_value if first_match_value is not None else 0
        return f"PASS(both count = {display})"

    from_display = "NULL"
    to_display = "NULL"
    if first_mismatch is not None:
        from_display = "NULL" if first_mismatch[0] is None else str(first_mismatch[0])
        to_display = "NULL" if first_mismatch[1] is None else str(first_mismatch[1])
    return f"FAIL(from count : {from_display}, to count : {to_display})"
