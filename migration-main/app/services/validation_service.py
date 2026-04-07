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
