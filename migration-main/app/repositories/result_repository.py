"""NEXT_SQL_INFO ?? ??/?? ?? repository."""

from app.db import get_connection, get_result_table
from app.common import SqlInfoJob

_COLUMN_LENGTH_CACHE: dict[str, dict[str, int]] = {}
_AVAILABLE_COLUMNS_CACHE: dict[str, set[str]] = {}
_CORRECT_COLUMN_MAP = {
    "TOBE": "TOBE_CORRECT_SQL",
    "BIND": "BIND_CORRECT_SQL",
    "TEST": "TEST_CORRECT_SQL",
}
_LEGACY_CORRECT_COLUMN = "CORRECT_SQL"


def _to_text(value, default: str = "") -> str:
    if value is None:
        return default
    if hasattr(value, "read"):
        value = value.read()
    if value is None:
        return default
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def _to_optional_text(value) -> str | None:
    if value is None:
        return None
    return _to_text(value)


def _get_column_data_lengths(table: str) -> dict[str, int]:
    normalized_table = table.upper()
    if normalized_table in _COLUMN_LENGTH_CACHE:
        return _COLUMN_LENGTH_CACHE[normalized_table]

    query = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = :1
    """
    lengths: dict[str, int] = {}
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, [normalized_table])
        for col_name, data_type, data_length in cursor.fetchall():
            col = _to_text(col_name).upper()
            dtype = _to_text(data_type).upper()
            if "CLOB" in dtype:
                continue
            try:
                lengths[col] = int(data_length)
            except Exception:
                continue

    _COLUMN_LENGTH_CACHE[normalized_table] = lengths
    return lengths


def _get_available_columns(table: str) -> set[str]:
    normalized_table = table.upper()
    if normalized_table in _AVAILABLE_COLUMNS_CACHE:
        return _AVAILABLE_COLUMNS_CACHE[normalized_table]

    query = """
        SELECT COLUMN_NAME
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = :1
    """
    columns: set[str] = set()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, [normalized_table])
        for (col_name,) in cursor.fetchall():
            columns.add(_to_text(col_name).upper())

    _AVAILABLE_COLUMNS_CACHE[normalized_table] = columns
    return columns


def _row_to_sql_info_job(row) -> SqlInfoJob:
    return SqlInfoJob(
        row_id=row[0],
        tag_kind=_to_text(row[1]),
        space_nm=_to_text(row[2]),
        sql_id=_to_text(row[3]),
        fr_sql_text=_to_text(row[4]),
        target_table=_to_optional_text(row[5]),
        edit_fr_sql=_to_optional_text(row[6]),
        to_sql_text=_to_optional_text(row[7]),
        bind_sql=_to_optional_text(row[8]),
        bind_set=_to_optional_text(row[9]),
        test_sql=_to_optional_text(row[10]),
        status=_to_optional_text(row[11]),
        log_text=_to_optional_text(row[12]),
        upd_ts=row[13],
        edited_yn=_to_optional_text(row[14]),
        tobe_correct_sql=_to_optional_text(row[15]) if len(row) > 15 else None,
        bind_correct_sql=_to_optional_text(row[16]) if len(row) > 16 else None,
        test_correct_sql=_to_optional_text(row[17]) if len(row) > 17 else None,
    )


def get_pending_jobs() -> list[SqlInfoJob]:
    table = get_result_table()
    available_columns = _get_available_columns(table)
    select_correct_cols = ", ".join(
        column
        if column in available_columns
        else f"CAST(NULL AS VARCHAR2(4000)) AS {column}"
        for column in ("TOBE_CORRECT_SQL", "BIND_CORRECT_SQL", "TEST_CORRECT_SQL")
    )

    query = f"""
        SELECT ROWIDTOCHAR(ROWID) AS RID,
               TAG_KIND, SPACE_NM, SQL_ID, FR_SQL_TEXT, TARGET_TABLE, EDIT_FR_SQL,
               TO_SQL_TEXT, BIND_SQL, BIND_SET, TEST_SQL, STATUS, LOG,
               UPD_TS, EDITED_YN, {select_correct_cols}
        FROM {table}
        WHERE UPPER(TRIM(STATUS)) = 'FAIL'
        ORDER BY UPD_TS NULLS FIRST, TO_CHAR(SPACE_NM), TO_CHAR(SQL_ID)
    """

    jobs: list[SqlInfoJob] = []
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            jobs.append(_row_to_sql_info_job(row))
    return jobs


def increment_batch_count(row_id: str) -> None:
    table = get_result_table()
    lengths = _get_column_data_lengths(table)
    if "BATCH_CNT" in lengths:
        query = f"""
            UPDATE {table}
            SET BATCH_CNT = NVL(BATCH_CNT, 0) + 1,
                UPD_TS = CURRENT_TIMESTAMP
            WHERE ROWID = CHARTOROWID(:1)
        """
    else:
        query = f"""
            UPDATE {table}
            SET UPD_TS = CURRENT_TIMESTAMP
            WHERE ROWID = CHARTOROWID(:1)
        """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, [row_id])
        conn.commit()


def update_cycle_result(
    row_id: str,
    tobe_sql: str,
    bind_sql: str,
    bind_set: str | None,
    test_sql: str,
    status: str,
    final_log: str,
):
    table = get_result_table()
    payload = _fit_payload_to_column_limits(
        table=table,
        values={
            "TO_SQL_TEXT": tobe_sql,
            "BIND_SQL": bind_sql,
            "BIND_SET": bind_set,
            "TEST_SQL": test_sql,
            "STATUS": status,
            "LOG": final_log,
        },
    )
    query = f"""
        UPDATE {table}
        SET TO_SQL_TEXT = :1,
            BIND_SQL = :2,
            BIND_SET = :3,
            TEST_SQL = :4,
            STATUS = :5,
            LOG = :6,
            UPD_TS = CURRENT_TIMESTAMP
        WHERE ROWID = CHARTOROWID(:7)
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            query,
            [
                payload["TO_SQL_TEXT"],
                payload["BIND_SQL"],
                payload["BIND_SET"],
                payload["TEST_SQL"],
                payload["STATUS"],
                payload["LOG"],
                row_id,
            ],
        )
        conn.commit()


def get_feedback_corpus_rows(correct_kind: str, limit: int = 2000) -> list[dict[str, str]]:
    table = get_result_table()
    safe_limit = max(1, min(limit, 20000))
    normalized_kind = (correct_kind or "").strip().upper()
    preferred_correct_column = _CORRECT_COLUMN_MAP.get(normalized_kind)
    if not preferred_correct_column:
        raise ValueError(f"Unsupported correct SQL kind: {correct_kind}")

    available_columns = _get_available_columns(table)
    if preferred_correct_column.upper() in available_columns:
        correct_column = preferred_correct_column
    elif _LEGACY_CORRECT_COLUMN in available_columns:
        correct_column = _LEGACY_CORRECT_COLUMN
    else:
        return []

    query = f"""
        SELECT ROWIDTOCHAR(ROWID) AS RID,
               TO_CHAR(SPACE_NM),
               TO_CHAR(SQL_ID),
               FR_SQL_TEXT,
               EDIT_FR_SQL,
               TO_SQL_TEXT,
               {correct_column} AS CORRECT_SQL,
               EDITED_YN,
               UPD_TS
        FROM (
            SELECT ROWIDTOCHAR(ROWID) AS RID,
                   SPACE_NM, SQL_ID, FR_SQL_TEXT, EDIT_FR_SQL, TO_SQL_TEXT,
                   {correct_column} AS CORRECT_SQL, EDITED_YN, UPD_TS
            FROM {table}
            WHERE (EDITED_YN = 'Y' OR {correct_column} IS NOT NULL)
              AND {correct_column} IS NOT NULL
            ORDER BY UPD_TS DESC
        )
        WHERE ROWNUM <= {safe_limit}
    """

    rows: list[dict[str, str]] = []
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            rows.append(
                {
                    "row_id": _to_text(row[0]),
                    "space_nm": _to_text(row[1]),
                    "sql_id": _to_text(row[2]),
                    "fr_sql_text": _to_text(row[3]),
                    "edit_fr_sql": _to_optional_text(row[4]) or "",
                    "to_sql_text": _to_optional_text(row[5]) or "",
                    "correct_sql": _to_text(row[6]),
                    "correct_kind": normalized_kind,
                    "edited_yn": _to_text(row[7]),
                    "upd_ts": _to_text(row[8]),
                }
            )
    return rows


def _fit_payload_to_column_limits(
    table: str,
    values: dict[str, str | None],
) -> dict[str, str | None]:
    lengths = _get_column_data_lengths(table)
    fitted: dict[str, str | None] = {}
    for column, value in values.items():
        if value is None:
            fitted[column] = None
            continue
        limit = lengths.get(column.upper())
        text = _to_text(value, default="")
        fitted[column] = _truncate_utf8_by_bytes(text, limit) if limit else text
    return fitted


def _truncate_utf8_by_bytes(text: str, byte_limit: int) -> str:
    if byte_limit <= 0:
        return ""
    encoded = text.encode("utf-8", errors="ignore")
    if len(encoded) <= byte_limit:
        return text
    return encoded[:byte_limit].decode("utf-8", errors="ignore")
