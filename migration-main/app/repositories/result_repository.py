"""NEXT_SQL_INFO ?? ??/?? ?? repository."""

from app.db import get_connection, get_result_table, split_table_owner_and_name
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
    """Convert Oracle values and LOBs into plain text."""
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
    """Convert a DB value into optional text while preserving NULL."""
    if value is None:
        return None
    return _to_text(value)


def _get_column_data_lengths(table: str) -> dict[str, int]:
    """Load byte-length limits for writable NEXT_SQL_INFO columns."""
    owner, normalized_table = split_table_owner_and_name(table)
    cache_key = f"{owner or ''}.{normalized_table}"
    if cache_key in _COLUMN_LENGTH_CACHE:
        return _COLUMN_LENGTH_CACHE[cache_key]

    if owner:
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :1
              AND TABLE_NAME = :2
        """
        params = [owner, normalized_table]
    else:
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = :1
        """
        params = [normalized_table]
    lengths: dict[str, int] = {}
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        for col_name, data_type, data_length in cursor.fetchall():
            col = _to_text(col_name).upper()
            dtype = _to_text(data_type).upper()
            if "CLOB" in dtype:
                continue
            try:
                lengths[col] = int(data_length)
            except Exception:
                continue

    _COLUMN_LENGTH_CACHE[cache_key] = lengths
    return lengths


def _get_available_columns(table: str) -> set[str]:
    """Load the set of columns that exist on the configured result table."""
    owner, normalized_table = split_table_owner_and_name(table)
    cache_key = f"{owner or ''}.{normalized_table}"
    if cache_key in _AVAILABLE_COLUMNS_CACHE:
        return _AVAILABLE_COLUMNS_CACHE[cache_key]

    if owner:
        query = """
            SELECT COLUMN_NAME
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :1
              AND TABLE_NAME = :2
        """
        params = [owner, normalized_table]
    else:
        query = """
            SELECT COLUMN_NAME
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = :1
        """
        params = [normalized_table]
    columns: set[str] = set()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        for (col_name,) in cursor.fetchall():
            columns.add(_to_text(col_name).upper())

    _AVAILABLE_COLUMNS_CACHE[cache_key] = columns
    return columns


def _can_select_column(table: str, column_name: str) -> bool:
    """Check whether a column can be selected in the current Oracle schema."""
    query = f"SELECT {column_name} FROM {table} WHERE 1 = 0"
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
        return True
    except Exception:
        return False


def _row_to_sql_info_job(row) -> SqlInfoJob:
    """Map one NEXT_SQL_INFO query row into the runtime job dataclass."""
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
        good_sql=_to_optional_text(row[18]) if len(row) > 18 else None,
        tuning_status=_to_optional_text(row[19]) if len(row) > 19 else None,
        good_test_sql=_to_optional_text(row[20]) if len(row) > 20 else None,
    )


def get_pending_jobs() -> list[SqlInfoJob]:
    """Load SQL migration rows that are currently marked as FAIL."""
    table = get_result_table()
    available_columns = _get_available_columns(table)
    select_correct_cols = ", ".join(
        column
        if column in available_columns
        else f"CAST(NULL AS VARCHAR2(4000)) AS {column}"
        for column in ("TOBE_CORRECT_SQL", "BIND_CORRECT_SQL", "TEST_CORRECT_SQL", "GOOD_SQL", "TUNING_STATUS", "GOOD_TEST_SQL")
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
    """Increment BATCH_CNT when available, otherwise only touch UPD_TS."""
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
    """Persist final artifacts and status for one processed NEXT_SQL_INFO row."""
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


def update_tuning_result(
    row_id: str,
    good_sql: str | None,
    good_test_sql: str | None,
    tuning_status: str,
) -> None:
    """Persist tuning-stage final fields back to NEXT_SQL_INFO."""
    table = get_result_table()
    payload = _fit_payload_to_column_limits(
        table=table,
        values={
            "GOOD_SQL": good_sql,
            "GOOD_TEST_SQL": good_test_sql,
            "TUNING_STATUS": tuning_status,
        },
    )
    query = f"""
        UPDATE {table}
        SET GOOD_SQL = :1,
            GOOD_TEST_SQL = :2,
            TUNING_STATUS = :3,
            TUNING_UPD_TS = CURRENT_TIMESTAMP
        WHERE ROWID = CHARTOROWID(:4)
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            query,
            [
                payload["GOOD_SQL"],
                payload["GOOD_TEST_SQL"],
                payload["TUNING_STATUS"],
                row_id,
            ],
        )
        conn.commit()


def insert_tuning_log(
    space_nm: str,
    sql_id: str,
    tuning_status: str,
    llm_used_yn: str,
    applied_rule_ids: str | None,
    diff_summary: str | None,
    error_message: str | None,
) -> None:
    """Insert one summary tuning log row when the Oracle log table exists."""
    log_table = "NEXT_SQL_TUNING_LOG"
    sequence_name = "SEQ_NEXT_SQL_TUNING_LOG"
    available_tables_query = """
        SELECT COUNT(*)
        FROM USER_TABLES
        WHERE TABLE_NAME = :1
    """
    available_sequences_query = """
        SELECT COUNT(*)
        FROM USER_SEQUENCES
        WHERE SEQUENCE_NAME = :1
    """
    insert_sql = f"""
        INSERT INTO {log_table} (
            TUNING_ID,
            SPACE_NM,
            SQL_ID,
            TUNING_STATUS,
            LLM_USED_YN,
            APPLIED_RULE_IDS,
            DIFF_SUMMARY,
            ERROR_MESSAGE,
            CREATED_AT,
            UPDATED_AT
        ) VALUES (
            {sequence_name}.NEXTVAL,
            :1,
            :2,
            :3,
            :4,
            :5,
            :6,
            :7,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        )
    """

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(available_tables_query, [log_table])
        if int(cursor.fetchone()[0]) <= 0:
            return
        cursor.execute(available_sequences_query, [sequence_name])
        if int(cursor.fetchone()[0]) <= 0:
            return
        cursor.execute(
            insert_sql,
            [
                _to_text(space_nm),
                _to_text(sql_id),
                _to_text(tuning_status),
                (_to_text(llm_used_yn or "N")[:1] or "N").upper(),
                _to_optional_text(applied_rule_ids),
                _to_optional_text(diff_summary),
                _to_optional_text(error_message),
            ],
        )
        conn.commit()


def get_feedback_corpus_rows(correct_kind: str, limit: int = 2000) -> list[dict[str, str]]:
    """Load RAG corpus rows for one stage-specific correct SQL kind."""
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

    if not _can_select_column(table, correct_column):
        if correct_column != _LEGACY_CORRECT_COLUMN and _LEGACY_CORRECT_COLUMN in available_columns:
            if _can_select_column(table, _LEGACY_CORRECT_COLUMN):
                correct_column = _LEGACY_CORRECT_COLUMN
            else:
                return []
        else:
            return []

    query = f"""
        SELECT ROWIDTOCHAR(ROWID) AS RID,
               TO_CHAR(SPACE_NM),
               TO_CHAR(SQL_ID),
               FR_SQL_TEXT,
               EDIT_FR_SQL,
               TO_SQL_TEXT,
               CORRECT_SQL,
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


def get_tobe_rag_corpus_rows(
    limit: int = 2000,
    allow_legacy_fallback: bool = True,
) -> list[dict[str, str]]:
    """Load TOBE-stage RAG corpus rows using TOBE_CORRECT_SQL with optional legacy fallback."""
    table = get_result_table()
    safe_limit = max(1, min(limit, 20000))
    available_columns = _get_available_columns(table)

    has_tobe_correct = "TOBE_CORRECT_SQL" in available_columns
    has_legacy_correct = _LEGACY_CORRECT_COLUMN in available_columns and allow_legacy_fallback
    if not has_tobe_correct and not has_legacy_correct:
        return []

    tobe_expr = "TOBE_CORRECT_SQL" if has_tobe_correct else "CAST(NULL AS VARCHAR2(4000))"
    legacy_expr = _LEGACY_CORRECT_COLUMN if has_legacy_correct else "CAST(NULL AS VARCHAR2(4000))"

    query = f"""
        SELECT ROWIDTOCHAR(ROWID) AS RID,
               TO_CHAR(SPACE_NM),
               TO_CHAR(SQL_ID),
               TO_CHAR(TAG_KIND),
               FR_SQL_TEXT,
               EDIT_FR_SQL,
               TO_SQL_TEXT,
               TARGET_TABLE,
               {tobe_expr} AS TOBE_CORRECT_SQL,
               {legacy_expr} AS LEGACY_CORRECT_SQL,
               EDITED_YN,
               UPD_TS
        FROM (
            SELECT ROWIDTOCHAR(ROWID) AS RID,
                   SPACE_NM,
                   SQL_ID,
                   TAG_KIND,
                   FR_SQL_TEXT,
                   EDIT_FR_SQL,
                   TO_SQL_TEXT,
                   TARGET_TABLE,
                   {tobe_expr} AS TOBE_CORRECT_SQL,
                   {legacy_expr} AS LEGACY_CORRECT_SQL,
                   EDITED_YN,
                   UPD_TS
            FROM {table}
            WHERE (
                ({tobe_expr}) IS NOT NULL
                {"OR (" + legacy_expr + ") IS NOT NULL" if has_legacy_correct else ""}
            )
            ORDER BY UPD_TS DESC
        )
        WHERE ROWNUM <= {safe_limit}
    """

    rows: list[dict[str, str]] = []
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            tobe_correct_sql = _to_optional_text(row[8]) or ""
            legacy_correct_sql = _to_optional_text(row[9]) or ""
            correct_sql = tobe_correct_sql or legacy_correct_sql
            if not correct_sql.strip():
                continue
            rows.append(
                {
                    "row_id": _to_text(row[0]),
                    "space_nm": _to_text(row[1]),
                    "sql_id": _to_text(row[2]),
                    "tag_kind": _to_text(row[3]),
                    "fr_sql_text": _to_text(row[4]),
                    "edit_fr_sql": _to_optional_text(row[5]) or "",
                    "to_sql_text": _to_optional_text(row[6]) or "",
                    "target_table": _to_optional_text(row[7]) or "",
                    "tobe_correct_sql": tobe_correct_sql,
                    "legacy_correct_sql": legacy_correct_sql,
                    "correct_sql": correct_sql,
                    "edited_yn": _to_text(row[10]),
                    "upd_ts": _to_text(row[11]),
                }
            )
    return rows


def _fit_payload_to_column_limits(
    table: str,
    values: dict[str, str | None],
) -> dict[str, str | None]:
    """Trim text payload values to the byte lengths allowed by the table schema."""
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
    """Trim UTF-8 text without breaking multi-byte character boundaries."""
    if byte_limit <= 0:
        return ""
    encoded = text.encode("utf-8", errors="ignore")
    if len(encoded) <= byte_limit:
        return text
    return encoded[:byte_limit].decode("utf-8", errors="ignore")
