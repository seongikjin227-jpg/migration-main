from app.config import get_connection, get_result_table
from app.models import SqlInfoJob

_COLUMN_LENGTH_CACHE: dict[str, dict[str, int]] = {}


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


def _row_to_sql_info_job(row) -> SqlInfoJob:
    return SqlInfoJob(
        row_id=row[0],
        tag_kind=_to_text(row[1]),
        space_nm=_to_text(row[2]),
        sql_id=_to_text(row[3]),
        fr_sql_text=_to_text(row[4]),
        edit_fr_sql=_to_optional_text(row[5]),
        to_sql_text=_to_optional_text(row[6]),
        bind_sql=_to_optional_text(row[7]),
        bind_set=_to_optional_text(row[8]),
        test_sql=_to_optional_text(row[9]),
        status=_to_optional_text(row[10]),
        log_text=_to_optional_text(row[11]),
        upd_ts=row[12],
        edited_yn=_to_optional_text(row[13]),
        correct_sql=_to_optional_text(row[14]),
    )


def get_pending_jobs() -> list[SqlInfoJob]:
    """Load retry 대상 jobs from NEXT_SQL_INFO (STATUS='FAIL')."""
    table = get_result_table()
    query = f"""
        SELECT ROWIDTOCHAR(ROWID) AS RID,
               TAG_KIND, SPACE_NM, SQL_ID, FR_SQL_TEXT, EDIT_FR_SQL,
               TO_SQL_TEXT, BIND_SQL, BIND_SET, TEST_SQL, STATUS, LOG,
               UPD_TS, EDITED_YN, CORRECT_SQL
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


def get_feedback_examples(job: SqlInfoJob, limit: int = 5) -> list[dict[str, str]]:
    """
    Build feedback examples from NEXT_SQL_INFO.EDITED_YN and CORRECT_SQL.
    """
    table = get_result_table()
    safe_limit = max(1, min(limit, 20))
    query = f"""
        SELECT EDITED_YN, CORRECT_SQL, TO_SQL_TEXT
        FROM (
            SELECT EDITED_YN, CORRECT_SQL, TO_SQL_TEXT
            FROM {table}
            WHERE TO_CHAR(SPACE_NM) = :1
              AND TO_CHAR(SQL_ID) = :2
              AND (EDITED_YN = 'Y' OR CORRECT_SQL IS NOT NULL)
            ORDER BY UPD_TS DESC
        )
        WHERE ROWNUM <= {safe_limit}
    """
    examples: list[dict[str, str]] = []
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, [job.space_nm, job.sql_id])
        for row in cursor.fetchall():
            examples.append(
                {
                    "edited_yn": _to_text(row[0]),
                    "correct_sql": _to_text(row[1]),
                    "generated_sql": _to_text(row[2]),
                }
            )
    return examples


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
            # Do not truncate LOB columns.
            if "CLOB" in dtype:
                continue
            try:
                lengths[col] = int(data_length)
            except Exception:
                continue

    _COLUMN_LENGTH_CACHE[normalized_table] = lengths
    return lengths


def _truncate_utf8_by_bytes(text: str, byte_limit: int) -> str:
    if byte_limit <= 0:
        return ""
    encoded = text.encode("utf-8", errors="ignore")
    if len(encoded) <= byte_limit:
        return text
    return encoded[:byte_limit].decode("utf-8", errors="ignore")
