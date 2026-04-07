from app.config import get_connection, get_result_table
from app.models import SqlInfoJob


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
        use_yn=row[12],
        target_yn=row[13],
        upd_ts=row[14],
        edited_yn=_to_optional_text(row[15]),
        correct_sql=_to_optional_text(row[16]),
    )


def get_pending_jobs() -> list[SqlInfoJob]:
    """Load pending jobs from NEXT_SQL_INFO."""
    table = get_result_table()
    query = f"""
        SELECT ROWIDTOCHAR(ROWID) AS RID,
               TAG_KIND, SPACE_NM, SQL_ID, FR_SQL_TEXT, EDIT_FR_SQL,
               TO_SQL_TEXT, BIND_SQL, BIND_SET, TEST_SQL, STATUS, LOG,
               USE_YN, TARGET_YN, UPD_TS, EDITED_YN, CORRECT_SQL
        FROM {table}
        WHERE USE_YN = 'Y'
          AND TARGET_YN = 'Y'
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
    bind_set: str,
    test_sql: str,
    status: str,
    final_log: str,
):
    table = get_result_table()
    query = f"""
        UPDATE {table}
        SET TO_SQL_TEXT = :1,
            BIND_SQL = :2,
            BIND_SET = :3,
            TEST_SQL = :4,
            STATUS = :5,
            LOG = :6,
            TARGET_YN = 'N',
            UPD_TS = CURRENT_TIMESTAMP
        WHERE ROWID = CHARTOROWID(:7)
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, [tobe_sql, bind_sql, bind_set, test_sql, status, final_log, row_id])
        conn.commit()


def finalize_failed_job(row_id: str, status: str, final_log: str):
    table = get_result_table()
    query = f"""
        UPDATE {table}
        SET STATUS = :1,
            LOG = :2,
            TARGET_YN = 'N',
            UPD_TS = CURRENT_TIMESTAMP
        WHERE ROWID = CHARTOROWID(:3)
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, [status, final_log, row_id])
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
