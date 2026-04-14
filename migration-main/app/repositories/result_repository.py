"""NEXT_SQL_INFO 작업 조회/결과 저장 리포지토리."""

from app.config import get_connection, get_result_table
from app.models import SqlInfoJob

_COLUMN_LENGTH_CACHE: dict[str, dict[str, int]] = {}


def _to_text(value, default: str = "") -> str:
    """DB 드라이버 값(LOB 포함)을 문자열로 정규화한다."""
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
    """NULL은 None으로 유지하고, 값이 있으면 문자열로 변환한다."""
    if value is None:
        return None
    return _to_text(value)


def _row_to_sql_info_job(row) -> SqlInfoJob:
    """DB row 튜플을 SqlInfoJob으로 매핑한다."""
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
        correct_sql=_to_optional_text(row[15]),
    )


def get_pending_jobs() -> list[SqlInfoJob]:
    """재처리 대상(`STATUS='FAIL'`) 작업 목록을 조회한다."""
    table = get_result_table()
    query = f"""
        SELECT ROWIDTOCHAR(ROWID) AS RID,
               TAG_KIND, SPACE_NM, SQL_ID, FR_SQL_TEXT, TARGET_TABLE, EDIT_FR_SQL,
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
    """ROWID 기준으로 산출물/상태/로그를 갱신한다."""
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


def get_feedback_corpus_rows(limit: int = 2000) -> list[dict[str, str]]:
    """RAG 인덱싱용 정답 SQL 코퍼스를 조회한다.

    - 사람이 수정했거나(`EDITED_YN='Y'`) 정답 SQL(`CORRECT_SQL`)이 있는 데이터만 대상.
    - 코퍼스 동기화 비용을 제어하기 위해 limit 상한을 둔다.
    """
    table = get_result_table()
    safe_limit = max(1, min(limit, 20000))
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
                   CORRECT_SQL, EDITED_YN, UPD_TS
            FROM {table}
            WHERE (EDITED_YN = 'Y' OR CORRECT_SQL IS NOT NULL)
              AND CORRECT_SQL IS NOT NULL
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
                    "edited_yn": _to_text(row[7]),
                    "upd_ts": _to_text(row[8]),
                }
            )
    return rows


def _fit_payload_to_column_limits(
    table: str,
    values: dict[str, str | None],
) -> dict[str, str | None]:
    """비 LOB 컬럼은 byte 길이를 맞춰 잘라 저장 실패를 방지한다."""
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
    """USER_TAB_COLUMNS에서 컬럼 길이를 읽어 캐시한다."""
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
            # CLOB은 길이 자르기를 적용하지 않는다.
            if "CLOB" in dtype:
                continue
            try:
                lengths[col] = int(data_length)
            except Exception:
                continue

    _COLUMN_LENGTH_CACHE[normalized_table] = lengths
    return lengths


def _truncate_utf8_by_bytes(text: str, byte_limit: int) -> str:
    """UTF-8 byte 기준으로 문자열을 잘라 유효한 문자 경계를 유지한다."""
    if byte_limit <= 0:
        return ""
    encoded = text.encode("utf-8", errors="ignore")
    if len(encoded) <= byte_limit:
        return text
    return encoded[:byte_limit].decode("utf-8", errors="ignore")
