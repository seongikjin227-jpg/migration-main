"""Create or recreate Oracle schema objects required by the tuning pipeline."""

from __future__ import annotations

import argparse

from _bootstrap import ROOT_DIR  # noqa: F401
from app.db import get_connection, get_result_table


def _column_exists(cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM USER_TAB_COLUMNS WHERE TABLE_NAME = :1 AND COLUMN_NAME = :2",
        [table_name.upper(), column_name.upper()],
    )
    return int(cursor.fetchone()[0]) > 0


def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute("SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = :1", [table_name.upper()])
    return int(cursor.fetchone()[0]) > 0


def _sequence_exists(cursor, sequence_name: str) -> bool:
    cursor.execute("SELECT COUNT(*) FROM USER_SEQUENCES WHERE SEQUENCE_NAME = :1", [sequence_name.upper()])
    return int(cursor.fetchone()[0]) > 0


def _rename_column_if_needed(cursor, table_name: str, old_name: str, new_name: str) -> None:
    if _column_exists(cursor, table_name, old_name) and not _column_exists(cursor, table_name, new_name):
        cursor.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")
        print(f"renamed column {old_name} -> {new_name}")


def _ensure_next_sql_info_columns(cursor, result_table: str) -> None:
    _rename_column_if_needed(cursor, result_table, "GOOD_SQL", "TUNED_SQL")
    _rename_column_if_needed(cursor, result_table, "GOOD_TEST_SQL", "TUNED_TEST_SQL")

    for column_name, ddl in (
        ("TUNED_SQL", f"ALTER TABLE {result_table} ADD TUNED_SQL CLOB"),
        ("TUNED_TEST_SQL", f"ALTER TABLE {result_table} ADD TUNED_TEST_SQL CLOB"),
        ("TUNING_STATUS", f"ALTER TABLE {result_table} ADD TUNING_STATUS VARCHAR2(50)"),
        ("TUNING_UPD_TS", f"ALTER TABLE {result_table} ADD TUNING_UPD_TS TIMESTAMP"),
    ):
        if not _column_exists(cursor, result_table, column_name):
            cursor.execute(ddl)
            print(f"added column {column_name}")


def _drop_tuning_log_objects(cursor) -> None:
    if _table_exists(cursor, "NEXT_SQL_TUNING_LOG"):
        cursor.execute("DROP TABLE NEXT_SQL_TUNING_LOG PURGE")
        print("dropped table NEXT_SQL_TUNING_LOG")
    if _sequence_exists(cursor, "SEQ_NEXT_SQL_TUNING_LOG"):
        cursor.execute("DROP SEQUENCE SEQ_NEXT_SQL_TUNING_LOG")
        print("dropped sequence SEQ_NEXT_SQL_TUNING_LOG")


def _create_tuning_log_objects(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE NEXT_SQL_TUNING_LOG (
            TUNING_ID                    NUMBER PRIMARY KEY,
            EXECUTION_ID                 VARCHAR2(64),
            ROW_ID                       VARCHAR2(50),
            SPACE_NM                     VARCHAR2(200) NOT NULL,
            SQL_ID                       VARCHAR2(200) NOT NULL,
            TAG_KIND                     VARCHAR2(50),
            TUNING_STATUS                VARCHAR2(50) NOT NULL,
            JOB_STATUS                   VARCHAR2(50),
            FINAL_STAGE                  VARCHAR2(100),
            RETRY_COUNT                  NUMBER DEFAULT 0 NOT NULL,
            LLM_USED_YN                  CHAR(1) DEFAULT 'N' NOT NULL,
            TOBE_SQL                     CLOB,
            SOURCE_SQL_RAW              CLOB,
            SOURCE_SQL_PREPROCESSED     CLOB,
            SOURCE_SQL_NORMALIZED       CLOB,
            RETRIEVAL_QUERY_TEXT        CLOB,
            TOBE_RAG_DEBUG_JSON         CLOB,
            TOBE_FEEDBACK_EXAMPLES_JSON CLOB,
            RETRIEVED_RULE_IDS_JSON     CLOB,
            RETRIEVED_CASE_IDS_JSON     CLOB,
            APPLIED_RULE_IDS            CLOB,
            DIFF_SUMMARY                CLOB,
            ERROR_MESSAGE               CLOB,
            CREATED_AT                  TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
            UPDATED_AT                  TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
            CONSTRAINT CHK_NSTL_LLM_USED_YN CHECK (LLM_USED_YN IN ('Y', 'N'))
        )
        """
    )
    print("created table NEXT_SQL_TUNING_LOG")
    cursor.execute("CREATE INDEX IDX_NSTL_SQL_KEY ON NEXT_SQL_TUNING_LOG (SPACE_NM, SQL_ID, CREATED_AT)")
    print("created index IDX_NSTL_SQL_KEY")
    cursor.execute("CREATE INDEX IDX_NSTL_EXECUTION ON NEXT_SQL_TUNING_LOG (EXECUTION_ID)")
    print("created index IDX_NSTL_EXECUTION")
    cursor.execute(
        """
        CREATE SEQUENCE SEQ_NEXT_SQL_TUNING_LOG
            START WITH 1
            INCREMENT BY 1
            NOCACHE
            NOCYCLE
        """
    )
    print("created sequence SEQ_NEXT_SQL_TUNING_LOG")


def _apply_tuning_log_comments(cursor) -> None:
    comment_map = {
        "__TABLE__": "SQL 튜닝 실행 이력과 RAG 추적 정보를 저장하는 로그 테이블",
        "TUNING_ID": "튜닝 로그 고유 식별자",
        "EXECUTION_ID": "한 번의 배치/재시도 실행을 구분하는 실행 ID",
        "ROW_ID": "원본 NEXT_SQL_INFO 행의 Oracle ROWID 문자값",
        "SPACE_NM": "원본 SQL이 속한 업무/스페이스 이름",
        "SQL_ID": "원본 SQL 식별자",
        "TAG_KIND": "SQL 유형 구분값 (예: SELECT, INSERT, UPDATE)",
        "TUNING_STATUS": "튜닝 파이프라인 최종 상태값",
        "JOB_STATUS": "배치 처리 관점의 작업 성공/실패 상태",
        "FINAL_STAGE": "해당 로그가 기록된 최종 처리 단계명",
        "RETRY_COUNT": "현재 실행에서 누적된 재시도 횟수",
        "LLM_USED_YN": "LLM 사용 여부",
        "TOBE_SQL": "튜닝 대상이 된 TOBE SQL 본문",
        "SOURCE_SQL_RAW": "RAG 질의 기준이 된 원본 SOURCE SQL",
        "SOURCE_SQL_PREPROCESSED": "MyBatis 전처리 등이 반영된 SOURCE SQL",
        "SOURCE_SQL_NORMALIZED": "정규화 후 비교/검색에 사용된 SOURCE SQL",
        "RETRIEVAL_QUERY_TEXT": "RAG 검색에 사용한 요약 질의 텍스트",
        "TOBE_RAG_DEBUG_JSON": "TOBE RAG 검색/후보 병합 디버그 전체 JSON",
        "TOBE_FEEDBACK_EXAMPLES_JSON": "TOBE 프롬프트에 주입된 피드백 예시 JSON",
        "RETRIEVED_RULE_IDS_JSON": "RAG 및 튜닝 단계에서 검색/선정된 규칙 ID 목록 JSON",
        "RETRIEVED_CASE_IDS_JSON": "RAG 및 튜닝 단계에서 검색/선정된 사례 ID 목록 JSON",
        "APPLIED_RULE_IDS": "실제로 적용 또는 제안된 규칙 ID 목록",
        "DIFF_SUMMARY": "튜닝 전후 차이 또는 적용 규칙 요약",
        "ERROR_MESSAGE": "처리 실패 또는 검증 실패 시 오류 메시지",
        "CREATED_AT": "로그 행 생성 시각",
        "UPDATED_AT": "로그 행 마지막 갱신 시각",
    }
    table_comment = comment_map["__TABLE__"].replace("'", "''")
    cursor.execute(f"COMMENT ON TABLE NEXT_SQL_TUNING_LOG IS '{table_comment}'")
    print("applied table comment NEXT_SQL_TUNING_LOG")
    for column_name, comment in comment_map.items():
        if column_name == "__TABLE__":
            continue
        escaped = comment.replace("'", "''")
        cursor.execute(f"COMMENT ON COLUMN NEXT_SQL_TUNING_LOG.{column_name} IS '{escaped}'")
        print(f"applied column comment {column_name}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ensure Oracle tuning schema objects exist.")
    parser.add_argument(
        "--recreate-log-table",
        action="store_true",
        help="Drop and recreate NEXT_SQL_TUNING_LOG and its sequence with the new trace-oriented schema.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    result_table = get_result_table().split(".")[-1].upper()
    with get_connection() as conn:
        cursor = conn.cursor()
        _ensure_next_sql_info_columns(cursor, result_table)
        if args.recreate_log_table:
            _drop_tuning_log_objects(cursor)
            _create_tuning_log_objects(cursor)
        else:
            if not _table_exists(cursor, "NEXT_SQL_TUNING_LOG") or not _sequence_exists(cursor, "SEQ_NEXT_SQL_TUNING_LOG"):
                _drop_tuning_log_objects(cursor)
                _create_tuning_log_objects(cursor)
            else:
                print("NEXT_SQL_TUNING_LOG already exists; use --recreate-log-table to replace it.")
        if _table_exists(cursor, "NEXT_SQL_TUNING_LOG"):
            _apply_tuning_log_comments(cursor)
        conn.commit()


if __name__ == "__main__":
    main()
