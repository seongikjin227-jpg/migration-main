"""Create minimal Oracle schema objects required by the tuning pipeline."""

from __future__ import annotations

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


def main() -> None:
    result_table = get_result_table().split(".")[-1].upper()
    with get_connection() as conn:
        cursor = conn.cursor()

        for column_name, ddl in (
            ("GOOD_SQL", "ALTER TABLE NEXT_SQL_INFO ADD GOOD_SQL CLOB"),
            ("GOOD_TEST_SQL", "ALTER TABLE NEXT_SQL_INFO ADD GOOD_TEST_SQL CLOB"),
            ("TUNING_STATUS", "ALTER TABLE NEXT_SQL_INFO ADD TUNING_STATUS VARCHAR2(50)"),
            ("TUNING_UPD_TS", "ALTER TABLE NEXT_SQL_INFO ADD TUNING_UPD_TS TIMESTAMP"),
        ):
            if not _column_exists(cursor, result_table, column_name):
                cursor.execute(ddl)
                print(f"added column {column_name}")

        if not _table_exists(cursor, "NEXT_SQL_TUNING_LOG"):
            cursor.execute(
                """
                CREATE TABLE NEXT_SQL_TUNING_LOG (
                    TUNING_ID         NUMBER PRIMARY KEY,
                    SPACE_NM          VARCHAR2(200) NOT NULL,
                    SQL_ID            VARCHAR2(200) NOT NULL,
                    TUNING_STATUS     VARCHAR2(50) NOT NULL,
                    LLM_USED_YN       CHAR(1) DEFAULT 'N' NOT NULL,
                    APPLIED_RULE_IDS  VARCHAR2(1000),
                    DIFF_SUMMARY      VARCHAR2(2000),
                    ERROR_MESSAGE     VARCHAR2(2000),
                    CREATED_AT        TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                    UPDATED_AT        TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                    CONSTRAINT CHK_NSTL_LLM_USED_YN CHECK (LLM_USED_YN IN ('Y', 'N'))
                )
                """
            )
            print("created table NEXT_SQL_TUNING_LOG")

        if not _sequence_exists(cursor, "SEQ_NEXT_SQL_TUNING_LOG"):
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

        conn.commit()


if __name__ == "__main__":
    main()
