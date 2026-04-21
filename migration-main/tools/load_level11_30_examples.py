"""Load sample mapper SQL IDs level11~30 into NEXT_SQL_INFO."""

from __future__ import annotations

import json
from pathlib import Path

import oracledb

from _bootstrap import ROOT_DIR  # noqa: F401
from app.db import get_connection, get_result_table
from app.services.xml_parser_service import parse_single_mapper_xml


XML_PATH = ROOT_DIR / "sample_mappers" / "hr_level_11_30_mapper.xml"
SPACE_NM = "HR_LEVEL_SCENARIOS"
TARGET_TABLES_BY_SQL_ID = {
    "level11": ["EMPLOYEES"],
    "level12": ["EMPLOYEES"],
    "level13": ["EMPLOYEES"],
    "level14": ["EMPLOYEES", "JOBS"],
    "level15": ["EMPLOYEES", "JOBS"],
    "level16": ["EMPLOYEES", "JOBS"],
    "level17": ["EMPLOYEES", "DEPARTMENTS", "JOBS"],
    "level18": ["EMPLOYEES", "DEPARTMENTS", "JOBS"],
    "level19": ["EMPLOYEES", "DEPARTMENTS", "JOBS"],
    "level20": ["EMPLOYEES", "DEPARTMENTS", "JOBS"],
    "level21": ["DEPARTMENTS", "EMPLOYEES"],
    "level22": ["DEPARTMENTS", "EMPLOYEES"],
    "level23": ["DEPARTMENTS", "EMPLOYEES"],
    "level24": ["EMPLOYEES", "JOB_HISTORY"],
    "level25": ["EMPLOYEES", "JOB_HISTORY"],
    "level26": ["EMPLOYEES", "JOB_HISTORY"],
    "level27": ["EMPLOYEES", "JOBS"],
    "level28": ["EMPLOYEES", "JOBS"],
    "level29": ["DEPARTMENTS", "EMPLOYEES"],
    "level30": ["DEPARTMENTS", "EMPLOYEES"],
}


def main() -> None:
    if not XML_PATH.exists():
        raise FileNotFoundError(f"Mapper XML not found: {XML_PATH}")

    items = parse_single_mapper_xml(Path(XML_PATH))
    if not items:
        raise RuntimeError(f"No SQL items parsed from {XML_PATH}")

    merge_sql = f"""
        MERGE INTO {get_result_table()} T
        USING (
            SELECT :tag_kind AS TAG_KIND,
                   :space_nm AS SPACE_NM,
                   :sql_id AS SQL_ID,
                   :fr_sql_text AS FR_SQL_TEXT,
                   :target_table AS TARGET_TABLE
            FROM DUAL
        ) S
        ON (TO_CHAR(T.SPACE_NM) = TO_CHAR(S.SPACE_NM) AND TO_CHAR(T.SQL_ID) = TO_CHAR(S.SQL_ID))
        WHEN MATCHED THEN
            UPDATE SET
                T.TAG_KIND = S.TAG_KIND,
                T.FR_SQL_TEXT = S.FR_SQL_TEXT,
                T.TARGET_TABLE = S.TARGET_TABLE,
                T.UPD_TS = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (TAG_KIND, SPACE_NM, SQL_ID, FR_SQL_TEXT, TARGET_TABLE, UPD_TS)
            VALUES (S.TAG_KIND, S.SPACE_NM, S.SQL_ID, S.FR_SQL_TEXT, S.TARGET_TABLE, CURRENT_TIMESTAMP)
    """

    upserted = 0
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.setinputsizes(
            tag_kind=oracledb.DB_TYPE_VARCHAR,
            space_nm=oracledb.DB_TYPE_VARCHAR,
            sql_id=oracledb.DB_TYPE_VARCHAR,
            fr_sql_text=oracledb.DB_TYPE_CLOB,
            target_table=oracledb.DB_TYPE_CLOB,
        )
        for item in items:
            sql_id = item.sql_id.strip()
            target_tables = TARGET_TABLES_BY_SQL_ID.get(sql_id)
            if not target_tables:
                raise KeyError(f"Missing target tables for SQL_ID={sql_id}")
            cursor.execute(
                merge_sql,
                {
                    "tag_kind": "SELECT",
                    "space_nm": SPACE_NM,
                    "sql_id": sql_id,
                    "fr_sql_text": item.fr_sql_text,
                    "target_table": json.dumps(target_tables, ensure_ascii=False),
                },
            )
            upserted += 1
        conn.commit()

    print(f"Loaded {upserted} rows into {get_result_table()} from {XML_PATH.name}")


if __name__ == "__main__":
    main()
