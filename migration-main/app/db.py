import os
from pathlib import Path

import oracledb
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env", override=True)
_CLIENT_INITIALIZED = False


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable '{name}' is not set.")
    return value


def get_oracle_schema() -> str:
    return (os.getenv("ORACLE_SCHEMA") or "").strip().upper()


def qualify_table_name(table_name: str) -> str:
    schema = get_oracle_schema()
    clean_table = (table_name or "").strip()
    if not schema or not clean_table or "." in clean_table:
        return clean_table
    return f"{schema}.{clean_table}"


def split_table_owner_and_name(table_name: str) -> tuple[str | None, str]:
    clean_table = (table_name or "").strip().upper()
    if "." in clean_table:
        owner, name = clean_table.split(".", 1)
        return owner, name
    schema = get_oracle_schema()
    return (schema or None), clean_table


def get_connection():
    global _CLIENT_INITIALIZED

    if not _CLIENT_INITIALIZED:
        lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR", r"C:\oracle\instantclient_21c")
        try:
            oracledb.init_oracle_client(lib_dir=lib_dir)
        except oracledb.ProgrammingError:
            pass
        _CLIENT_INITIALIZED = True

    user = _get_required_env("ORACLE_USER")
    password = _get_required_env("ORACLE_PASSWORD")
    dsn = _get_required_env("ORACLE_DSN")
    return oracledb.connect(user=user, password=password, dsn=dsn)


def get_mapping_rule_table() -> str:
    return qualify_table_name("NEXT_MIG_INFO")


def get_mapping_rule_detail_table() -> str:
    return qualify_table_name("NEXT_MIG_INFO_DTL")


def get_result_table() -> str:
    return qualify_table_name("NEXT_SQL_INFO")
