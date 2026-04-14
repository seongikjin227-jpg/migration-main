"""환경설정 및 Oracle 연결 팩토리."""

import os
from pathlib import Path

import oracledb
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
# OS 환경변수보다 프로젝트 .env 값을 우선 적용한다.
load_dotenv(ROOT_DIR / ".env", override=True)
_CLIENT_INITIALIZED = False


def _get_required_env(name: str) -> str:
    """필수 환경변수를 읽고, 누락 시 즉시 예외를 발생시킨다."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable '{name}' is not set.")
    return value


def get_connection():
    """`.env` 기반 Oracle 연결을 생성한다.

    Oracle Client 초기화는 프로세스 단위로 1회만 가능하므로 플래그로 보호한다.
    """
    global _CLIENT_INITIALIZED

    if not _CLIENT_INITIALIZED:
        lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR", r"C:\oracle\instantclient_21c")
        try:
            oracledb.init_oracle_client(lib_dir=lib_dir)
        except oracledb.ProgrammingError:
            # init_oracle_client는 프로세스 내 1회만 호출 가능.
            # 이미 초기화된 경우 그대로 진행.
            pass
        _CLIENT_INITIALIZED = True

    user = _get_required_env("ORACLE_USER")
    password = _get_required_env("ORACLE_PASSWORD")
    dsn = _get_required_env("ORACLE_DSN")

    return oracledb.connect(user=user, password=password, dsn=dsn)


def get_mapping_rule_table() -> str:
    """매핑 마스터 테이블명을 중앙 관리한다."""
    return "NEXT_MIG_INFO"


def get_mapping_rule_detail_table() -> str:
    """매핑 상세 테이블명을 중앙 관리한다."""
    return "NEXT_MIG_INFO_DTL"


def get_result_table() -> str:
    """결과 테이블명을 중앙 관리한다."""
    return "NEXT_SQL_INFO"
