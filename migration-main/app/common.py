import logging
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("migration_agent")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - [%(name)s] [%(levelname)s] - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


logger = _setup_logger()
_STOP_EVENT = threading.Event()


class AgentBaseException(Exception):
    """Agent ?? ??? ??? ??."""


class LLMRateLimitError(AgentBaseException):
    """LLM ?? ??(429/timeout ?)? ??? ??? ??."""


class DBSqlError(AgentBaseException):
    """DB SQL ?? ??(??/??/?? ?) ??."""


@dataclass
class SqlInfoJob:
    row_id: str
    tag_kind: str
    space_nm: str
    sql_id: str
    fr_sql_text: str
    target_table: Optional[str] = None
    edit_fr_sql: Optional[str] = None
    to_sql_text: Optional[str] = None
    bind_sql: Optional[str] = None
    bind_set: Optional[str] = None
    test_sql: Optional[str] = None
    status: Optional[str] = None
    log_text: Optional[str] = None
    upd_ts: Optional[datetime] = None
    edited_yn: Optional[str] = None
    tobe_correct_sql: Optional[str] = None
    bind_correct_sql: Optional[str] = None
    test_correct_sql: Optional[str] = None

    @property
    def source_sql(self) -> str:
        edited = (self.edit_fr_sql or "").strip()
        return edited if edited else (self.fr_sql_text or "")


@dataclass
class MappingRuleItem:
    map_type: str
    fr_table: str
    fr_col: str
    to_table: str
    to_col: str


def request_stop() -> None:
    _STOP_EVENT.set()


def clear_stop() -> None:
    _STOP_EVENT.clear()


def is_stop_requested() -> bool:
    return _STOP_EVENT.is_set()
