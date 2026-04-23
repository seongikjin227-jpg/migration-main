import logging
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


def _setup_logger() -> logging.Logger:
    """Create the process-wide application logger used by the batch runtime."""
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
    """Base exception for domain-specific migration agent errors."""


class LLMRateLimitError(AgentBaseException):
    """Raised when the LLM provider fails with retryable throttling/timeout issues."""


class DBSqlError(AgentBaseException):
    """Raised when generated or executed SQL is invalid for runtime execution."""


@dataclass
class SqlInfoJob:
    """One pending row loaded from NEXT_SQL_INFO."""
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
    tuned_sql: Optional[str] = None
    tuned_test_sql: Optional[str] = None
    tuning_status: Optional[str] = None
    status: Optional[str] = None
    log_text: Optional[str] = None
    upd_ts: Optional[datetime] = None
    edited_yn: Optional[str] = None
    bind_correct_sql: Optional[str] = None
    block_rag_content: Optional[str] = None

    @property
    def source_sql(self) -> str:
        """Return the effective source SQL, preferring edited SQL when present."""
        edited = (self.edit_fr_sql or "").strip()
        return edited if edited else (self.fr_sql_text or "")


@dataclass
class MappingRuleItem:
    """One mapping-rule row used to guide TOBE generation and logging."""
    map_type: str
    fr_table: str
    fr_col: str
    to_table: str
    to_col: str
    map_id: Optional[str] = None


def request_stop() -> None:
    """Signal the runtime that current and future cycles should stop."""
    _STOP_EVENT.set()


def clear_stop() -> None:
    """Clear the global stop flag before normal scheduler operation begins."""
    _STOP_EVENT.clear()


def is_stop_requested() -> bool:
    """Return whether a graceful shutdown has been requested."""
    return _STOP_EVENT.is_set()
