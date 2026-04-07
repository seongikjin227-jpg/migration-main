from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class SqlInfoJob:
    """Work item loaded from NEXT_SQL_INFO."""

    row_id: str
    tag_kind: str
    space_nm: str
    sql_id: str
    fr_sql_text: str
    edit_fr_sql: Optional[str] = None
    to_sql_text: Optional[str] = None
    bind_sql: Optional[str] = None
    bind_set: Optional[str] = None
    test_sql: Optional[str] = None
    status: Optional[str] = None
    log_text: Optional[str] = None
    use_yn: Optional[str] = None
    target_yn: Optional[str] = None
    upd_ts: Optional[datetime] = None
    edited_yn: Optional[str] = None
    correct_sql: Optional[str] = None

    @property
    def source_sql(self) -> str:
        edited = (self.edit_fr_sql or "").strip()
        return edited if edited else (self.fr_sql_text or "")


@dataclass
class MappingRuleItem:
    """Single mapping-rule row loaded from NEXT_MIG_INFO."""

    map_type: str
    fr_table: str
    fr_col: str
    to_table: str
    to_col: str
