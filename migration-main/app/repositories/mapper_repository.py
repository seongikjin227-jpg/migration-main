"""Mapping rule repository."""

from app.common import MappingRuleItem
from app.db import (
    get_connection,
    get_mapping_rule_detail_table,
    get_mapping_rule_table,
)


def _to_text(value, default: str = "") -> str:
    """Convert Oracle values and LOBs into plain text."""
    if value is None:
        return default
    if hasattr(value, "read"):
        value = value.read()
    if value is None:
        return default
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def get_all_mapping_rules() -> list[MappingRuleItem]:
    """Load mapping rules from NEXT_MIG_INFO + NEXT_MIG_INFO_DTL."""
    map_table = get_mapping_rule_table()
    detail_table = get_mapping_rule_detail_table()
    query = f"""
        SELECT M.MAP_ID, M.FR_TABLE, D.FR_COL, M.TO_TABLE, D.TO_COL
        FROM {map_table} M
        JOIN {detail_table} D
          ON M.MAP_ID = D.MAP_ID
        ORDER BY M.MAP_ID, D.MAP_DTL
    """

    rules: list[MappingRuleItem] = []
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            rules.append(
                MappingRuleItem(
                    map_type="",
                    fr_table=_to_text(row[1]),
                    fr_col=_to_text(row[2]),
                    to_table=_to_text(row[3]),
                    to_col=_to_text(row[4]),
                    map_id=_to_text(row[0]),
                )
            )
    return rules
