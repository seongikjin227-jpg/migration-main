"""NEXT_MIG_LOG insert repository."""

from __future__ import annotations

from app.db import get_connection, get_migration_log_table, split_table_owner_and_name


_COLUMN_LENGTH_CACHE: dict[str, dict[str, int]] = {}


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


def _get_column_data_lengths(table: str) -> dict[str, int]:
    """Load byte-length limits for the target log table columns."""
    owner, normalized_table = split_table_owner_and_name(table)
    cache_key = f"{owner or ''}.{normalized_table}"
    if cache_key in _COLUMN_LENGTH_CACHE:
        return _COLUMN_LENGTH_CACHE[cache_key]

    if owner:
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :1
              AND TABLE_NAME = :2
        """
        params = [owner, normalized_table]
    else:
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = :1
        """
        params = [normalized_table]

    lengths: dict[str, int] = {}
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        for col_name, data_type, data_length in cursor.fetchall():
            col = _to_text(col_name).upper()
            dtype = _to_text(data_type).upper()
            if "CLOB" in dtype:
                continue
            try:
                lengths[col] = int(data_length)
            except Exception:
                continue

    _COLUMN_LENGTH_CACHE[cache_key] = lengths
    return lengths


def _fit_payload_to_column_limits(table: str, values: dict[str, str | int | None]) -> dict[str, str | int | None]:
    """Trim text payload values to the byte lengths allowed by the table schema."""
    lengths = _get_column_data_lengths(table)
    fitted: dict[str, str | int | None] = {}
    for column, value in values.items():
        if value is None:
            fitted[column] = None
            continue
        if isinstance(value, int):
            fitted[column] = value
            continue
        limit = lengths.get(column.upper())
        text = _to_text(value, default="")
        fitted[column] = _truncate_utf8_by_bytes(text, limit) if limit else text
    return fitted


def _truncate_utf8_by_bytes(text: str, byte_limit: int) -> str:
    """Trim UTF-8 text without breaking multi-byte character boundaries."""
    if byte_limit <= 0:
        return ""
    encoded = text.encode("utf-8", errors="ignore")
    if len(encoded) <= byte_limit:
        return text
    return encoded[:byte_limit].decode("utf-8", errors="ignore")


def insert_migration_logs(
    map_ids: list[str],
    log_type: str,
    step_name: str,
    status: str,
    message: str,
    retry_count: int,
    mig_kind: str = "SQL_MIG",
) -> None:
    """Insert one operational log row into NEXT_MIG_LOG.

    Multiple MAP_ID values are stored in the MAP_ID column as a single
    comma-separated string because the runtime logs one stage event per job.
    """
    table = get_migration_log_table()
    payload = _fit_payload_to_column_limits(
        table=table,
        values={
            "MIG_KIND": mig_kind,
            "LOG_TYPE": log_type,
            "STEP_NAME": step_name,
            "STATUS": status,
            "MESSAGE": message,
            "RETRY_COUNT": retry_count,
        },
    )
    effective_map_ids = [str(map_id).strip() for map_id in map_ids if str(map_id).strip()]
    effective_map_id = ",".join(effective_map_ids) if effective_map_ids else None

    insert_sql = f"""
        INSERT INTO {table} (
            LOG_ID, MAP_ID, MIG_KIND, LOG_TYPE, STEP_NAME, STATUS, MESSAGE, RETRY_COUNT, CREATED_AT
        )
        VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, CURRENT_TIMESTAMP
        )
    """

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT NVL(MAX(LOG_ID), 0) + 1 FROM {table}")
        next_log_id = int(cursor.fetchone()[0] or 1)
        cursor.execute(
            insert_sql,
            [
                next_log_id,
                effective_map_id,
                payload["MIG_KIND"],
                payload["LOG_TYPE"],
                payload["STEP_NAME"],
                payload["STATUS"],
                payload["MESSAGE"],
                payload["RETRY_COUNT"],
            ],
        )
        conn.commit()
