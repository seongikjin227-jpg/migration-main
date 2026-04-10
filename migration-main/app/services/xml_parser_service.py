from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

import oracledb

from app.config import get_connection, get_result_table
from app.logger import logger


SUPPORTED_TAGS = {"select", "insert", "update", "delete", "sql"}
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "DATA"
INCLUDE_PATTERN = re.compile(
    r"<include\b[^>]*\brefid\s*=\s*['\"]([^'\"]+)['\"][^>]*>(?:.*?)</include>"
    r"|<include\b[^>]*\brefid\s*=\s*['\"]([^'\"]+)['\"][^>]*/\s*>",
    flags=re.IGNORECASE | re.DOTALL,
)


@dataclass
class ParsedSqlItem:
    tag_kind: str
    space_nm: str
    sql_id: str
    fr_sql_text: str
    target_table: list[str]
    source_file: str

    def to_json_payload(self) -> dict[str, Any]:
        return {
            "TAG_KIND": self.tag_kind,
            "SPACE_NM": self.space_nm,
            "SQL_ID": self.sql_id,
            "FR_SQL_TEXT": self.fr_sql_text,
            "TARGET_TABLE": self.target_table,
            "SOURCE_FILE": self.source_file,
        }


def _to_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if hasattr(value, "read"):
        value = value.read()
    if value is None:
        return default
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Required environment variable '{name}' is not set.")
    return value


def _safe_filename_component(text: str) -> str:
    return re.sub(r'[<>:"/\\|?*]+', "_", text.strip()) or "_"


def _local_tag_name(tag_name: str) -> str:
    # Handles namespaced XML tag names such as {namespace}select.
    if "}" in tag_name:
        return tag_name.split("}", 1)[1].lower()
    return tag_name.lower()


def _inner_xml(element: ET.Element) -> str:
    parts: list[str] = []
    if element.text:
        parts.append(element.text)
    for child in list(element):
        parts.append(ET.tostring(child, encoding="unicode"))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts).strip()


def _normalize_table_name(token: str) -> str:
    value = token.strip().strip(",").strip()
    value = re.sub(r"[;)]*$", "", value)
    value = value.strip('"').strip("'")
    if not value:
        return ""
    # Ignore non-table noise tokens such as '-', '--', separators.
    if not re.search(r"[A-Za-z0-9_]", value):
        return ""
    return value.upper()


def parse_single_mapper_xml(xml_path: Path) -> list[ParsedSqlItem]:
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as exc:
        logger.warning(f"[XMLParser] Failed to parse XML file: {xml_path} ({exc})")
        return []

    root = tree.getroot()
    namespace = (root.attrib.get("namespace") or "").strip()
    if not namespace:
        logger.warning(f"[XMLParser] Skipping mapper without namespace: {xml_path}")
        return []

    parsed_items: list[ParsedSqlItem] = []
    for elem in root.iter():
        local_name = _local_tag_name(elem.tag)
        if local_name not in SUPPORTED_TAGS:
            continue

        sql_id = (elem.attrib.get("id") or "").strip()
        if not sql_id:
            continue

        sql_text = _inner_xml(elem)
        parsed_items.append(
            ParsedSqlItem(
                tag_kind=local_name.upper(),
                space_nm=namespace,
                sql_id=sql_id,
                fr_sql_text=sql_text,
                target_table=[],
                source_file=str(xml_path),
            )
        )

    return parsed_items


def _resolve_output_dir(output_dir: str | None = None) -> Path:
    if output_dir:
        resolved = Path(output_dir)
    else:
        configured = os.getenv("XML_PARSER_DATA_DIR", "").strip()
        resolved = Path(configured) if configured else DEFAULT_OUTPUT_DIR
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _parse_target_tables_from_active_columns(*values: Any) -> list[str]:
    results: list[str] = []
    seen = set()
    for value in values:
        text = _to_text(value).strip()
        if not text:
            continue
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                tokens = [str(item) for item in parsed]
            else:
                tokens = re.split(r"[,\s;|]+", text)
        except Exception:
            tokens = re.split(r"[,\s;|]+", text)

        for token in tokens:
            normalized = _normalize_table_name(token)
            if not normalized or normalized in seen:
                continue
            results.append(normalized)
            seen.add(normalized)
    return results


def _load_target_table_map_from_active_table() -> dict[str, list[str]]:
    active_table = _validate_sql_identifier(_require_env("ACTIVE_SQL_ID_TABLE"))
    active_column = _validate_sql_identifier(os.getenv("ACTIVE_SQL_ID_COLUMN", "SQL_ID"))

    query = f"""
        SELECT TO_CHAR({active_column}),
               C_TABLES, R_TABLES, U_TABLES, D_TABLES
        FROM {active_table}
    """
    mapped: dict[str, list[str]] = {}
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            full_id = _to_text(row[0]).strip().upper()
            if not full_id:
                continue
            tables = _parse_target_tables_from_active_columns(row[1], row[2], row[3], row[4])
            if full_id in mapped:
                existing = mapped[full_id]
                seen = set(existing)
                for table_name in tables:
                    if table_name not in seen:
                        existing.append(table_name)
                        seen.add(table_name)
            else:
                mapped[full_id] = tables
    return mapped


def parse_mapper_dir_to_json(
    source_dir: str | None = None,
    output_dir: str | None = None,
) -> dict[str, int]:
    source_path = Path(source_dir or _require_env("MAPPER_XML_SOURCE_DIR"))
    if not source_path.exists() or not source_path.is_dir():
        raise ValueError(f"Mapper source directory does not exist: {source_path}")

    out_dir = _resolve_output_dir(output_dir)
    xml_files = sorted(source_path.rglob("*.xml"))
    target_table_map = _load_target_table_map_from_active_table()
    logger.info(f"[XMLParser] Stage1 started (source={source_path}, files={len(xml_files)})")

    total_items = 0
    written_files = 0
    for xml_file in xml_files:
        items = parse_single_mapper_xml(xml_file)
        if not items:
            continue

        json_rows: list[dict[str, Any]] = []
        for item in items:
            total_items += 1
            full_id = f"{item.space_nm}.{item.sql_id}".upper()
            item.target_table = target_table_map.get(full_id, [])
            json_rows.append(item.to_json_payload())

        rel_path = xml_file.relative_to(source_path).as_posix()
        file_stem = rel_path.rsplit(".", 1)[0].replace("/", "__")
        file_name = f"{_safe_filename_component(file_stem)}.json"
        file_path = out_dir / file_name
        file_path.write_text(
            json.dumps(json_rows, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        written_files += 1

    logger.info(
        f"[XMLParser] Stage1 completed (parsed_items={total_items}, json_files={written_files}, out_dir={out_dir})"
    )
    return {
        "xml_files": len(xml_files),
        "parsed_items": total_items,
        "json_files": written_files,
    }


def _load_json_payloads(data_dir: str | None = None) -> list[dict[str, Any]]:
    root = _resolve_output_dir(data_dir)
    payloads: list[dict[str, Any]] = []
    for file_path in sorted(root.glob("*.json")):
        try:
            raw = json.loads(file_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                payloads.append(raw)
                continue
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, dict):
                        payloads.append(item)
                continue
            logger.warning(f"[XMLParser] Unsupported JSON root type skipped: {file_path}")
        except Exception as exc:
            logger.warning(f"[XMLParser] Invalid JSON skipped: {file_path} ({exc})")
    return payloads


def upsert_json_to_next_sql_info(data_dir: str | None = None) -> dict[str, int]:
    table = get_result_table()
    payloads = _load_json_payloads(data_dir)
    logger.info(f"[XMLParser] Stage2 started (json_files={len(payloads)})")

    if not payloads:
        return {"upserted": 0}

    merge_sql = f"""
        MERGE INTO {table} T
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
            INSERT (
                TAG_KIND, SPACE_NM, SQL_ID, FR_SQL_TEXT, TARGET_TABLE, UPD_TS
            )
            VALUES (
                S.TAG_KIND, S.SPACE_NM, S.SQL_ID, S.FR_SQL_TEXT, S.TARGET_TABLE, CURRENT_TIMESTAMP
            )
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
        for payload in payloads:
            tag_kind = _to_text(payload.get("TAG_KIND")).strip().upper()
            space_nm = _to_text(payload.get("SPACE_NM")).strip()
            sql_id = _to_text(payload.get("SQL_ID")).strip()
            fr_sql_text = _to_text(payload.get("FR_SQL_TEXT"))
            target_table_value = payload.get("TARGET_TABLE")
            if isinstance(target_table_value, list):
                target_table = json.dumps(target_table_value, ensure_ascii=False)
            else:
                target_table = _to_text(target_table_value)

            if not (tag_kind and space_nm and sql_id):
                logger.warning(
                    f"[XMLParser] Missing key fields; skipped payload (space_nm={space_nm}, sql_id={sql_id})"
                )
                continue

            cursor.execute(
                merge_sql,
                {
                    "tag_kind": tag_kind,
                    "space_nm": space_nm,
                    "sql_id": sql_id,
                    "fr_sql_text": fr_sql_text,
                    "target_table": target_table,
                },
            )
            upserted += 1

        conn.commit()

    logger.info(f"[XMLParser] Stage2 completed (upserted={upserted})")
    return {"upserted": upserted}


def _parse_refid(refid: str, current_space: str) -> tuple[str, str]:
    clean_refid = (refid or "").strip()
    if not clean_refid:
        return current_space, clean_refid

    # Most common style: namespace.sqlId
    if "." in clean_refid:
        namespace, sql_id = clean_refid.rsplit(".", 1)
        if namespace and sql_id:
            return namespace, sql_id

    # Fallback: same namespace local ref.
    return current_space, clean_refid


def _resolve_include_text(
    sql_text: str,
    current_space: str,
    fragment_map: dict[tuple[str, str], str],
    stack: set[tuple[str, str]] | None = None,
    max_depth: int = 20,
) -> str:
    resolved = sql_text
    active_stack = set(stack or set())

    for _ in range(max_depth):
        changed = False

        def _replace(match: re.Match[str]) -> str:
            nonlocal changed
            refid = (match.group(1) or match.group(2) or "").strip()
            ref_space, ref_sql_id = _parse_refid(refid, current_space)
            key = (ref_space, ref_sql_id)

            # Support local fragment if qualified lookup misses.
            if key not in fragment_map and "." in refid:
                key = (current_space, refid)
                ref_space, ref_sql_id = key

            if key in active_stack:
                logger.warning(f"[XMLParser] include cycle detected: {ref_space}.{ref_sql_id}")
                return match.group(0)

            fragment_sql = fragment_map.get(key)
            if fragment_sql is None:
                logger.warning(f"[XMLParser] include ref not found: {refid}")
                return match.group(0)

            changed = True
            nested_stack = set(active_stack)
            nested_stack.add(key)
            return _resolve_include_text(
                sql_text=fragment_sql,
                current_space=ref_space,
                fragment_map=fragment_map,
                stack=nested_stack,
                max_depth=max_depth,
            )

        replaced = INCLUDE_PATTERN.sub(_replace, resolved)
        if not changed or replaced == resolved:
            resolved = replaced
            break
        resolved = replaced

    return resolved


def expand_include_to_edit_sql() -> dict[str, int]:
    table = get_result_table()
    logger.info("[XMLParser] Stage3 started")
    fetch_sql = f"""
        SELECT TO_CHAR(SPACE_NM), TO_CHAR(SQL_ID), TO_CHAR(TAG_KIND), FR_SQL_TEXT
        FROM {table}
    """
    rows: list[tuple[str, str, str, str]] = []
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(fetch_sql)
        for row in cursor.fetchall():
            rows.append(
                (
                    _to_text(row[0]).strip(),
                    _to_text(row[1]).strip(),
                    _to_text(row[2]).strip().upper(),
                    _to_text(row[3]),
                )
            )

    fragment_map: dict[tuple[str, str], str] = {}
    for space_nm, sql_id, _tag_kind, fr_sql_text in rows:
        fragment_map[(space_nm, sql_id)] = fr_sql_text

    updates: list[tuple[str, str, str]] = []
    include_candidates = 0
    for space_nm, sql_id, _tag_kind, fr_sql_text in rows:
        if "<include" not in fr_sql_text.lower():
            continue
        include_candidates += 1
        resolved = _resolve_include_text(
            sql_text=fr_sql_text,
            current_space=space_nm,
            fragment_map=fragment_map,
        ).strip()
        if resolved and resolved != fr_sql_text:
            updates.append((resolved, space_nm, sql_id))

    if not updates:
        logger.info(
            f"[XMLParser] Stage3 completed (include_candidates={include_candidates}, updated=0)"
        )
        return {"include_candidates": include_candidates, "updated": 0}

    update_sql = f"""
        UPDATE {table}
        SET EDIT_FR_SQL = :edit_fr_sql,
            UPD_TS = CURRENT_TIMESTAMP
        WHERE TO_CHAR(SPACE_NM) = :space_nm
          AND TO_CHAR(SQL_ID) = :sql_id
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.setinputsizes(
            edit_fr_sql=oracledb.DB_TYPE_CLOB,
            space_nm=oracledb.DB_TYPE_VARCHAR,
            sql_id=oracledb.DB_TYPE_VARCHAR,
        )
        for edit_fr_sql, space_nm, sql_id in updates:
            cursor.execute(
                update_sql,
                {"edit_fr_sql": edit_fr_sql, "space_nm": space_nm, "sql_id": sql_id},
            )
        conn.commit()

    logger.info(
        f"[XMLParser] Stage3 completed (include_candidates={include_candidates}, updated={len(updates)})"
    )
    return {"include_candidates": include_candidates, "updated": len(updates)}


def _validate_sql_identifier(name: str) -> str:
    normalized = (name or "").strip()
    if not normalized:
        raise ValueError("SQL identifier is empty.")
    if not re.fullmatch(r"[A-Za-z0-9_$.#]+", normalized):
        raise ValueError(f"Unsafe SQL identifier: {normalized}")
    return normalized


def _parse_stored_target_table(value: Any) -> list[str]:
    text = _to_text(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            result = []
            for item in parsed:
                normalized = _normalize_table_name(_to_text(item))
                if normalized:
                    result.append(normalized)
            return result
    except Exception:
        pass
    # Fallback: CSV-like storage.
    items = []
    for token in re.split(r"[,\s]+", text):
        normalized = _normalize_table_name(token)
        if normalized:
            items.append(normalized)
    return items


def _load_test_mapping_tables_from_env() -> set[str]:
    """
    Load test mapping tables from env var TEST_MAPPING_TABLES.
    Accepted formats:
    - comma/space/semicolon/pipe delimited string
    - JSON array string
    """
    raw = _require_env("TEST_MAPPING_TABLES")
    tokens: list[str] = []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            tokens = [str(item) for item in parsed]
        else:
            tokens = re.split(r"[,\s;|]+", raw)
    except Exception:
        tokens = re.split(r"[,\s;|]+", raw)

    mapped: set[str] = set()
    for token in tokens:
        normalized = _normalize_table_name(token)
        if not normalized:
            continue
        mapped.add(normalized)
        if "." in normalized:
            mapped.add(normalized.split(".")[-1])
    if not mapped:
        raise ValueError("TEST_MAPPING_TABLES is set but no valid table name was parsed.")
    return mapped


def cleanup_next_sql_info_rows() -> dict[str, int]:
    result_table = get_result_table()
    active_table = _validate_sql_identifier(_require_env("ACTIVE_SQL_ID_TABLE"))
    active_column = _validate_sql_identifier(os.getenv("ACTIVE_SQL_ID_COLUMN", "SQL_ID"))
    test_mapping_tables = _load_test_mapping_tables_from_env()

    logger.info(
        "[XMLParser] Stage4 started "
        f"(active_table={active_table}, active_column={active_column}, "
        f"test_mapping_tables={len(test_mapping_tables)})"
    )

    active_ids: set[str] = set()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT TO_CHAR({active_column}) FROM {active_table}")
        for (value,) in cursor.fetchall():
            full_id = _to_text(value).strip()
            if full_id:
                active_ids.add(full_id.upper())

    rows: list[tuple[str, str, str, Any]] = []
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT ROWIDTOCHAR(ROWID), TO_CHAR(SPACE_NM), TO_CHAR(SQL_ID), TARGET_TABLE
            FROM {result_table}
            """
        )
        rows.extend(cursor.fetchall())

    to_delete_rowids: list[str] = []
    deleted_not_active = 0
    deleted_not_in_test_mapping = 0

    for rowid, space_nm, sql_id, target_table_value in rows:
        space_text = _to_text(space_nm).strip()
        sql_text = _to_text(sql_id).strip()
        full_id = f"{space_text}.{sql_text}".upper()
        if full_id not in active_ids:
            to_delete_rowids.append(_to_text(rowid))
            deleted_not_active += 1
            continue

        target_tables = _parse_stored_target_table(target_table_value)
        if not target_tables:
            to_delete_rowids.append(_to_text(rowid))
            deleted_not_in_test_mapping += 1
            continue

        all_mapped = True
        for target_table in target_tables:
            table_key = target_table.upper()
            table_short = table_key.split(".")[-1]
            if table_key not in test_mapping_tables and table_short not in test_mapping_tables:
                all_mapped = False
                break

        if not all_mapped:
            to_delete_rowids.append(_to_text(rowid))
            deleted_not_in_test_mapping += 1

    if to_delete_rowids:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                f"DELETE FROM {result_table} WHERE ROWID = CHARTOROWID(:1)",
                [(rid,) for rid in to_delete_rowids],
            )
            conn.commit()

    remaining_total = 0
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {result_table}")
        remaining_total = int(cursor.fetchone()[0] or 0)

    logger.info(
        "[XMLParser] Stage4 completed "
        f"(deleted_total={len(to_delete_rowids)}, "
        f"deleted_not_active={deleted_not_active}, "
        f"deleted_not_in_test_mapping={deleted_not_in_test_mapping}, "
        f"remaining_total={remaining_total})"
    )
    return {
        "deleted_total": len(to_delete_rowids),
        "deleted_not_active": deleted_not_active,
        "deleted_not_in_test_mapping": deleted_not_in_test_mapping,
        "remaining_total": remaining_total,
    }


def run_all_xml_parser_stages(
    source_dir: str | None = None,
    output_dir: str | None = None,
) -> dict[str, dict[str, int]]:
    stage1 = parse_mapper_dir_to_json(source_dir=source_dir, output_dir=output_dir)
    stage2 = upsert_json_to_next_sql_info(data_dir=output_dir)
    stage3 = expand_include_to_edit_sql()
    stage4 = cleanup_next_sql_info_rows()
    return {
        "stage1": stage1,
        "stage2": stage2,
        "stage3": stage3,
        "stage4": stage4,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MyBatis XML parser utility stages")
    parser.add_argument(
        "stage",
        choices=["stage1", "stage2", "stage3", "stage4", "all"],
        help="Stage to run",
    )
    parser.add_argument("--source-dir", dest="source_dir", default=None, help="Mapper XML source directory")
    parser.add_argument("--output-dir", dest="output_dir", default=None, help="JSON output directory")
    return parser


def _main():
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.stage == "stage1":
        parse_mapper_dir_to_json(source_dir=args.source_dir, output_dir=args.output_dir)
        return
    if args.stage == "stage2":
        upsert_json_to_next_sql_info(data_dir=args.output_dir)
        return
    if args.stage == "stage3":
        expand_include_to_edit_sql()
        return
    if args.stage == "stage4":
        cleanup_next_sql_info_rows()
        return
    run_all_xml_parser_stages(source_dir=args.source_dir, output_dir=args.output_dir)


if __name__ == "__main__":
    _main()
