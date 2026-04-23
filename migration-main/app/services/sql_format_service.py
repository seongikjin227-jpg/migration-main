"""Formatting helpers for SQL persisted into DB columns."""

from __future__ import annotations

import re


_MYBATIS_TOKEN_PATTERN = re.compile(r"(#\{\s*[^}]+\s*\}|\$\{\s*[^}]+\s*\})")


def format_sql_for_storage(sql_text: str | None) -> str | None:
    """Return a consistently indented SQL string for DB persistence."""
    if sql_text is None:
        return None
    text = str(sql_text).replace("\ufeff", "").strip().rstrip(";").strip()
    if not text:
        return text

    placeholders: list[str] = []

    def _stash_placeholder(match: re.Match[str]) -> str:
        placeholders.append(match.group(1))
        return f"__MB_TOKEN_{len(placeholders) - 1}__"

    safe_text = _MYBATIS_TOKEN_PATTERN.sub(_stash_placeholder, text)
    try:
        import sqlglot  # type: ignore

        formatted = sqlglot.parse_one(safe_text, read="oracle").sql(dialect="oracle", pretty=True)
    except Exception:
        formatted = _fallback_format_sql(safe_text)

    for idx, token in enumerate(placeholders):
        formatted = formatted.replace(f"__MB_TOKEN_{idx}__", token)
    return formatted.strip()


def _fallback_format_sql(sql_text: str) -> str:
    """Apply a conservative keyword-based SQL formatter when parsing fails."""
    text = re.sub(r"\s+", " ", sql_text).strip()
    replacements = [
        (r"\bFROM\b", "\nFROM"),
        (r"\bWHERE\b", "\nWHERE"),
        (r"\bGROUP BY\b", "\nGROUP BY"),
        (r"\bORDER BY\b", "\nORDER BY"),
        (r"\bHAVING\b", "\nHAVING"),
        (r"\bUNION ALL\b", "\nUNION ALL"),
        (r"\bUNION\b", "\nUNION"),
        (r"\bFETCH FIRST\b", "\nFETCH FIRST"),
        (r"\bINNER JOIN\b", "\n  INNER JOIN"),
        (r"\bLEFT JOIN\b", "\n  LEFT JOIN"),
        (r"\bRIGHT JOIN\b", "\n  RIGHT JOIN"),
        (r"\bFULL JOIN\b", "\n  FULL JOIN"),
        (r"\bCROSS JOIN\b", "\n  CROSS JOIN"),
        (r"\bJOIN\b", "\n  JOIN"),
        (r"\bON\b", "\n    ON"),
        (r"\bAND\b", "\n  AND"),
        (r"\bOR\b", "\n  OR"),
    ]
    formatted = text
    for pattern, replacement in replacements:
        formatted = re.sub(pattern, replacement, formatted, flags=re.IGNORECASE)
    formatted = re.sub(r"\n{2,}", "\n", formatted)
    return formatted.strip()
