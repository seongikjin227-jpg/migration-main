"""Non-destructive SQL normalization for tuning review."""

import re


def normalize_sql_for_tuning(sql_text: str) -> tuple[str, list[str]]:
    """Normalize SQL shape without changing semantics."""
    text = (sql_text or "").replace("\ufeff", "").strip().rstrip(";").strip()
    notes: list[str] = []
    if not text:
        return "", ["empty_sql"]
    collapsed = re.sub(r"[ \t]+", " ", text)
    collapsed = re.sub(r"\n\s+\n", "\n", collapsed)
    if collapsed != text:
        notes.append("collapsed_whitespace")
    return collapsed.strip(), notes
