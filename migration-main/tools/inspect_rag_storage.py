"""Show where local RAG artifacts are stored and whether they exist."""

from __future__ import annotations

import os
from pathlib import Path

from _bootstrap import ROOT_DIR  # noqa: F401


def _resolve(env_name: str, default_path: Path) -> Path:
    return Path(os.getenv(env_name, str(default_path)))


def _print_path(label: str, path: Path) -> None:
    exists = path.exists()
    kind = "dir" if path.is_dir() else "file"
    size = path.stat().st_size if exists and path.is_file() else 0
    print(f"{label}:")
    print(f"  path={path}")
    print(f"  exists={exists}")
    print(f"  type={kind if exists else 'missing'}")
    if size:
        print(f"  size={size}")
    print("")


def main() -> None:
    rag_dir = ROOT_DIR / "data" / "rag"
    paths = {
        "RAG_DIR": rag_dir,
        "TOBE_RULE_VECTOR_DB_PATH": _resolve("TOBE_RULE_VECTOR_DB_PATH", rag_dir / "rule_catalog.db"),
        "BIND_RAG_DB_PATH": _resolve("BIND_RAG_DB_PATH", rag_dir / "rag.db"),
        "RAG_VECTOR_DB_PATH": _resolve("RAG_VECTOR_DB_PATH", rag_dir / "rag.db"),
    }
    for label, path in paths.items():
        _print_path(label, path)


if __name__ == "__main__":
    main()
