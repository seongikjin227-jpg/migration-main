"""Process entrypoint for the migration batch runtime.

This file is intentionally readable from top to bottom:
1. bootstrap runtime concerns
2. run startup sync
3. start the long-running batch scheduler
"""

import sys
from pathlib import Path


if __package__ in {None, ""}:
    ROOT_DIR = Path(__file__).resolve().parent.parent
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))

from app.runtime.main_flow import bootstrap_runtime, run_startup_sync, start_scheduler


def main() -> None:
    """Launch the migration runtime using explicit top-level flow steps."""
    # 프로세스 시작 시 부트스트랩, startup sync, scheduler 시작을 순서대로 실행한다.
    bootstrap_runtime()
    run_startup_sync()
    start_scheduler()


if __name__ == "__main__":
    main()
