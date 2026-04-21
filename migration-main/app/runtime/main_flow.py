"""Top-level runtime flow helpers exposed by `app.main`."""

from app.batch.app import (
    configure_runtime_bootstrap,
    run_startup_sync_cycle,
    start_batch_scheduler,
)


def bootstrap_runtime() -> None:
    """Prepare logging and runtime-level bootstrap concerns."""
    # 런타임 시작 전에 로깅과 공통 부트스트랩을 준비한다.
    configure_runtime_bootstrap()


def run_startup_sync() -> None:
    """Run the startup-only sync cycle before the scheduler loop starts."""
    # 스케줄러가 돌기 전에 startup 전용 동기화 사이클을 한 번 실행한다.
    run_startup_sync_cycle()


def start_scheduler() -> None:
    """Start the long-running scheduler loop."""
    # 이후에는 스케줄러를 시작해 주기적으로 polling cycle을 실행한다.
    start_batch_scheduler()
