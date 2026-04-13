"""스케줄러/워커 루프가 공유하는 종료 플래그."""

import threading


_STOP_EVENT = threading.Event()


def request_stop() -> None:
    """현재 프로세스에 협력적 종료를 요청한다."""
    _STOP_EVENT.set()


def clear_stop() -> None:
    """스케줄러 시작 전 종료 플래그를 초기화한다."""
    _STOP_EVENT.clear()


def is_stop_requested() -> bool:
    """종료 요청 여부를 반환한다."""
    return _STOP_EVENT.is_set()
