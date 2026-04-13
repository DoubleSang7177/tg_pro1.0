"""后台任务执行过程中的增量日志，供 GET /start_task/status 轮询读取。"""
from __future__ import annotations

from threading import Lock

_buf: dict[str, list[str]] = {}
_hl: dict[str, dict[str, str | None]] = {}
_events: dict[str, list[dict[str, str]]] = {}
_lock = Lock()
_MAX_LINES = 4000
_MAX_EVENTS = 4000


def progress_init(job_id: str) -> None:
    with _lock:
        _buf[job_id] = []
        _hl[job_id] = {"active_phone": None, "previous_phone": None, "connecting_phone": None}
        _events[job_id] = []


def progress_append(job_id: str, line: str) -> None:
    with _lock:
        arr = _buf.setdefault(job_id, [])
        arr.append(line)
        if len(arr) > _MAX_LINES:
            del arr[: len(arr) - _MAX_LINES]


def progress_snapshot(job_id: str) -> list[str]:
    with _lock:
        return list(_buf.get(job_id, []))


def progress_discard(job_id: str) -> None:
    with _lock:
        _buf.pop(job_id, None)
        _hl.pop(job_id, None)
        _events.pop(job_id, None)


def progress_event_append(job_id: str, user: str, status: str) -> None:
    with _lock:
        arr = _events.setdefault(job_id, [])
        arr.append(
            {
                "type": "progress",
                "user": str(user or "").strip(),
                "status": str(status or "").strip().lower(),
            }
        )
        if len(arr) > _MAX_EVENTS:
            del arr[: len(arr) - _MAX_EVENTS]


def progress_events_snapshot(job_id: str) -> list[dict[str, str]]:
    with _lock:
        return list(_events.get(job_id, []))


def progress_highlight_publish(
    job_id: str,
    *,
    active_phone: str | None,
    previous_phone: str | None,
    connecting_phone: str | None,
) -> None:
    with _lock:
        _hl[job_id] = {
            "active_phone": active_phone,
            "previous_phone": previous_phone,
            "connecting_phone": connecting_phone,
        }


def progress_highlight_snapshot(job_id: str) -> dict[str, str | None]:
    defaults = {"active_phone": None, "previous_phone": None, "connecting_phone": None}
    with _lock:
        cur = _hl.get(job_id)
        if not cur:
            return dict(defaults)
        return {**defaults, **cur}
