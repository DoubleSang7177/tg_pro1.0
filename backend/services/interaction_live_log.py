"""群组互动实时日志：按 job_id 轮询，最多保留 100 条。"""
from __future__ import annotations

from threading import Lock
from typing import Any

from cn_time import cn_hm

_MAX_LINES = 200

_sessions: dict[str, dict[str, Any]] = {}
_lock = Lock()


def init_session(job_id: str, owner_id: int, task_id: int) -> None:
    with _lock:
        _sessions[job_id] = {
            "owner_id": owner_id,
            "task_id": task_id,
            "status": "running",
            "logs": [],
        }


def append(
    job_id: str | None,
    *,
    level: str,
    account: str,
    group: str,
    emoji: str = "",
    message: str = "",
    layer: str = "",
    progress: str = "",
) -> None:
    if not job_id:
        return

    row = {
        "t": cn_hm(),
        "level": level,
        "account": account,
        "group": group,
        "emoji": emoji or "",
        "message": message or "",
        "layer": layer or "",
        "progress": progress or "",
    }
    with _lock:
        s = _sessions.get(job_id)
        if not s:
            return
        s["logs"].append(row)
        if len(s["logs"]) > _MAX_LINES:
            del s["logs"][: len(s["logs"]) - _MAX_LINES]


def finalize(job_id: str | None, status: str) -> None:
    if not job_id:
        return
    with _lock:
        s = _sessions.get(job_id)
        if s:
            s["status"] = status


def get_snapshot(job_id: str) -> dict[str, Any] | None:
    with _lock:
        s = _sessions.get(job_id)
        if not s:
            return None
        return {
            "owner_id": s["owner_id"],
            "task_id": s["task_id"],
            "status": s["status"],
            "logs": list(s["logs"]),
        }
