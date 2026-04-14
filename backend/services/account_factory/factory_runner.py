from __future__ import annotations

from collections import deque
from datetime import datetime
from threading import Lock
from typing import Any

_MAX_LOGS = 500
_lock = Lock()
_logs: deque[dict[str, Any]] = deque(maxlen=_MAX_LOGS)
_runtime: dict[str, Any] = {
    "status": "idle",  # idle / running / stopping
    "countries": ["ID", "PH", "BR", "IN"],
    "strategy": "balanced",
    "max_retries": 3,
}


def _now_hms() -> str:
    return datetime.now().strftime("%H:%M:%S")


def append_factory_log(level: str, module: str, message: str) -> None:
    with _lock:
        _logs.append(
            {
                "time": _now_hms(),
                "level": str(level or "INFO").upper(),
                "module": str(module or "SYSTEM").upper(),
                "message": str(message or "").strip() or "-",
            }
        )


def start_factory_task(*, countries: list[str], strategy: str, max_retries: int) -> dict[str, Any]:
    with _lock:
        _runtime["status"] = "running"
        _runtime["countries"] = countries or ["ID", "PH", "BR", "IN"]
        _runtime["strategy"] = strategy or "balanced"
        _runtime["max_retries"] = int(max_retries or 3)
    append_factory_log("INFO", "RUNNER", "开始执行注册流程")
    return get_runtime_snapshot()


def stop_factory_task() -> dict[str, Any]:
    with _lock:
        if _runtime.get("status") == "running":
            _runtime["status"] = "stopping"
    append_factory_log("WARN", "RUNNER", "收到停止信号，任务正在停止")
    with _lock:
        _runtime["status"] = "idle"
    append_factory_log("INFO", "RUNNER", "生产任务已停止")
    return get_runtime_snapshot()


def get_runtime_snapshot() -> dict[str, Any]:
    with _lock:
        return {
            "status": _runtime.get("status", "idle"),
            "countries": list(_runtime.get("countries") or []),
            "strategy": _runtime.get("strategy", "balanced"),
            "max_retries": int(_runtime.get("max_retries") or 3),
            "logs": list(_logs),
        }

