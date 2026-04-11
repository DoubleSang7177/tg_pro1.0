"""
协作式任务运行开关（全局 RUNNING 语义：True=继续执行，False=工作循环应退出）。

用户增长与群组互动共用同一标志；停止接口会将 RUNNING 置为 False。
"""
from __future__ import annotations

import threading
from typing import Optional

from cn_time import cn_hms

_lock = threading.Lock()
# 对外语义与需求一致：RUNNING 为 True 时任务继续
RUNNING: bool = False

_active_growth_job_id: Optional[str] = None
_active_interaction_job_id: Optional[str] = None


def task_run_start() -> None:
    global RUNNING
    with _lock:
        RUNNING = True


def task_run_stop() -> None:
    global RUNNING
    with _lock:
        RUNNING = False


def task_run_should_continue() -> bool:
    with _lock:
        return RUNNING


def register_growth_job(job_id: str) -> None:
    global _active_growth_job_id
    with _lock:
        _active_growth_job_id = job_id


def clear_growth_job() -> None:
    global _active_growth_job_id
    with _lock:
        _active_growth_job_id = None


def register_interaction_job(job_id: str) -> None:
    global _active_interaction_job_id
    with _lock:
        _active_interaction_job_id = job_id


def clear_interaction_job() -> None:
    global _active_interaction_job_id
    with _lock:
        _active_interaction_job_id = None


def stop_task_notify() -> None:
    """POST /stop-task：置 RUNNING=False，并向当前任务日志写入停止提示。"""
    from services.interaction_live_log import append as live_append
    from services.task_progress import progress_append

    gj: Optional[str]
    ij: Optional[str]
    with _lock:
        global RUNNING
        RUNNING = False
        gj = _active_growth_job_id
        ij = _active_interaction_job_id

    h = cn_hms()
    if gj:
        progress_append(gj, f"[{h}] 收到停止指令")
        progress_append(gj, f"[{h}] 正在停止…")
    if ij:
        live_append(
            ij,
            level="warn",
            account="—",
            group="SYSTEM",
            emoji="⏹",
            message="收到停止指令",
            layer="system",
            progress="—",
        )
        live_append(
            ij,
            level="warn",
            account="—",
            group="SYSTEM",
            emoji="⏳",
            message="正在停止…",
            layer="system",
            progress="—",
        )
