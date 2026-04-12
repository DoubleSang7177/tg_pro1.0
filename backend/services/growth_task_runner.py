"""
用户增长后台任务：封装为可停止的 TaskRunner（协作式中断）。
"""
from __future__ import annotations

import threading
import traceback
from typing import Any

from sqlalchemy.orm import Session

from cn_time import cn_hms
from database import SessionLocal
from logger import get_logger
from models import TaskRecord
from services.daily_reset import perform_daily_reset_if_needed
from services.task_progress import progress_append
from services.task_run_control import clear_growth_job, task_run_should_continue, task_run_stop
from services.telegram_service import run_task

log = get_logger("growth_task_runner")


class GrowthTaskRunner:
    """running 标志 + stop()；run() 内通过 should_continue 协作退出。"""

    __slots__ = ("job_id", "owner_id", "group", "users", "_running", "_lock")

    def __init__(self, job_id: str, owner_id: int, group: str, users: list[str]) -> None:
        self.job_id = job_id
        self.owner_id = owner_id
        self.group = group
        self.users = users
        self._running = True
        self._lock = threading.Lock()

    def should_continue(self) -> bool:
        with self._lock:
            if not self._running:
                return False
        return task_run_should_continue()

    def stop(self) -> None:
        with self._lock:
            self._running = False
        task_run_stop()
        h = cn_hms()
        progress_append(self.job_id, f"[{h}] 收到停止指令")
        progress_append(self.job_id, f"[{h}] 正在停止…")

    async def run(self) -> dict[str, Any]:
        """
        执行任务；成功返回 run_task 的 result dict。
        校验失败抛 ValueError；其它异常向上抛；结束时 running=False 并清理全局运行标志。
        """
        local_db: Session | None = None
        try:
            print(
                f"[growth_runner] run() 开始 job_id={self.job_id} owner_id={self.owner_id} "
                f"group={self.group!r} users={len(self.users)}",
                flush=True,
            )
            print("[growth_runner] SessionLocal() 前", flush=True)
            local_db = SessionLocal()
            print("[growth_runner] SessionLocal() 后，perform_daily_reset 前", flush=True)
            perform_daily_reset_if_needed(local_db)
            print("[growth_runner] perform_daily_reset 后，组装 config", flush=True)
            config: dict[str, Any] = {
                "groups": [self.group],
                "users": self.users,
                "owner_id": self.owner_id,
                "progress_job_id": self.job_id,
                "should_continue": self.should_continue,
            }
            print("[growth_runner] await run_task(config) 前", flush=True)
            result = await run_task(config)
            print(
                f"[growth_runner] await run_task(config) 后 status={result.get('status')} "
                f"stopped={result.get('stopped')}",
                flush=True,
            )
            if result.get("stopped"):
                progress_append(self.job_id, f"[{cn_hms()}] 已停止")
            task = TaskRecord(
                owner_id=self.owner_id,
                group_name=self.group,
                users_text="\n".join(self.users),
                accounts_path="auto_scan",
                status="stopped" if result.get("stopped") else str(result.get("status", "accepted")),
                result_text=str(result),
            )
            local_db.add(task)
            local_db.commit()
            local_db.refresh(task)
            log.info(
                "growth runner finished job_id=%s task_id=%s user_id=%s stopped=%s",
                self.job_id,
                task.id,
                self.owner_id,
                result.get("stopped"),
            )
            return result
        except Exception:
            print(f"[growth_runner] run() 异常 job_id={self.job_id}", flush=True)
            traceback.print_exc()
            if local_db is not None:
                try:
                    local_db.rollback()
                except Exception:
                    pass
            raise
        finally:
            with self._lock:
                self._running = False
            task_run_stop()
            clear_growth_job()
            if local_db is not None:
                local_db.close()
