import asyncio
import time
import traceback
import uuid
from threading import Lock
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import get_current_user_optional, require_user_or_admin
from cn_time import cn_hms
from database import get_db
from logger import get_logger
from models import TaskRecord, User
from services.task_progress import (
    progress_append,
    progress_discard,
    progress_events_snapshot,
    progress_highlight_snapshot,
    progress_init,
    progress_snapshot,
)
from services.daily_reset import perform_daily_reset_if_needed
from services.growth_task_runner import GrowthTaskRunner
from services.task_run_control import (
    register_growth_job,
    stop_task_notify,
    task_run_start,
)


router = APIRouter(tags=["task"])
log = get_logger("task")

_jobs: dict[str, dict[str, Any]] = {}
_jobs_lock = Lock()
_JOB_TTL_SEC = 3600

_growth_runners: dict[str, GrowthTaskRunner] = {}
_growth_runners_lock = Lock()

_RUN_STATE_MAP = {
    "queued": "idle",
    "running": "running",
    "completed": "idle",
    "stopped": "idle",
    "failed": "error",
}


def _purge_stale_jobs() -> None:
    now = time.time()
    stale = [k for k, v in _jobs.items() if now - v.get("created_at", 0) > _JOB_TTL_SEC]
    for k in stale:
        progress_discard(k)
        del _jobs[k]


class StartTaskRequest(BaseModel):
    group: str = Field(..., min_length=1, description="目标群组")
    users: list[str] = Field(..., min_length=1, description="用户列表")


async def _drive_growth_runner(job_id: str, runner: GrowthTaskRunner) -> None:
    print(f"[_drive_growth_runner] 进入 job_id={job_id}", flush=True)
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "running"
    print(f"[_drive_growth_runner] 状态已置 running，await runner.run() 前 job_id={job_id}", flush=True)
    try:
        result = await runner.run()
        print(f"[_drive_growth_runner] await runner.run() 后 job_id={job_id} stopped={result.get('stopped')}", flush=True)
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "stopped" if result.get("stopped") else "completed"
                _jobs[job_id]["result"] = result
                _jobs[job_id]["error"] = None
    except ValueError as exc:
        print(f"[_drive_growth_runner] ValueError job_id={job_id}: {exc}", flush=True)
        traceback.print_exc()
        log.warning("task validation_failed job_id=%s user_id=%s error=%s", job_id, runner.owner_id, exc)
        progress_append(job_id, f"[{cn_hms()}] 任务校验失败: {exc}")
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "failed"
                _jobs[job_id]["error"] = str(exc)
                _jobs[job_id]["result"] = None
    except Exception as exc:
        print(f"[_drive_growth_runner] Exception job_id={job_id}: {exc}", flush=True)
        traceback.print_exc()
        log.exception("task failed job_id=%s user_id=%s", job_id, runner.owner_id)
        progress_append(job_id, f"[{cn_hms()}] 任务异常中断: {exc}")
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "failed"
                _jobs[job_id]["error"] = f"任务执行失败: {exc}"
                _jobs[job_id]["result"] = None
    finally:
        with _growth_runners_lock:
            _growth_runners.pop(job_id, None)


@router.post("/tasks/{task_id}/stop")
def stop_growth_task_by_id(
    task_id: str,
    user: User = Depends(require_user_or_admin),
) -> dict:
    """按 job_id 停止用户增长任务（调用 TaskRunner.stop）。"""
    with _growth_runners_lock:
        runner = _growth_runners.get(task_id)
    if runner is None:
        raise HTTPException(status_code=404, detail="任务不存在或已结束")
    with _jobs_lock:
        job = _jobs.get(task_id)
    if job is not None and user.role != "admin" and job["owner_id"] != user.id:
        raise HTTPException(status_code=403, detail="无权停止该任务")
    runner.stop()
    log.info("growth stop by id task_id=%s user_id=%s", task_id, user.id)
    return {"ok": True, "message": "已发送停止指令"}


@router.post("/stop-task")
def stop_task(_user: User = Depends(require_user_or_admin)) -> dict:
    stop_task_notify()
    log.info("stop-task requested user_id=%s", _user.id)
    return {"ok": True, "message": "已发送停止指令"}


@router.post("/start_task")
async def start_task(
    payload: StartTaskRequest,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    try:
        print(
            "[start_task] 1 接收参数",
            {"group": payload.group, "users_count": len(payload.users), "user_id": user.id},
            flush=True,
        )
        print("[start_task] 2 调用 perform_daily_reset_if_needed 前", flush=True)
        perform_daily_reset_if_needed(db)
        print("[start_task] 3 perform_daily_reset_if_needed 后，db.commit 前", flush=True)
        db.commit()
        print("[start_task] 4 db.commit 后，注册 job 前", flush=True)
        job_id = uuid.uuid4().hex
        with _jobs_lock:
            _purge_stale_jobs()
            _jobs[job_id] = {
                "owner_id": user.id,
                "status": "queued",
                "created_at": time.time(),
                "error": None,
                "result": None,
                "group": payload.group,
                "users_count": len(payload.users),
            }
        print(f"[start_task] 5 _jobs 已写入 job_id={job_id}", flush=True)
        progress_init(job_id)
        print("[start_task] 6 progress_init 完成", flush=True)
        task_run_start()
        register_growth_job(job_id)
        print("[start_task] 7 task_run_start + register_growth_job 完成", flush=True)
        # 管理员任务使用全量账号池（不按 owner_id 过滤），与 /accounts 展示口径一致
        account_owner_id = None if user.role == "admin" else user.id
        print(f"[DEBUG] 当前用户: {user.id}", flush=True)
        print(f"[DEBUG] owner_id过滤: {account_owner_id}", flush=True)
        runner = GrowthTaskRunner(
            job_id,
            user.id,
            payload.group,
            payload.users,
            account_owner_id=account_owner_id,
        )
        with _growth_runners_lock:
            _growth_runners[job_id] = runner
        print("[start_task] 8 GrowthTaskRunner 已登记", flush=True)
        log.info(
            "task queued job_id=%s user_id=%s groups=%s users_count=%s",
            job_id,
            user.id,
            [payload.group],
            len(payload.users),
        )
        print("[start_task] 9 loop.create_task(_drive_growth_runner) 前", flush=True)
        _loop = asyncio.get_running_loop()
        _loop.create_task(_drive_growth_runner(job_id, runner))
        print("[start_task] 10 create_task 已提交，即将 return job_id", flush=True)
        return {"ok": True, "job_id": job_id}
    except Exception:
        print("[start_task] 异常 — 完整 traceback:", flush=True)
        traceback.print_exc()
        raise


@router.get("/start_task/status/{job_id}")
def task_job_status(
    job_id: str,
    user: User = Depends(require_user_or_admin),
) -> dict:
    with _jobs_lock:
        job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在或已过期")
    if user.role != "admin" and job["owner_id"] != user.id:
        raise HTTPException(status_code=403, detail="无权查看该任务")
    status = job["status"]
    hl = progress_highlight_snapshot(job_id)
    out: dict[str, Any] = {
        "ok": True,
        "job_id": job_id,
        "status": status,
        "run_state": _RUN_STATE_MAP.get(status, "idle"),
        "group": job.get("group"),
        "users_count": job.get("users_count"),
        "error": job.get("error"),
        "progress_logs": progress_snapshot(job_id),
        "progress_events": progress_events_snapshot(job_id),
        "highlight_active_phone": hl["active_phone"],
        "highlight_previous_phone": hl["previous_phone"],
        "highlight_connecting_phone": hl["connecting_phone"],
    }
    if status in ("completed", "stopped") and job.get("result") is not None:
        out["data"] = job["result"]
    return out


@router.get("/tasks")
def list_tasks(
    user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
) -> dict:
    query = db.query(TaskRecord).order_by(TaskRecord.id.desc())
    if user is not None and user.role != "admin":
        query = query.filter(TaskRecord.owner_id == user.id)
    rows = query.all()
    log.info(
        "task list user_id=%s role=%s count=%s",
        user.id if user else None,
        user.role if user else "guest",
        len(rows),
    )
    return {
        "ok": True,
        "tasks": [
            {
                "id": row.id,
                "owner_id": row.owner_id,
                "group": row.group_name,
                "users": row.users_text.splitlines(),
                "accounts_path": row.accounts_path,
                "status": row.status,
                "result": row.result_text,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
            for row in rows
        ],
    }
