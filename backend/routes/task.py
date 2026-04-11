import asyncio
import time
import uuid
from threading import Lock
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_user_or_admin
from cn_time import cn_hms
from database import SessionLocal, get_db
from logger import get_logger
from models import TaskRecord, User
from services.daily_reset import perform_daily_reset_if_needed
from services.task_progress import (
    progress_append,
    progress_discard,
    progress_highlight_snapshot,
    progress_init,
    progress_snapshot,
)
from services.task_run_control import (
    clear_growth_job,
    register_growth_job,
    stop_task_notify,
    task_run_start,
    task_run_stop,
)
from services.telegram_service import run_task


router = APIRouter(tags=["task"])
log = get_logger("task")

_jobs: dict[str, dict[str, Any]] = {}
_jobs_lock = Lock()
_JOB_TTL_SEC = 3600


def _purge_stale_jobs() -> None:
    now = time.time()
    stale = [k for k, v in _jobs.items() if now - v.get("created_at", 0) > _JOB_TTL_SEC]
    for k in stale:
        progress_discard(k)
        del _jobs[k]


class StartTaskRequest(BaseModel):
    group: str = Field(..., min_length=1, description="目标群组")
    users: list[str] = Field(..., min_length=1, description="用户列表")


async def _run_task_job(job_id: str, owner_id: int, group: str, users: list[str]) -> None:
    local_db: Session | None = None
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "running"
    try:
        local_db = SessionLocal()
        perform_daily_reset_if_needed(local_db)
        config = {
            "groups": [group],
            "users": users,
            "owner_id": owner_id,
            "progress_job_id": job_id,
        }
        result = await run_task(config)
        if result.get("stopped"):
            progress_append(job_id, f"[{cn_hms()}] 已停止")
        task = TaskRecord(
            owner_id=owner_id,
            group_name=group,
            users_text="\n".join(users),
            accounts_path="auto_scan",
            status="stopped" if result.get("stopped") else str(result.get("status", "accepted")),
            result_text=str(result),
        )
        local_db.add(task)
        local_db.commit()
        local_db.refresh(task)
        log.info("task finished job_id=%s task_id=%s user_id=%s stopped=%s", job_id, task.id, owner_id, result.get("stopped"))
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "stopped" if result.get("stopped") else "completed"
                _jobs[job_id]["result"] = result
                _jobs[job_id]["error"] = None
    except ValueError as exc:
        log.warning("task validation_failed job_id=%s user_id=%s error=%s", job_id, owner_id, exc)
        progress_append(job_id, f"[{cn_hms()}] 任务校验失败: {exc}")
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "failed"
                _jobs[job_id]["error"] = str(exc)
                _jobs[job_id]["result"] = None
    except Exception as exc:
        log.exception("task failed job_id=%s user_id=%s", job_id, owner_id)
        progress_append(job_id, f"[{cn_hms()}] 任务异常中断: {exc}")
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "failed"
                _jobs[job_id]["error"] = f"任务执行失败: {exc}"
                _jobs[job_id]["result"] = None
    finally:
        task_run_stop()
        clear_growth_job()
        if local_db is not None:
            local_db.close()


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
    perform_daily_reset_if_needed(db)
    db.commit()
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
    progress_init(job_id)
    task_run_start()
    register_growth_job(job_id)
    log.info(
        "task queued job_id=%s user_id=%s groups=%s users_count=%s",
        job_id,
        user.id,
        [payload.group],
        len(payload.users),
    )
    asyncio.create_task(_run_task_job(job_id, user.id, payload.group, payload.users))
    return {"ok": True, "job_id": job_id}


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
        "group": job.get("group"),
        "users_count": job.get("users_count"),
        "error": job.get("error"),
        "progress_logs": progress_snapshot(job_id),
        "highlight_active_phone": hl["active_phone"],
        "highlight_previous_phone": hl["previous_phone"],
        "highlight_connecting_phone": hl["connecting_phone"],
    }
    if status in ("completed", "stopped") and job.get("result") is not None:
        out["data"] = job["result"]
    return out


@router.get("/tasks")
def list_tasks(
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    query = db.query(TaskRecord).order_by(TaskRecord.id.desc())
    if user.role != "admin":
        query = query.filter(TaskRecord.owner_id == user.id)
    rows = query.all()
    log.info("task list user_id=%s role=%s count=%s", user.id, user.role, len(rows))
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
