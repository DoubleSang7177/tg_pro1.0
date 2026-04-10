from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_user_or_admin
from database import get_db
from logger import get_logger
from models import TaskRecord, User
from services.daily_reset import perform_daily_reset_if_needed
from services.telegram_service import run_task


router = APIRouter(tags=["task"])
log = get_logger("task")


class StartTaskRequest(BaseModel):
    group: str = Field(..., min_length=1, description="目标群组")
    users: list[str] = Field(..., min_length=1, description="用户列表")


@router.post("/start_task")
async def start_task(
    payload: StartTaskRequest,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    perform_daily_reset_if_needed(db)
    log.info(
        "task start user_id=%s groups=%s users_count=%s accounts_path=%s",
        user.id,
        [payload.group],
        len(payload.users),
        "auto_scan",
    )
    config = {
        "groups": [payload.group],
        "users": payload.users,
        "owner_id": user.id,
    }
    try:
        result = await run_task(config)
        task = TaskRecord(
            owner_id=user.id,
            group_name=payload.group,
            users_text="\n".join(payload.users),
            accounts_path="auto_scan",
            status=str(result.get("status", "accepted")),
            result_text=str(result),
        )
        db.add(task)
        db.commit()
        db.refresh(task)
        log.info("task accepted task_id=%s user_id=%s", task.id, user.id)
        return {"ok": True, "data": result}
    except ValueError as exc:
        log.warning("task validation_failed user_id=%s error=%s", user.id, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        log.exception("task failed user_id=%s", user.id)
        raise HTTPException(status_code=500, detail=f"任务启动失败: {exc}") from exc


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
