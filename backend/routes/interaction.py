import threading
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_user_or_admin
from database import get_db
from logger import get_logger
from models import AccountFile, Group, InteractionTask, User
from services.account_status import ST_DAILY_LIMITED, ST_NORMAL, recover_and_normalize
from services.daily_reset import perform_daily_reset_if_needed
from services.interaction_service import run_interaction_task_sync
from services.telegram_service import _normalize_chat_identifier

router = APIRouter(tags=["interaction"])
log = get_logger("interaction")


class CreateInteractionTaskBody(BaseModel):
    groups: list[str] = Field(..., min_length=1, description="目标群组 username 列表")
    scan_limit: int = Field(300, ge=10, le=5000)


def _pick_engagement_accounts(db: Session, owner_id: int) -> list[AccountFile]:
    """非风控：可用 + 当日受限（与账号列表 active + limited 一致，不含 risk_suspected）。"""
    from datetime import datetime, timezone

    now_utc = datetime.now(timezone.utc)
    q = db.query(AccountFile).filter(AccountFile.owner_id == owner_id).order_by(AccountFile.id.desc())
    out: list[AccountFile] = []
    for row in q.all():
        recover_and_normalize(row, now_utc)
        if row.status not in (ST_NORMAL, ST_DAILY_LIMITED):
            continue
        out.append(row)
    return out


def _task_to_dict(row: InteractionTask) -> dict[str, Any]:
    groups = list(row.target_groups or [])
    acc_ids = list(row.account_ids or [])
    return {
        "id": row.id,
        "owner_id": row.owner_id,
        "groups": groups,
        "group_count": len(groups),
        "account_ids": acc_ids,
        "account_count": len(acc_ids),
        "status": row.status,
        "success_count": row.success_count or 0,
        "fail_count": row.fail_count or 0,
        "scan_limit": row.scan_limit or 300,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


@router.post("/interaction/tasks")
def create_interaction_task(
    body: CreateInteractionTaskBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    perform_daily_reset_if_needed(db)
    db.commit()

    normalized = [_normalize_chat_identifier(g) for g in body.groups if str(g).strip()]
    if not normalized:
        raise HTTPException(status_code=400, detail="请至少选择一个群组")
    normalized = list(dict.fromkeys(normalized))

    existing = db.query(Group).filter(Group.username.in_(normalized)).all()
    found = {g.username for g in existing}
    missing = [g for g in normalized if g not in found]
    if missing:
        raise HTTPException(status_code=400, detail=f"以下群组不在目标列表中: {', '.join(missing)}")

    owner_id = user.id
    accounts = _pick_engagement_accounts(db, owner_id)
    if not accounts:
        raise HTTPException(status_code=400, detail="没有符合条件的账号（需要可用或当日受限，不含风控列）")

    task = InteractionTask(
        owner_id=owner_id,
        target_groups=normalized,
        account_ids=[a.id for a in accounts],
        status="pending",
        success_count=0,
        fail_count=0,
        scan_limit=int(body.scan_limit),
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    tid = task.id
    log.info("interaction task created id=%s user=%s groups=%s accounts=%s", tid, user.id, len(normalized), len(accounts))

    def _runner() -> None:
        run_interaction_task_sync(tid)

    threading.Thread(target=_runner, name=f"interaction-{tid}", daemon=True).start()
    return {"ok": True, "task": _task_to_dict(task)}


@router.get("/interaction/tasks")
def list_interaction_tasks(
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    q = db.query(InteractionTask).order_by(InteractionTask.id.desc())
    if user.role != "admin":
        q = q.filter(InteractionTask.owner_id == user.id)
    rows = q.limit(200).all()
    return {"ok": True, "tasks": [_task_to_dict(r) for r in rows]}
