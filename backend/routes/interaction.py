import threading
import uuid
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
from services.interaction_live_log import get_snapshot as interaction_live_snapshot
from services.interaction_live_log import init_session as interaction_live_init
from services.interaction_service import run_interaction_task_sync
from services.task_run_control import register_interaction_job, task_run_start
from services.telegram_service import _normalize_chat_identifier

router = APIRouter(tags=["interaction"])
log = get_logger("interaction")


class CreateInteractionTaskBody(BaseModel):
    groups: list[str] = Field(..., min_length=1, description="目标群组 username 列表")
    scan_limit: int = Field(300, ge=10, le=5000)
    valid_only: bool = Field(
        False,
        description="为 True 时仅执行数据库中存在的群组，忽略未知项",
    )


class RegisterTargetGroupsBody(BaseModel):
    usernames: list[str] = Field(..., min_length=1, description="写入 groups 表的群组 username")


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


def _partition_groups(db: Session, normalized: list[str]) -> tuple[list[str], list[str]]:
    if not normalized:
        return [], []
    rows = db.query(Group).filter(Group.username.in_(normalized)).all()
    found = {g.username for g in rows}
    valid = [x for x in normalized if x in found]
    invalid = [x for x in normalized if x not in found]
    return valid, invalid


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


@router.post("/interaction/target-groups/register")
def register_target_groups(
    body: RegisterTargetGroupsBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    """将未在 groups 表的 username 写入库（与系统目标群组数据源统一）。"""
    perform_daily_reset_if_needed(db)
    normalized = [_normalize_chat_identifier(u) for u in body.usernames if str(u).strip()]
    normalized = list(dict.fromkeys(normalized))
    if not normalized:
        raise HTTPException(status_code=400, detail="请提供至少一个群组")

    existing = db.query(Group).filter(Group.username.in_(normalized)).all()
    already = {g.username for g in existing}
    have = set(already)
    added: list[str] = []
    for un in normalized:
        if un in have:
            continue
        db.add(
            Group(
                username=un,
                title=un,
                status="normal",
                daily_limit=30,
                members_count=0,
                total_added=0,
                today_added=0,
                yesterday_added=0,
                yesterday_left=0,
                failed_streak=0,
            )
        )
        added.append(un)
        have.add(un)
    db.commit()
    skipped = [u for u in normalized if u in already]
    log.info("interaction register_target_groups user=%s added=%s skipped=%s", user.id, added, skipped)
    return {"ok": True, "added": added, "skipped": skipped}


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

    valid, invalid = _partition_groups(db, normalized)
    if body.valid_only:
        if not valid:
            raise HTTPException(
                status_code=400,
                detail="没有已在目标群组库中的项，请先在「目标群组」中登记或勾选仅有效项",
            )
        normalized = valid
    elif invalid:
        return {
            "ok": False,
            "code": "UNKNOWN_GROUPS",
            "valid_groups": valid,
            "invalid_groups": invalid,
            "message": "部分群组不在目标群组库中",
        }

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
    job_id = uuid.uuid4().hex
    interaction_live_init(job_id, owner_id=user.id, task_id=tid)
    log.info(
        "interaction task created id=%s job=%s user=%s groups=%s accounts=%s",
        tid,
        job_id,
        user.id,
        len(normalized),
        len(accounts),
    )

    task_run_start()
    register_interaction_job(job_id)

    def _runner() -> None:
        run_interaction_task_sync(tid, job_id)

    threading.Thread(target=_runner, name=f"interaction-{tid}", daemon=True).start()
    return {"ok": True, "job_id": job_id, "task": _task_to_dict(task)}


@router.get("/interaction/live/{job_id}")
def interaction_live_status(
    job_id: str,
    user: User = Depends(require_user_or_admin),
) -> dict:
    snap = interaction_live_snapshot(job_id)
    if snap is None:
        raise HTTPException(status_code=404, detail="会话不存在或已过期")
    if user.role != "admin" and snap["owner_id"] != user.id:
        raise HTTPException(status_code=403, detail="无权查看该会话")
    return {
        "ok": True,
        "job_id": job_id,
        "task_id": snap["task_id"],
        "status": snap["status"],
        "logs": snap["logs"],
    }


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
