from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_admin
from database import get_db
from models import TaskRecord, User


router = APIRouter(prefix="/users", tags=["users"])

ONLINE_WINDOW_SEC = 300  # 5 分钟内有任务活动视为在线


class UpdateUserRoleRequest(BaseModel):
    role: str = Field(..., pattern="^(admin|user)$")


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _task_action_label(status: str) -> str:
    s = (status or "").lower()
    if s in ("pending", "queued", "idle"):
        return "创建任务"
    if s in ("running", "connecting", "starting"):
        return "启动任务"
    if s in ("stopped", "cancelled", "paused"):
        return "停止任务"
    if s in ("error", "failed"):
        return "任务失败"
    if s in ("completed", "success", "done", "accepted"):
        return "完成任务"
    return "任务更新"


def _build_user_analytics(db: Session, users: list[User]) -> tuple[dict, dict[int, dict]]:
    """基于 task_records 聚合行为数据（无任务时回退到账号创建时间）。"""
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    now = datetime.now(timezone.utc)

    per: dict[int, dict] = defaultdict(
        lambda: {
            "action_count_today": 0,
            "total_actions": 0,
            "last_active_at": None,
            "activity_log": [],
        }
    )

    # 正序：累计总数、今日数、最后活跃时间
    for t in db.query(TaskRecord).order_by(TaskRecord.created_at.asc()).all():
        oid = t.owner_id
        p = per[oid]
        p["total_actions"] += 1
        ca = _ensure_utc(t.created_at)
        if ca is not None and ca >= today_start:
            p["action_count_today"] += 1
        if ca is not None:
            p["last_active_at"] = ca.isoformat()

    # 倒序：最近 10 条操作描述
    for t in db.query(TaskRecord).order_by(TaskRecord.created_at.desc()).all():
        oid = t.owner_id
        p = per[oid]
        if len(p["activity_log"]) >= 10:
            continue
        ca = _ensure_utc(t.created_at)
        at_iso = ca.isoformat() if ca else ""
        group = (t.group_name or "").strip()[:48]
        detail = group if group else f"任务 #{t.id}"
        p["activity_log"].append(
            {
                "at": at_iso,
                "message": _task_action_label(t.status),
                "detail": detail,
            }
        )

    user_payload: dict[int, dict] = {}
    for u in users:
        pid = u.id
        base = per[pid]
        created = _ensure_utc(u.created_at)
        created_iso = created.isoformat() if created else None
        last_iso = base["last_active_at"] or created_iso
        last_dt = None
        if last_iso:
            try:
                last_dt = _ensure_utc(datetime.fromisoformat(last_iso.replace("Z", "+00:00")))
            except ValueError:
                last_dt = None
        if last_dt is None:
            status = "offline"
        else:
            delta = (now - last_dt).total_seconds()
            status = "online" if delta <= ONLINE_WINDOW_SEC else "offline"

        user_payload[pid] = {
            "action_count_today": int(base["action_count_today"]),
            "total_actions": int(base["total_actions"]),
            "last_active_at": last_iso,
            "status": status,
            "activity_log": list(base["activity_log"]),
        }

    active_today = sum(1 for u in users if user_payload[u.id]["action_count_today"] > 0)
    today_actions = sum(user_payload[u.id]["action_count_today"] for u in users)
    admin_count = sum(1 for u in users if (u.role or "").lower() == "admin")

    summary = {
        "total_users": len(users),
        "active_users_today": active_today,
        "today_actions": today_actions,
        "admin_count": admin_count,
    }
    return summary, user_payload


@router.get("")
def list_users(_admin: User = Depends(require_admin), db: Session = Depends(get_db)) -> dict:
    users = db.query(User).order_by(User.id.asc()).all()
    summary, analytics = _build_user_analytics(db, users)
    return {
        "ok": True,
        "summary": summary,
        "users": [
            {
                "id": u.id,
                "username": u.username,
                "role": u.role,
                "avatar_url": u.avatar_url,
                "created_at": u.created_at.isoformat() if u.created_at else None,
                "stats": analytics[u.id],
            }
            for u in users
        ],
    }


@router.put("/{user_id}/role")
def update_user_role(
    user_id: int,
    payload: UpdateUserRoleRequest,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    target = db.query(User).filter(User.id == user_id).first()
    if target is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    if target.id == admin.id and payload.role != "admin":
        raise HTTPException(status_code=400, detail="不能取消自己的管理员权限")

    target.role = payload.role
    db.add(target)
    db.commit()
    return {"ok": True, "id": target.id, "role": target.role}
