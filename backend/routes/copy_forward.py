"""Copy 转发：机器人库 + 任务 + 日志 API"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, joinedload

from auth import get_current_user_optional, require_admin, require_user_or_admin
from database import get_db
from models import CopyBot, CopyTask, ForwardRecord, User
from services import copy_forward_service as cfs

router = APIRouter(prefix="/copy", tags=["copy_forward"])


def _is_copy_admin(user: User) -> bool:
    return (user.role or "").lower() == "admin"


def _can_modify_copy_task(user: User, task: CopyTask) -> bool:
    return _is_copy_admin(user) or task.owner_id == user.id


def _copy_task_by_id(db: Session, task_id: int) -> CopyTask | None:
    return db.query(CopyTask).filter(CopyTask.id == task_id).first()


def _copy_task_with_owner(db: Session, task_id: int) -> CopyTask | None:
    return (
        db.query(CopyTask)
        .options(joinedload(CopyTask.owner))
        .filter(CopyTask.id == task_id)
        .first()
    )


class CopyBotCreate(BaseModel):
    api_id: int = Field(..., ge=1)
    api_hash: str = Field(..., min_length=8, max_length=64)
    bot_token: str = Field(..., min_length=20, max_length=256)


class CopyTaskCreate(BaseModel):
    source_channel: str = Field(..., min_length=1, max_length=255)
    target_channel: str = Field(..., min_length=1, max_length=255)
    bot_id: int = Field(..., ge=1)


def _mask(s: str, keep: int = 4) -> str:
    t = (s or "").strip()
    if len(t) <= keep * 2:
        return "***"
    return t[:keep] + "…" + t[-keep:]


def _task_to_dict(r: CopyTask, day: str) -> dict:
    today = r.today_forwarded if r.stats_utc_date == day else 0
    owner_username = getattr(getattr(r, "owner", None), "username", None)
    return {
        "id": r.id,
        "owner_id": r.owner_id,
        "owner_username": owner_username,
        "source_channel": r.source_channel,
        "target_channel": r.target_channel,
        "bot_id": r.bot_id,
        "status": r.status,
        "last_run_at": r.last_run_at.isoformat() if r.last_run_at else None,
        "last_error": r.last_error,
        "total_forwarded": r.total_forwarded or 0,
        "today_forwarded": today,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


@router.get("/bots", response_model=dict)
def list_bots(user: User | None = Depends(get_current_user_optional), db: Session = Depends(get_db)):
    _ = user
    rows = db.query(CopyBot).order_by(CopyBot.id.desc()).all()
    for r in rows:
        cfs.reconcile_copy_bot_session_name(r, db)
    items = [
        {
            "id": r.id,
            "api_id": r.api_id,
            "api_hash_masked": _mask(r.api_hash, 3),
            "bot_token_masked": _mask(r.bot_token, 6),
            "session_name": r.session_name,
            "session_ready": cfs.bot_session_ready(r),
            "session_ok": cfs.bot_session_ready(r) and (r.status or "").lower() == "active",
            "status": r.status,
            "last_error": r.last_error,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "is_running_task": cfs.bot_has_active_copy_tasks_sync(r.id),
        }
        for r in rows
    ]
    return {"ok": True, "bots": items}


@router.post("/bots", response_model=dict)
def create_bot(
    payload: CopyBotCreate,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    row = CopyBot(
        owner_id=user.id,
        api_id=payload.api_id,
        api_hash=payload.api_hash.strip(),
        bot_token=payload.bot_token.strip(),
        session_name=None,
        status="active",
        last_error=None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    try:
        name = cfs.bootstrap_new_bot_session(
            row.id,
            int(payload.api_id),
            payload.api_hash.strip(),
            payload.bot_token.strip(),
        )
    except Exception as exc:
        row.status = "error"
        row.last_error = str(exc)[:2000]
        db.add(row)
        db.commit()
        raise HTTPException(status_code=400, detail=f"创建 session 失败: {exc}") from exc
    row.session_name = name
    row.status = "active"
    row.last_error = None
    db.add(row)
    db.commit()
    db.refresh(row)
    cfs.append_log("info", f"新增机器人 id={row.id} session={name}", bot_id=row.id)
    return {"ok": True, "id": row.id, "session_name": name}


@router.delete("/bots/{bot_id}", response_model=dict)
def delete_bot(
    bot_id: int,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    row = db.query(CopyBot).filter(CopyBot.id == bot_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="机器人不存在")
    cfs.wait_force_disconnect_bot(bot_id)
    tasks = db.query(CopyTask).filter(CopyTask.bot_id == bot_id).all()
    for t in tasks:
        cfs.wait_pause_task(t.id)
    task_ids = [x.id for x in tasks]
    if task_ids:
        db.query(ForwardRecord).filter(ForwardRecord.task_id.in_(task_ids)).delete(synchronize_session=False)
    for t in tasks:
        db.delete(t)
    if row.session_name:
        sn = row.session_name.strip()
        for p in cfs.SESSIONS_DIR.glob(f"{sn}.session*"):
            try:
                p.unlink()
            except OSError:
                pass
    db.delete(row)
    db.commit()
    return {"ok": True}


@router.post("/bots/{bot_id}/session", response_model=dict)
async def upload_bot_session(
    bot_id: int,
    file: UploadFile = File(...),
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    row = db.query(CopyBot).filter(CopyBot.id == bot_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="机器人不存在")
    content = await file.read()
    if len(content) < 32:
        raise HTTPException(status_code=400, detail="session 文件过小")
    cfs.SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    name = f"bot_{bot_id}"
    dest = cfs.SESSIONS_DIR / f"{name}.session"
    for p in cfs.SESSIONS_DIR.glob(f"{name}.session*"):
        try:
            p.unlink()
        except OSError:
            pass
    dest.write_bytes(content)
    try:
        cfs.verify_session_connect(int(row.api_id), row.api_hash, name, bot_id=bot_id)
    except Exception as exc:
        try:
            dest.unlink(missing_ok=True)
        except OSError:
            pass
        raise HTTPException(status_code=400, detail=f"session 无效或与 api_id/api_hash 不匹配: {exc}") from exc
    row.session_name = name
    row.status = "active"
    row.last_error = None
    db.add(row)
    db.commit()
    cfs.append_log("info", f"已手动导入 session bot_id={bot_id}", bot_id=bot_id)
    return {"ok": True, "session_name": name}


@router.post("/bots/{bot_id}/reset", response_model=dict)
def reset_bot_error(
    bot_id: int,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    row = db.query(CopyBot).filter(CopyBot.id == bot_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="机器人不存在")
    row.status = "active"
    row.last_error = None
    db.add(row)
    db.commit()
    return {"ok": True}


@router.get("/tasks", response_model=dict)
def list_tasks(user: User | None = Depends(get_current_user_optional), db: Session = Depends(get_db)):
    _ = user
    rows = (
        db.query(CopyTask)
        .options(joinedload(CopyTask.owner))
        .order_by(CopyTask.id.desc())
        .all()
    )
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    items = [_task_to_dict(r, day) for r in rows]
    return {"ok": True, "tasks": items}


@router.post("/tasks", response_model=dict)
def create_task(
    payload: CopyTaskCreate,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
):
    bot = db.query(CopyBot).filter(CopyBot.id == payload.bot_id).first()
    if not bot:
        raise HTTPException(status_code=400, detail="请从机器人库选择有效的 Bot")
    cfs.reconcile_copy_bot_session_name(bot, db)
    if not cfs.bot_session_ready(bot):
        raise HTTPException(status_code=400, detail="该 Bot 尚未生成 session，无法创建转发任务")
    src = payload.source_channel.strip()
    tgt = payload.target_channel.strip()
    if src.lower() == tgt.lower():
        raise HTTPException(status_code=400, detail="来源与目标不能相同")

    row = CopyTask(
        owner_id=user.id,
        source_channel=src,
        target_channel=tgt,
        bot_id=bot.id,
        status="idle",
        total_forwarded=0,
        today_forwarded=0,
        stats_utc_date=None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    cfs.append_log("info", f"新建转发任务 id={row.id}", task_id=row.id, bot_id=bot.id)
    return {"ok": True, "id": row.id}


@router.post("/tasks/{task_id}/start", response_model=dict)
def start_task_route(task_id: int, user: User = Depends(require_user_or_admin), db: Session = Depends(get_db)):
    row = _copy_task_by_id(db, task_id)
    if not row:
        raise HTTPException(status_code=404, detail="任务不存在")
    if not _can_modify_copy_task(user, row):
        raise HTTPException(status_code=403, detail="无权限操作该任务")
    if row.status in ("running", "starting"):
        raise HTTPException(status_code=400, detail="任务已在运行或正在启动中")
    bot = db.query(CopyBot).filter(CopyBot.id == row.bot_id).first()
    if not bot:
        raise HTTPException(status_code=400, detail="关联 Bot 不存在")
    cfs.reconcile_copy_bot_session_name(bot, db)
    if not cfs.bot_session_ready(bot):
        raise HTTPException(status_code=400, detail="未生成 session：请导入 session 或由管理员重新录入 Bot")
    row.status = "starting"
    row.last_error = None
    db.add(row)
    db.commit()
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cfs.schedule_start_task(task_id)
    row_out = _copy_task_with_owner(db, task_id)
    return {"ok": True, "message": "已提交启动", "task": _task_to_dict(row_out, day) if row_out else None}


@router.post("/tasks/{task_id}/pause", response_model=dict)
def pause_task_route(task_id: int, user: User = Depends(require_user_or_admin), db: Session = Depends(get_db)):
    row = _copy_task_by_id(db, task_id)
    if not row:
        raise HTTPException(status_code=404, detail="任务不存在")
    if not _can_modify_copy_task(user, row):
        raise HTTPException(status_code=403, detail="无权限操作该任务")
    cfs.schedule_pause_task(task_id)
    cfs.wait_pause_task(task_id)
    db.expire_all()
    row2 = _copy_task_with_owner(db, task_id)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "ok": True,
        "message": "已暂停",
        "task": _task_to_dict(row2, day) if row2 else None,
    }


@router.delete("/tasks/{task_id}", response_model=dict)
def delete_task_route(task_id: int, user: User = Depends(require_user_or_admin), db: Session = Depends(get_db)):
    row = _copy_task_by_id(db, task_id)
    if not row:
        raise HTTPException(status_code=404, detail="任务不存在")
    if not _can_modify_copy_task(user, row):
        raise HTTPException(status_code=403, detail="无权限操作该任务")
    cfs.wait_pause_task(task_id)
    db.query(ForwardRecord).filter(ForwardRecord.task_id == task_id).delete(synchronize_session=False)
    db.delete(row)
    db.commit()
    cfs.append_log("warn", f"任务已删除 id={task_id}", task_id=task_id)
    return {"ok": True}


@router.get("/logs", response_model=dict)
def get_logs(limit: int = 200, user: User | None = Depends(get_current_user_optional)):
    _ = user
    return {"ok": True, "logs": cfs.log_snapshot(min(limit, 500))}
