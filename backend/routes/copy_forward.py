"""Copy 转发：机器人库 + 任务 + 日志 API"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, joinedload

from auth import get_current_user_optional, require_admin, require_user_or_admin
from database import get_db
from models import CopyBot, CopyListenerAccount, CopyTask, ForwardRecord, User
from services import copy_forward_service as cfs
from services import copy_listener_service as cls

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
    bot_token: str = Field(..., min_length=20, max_length=256)


class CopyTaskCreate(BaseModel):
    source_channel: str = Field(..., min_length=1, max_length=255)
    target_channel: str = Field(..., min_length=1, max_length=255)
    bot_id: int = Field(..., ge=1)
    listener_id: int | None = Field(None, ge=1)


class ListenerSendCodeBody(BaseModel):
    phone: str = Field(..., min_length=5)


class ListenerLoginBody(BaseModel):
    phone: str = Field(..., min_length=5)
    code: str = Field("", max_length=32)
    phone_code_hash: str = Field("")
    password: str | None = Field(None, max_length=256)


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
        "listener_id": r.listener_id,
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


@router.get("/listeners", response_model=dict)
def list_listeners(user: User | None = Depends(get_current_user_optional), db: Session = Depends(get_db)):
    _ = user
    rows = db.query(CopyListenerAccount).order_by(CopyListenerAccount.id.desc()).all()
    items = []
    for r in rows:
        run_count = db.query(CopyTask).filter(CopyTask.listener_id == r.id, CopyTask.status == "running").count()
        session_ready = cls.session_ready(r.session_name)
        session_status = "ACTIVE"
        if run_count > 0:
            session_status = "IN_USE"
        elif not session_ready or (r.status or "").lower() == "error":
            session_status = "EXPIRED"
        items.append(
            {
                "id": r.id,
                "phone": r.phone,
                "session_name": r.session_name,
                "session_ready": session_ready,
                "session_status": session_status,
                "status": r.status,
                "enabled": bool(r.enabled),
                "last_error": r.last_error,
                "last_seen_at": r.last_seen_at.isoformat() if r.last_seen_at else None,
                "running_tasks": run_count,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
        )
    return {"ok": True, "listeners": items}


@router.post("/listeners/send_code", response_model=dict)
async def listener_send_code(
    payload: ListenerSendCodeBody,
    user: User = Depends(require_admin),
):
    _ = user
    res = await cls.send_code_request(payload.phone)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "发送验证码失败"))
    cfs.append_log(
        "info",
        f"[LISTENER] 验证码已发送 phone={res.get('phone')} type={res.get('sent_type') or '-'} next={res.get('next_type') or '-'}",
    )
    return res


@router.post("/listeners/login", response_model=dict)
async def listener_login(
    payload: ListenerLoginBody,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    _ = user
    res = await cls.complete_login(
        db,
        user.id,
        payload.phone,
        payload.code,
        (payload.phone_code_hash or "").strip(),
        payload.password,
    )
    if res.get("need_password"):
        return {"need_password": True}
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "监听账号登录失败"))
    cfs.append_log("info", f"[LISTENER] 登录成功 phone={res.get('phone')}")
    return res


@router.post("/listeners/{listener_id}/enable", response_model=dict)
def enable_listener(listener_id: int, user: User = Depends(require_admin), db: Session = Depends(get_db)):
    _ = user
    row = db.query(CopyListenerAccount).filter(CopyListenerAccount.id == listener_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="监听账号不存在")
    row.enabled = 1
    row.status = "active"
    row.last_error = None
    db.add(row)
    db.commit()
    return {"ok": True}


@router.post("/listeners/{listener_id}/disable", response_model=dict)
def disable_listener(listener_id: int, user: User = Depends(require_admin), db: Session = Depends(get_db)):
    _ = user
    row = db.query(CopyListenerAccount).filter(CopyListenerAccount.id == listener_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="监听账号不存在")
    row.enabled = 0
    row.status = "disconnected"
    db.add(row)
    db.commit()
    cfs.schedule_stop_listener(listener_id)
    return {"ok": True}


@router.delete("/listeners/{listener_id}", response_model=dict)
def delete_listener(listener_id: int, user: User = Depends(require_admin), db: Session = Depends(get_db)):
    _ = user
    row = db.query(CopyListenerAccount).filter(CopyListenerAccount.id == listener_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="监听账号不存在")
    cfs.schedule_stop_listener(listener_id)
    db.query(CopyTask).filter(CopyTask.listener_id == listener_id).update({"listener_id": None})
    db.delete(row)
    db.commit()
    return {"ok": True}


@router.post("/bots", response_model=dict)
def create_bot(
    payload: CopyBotCreate,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    row = CopyBot(
        owner_id=user.id,
        api_id=0,
        api_hash="GLOBAL",
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
        cfs.verify_session_connect(name, bot_id=bot_id)
    except Exception as exc:
        try:
            dest.unlink(missing_ok=True)
        except OSError:
            pass
        raise HTTPException(status_code=400, detail=f"session 无效或与系统全局 Telegram 配置不匹配: {exc}") from exc
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
    listener_id = payload.listener_id
    if listener_id:
        listener = db.query(CopyListenerAccount).filter(CopyListenerAccount.id == listener_id).first()
        if not listener:
            raise HTTPException(status_code=400, detail="选择的监听账号不存在")
        if not bool(listener.enabled) or (listener.status or "").lower() != "active":
            raise HTTPException(status_code=400, detail="监听账号不可用，请更换")
        if not cls.session_ready(listener.session_name):
            raise HTTPException(status_code=400, detail="监听账号 session 缺失，请重新登录")
    else:
        auto_listener = (
            db.query(CopyListenerAccount)
            .filter(CopyListenerAccount.enabled == 1, CopyListenerAccount.status == "active")
            .order_by(CopyListenerAccount.last_seen_at.asc().nullsfirst(), CopyListenerAccount.id.asc())
            .first()
        )
        if not auto_listener:
            raise HTTPException(status_code=400, detail="请先配置可用监听账号（Listener）")
        listener_id = auto_listener.id

    row = CopyTask(
        owner_id=user.id,
        source_channel=src,
        target_channel=tgt,
        bot_id=bot.id,
        listener_id=listener_id,
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
