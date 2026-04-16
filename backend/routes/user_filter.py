from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from telethon import TelegramClient
from telethon.errors import (
    ApiIdInvalidError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    RPCError,
    SessionPasswordNeededError,
)

from auth import require_user_or_admin
from database import get_db
from models import FilterAccount, Proxy, ScraperTask, User, UserFilterResult, UserFilterTask
from settings import TELEGRAM_API_HASH, TELEGRAM_API_ID
from services.user_filter_service import (
    export_results_csv,
    init_live,
    live_snapshot,
    request_stop,
    spawn_task,
)

router = APIRouter(prefix="/user-filter", tags=["user-filter"])


class CreateFilterTaskBody(BaseModel):
    name: str = Field("用户筛选任务", max_length=255)
    source_task_id: int = Field(..., ge=1)
    test_group: str = Field(..., min_length=3, max_length=255)
    real_verify_enabled: bool = Field(False)
    real_verify_ratio: float = Field(0.1, ge=0.0, le=1.0)


class CreateFilterAccountBody(BaseModel):
    type: str = Field("probe", pattern="^(probe|real)$")
    phone: str | None = Field(None, min_length=5, max_length=32)
    api_id: int | None = Field(None)
    api_hash: str | None = Field(None, max_length=128)
    session_path: str = Field(..., min_length=3, max_length=500)
    status: str = Field("active", pattern="^(active|banned|idle)$")
    proxy_id: int | None = Field(None)


class FilterAccountSendCodeBody(BaseModel):
    type: str = Field("probe", pattern="^(probe|real)$")
    phone: str = Field(..., min_length=5, max_length=32)
    api_id: int | None = Field(None)
    api_hash: str | None = Field(None, max_length=128)
    session_path: str | None = Field(None, max_length=500)


class FilterAccountLoginBody(BaseModel):
    type: str = Field("probe", pattern="^(probe|real)$")
    phone: str = Field(..., min_length=5, max_length=32)
    code: str | None = Field(None, max_length=32)
    phone_code_hash: str | None = Field(None, max_length=128)
    password: str | None = Field(None, max_length=256)
    api_id: int | None = Field(None)
    api_hash: str | None = Field(None, max_length=128)
    session_path: str | None = Field(None, max_length=500)
    proxy_id: int | None = Field(None)


def _normalize_phone_e164(phone: str) -> str:
    digits = "".join(ch for ch in str(phone or "").strip() if ch.isdigit())
    if len(digits) < 8:
        raise HTTPException(status_code=400, detail="手机号格式无效")
    return f"+{digits}"


def _resolve_session_base_from_path(raw: str | None, phone_e164: str | None = None) -> Path:
    s = str(raw or "").strip()
    if s:
        p = Path(s)
        if not p.is_absolute():
            p = (Path(__file__).resolve().parent.parent / p).resolve()
    else:
        sessions_dir = (Path(__file__).resolve().parent.parent / "sessions" / "filter").resolve()
        sessions_dir.mkdir(parents=True, exist_ok=True)
        digits = "".join(ch for ch in str(phone_e164 or "") if ch.isdigit()) or "unknown"
        p = sessions_dir / f"filter_{digits}"
    if p.suffix == ".session":
        p = p.with_suffix("")
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _guess_phone_from_session(path_base: Path) -> str | None:
    stem = path_base.stem
    m = re.search(r"(\d{8,15})", stem)
    if not m:
        return None
    return f"+{m.group(1)}"


def _upsert_filter_account(
    db: Session,
    *,
    owner_id: int,
    acc_type: str,
    phone: str,
    api_id: int | None,
    api_hash: str | None,
    session_path: str,
    status: str,
    proxy_id: int | None,
) -> FilterAccount:
    row = (
        db.query(FilterAccount)
        .filter(FilterAccount.owner_id == owner_id, FilterAccount.type == acc_type, FilterAccount.phone == phone)
        .order_by(FilterAccount.id.desc())
        .first()
    )
    if row is None:
        row = FilterAccount(
            owner_id=owner_id,
            type=acc_type,
            phone=phone,
            api_id=api_id,
            api_hash=api_hash,
            session_path=session_path,
            status=status,
            proxy_id=proxy_id,
        )
    else:
        row.api_id = api_id
        row.api_hash = api_hash
        row.session_path = session_path
        row.status = status
        row.proxy_id = proxy_id
    row.last_used_at = datetime.now(timezone.utc)
    db.add(row)
    if proxy_id:
        proxy = db.query(Proxy).filter(Proxy.id == int(proxy_id)).first()
        if proxy is not None:
            # 优先级：real > probe > listener > scraper > growth > unknown
            current = str(proxy.usage_type or "unknown").lower()
            target = "real" if acc_type == "real" else "probe"
            order = {"unknown": 0, "growth": 1, "scraper": 2, "listener": 3, "probe": 4, "real": 5}
            if order.get(target, 0) >= order.get(current, 0):
                proxy.usage_type = target
                db.add(proxy)
    db.commit()
    db.refresh(row)
    return row


def _task_to_dict(row: UserFilterTask) -> dict:
    return {
        "id": row.id,
        "name": row.name,
        "owner_id": row.owner_id,
        "source_group_id": row.source_group_id,
        "source_task_id": row.source_task_id,
        "status": row.status,
        "total_users": row.total_users or 0,
        "processed_users": row.processed_users or 0,
        "success_count": row.success_count or 0,
        "fail_count": row.fail_count or 0,
        "real_verify_enabled": bool(row.real_verify_enabled),
        "real_verify_ratio": float(row.real_verify_ratio or 0.0),
        "last_error": row.last_error,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _account_to_dict(row: FilterAccount) -> dict:
    return {
        "id": row.id,
        "owner_id": row.owner_id,
        "type": row.type,
        "phone": row.phone,
        "api_id": row.api_id,
        "api_hash": row.api_hash,
        "session_path": row.session_path,
        "status": row.status,
        "last_used_at": row.last_used_at.isoformat() if row.last_used_at else None,
        "proxy_id": row.proxy_id,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


@router.get("/sources")
def list_filter_sources(
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    q = db.query(ScraperTask).filter(ScraperTask.status == "done", ScraperTask.user_count > 0).order_by(ScraperTask.id.desc())
    rows = q.limit(200).all()
    return {
        "ok": True,
        "sources": [
            {
                "id": r.id,
                "group_link": r.group_link,
                "group_name": r.group_name,
                "user_count": r.user_count,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "owner_scope": "global" if user.role == "admin" else "self",
            }
            for r in rows
        ],
    }


@router.post("/tasks")
def create_filter_task(
    body: CreateFilterTaskBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    source = db.query(ScraperTask).filter(ScraperTask.id == body.source_task_id).first()
    if source is None or source.status != "done":
        raise HTTPException(status_code=400, detail="来源采集任务不存在或未完成")
    test_group = str(body.test_group or "").strip()
    if not test_group:
        raise HTTPException(status_code=400, detail="测试群组不能为空")

    task = UserFilterTask(
        owner_id=user.id,
        name=(body.name or "").strip()[:255] or "用户筛选任务",
        source_group_id=test_group[:255],
        source_task_id=source.id,
        status="idle",
        total_users=int(source.user_count or 0),
        processed_users=0,
        success_count=0,
        fail_count=0,
        real_verify_enabled=1 if body.real_verify_enabled else 0,
        real_verify_ratio=float(body.real_verify_ratio),
    )
    db.add(task)
    db.commit()
    db.refresh(task)

    job_id = uuid.uuid4().hex
    init_live(job_id, user.id, task.id)
    spawn_task(task.id, job_id)
    return {"ok": True, "job_id": job_id, "task": _task_to_dict(task)}


@router.post("/tasks/{task_id}/stop")
def stop_filter_task(
    task_id: int,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    row = db.query(UserFilterTask).filter(UserFilterTask.id == task_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    if user.role != "admin" and row.owner_id != user.id:
        raise HTTPException(status_code=403, detail="无权操作该任务")
    request_stop(task_id=task_id)
    return {"ok": True}


@router.get("/tasks")
def list_filter_tasks(
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    q = db.query(UserFilterTask).order_by(UserFilterTask.id.desc())
    if user.role != "admin":
        q = q.filter(UserFilterTask.owner_id == user.id)
    rows = q.limit(200).all()
    return {"ok": True, "tasks": [_task_to_dict(r) for r in rows]}


@router.get("/tasks/{task_id}/results")
def list_filter_results(
    task_id: int,
    can_invite: int | None = None,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    task = db.query(UserFilterTask).filter(UserFilterTask.id == task_id).first()
    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    if user.role != "admin" and task.owner_id != user.id:
        raise HTTPException(status_code=403, detail="无权查看该任务")

    q = db.query(UserFilterResult).filter(UserFilterResult.task_id == task_id).order_by(UserFilterResult.id.desc())
    if can_invite in (0, 1):
        q = q.filter(UserFilterResult.can_invite == int(can_invite))
    rows = q.limit(5000).all()
    return {
        "ok": True,
        "results": [
            {
                "id": r.id,
                "task_id": r.task_id,
                "user_id": r.user_id,
                "username": r.username,
                "phone": r.phone,
                "can_invite": bool(r.can_invite),
                "fail_reason": r.fail_reason,
                "probe_account_id": r.probe_account_id,
                "verified_by_real": bool(r.verified_by_real),
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
    }


@router.get("/live/{job_id}")
def get_filter_live(job_id: str, user: User = Depends(require_user_or_admin)) -> dict:
    snap = live_snapshot(job_id)
    if snap is None:
        raise HTTPException(status_code=404, detail="会话不存在")
    if user.role != "admin" and int(snap["owner_id"]) != int(user.id):
        raise HTTPException(status_code=403, detail="无权查看")
    return {"ok": True, "job_id": job_id, "status": snap["status"], "logs": snap["logs"], "task_id": snap["task_id"]}


@router.get("/download/{task_id}")
def download_filter_results(
    task_id: int,
    scope: str = "all",
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
):
    task = db.query(UserFilterTask).filter(UserFilterTask.id == task_id).first()
    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    if user.role != "admin" and task.owner_id != user.id:
        raise HTTPException(status_code=403, detail="无权下载")
    only_invitable = str(scope).lower() in ("invitable", "success", "can_invite")
    p = export_results_csv(task_id, only_invitable=only_invitable)
    if not p.is_file():
        raise HTTPException(status_code=404, detail="导出文件不存在")
    return FileResponse(
        path=p,
        media_type="text/csv; charset=utf-8",
        filename=p.name,
        headers={"Content-Disposition": f'attachment; filename="{p.name}"'},
    )


@router.get("/accounts")
def list_filter_accounts(
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    q = db.query(FilterAccount).order_by(FilterAccount.id.desc())
    if user.role != "admin":
        q = q.filter(FilterAccount.owner_id == user.id)
    rows = q.limit(500).all()
    return {"ok": True, "accounts": [_account_to_dict(r) for r in rows]}


@router.post("/accounts")
def create_filter_account(
    body: CreateFilterAccountBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    session_base = _resolve_session_base_from_path(body.session_path)
    session_file = session_base.with_suffix(".session")
    if not session_file.is_file():
        raise HTTPException(status_code=400, detail="session 文件不存在，请检查路径")
    phone = (body.phone or "").strip()
    if not phone:
        phone = _guess_phone_from_session(session_base) or f"session:{session_base.name}"
    row = _upsert_filter_account(
        db,
        owner_id=user.id,
        acc_type=body.type,
        phone=phone,
        api_id=body.api_id,
        api_hash=(body.api_hash or "").strip() or None,
        session_path=str(session_base.resolve()),
        status=body.status,
        proxy_id=body.proxy_id,
    )
    return {"ok": True, "account": _account_to_dict(row)}


@router.post("/accounts/send_code")
async def send_filter_account_code(
    body: FilterAccountSendCodeBody,
    user: User = Depends(require_user_or_admin),
) -> dict:
    phone = _normalize_phone_e164(body.phone)
    api_id = int(body.api_id or TELEGRAM_API_ID)
    api_hash = (body.api_hash or TELEGRAM_API_HASH).strip()
    if not api_hash:
        raise HTTPException(status_code=400, detail="api_hash 不能为空")
    session_base = _resolve_session_base_from_path(body.session_path, phone)
    client = TelegramClient(str(session_base), api_id, api_hash)
    try:
        await client.connect()
        try:
            sent = await client.send_code_request(phone)
        except PhoneNumberInvalidError:
            raise HTTPException(status_code=400, detail="手机号无效或未开通 Telegram")
        except ApiIdInvalidError:
            raise HTTPException(status_code=400, detail="API_ID / API_HASH 配置无效")
        except FloodWaitError as exc:
            raise HTTPException(status_code=429, detail=f"请求过频，请 {exc.seconds}s 后再试")
        except RPCError as exc:
            raise HTTPException(status_code=400, detail=f"发送验证码失败: {exc.__class__.__name__}")
        hash_ = getattr(sent, "phone_code_hash", None)
        if not hash_:
            raise HTTPException(status_code=500, detail="未获取到 phone_code_hash")
        return {
            "ok": True,
            "phone": phone,
            "phone_code_hash": hash_,
            "session_path": str(session_base.resolve()),
        }
    finally:
        await client.disconnect()


@router.post("/accounts/login")
async def login_filter_account(
    body: FilterAccountLoginBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    phone = _normalize_phone_e164(body.phone)
    api_id = int(body.api_id or TELEGRAM_API_ID)
    api_hash = (body.api_hash or TELEGRAM_API_HASH).strip()
    if not api_hash:
        raise HTTPException(status_code=400, detail="api_hash 不能为空")
    session_base = _resolve_session_base_from_path(body.session_path, phone)
    client = TelegramClient(str(session_base), api_id, api_hash)
    code = (body.code or "").strip()
    pch = (body.phone_code_hash or "").strip()
    pwd = (body.password or "").strip()
    try:
        await client.connect()
        # 只要用户填写了二步验证密码，就优先走密码登录分支，避免被残留验证码干扰。
        if pwd:
            try:
                await client.sign_in(password=pwd)
            except PasswordHashInvalidError:
                raise HTTPException(status_code=400, detail="密码错误")
            except FloodWaitError as exc:
                raise HTTPException(status_code=429, detail=f"请求过频，请 {exc.seconds}s 后再试")
            except RPCError as exc:
                raise HTTPException(status_code=400, detail=f"登录失败: {exc.__class__.__name__}: {exc}")
        else:
            if not code or not pch:
                raise HTTPException(status_code=400, detail="请先发送验证码并填写验证码")
            try:
                await client.sign_in(phone, code, phone_code_hash=pch)
            except PhoneCodeInvalidError:
                raise HTTPException(status_code=400, detail="验证码错误")
            except PhoneCodeExpiredError:
                raise HTTPException(status_code=400, detail="验证码已过期，请重新发送")
            except SessionPasswordNeededError:
                return {"need_password": True}
            except FloodWaitError as exc:
                raise HTTPException(status_code=429, detail=f"请求过频，请 {exc.seconds}s 后再试")
            except RPCError as exc:
                raise HTTPException(status_code=400, detail=f"登录失败: {exc.__class__.__name__}: {exc}")
        if not await client.is_user_authorized():
            raise HTTPException(status_code=400, detail="登录未生效，请重试")
    finally:
        await client.disconnect()

    row = _upsert_filter_account(
        db,
        owner_id=user.id,
        acc_type=body.type,
        phone=phone,
        api_id=api_id,
        api_hash=api_hash,
        session_path=str(session_base.resolve()),
        status="active",
        proxy_id=body.proxy_id,
    )
    return {"ok": True, "account": _account_to_dict(row)}


@router.delete("/accounts/{account_id}")
def delete_filter_account(
    account_id: int,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    row = db.query(FilterAccount).filter(FilterAccount.id == account_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail="账号不存在")
    if user.role != "admin" and row.owner_id != user.id:
        raise HTTPException(status_code=403, detail="无权删除")
    db.delete(row)
    db.commit()
    return {"ok": True}
