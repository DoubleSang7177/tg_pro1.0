from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import case, func
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


def _resolve_account_health(row: FilterAccount, stats: dict | None = None) -> dict:
    st = str(getattr(row, "status", "") or "").lower()
    stats = stats or {}
    flood_count = int(stats.get("flood_count", 0) or 0)
    fail_total = int(stats.get("fail_total", 0) or 0)
    recent_total = int(stats.get("recent_total", 0) or 0)

    if st == "banned":
        return {"state": "abnormal", "label": "异常", "reason": "账号已封禁"}
    if flood_count > 0:
        return {
            "state": "abnormal",
            "label": "异常",
            "reason": f"近期出现 FLOOD({flood_count})，疑似风控",
        }
    if st == "idle":
        return {"state": "warning", "label": "待命", "reason": "账号当前空闲未激活"}
    if fail_total > 0 and recent_total > 0 and fail_total >= max(3, int(recent_total * 0.8)):
        return {"state": "warning", "label": "关注", "reason": "近期失败比例偏高"}
    return {"state": "healthy", "label": "健康", "reason": "近期状态正常"}


def _account_to_dict(row: FilterAccount, stats: dict | None = None) -> dict:
    health = _resolve_account_health(row, stats)
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
        "health_state": health["state"],
        "health_label": health["label"],
        "health_reason": health["reason"],
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
        # 采集来源名称：用于 UI 展示
        source_group_id=(source.group_link or source.group_name or "")[:255],
        source_task_id=source.id,
        # 筛选阶段 Invite 的目标群组：用于筛选执行
        test_group=test_group[:255],
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


@router.get("/results/direct-invitable/latest")
def list_latest_direct_invitable_users(
    limit: int = 5000,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    """用户增长用：口径对齐“用户筛选页未选择具体任务”时的可用用户池。"""
    lim = max(1, min(int(limit or 5000), 20000))
    q_task = db.query(UserFilterTask).order_by(UserFilterTask.id.desc())
    if user.role != "admin":
        q_task = q_task.filter(UserFilterTask.owner_id == user.id)
    task_rows = q_task.limit(500).all()

    # 与前端“筛选结果（未选具体任务）”一致：仅保留同来源最新一次筛选记录。
    seen_source_task: set[str] = set()
    latest_only_tasks: list[UserFilterTask] = []
    for t in task_rows:
        source_key = str(getattr(t, "source_task_id", "") or "")
        if source_key in seen_source_task:
            continue
        seen_source_task.add(source_key)
        latest_only_tasks.append(t)

    task_ids: list[int] = []
    for t in latest_only_tasks:
        st = str(getattr(t, "status", "") or "").lower()
        if st in {"finished", "completed", "stopped", "failed"}:
            tid = int(getattr(t, "id", 0) or 0)
            if tid > 0:
                task_ids.append(tid)
        if len(task_ids) >= 10:  # 与前端合并任务上限保持一致
            break

    if not task_ids:
        return {"ok": True, "usernames": [], "total": 0}

    q = (
        db.query(UserFilterResult)
        .filter(UserFilterResult.task_id.in_(task_ids))
        .filter(UserFilterResult.can_invite == 0)
        .order_by(UserFilterResult.id.desc())
    )
    rows = q.limit(max(lim * 4, lim)).all()
    usernames: list[str] = []
    seen: set[str] = set()
    for r in rows:
        u = str(r.username or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        usernames.append(u)
        if len(usernames) >= lim:
            break
    return {"ok": True, "usernames": usernames, "total": len(usernames)}


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
    scope_norm = str(scope).lower()
    # 语义兼容：
    # - direct_invitable: 可直接拉群（后端 can_invite=0）
    # - link_only: 需邀请链接（后端 can_invite=1）
    # 历史 invitable/success/can_invite 视作 link_only。
    if scope_norm in ("invitable", "success", "can_invite", "link_only"):
        filter_mode = "link_only"
    elif scope_norm in ("direct_invitable", "direct", "usable"):
        filter_mode = "direct_invitable"
    else:
        filter_mode = "all"
    p = export_results_csv(task_id, filter_mode=filter_mode)
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

    account_ids = [int(r.id) for r in rows if getattr(r, "id", None) is not None]
    stats_map: dict[int, dict] = {}
    if account_ids:
        recent_task_q = db.query(UserFilterTask.id).order_by(UserFilterTask.id.desc())
        if user.role != "admin":
            recent_task_q = recent_task_q.filter(UserFilterTask.owner_id == user.id)
        recent_task_ids = [int(x[0]) for x in recent_task_q.limit(20).all() if x and x[0] is not None]

        if recent_task_ids:
            agg_rows = (
                db.query(
                    UserFilterResult.probe_account_id,
                    func.count(UserFilterResult.id),
                    func.sum(
                        case(
                            (func.upper(func.coalesce(UserFilterResult.fail_reason, "")) == "FLOOD", 1),
                            else_=0,
                        )
                    ),
                    func.sum(
                        case(
                            (func.coalesce(UserFilterResult.fail_reason, "") != "", 1),
                            else_=0,
                        )
                    ),
                )
                .filter(
                    UserFilterResult.probe_account_id.in_(account_ids),
                    UserFilterResult.task_id.in_(recent_task_ids),
                )
                .group_by(UserFilterResult.probe_account_id)
                .all()
            )
            for aid, total_cnt, flood_cnt, fail_cnt in agg_rows:
                if aid is None:
                    continue
                stats_map[int(aid)] = {
                    "recent_total": int(total_cnt or 0),
                    "flood_count": int(flood_cnt or 0),
                    "fail_total": int(fail_cnt or 0),
                }

    return {"ok": True, "accounts": [_account_to_dict(r, stats_map.get(int(r.id), {})) for r in rows]}


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
