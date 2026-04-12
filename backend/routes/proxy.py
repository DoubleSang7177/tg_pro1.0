from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy import or_
from sqlalchemy.orm import Session

from auth import get_current_user_optional, require_admin
from database import SessionLocal, get_db
from logger import get_logger
from models import AccountFile, Proxy, User
from services.proxy_check_service import CHECK_JOBS, get_check_job, run_checks_for_ids, run_manual_check_job
from services.proxy_service import assign_proxy_to_account, import_proxies_from_file, import_proxies_from_text


router = APIRouter(tags=["proxy"])
_proxy_check_route_log = get_logger("proxy_check")


def _proxy_geo_public(p: Proxy) -> dict:
    st = (p.proxy_status or "unknown").lower()
    return {
        "check_ip": p.proxy_ip or "",
        "check_country": p.proxy_country or "",
        "check_city": p.proxy_city or "",
        "country_code": (p.proxy_country_code or "").strip(),
        "check_status": st if st in ("ok", "dead", "unknown") else "unknown",
    }


class ProxyMatchBody(BaseModel):
    match_unbound: bool = True
    match_dead_proxy: bool = False


@router.get("/proxy/pool")
def list_proxy_pool(
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    rows = db.query(Proxy).order_by(Proxy.id.asc()).all()
    items = []
    for p in rows:
        if p.username:
            addr = f"{p.host}:{p.port}@{p.username}:***"
        else:
            addr = f"{p.host}:{p.port}"
        geo = _proxy_geo_public(p)
        items.append(
            {
                "id": p.id,
                "address": addr,
                "status": p.status or "idle",
                "assigned_account_id": p.assigned_account_id,
                **geo,
            }
        )
    return {"ok": True, "items": items}


@router.post("/proxy/pool/check")
async def start_proxy_pool_check(
    _admin: User = Depends(require_admin),
) -> dict:
    # 独立短会话：避免与本请求鉴权用的 get_db 会话并存，减少 SQLite 与后台写入抢锁导致长时间挂起
    db = SessionLocal()
    try:
        q = (
            db.query(Proxy)
            .filter(
                or_(
                    Proxy.proxy_status.is_(None),
                    Proxy.proxy_status == "",
                    Proxy.proxy_status.in_(["unknown", "dead"]),
                )
            )
            .order_by(Proxy.id.asc())
        )
        rows = q.all()
        ids = [p.id for p in rows]
    finally:
        db.close()

    if not ids:
        return {
            "ok": True,
            "job_id": None,
            "count": 0,
            "message": "没有待检测的代理（均为可用 ok 或列表为空）",
        }
    job_id = uuid4().hex
    CHECK_JOBS[job_id] = {"logs": [], "done": False}
    # 先写入一条同步日志，避免 BackgroundTasks+线程池投递协程失败时前端一直空白
    CHECK_JOBS[job_id]["logs"].append(f"[INFO] 任务已受理，共 {len(ids)} 条，正在启动异步检测…")
    task = asyncio.create_task(run_manual_check_job(job_id, ids))

    def _done(t: asyncio.Task) -> None:
        try:
            t.result()
        except Exception as exc:
            _proxy_check_route_log.exception("proxy check job crashed: %s", exc)
            if job_id in CHECK_JOBS:
                CHECK_JOBS[job_id]["logs"].append(f"[ERROR] 检测任务异常: {exc}")
                CHECK_JOBS[job_id]["done"] = True

    task.add_done_callback(_done)
    return {"ok": True, "job_id": job_id, "count": len(ids)}


@router.get("/proxy/pool/check-job/{job_id}")
def get_proxy_check_job(
    job_id: str,
    _admin: User = Depends(require_admin),
) -> dict:
    job = get_check_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在或已过期")
    return {"ok": True, "logs": list(job["logs"]), "done": bool(job["done"])}


@router.post("/proxy/pool/dedupe")
def dedupe_proxy_pool(
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    rows = db.query(Proxy).order_by(Proxy.id.asc()).all()
    by_key: dict[tuple[str, int, str, str], int] = {}
    removed = 0
    for p in rows:
        key = (p.host, int(p.port), p.username or "", p.password or "")
        if key not in by_key:
            by_key[key] = p.id
            continue
        if p.assigned_account_id is not None:
            continue
        if (p.status or "").lower() != "idle":
            continue
        db.delete(p)
        removed += 1
    db.commit()
    return {"ok": True, "removed": removed}


@router.post("/proxy/match")
def run_proxy_match(
    body: ProxyMatchBody,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    proxy_by_id = {p.id: p for p in db.query(Proxy).all()}
    accounts = db.query(AccountFile).order_by(AccountFile.id.asc()).all()
    candidate_ids: list[int] = []
    seen: set[int] = set()
    for a in accounts:
        if body.match_unbound:
            if (a.proxy_type or "").lower() == "direct" and not a.proxy_id:
                if a.id not in seen:
                    candidate_ids.append(a.id)
                    seen.add(a.id)
        if body.match_dead_proxy and a.proxy_id:
            pr = proxy_by_id.get(a.proxy_id)
            if pr is not None and (pr.status or "").lower() == "dead":
                if a.id not in seen:
                    candidate_ids.append(a.id)
                    seen.add(a.id)

    logs: list[str] = []
    assigned = 0
    for aid in candidate_ids:
        a = db.query(AccountFile).filter(AccountFile.id == aid).first()
        if a is None:
            continue
        if a.proxy_id:
            pr = db.query(Proxy).filter(Proxy.id == a.proxy_id).first()
            if pr is not None and (pr.status or "").lower() == "dead":
                pr.assigned_account_id = None
                db.add(pr)
                a.proxy_id = None
                a.proxy_type = "direct"
                db.add(a)
                db.flush()
                logs.append(f"[解绑失效代理] {a.phone}")

        r = assign_proxy_to_account(a)
        db.commit()
        if r.get("ok"):
            assigned += 1
            logs.append(f"[分配成功] {a.phone} → proxy #{r.get('proxy_id')}")
        else:
            logs.append(f"[跳过] {a.phone} — {r.get('warning', '未知原因')}")

    return {
        "ok": True,
        "logs": logs,
        "assigned_count": assigned,
        "candidates": len(candidate_ids),
    }


@router.post("/proxy/upload")
async def upload_proxy_file(
    file: UploadFile = File(...),
    _admin: User = Depends(require_admin),
) -> dict:
    name = (file.filename or "").lower()
    content = await file.read()

    if name.endswith(".txt") or name.endswith(".text"):
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            text = content.decode("utf-8", errors="replace")
        imported_count, new_ids = import_proxies_from_text(text)
        if new_ids:
            t = asyncio.create_task(run_checks_for_ids(new_ids, job_id=None))

            def _log_import_check(err_task: asyncio.Task) -> None:
                try:
                    err_task.result()
                except Exception as exc:
                    _proxy_check_route_log.exception("import auto-check failed: %s", exc)

            t.add_done_callback(_log_import_check)
        return {
            "ok": True,
            "imported_count": imported_count,
            "format": "txt",
            "check_scheduled": len(new_ids),
        }

    if name.endswith(".json"):
        target = Path(__file__).resolve().parents[2] / "proxy_config_plus.json"
        target.write_bytes(content)
        imported_count, new_ids = import_proxies_from_file()
        if new_ids:
            t = asyncio.create_task(run_checks_for_ids(new_ids, job_id=None))

            def _log_import_check_json(err_task: asyncio.Task) -> None:
                try:
                    err_task.result()
                except Exception as exc:
                    _proxy_check_route_log.exception("import json auto-check failed: %s", exc)

            t.add_done_callback(_log_import_check_json)
        return {
            "ok": True,
            "imported_count": imported_count,
            "format": "json",
            "check_scheduled": len(new_ids),
        }

    raise HTTPException(
        status_code=400,
        detail="仅支持 .txt / .text（每行一条 host:port@用户名:密码）或 .json（proxy_config_plus 结构）",
    )


@router.get("/proxy")
def list_proxies(
    _user: User | None = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
) -> dict:
    account_rows = db.query(AccountFile).order_by(AccountFile.id.asc()).all()
    proxy_rows = db.query(Proxy).order_by(Proxy.id.asc()).all()
    proxy_map = {p.id: p for p in proxy_rows}

    account_total = len(account_rows)
    accounts_with_proxy = 0
    accounts_direct = 0
    bound_dead_proxy_accounts = 0
    items = []
    for a in account_rows:
        proxy_obj = proxy_map.get(a.proxy_id) if a.proxy_id else None
        if (a.proxy_type or "").lower() == "direct":
            accounts_direct += 1
        else:
            accounts_with_proxy += 1
        if proxy_obj is not None and (proxy_obj.status or "").lower() == "dead":
            bound_dead_proxy_accounts += 1

        proxy_value = "-"
        status = "idle" if (a.proxy_type or "").lower() == "direct" else "used"
        geo = {
            "check_ip": "",
            "check_country": "",
            "check_city": "",
            "country_code": "",
            "check_status": "unknown",
        }
        if proxy_obj is not None:
            proxy_value = f"{proxy_obj.host}:{proxy_obj.port}@{proxy_obj.username}:{proxy_obj.password}"
            status = proxy_obj.status
            geo = _proxy_geo_public(proxy_obj)

        items.append(
            {
                "id": a.id,
                "phone": a.phone,
                "proxy_type": a.proxy_type,
                "proxy_value": proxy_value,
                "status": status,
                "proxy_id": a.proxy_id,
                **geo,
            }
        )

    return {
        "ok": True,
        "summary": {
            "account_total": account_total,
            "accounts_with_proxy": accounts_with_proxy,
            "accounts_direct": accounts_direct,
            "bound_dead_proxy_accounts": bound_dead_proxy_accounts,
        },
        "items": items,
    }


@router.post("/proxy/{proxy_id}/mark_dead")
def mark_proxy_dead(
    proxy_id: int,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if proxy is None:
        raise HTTPException(status_code=404, detail="代理不存在")
    proxy.status = "dead"
    db.add(proxy)
    db.commit()
    return {"ok": True, "id": proxy.id, "status": proxy.status}


@router.post("/proxy/{proxy_id}/unbind")
def unbind_proxy(
    proxy_id: int,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if proxy is None:
        raise HTTPException(status_code=404, detail="代理不存在")

    if proxy.assigned_account_id:
        account = db.query(AccountFile).filter(AccountFile.id == proxy.assigned_account_id).first()
        if account is not None:
            account.proxy_id = None
            account.proxy_type = "direct"
            db.add(account)

    proxy.assigned_account_id = None
    proxy.status = "idle"
    db.add(proxy)
    db.commit()
    return {"ok": True, "id": proxy.id, "status": proxy.status}
