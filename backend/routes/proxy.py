from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from auth import get_current_user_optional, require_admin
from database import SessionLocal, get_db
from logger import get_logger
from models import AccountFile, CopyListenerAccount, FilterAccount, Proxy, ScraperAccount, User
from services.proxy_check_service import (
    CHECK_JOBS,
    RUNNING_TASKS,
    fetch_pending_proxy_ids_sync,
    get_check_job,
    run_checks_for_ids,
    run_manual_check_job,
)
from services.proxy_service import import_proxies_from_file, import_proxies_from_text


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


def _pick_proxy_usage_type(roles: set[str]) -> str:
    order = ["unknown", "growth", "scraper", "listener", "probe", "real"]
    rank = {k: i for i, k in enumerate(order)}
    best = "unknown"
    for r in roles:
        rr = str(r or "unknown").lower()
        if rank.get(rr, 0) >= rank.get(best, 0):
            best = rr
    return best


class ProxyMatchBody(BaseModel):
    match_unbound: bool = True
    match_dead_proxy: bool = False


class ProxyUsageTypeBody(BaseModel):
    usage_type: str


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
    ids = fetch_pending_proxy_ids_sync()
    if not ids:
        return {
            "ok": True,
            "job_id": None,
            "count": 0,
            "message": "没有待检测的代理（均为可用 ok 或列表为空）",
        }
    job_id = uuid4().hex
    CHECK_JOBS[job_id] = {"logs": [], "done": False, "cancel": False}
    CHECK_JOBS[job_id]["logs"].append("[INFO] 任务已受理，正在启动检测...")
    loop = asyncio.get_running_loop()
    task = loop.create_task(run_manual_check_job(job_id))
    RUNNING_TASKS[job_id] = task

    def _cleanup(t: asyncio.Task) -> None:
        RUNNING_TASKS.pop(job_id, None)
        if job_id not in CHECK_JOBS:
            return
        if t.cancelled():
            CHECK_JOBS[job_id]["done"] = True
            return
        exc = t.exception()
        if exc is not None:
            CHECK_JOBS[job_id]["logs"].append(f"[ERROR] 任务异常: {exc}")
            _proxy_check_route_log.error(
                "proxy check job failed job_id=%s",
                job_id,
                exc_info=(type(exc), exc, exc.__traceback__),
            )
        CHECK_JOBS[job_id]["done"] = True

    task.add_done_callback(_cleanup)
    return {"ok": True, "job_id": job_id, "count": len(ids)}


@router.post("/proxy/pool/check/cancel/{job_id}")
def cancel_proxy_pool_check(
    job_id: str,
    _admin: User = Depends(require_admin),
) -> dict:
    job = get_check_job(job_id)
    if job is None:
        return {"ok": False, "message": "任务不存在或已结束"}
    if job.get("done"):
        return {"ok": False, "message": "任务已结束"}
    job["cancel"] = True
    return {"ok": True, "message": "已发送取消指令"}


@router.post("/proxy/pool/check/stop/{job_id}")
def stop_proxy_pool_check(
    job_id: str,
    _admin: User = Depends(require_admin),
) -> dict:
    """立即中断检测：协作取消 + 取消 asyncio 任务，并标记 job 已结束。"""
    job = get_check_job(job_id)
    if job is None:
        return {"ok": False, "message": "任务不存在或已结束"}
    if job.get("done"):
        return {"ok": False, "message": "任务已结束"}
    job["cancel"] = True
    job["logs"].append("[INFO] 用户中断检测任务")
    job["done"] = True
    task = RUNNING_TASKS.get(job_id)
    if task is not None and not task.done():
        task.cancel()
    return {"ok": True, "message": "已停止检测"}


@router.get("/proxy/pool/check-job/{job_id}")
def get_proxy_check_job(
    job_id: str,
    _admin: User = Depends(require_admin),
) -> dict:
    job = get_check_job(job_id)
    if job is None:
        return {"ok": True, "logs": [], "done": True, "cancel": False}
    return {
        "ok": True,
        "logs": list(job["logs"]),
        "done": bool(job["done"]),
        "cancel": bool(job.get("cancel")),
    }


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
    growth_accounts = db.query(AccountFile).order_by(AccountFile.id.asc()).all()
    filter_accounts = db.query(FilterAccount).order_by(FilterAccount.id.asc()).all()
    scraper_accounts = db.query(ScraperAccount).order_by(ScraperAccount.id.asc()).all()
    listener_accounts = db.query(CopyListenerAccount).order_by(CopyListenerAccount.id.asc()).all()

    candidate_refs: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()

    def _add_candidate(kind: str, rid: int) -> None:
        key = (kind, int(rid))
        if key in seen:
            return
        seen.add(key)
        candidate_refs.append(key)

    for a in growth_accounts:
        if body.match_unbound and (a.proxy_type or "").lower() == "direct" and not a.proxy_id:
            _add_candidate("growth", a.id)
        if body.match_dead_proxy and a.proxy_id:
            pr = proxy_by_id.get(a.proxy_id)
            if pr is not None and (pr.status or "").lower() == "dead":
                _add_candidate("growth", a.id)

    for a in filter_accounts:
        if body.match_unbound and not a.proxy_id:
            _add_candidate("filter", a.id)
        if body.match_dead_proxy and a.proxy_id:
            pr = proxy_by_id.get(a.proxy_id)
            if pr is not None and (pr.status or "").lower() == "dead":
                _add_candidate("filter", a.id)

    for a in scraper_accounts:
        if body.match_unbound and not a.proxy_id:
            _add_candidate("scraper", a.id)
        if body.match_dead_proxy and a.proxy_id:
            pr = proxy_by_id.get(a.proxy_id)
            if pr is not None and (pr.status or "").lower() == "dead":
                _add_candidate("scraper", a.id)

    for a in listener_accounts:
        if body.match_unbound and not a.proxy_id:
            _add_candidate("listener", a.id)
        if body.match_dead_proxy and a.proxy_id:
            pr = proxy_by_id.get(a.proxy_id)
            if pr is not None and (pr.status or "").lower() == "dead":
                _add_candidate("listener", a.id)

    def _pick_idle_proxy() -> Proxy | None:
        return db.query(Proxy).filter(Proxy.status == "idle").order_by(Proxy.id.asc()).first()

    logs: list[str] = []
    assigned = 0
    for kind, rid in candidate_refs:
        row = None
        if kind == "growth":
            row = db.query(AccountFile).filter(AccountFile.id == rid).first()
        elif kind == "filter":
            row = db.query(FilterAccount).filter(FilterAccount.id == rid).first()
        elif kind == "scraper":
            row = db.query(ScraperAccount).filter(ScraperAccount.id == rid).first()
        elif kind == "listener":
            row = db.query(CopyListenerAccount).filter(CopyListenerAccount.id == rid).first()
        if row is None:
            continue

        phone = str(getattr(row, "phone", "") or f"{kind}#{rid}")
        row_proxy_id = getattr(row, "proxy_id", None)

        if row_proxy_id:
            pr = db.query(Proxy).filter(Proxy.id == row_proxy_id).first()
            if pr is not None and (pr.status or "").lower() == "dead":
                if kind == "growth":
                    pr.assigned_account_id = None
                db.add(pr)
                setattr(row, "proxy_id", None)
                if kind == "growth":
                    row.proxy_type = "direct"
                db.add(row)
                db.flush()
                logs.append(f"[解绑失效代理] [{kind}] {phone}")

        proxy = _pick_idle_proxy()
        if proxy is None:
            logs.append(f"[跳过] [{kind}] {phone} — 无代理库存")
            continue

        proxy.status = "used"
        if kind == "growth":
            proxy.assigned_account_id = row.id
            proxy.usage_type = "growth"
            row.proxy_type = "proxy"
        elif kind == "filter":
            proxy.usage_type = "real" if str(getattr(row, "type", "")).lower() == "real" else "probe"
        elif kind == "scraper":
            proxy.usage_type = "scraper"
        elif kind == "listener":
            proxy.usage_type = "listener"
        setattr(row, "proxy_id", proxy.id)
        db.add(proxy)
        db.add(row)
        db.commit()
        assigned += 1
        logs.append(f"[分配成功] [{kind}] {phone} → proxy #{proxy.id}")

    return {
        "ok": True,
        "logs": logs,
        "assigned_count": assigned,
        "candidates": len(candidate_refs),
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
            loop = asyncio.get_running_loop()
            t = loop.create_task(run_checks_for_ids(new_ids, job_id=None))

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
            loop = asyncio.get_running_loop()
            t = loop.create_task(run_checks_for_ids(new_ids, job_id=None))

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
    filter_rows_all = db.query(FilterAccount).order_by(FilterAccount.id.asc()).all()
    scraper_rows_all = db.query(ScraperAccount).order_by(ScraperAccount.id.asc()).all()
    listener_rows_all = db.query(CopyListenerAccount).order_by(CopyListenerAccount.id.asc()).all()
    proxy_rows = db.query(Proxy).order_by(Proxy.id.asc()).all()
    proxy_map = {p.id: p for p in proxy_rows}
    usage_roles: dict[int, set[str]] = {p.id: {str(p.usage_type or "unknown").lower()} for p in proxy_rows}
    for fa in filter_rows_all:
        if not fa.proxy_id:
            continue
        usage_roles.setdefault(int(fa.proxy_id), {"unknown"}).add("real" if fa.type == "real" else "probe")
    for a in account_rows:
        if a.proxy_id:
            usage_roles.setdefault(int(a.proxy_id), {"unknown"}).add("growth")
    for sa in scraper_rows_all:
        if sa.proxy_id:
            usage_roles.setdefault(int(sa.proxy_id), {"unknown"}).add("scraper")
    for la in listener_rows_all:
        if la.proxy_id:
            usage_roles.setdefault(int(la.proxy_id), {"unknown"}).add("listener")

    account_total = len(account_rows) + len(filter_rows_all) + len(scraper_rows_all) + len(listener_rows_all)
    accounts_with_proxy = 0
    accounts_direct = 0
    bound_dead_proxy_accounts = 0
    items = []

    def _append_row(
        *,
        row_id: int | str,
        sort_id: int,
        phone: str,
        proxy_type: str,
        proxy_id: int | None,
        default_usage_type: str = "unknown",
    ) -> None:
        nonlocal accounts_with_proxy, accounts_direct, bound_dead_proxy_accounts, items
        proxy_obj = proxy_map.get(proxy_id) if proxy_id else None
        if (proxy_type or "").lower() == "direct":
            accounts_direct += 1
        else:
            accounts_with_proxy += 1
        if proxy_obj is not None and (proxy_obj.status or "").lower() == "dead":
            bound_dead_proxy_accounts += 1

        proxy_value = "-"
        status = "idle" if (proxy_type or "").lower() == "direct" else "used"
        geo = {
            "check_ip": "",
            "check_country": "",
            "check_city": "",
            "country_code": "",
            "check_status": "unknown",
        }
        if proxy_obj is not None:
            if proxy_obj.username:
                proxy_value = f"@{proxy_obj.username}:{proxy_obj.password or ''}"
            else:
                proxy_value = f"{proxy_obj.host}:{proxy_obj.port}"
            status = proxy_obj.status
            geo = _proxy_geo_public(proxy_obj)
        usage_type = (
            _pick_proxy_usage_type(usage_roles.get(int(proxy_id or 0), {"unknown"}))
            if proxy_id
            else str(default_usage_type or "unknown").lower()
        )

        items.append(
            {
                "id": row_id,
                "sort_id": sort_id,
                "phone": phone,
                "proxy_type": proxy_type,
                "proxy_value": proxy_value,
                "usage_type": usage_type,
                "status": status,
                "proxy_id": proxy_id,
                **geo,
            }
        )

    for a in account_rows:
        _append_row(
            row_id=a.id,
            sort_id=int(a.id),
            phone=a.phone,
            proxy_type=a.proxy_type,
            proxy_id=a.proxy_id,
            default_usage_type="growth",
        )

    for fa in filter_rows_all:
        _append_row(
            row_id=f"f-{fa.id}",
            sort_id=1_000_000 + int(fa.id),
            phone=fa.phone,
            proxy_type="proxy" if fa.proxy_id else "direct",
            proxy_id=fa.proxy_id,
            default_usage_type="real" if fa.type == "real" else "probe",
        )

    for sa in scraper_rows_all:
        _append_row(
            row_id=f"s-{sa.id}",
            sort_id=2_000_000 + int(sa.id),
            phone=sa.phone,
            proxy_type="proxy" if sa.proxy_id else "direct",
            proxy_id=sa.proxy_id,
            default_usage_type="scraper",
        )

    for la in listener_rows_all:
        _append_row(
            row_id=f"l-{la.id}",
            sort_id=3_000_000 + int(la.id),
            phone=la.phone,
            proxy_type="proxy" if la.proxy_id else "direct",
            proxy_id=la.proxy_id,
            default_usage_type="listener",
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

    touched = 0
    if proxy.assigned_account_id:
        account = db.query(AccountFile).filter(AccountFile.id == proxy.assigned_account_id).first()
        if account is not None:
            account.proxy_id = None
            account.proxy_type = "direct"
            db.add(account)
            touched += 1

    # 用户筛选模块账号同样可能绑定该代理，需一并解绑
    fa_rows = db.query(FilterAccount).filter(FilterAccount.proxy_id == proxy.id).all()
    for fa in fa_rows:
        fa.proxy_id = None
        db.add(fa)
        touched += 1
    sa_rows = db.query(ScraperAccount).filter(ScraperAccount.proxy_id == proxy.id).all()
    for sa in sa_rows:
        sa.proxy_id = None
        db.add(sa)
        touched += 1
    la_rows = db.query(CopyListenerAccount).filter(CopyListenerAccount.proxy_id == proxy.id).all()
    for la in la_rows:
        la.proxy_id = None
        db.add(la)
        touched += 1

    proxy.assigned_account_id = None
    proxy.status = "idle"
    db.add(proxy)
    db.commit()
    return {"ok": True, "id": proxy.id, "status": proxy.status, "affected_accounts": touched}


@router.post("/proxy/{proxy_id}/usage_type")
def update_proxy_usage_type(
    proxy_id: int,
    body: ProxyUsageTypeBody,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if proxy is None:
        raise HTTPException(status_code=404, detail="代理不存在")
    allowed = {"growth", "scraper", "listener", "probe", "real", "unknown"}
    usage = str(body.usage_type or "").strip().lower()
    if usage not in allowed:
        raise HTTPException(status_code=400, detail="usage_type 非法")
    proxy.usage_type = usage
    db.add(proxy)
    db.commit()
    return {"ok": True, "id": proxy.id, "usage_type": proxy.usage_type}
