"""代理出口 IP / 国家检测（异步、限流）；不改变绑定与任务逻辑。"""

from __future__ import annotations

import asyncio
import sys
from typing import Any
from urllib.parse import quote

import httpx
from sqlalchemy import or_
from sqlalchemy.orm import Session

try:
    from database import SessionLocal
    from logger import get_logger
    from models import Proxy
except ModuleNotFoundError:
    from backend.database import SessionLocal
    from backend.logger import get_logger
    from backend.models import Proxy

log = get_logger("proxy_check")

IP_API_URL = "http://ip-api.com/json"
# httpx：连接 + 读响应均有上限，避免半开连接卡死
HTTPX_TIMEOUT = httpx.Timeout(8.0, connect=5.0)
# 单条代理检测总预算（含同步 SQLite 提交）
PER_PROXY_TOTAL_TIMEOUT = 25.0
MAX_CONCURRENT = 5
# 每处理 N 条后 await asyncio.sleep(0)，让出事件循环
CHUNK_YIELD_EVERY = 5

_check_semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# job_id -> {"logs": list[str], "done": bool, "cancel": bool}
CHECK_JOBS: dict[str, dict[str, Any]] = {}
# 强引用 asyncio.Task，避免被 GC；任务结束后由 done_callback 弹出
RUNNING_TASKS: dict[str, asyncio.Task[Any]] = {}


def fetch_pending_proxy_ids_sync() -> list[int]:
    """与「检测代理」按钮相同条件：unknown / dead / 空。"""
    db = SessionLocal()
    try:
        q = (
            db.query(Proxy.id)
            .filter(
                or_(
                    Proxy.proxy_status.is_(None),
                    Proxy.proxy_status == "",
                    Proxy.proxy_status.in_(["unknown", "dead"]),
                )
            )
            .order_by(Proxy.id.asc())
        )
        return [row[0] for row in q.all()]
    finally:
        db.close()


def _append_job_log(job_id: str | None, line: str) -> None:
    if job_id and job_id in CHECK_JOBS:
        CHECK_JOBS[job_id]["logs"].append(line)


def _job_cancelled(job_id: str | None) -> bool:
    if not job_id or job_id not in CHECK_JOBS:
        return False
    return bool(CHECK_JOBS[job_id].get("cancel"))


def _proxy_short_label_sync(proxy_id: int) -> str:
    db = SessionLocal()
    try:
        row = db.query(Proxy).filter(Proxy.id == proxy_id).first()
        if row is None:
            return f"#{proxy_id}"
        return f"#{row.id} {row.host}:{row.port}"
    finally:
        db.close()


def _proxy_url_for_httpx(host: str, port: int, username: str | None, password: str | None) -> str:
    u = quote(username or "", safe="")
    p = quote(password or "", safe="")
    if username is not None or password is not None:
        return f"http://{u}:{p}@{host}:{port}"
    return f"http://{host}:{port}"


def _mark_proxy_check_dead_sync(proxy_id: int) -> None:
    db = SessionLocal()
    try:
        row = db.query(Proxy).filter(Proxy.id == proxy_id).first()
        if row is None:
            return
        row.proxy_status = "dead"
        row.proxy_ip = None
        row.proxy_country = None
        row.proxy_city = None
        row.proxy_country_code = None
        db.add(row)
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


async def check_proxy(proxy_id: int, job_id: str | None = None) -> None:
    """
    对单条代理做出口检测并写库。异常吞掉并标记 dead，不向外抛。
    """
    if _job_cancelled(job_id):
        return
    db = SessionLocal()
    try:
        row = db.query(Proxy).filter(Proxy.id == proxy_id).first()
        if row is None:
            return
        if _job_cancelled(job_id):
            return
        await _check_proxy_session(row, db, job_id)
    except Exception as exc:
        log.exception("proxy check unexpected proxy_id=%s: %s", proxy_id, exc)
        label = f"#{proxy_id}"
        try:
            label = _proxy_short_label_sync(proxy_id)
        except Exception:
            pass
        try:
            db.rollback()
            row = db.query(Proxy).filter(Proxy.id == proxy_id).first()
            if row:
                row.proxy_status = "dead"
                row.proxy_ip = None
                row.proxy_country = None
                row.proxy_city = None
                row.proxy_country_code = None
                db.add(row)
                db.commit()
        except Exception:
            db.rollback()
        _append_job_log(job_id, f"[ERROR] {label} 检测失败")
    finally:
        db.close()


async def _check_proxy_session(proxy: Proxy, db: Session, job_id: str | None) -> None:
    async with _check_semaphore:
        if _job_cancelled(job_id):
            return
        proxy_url = _proxy_url_for_httpx(
            proxy.host, int(proxy.port), proxy.username, proxy.password
        )
        label = f"#{proxy.id} {proxy.host}:{proxy.port}"
        try:
            async with httpx.AsyncClient(
                proxy=proxy_url,
                timeout=HTTPX_TIMEOUT,
                follow_redirects=True,
            ) as client:
                resp = await client.get(IP_API_URL)
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            proxy.proxy_status = "dead"
            proxy.proxy_ip = None
            proxy.proxy_country = None
            proxy.proxy_city = None
            proxy.proxy_country_code = None
            db.add(proxy)
            db.commit()
            log.info("[ERROR] %s 检测失败 (%s)", label, exc)
            _append_job_log(job_id, f"[ERROR] {label} 检测失败")
            return

        try:
            if (data.get("status") or "") != "success":
                raise ValueError(data.get("message") or "ip-api 非 success")
            ip = str(data.get("query") or "").strip()
            country = str(data.get("country") or "").strip()
            city = str(data.get("city") or "").strip()
            cc = str(data.get("countryCode") or "").strip()[:4] or None
            proxy.proxy_ip = ip or None
            proxy.proxy_country = country or None
            proxy.proxy_city = city or None
            proxy.proxy_country_code = cc
            proxy.proxy_status = "ok"
            db.add(proxy)
            db.commit()
            country_disp = country or "?"
            ok_msg = f"[SUCCESS] {label} 检测成功 → {country_disp}"
            log.info("%s", ok_msg)
            _append_job_log(job_id, ok_msg)
        except Exception as exc:
            proxy.proxy_status = "dead"
            proxy.proxy_ip = None
            proxy.proxy_country = None
            proxy.proxy_city = None
            proxy.proxy_country_code = None
            db.add(proxy)
            db.commit()
            log.info("[ERROR] %s 检测失败 (%s)", label, exc)
            _append_job_log(job_id, f"[ERROR] {label} 检测失败")


async def _check_proxy_bounded(proxy_id: int, job_id: str | None) -> None:
    if _job_cancelled(job_id):
        return
    try:
        await asyncio.wait_for(check_proxy(proxy_id, job_id), timeout=PER_PROXY_TOTAL_TIMEOUT)
    except asyncio.TimeoutError:
        log.warning("proxy check timeout proxy_id=%s", proxy_id)
        label = await asyncio.to_thread(_proxy_short_label_sync, proxy_id)
        _append_job_log(job_id, f"[ERROR] {label} 检测失败")
        await asyncio.to_thread(_mark_proxy_check_dead_sync, proxy_id)


async def run_checks_for_ids(proxy_ids: list[int], job_id: str | None = None) -> None:
    if not proxy_ids:
        return
    info = f"[INFO] 开始检测代理，共 {len(proxy_ids)} 条（并发上限 {MAX_CONCURRENT}）"
    log.info(info)
    _append_job_log(job_id, info)
    user_cancelled = False
    for i in range(0, len(proxy_ids), CHUNK_YIELD_EVERY):
        if job_id and _job_cancelled(job_id):
            user_cancelled = True
            _append_job_log(job_id, "[INFO] 检测已由用户取消，后续条目不再执行")
            break
        chunk = proxy_ids[i : i + CHUNK_YIELD_EVERY]
        results = await asyncio.gather(
            *(_check_proxy_bounded(pid, job_id) for pid in chunk),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, BaseException):
                _append_job_log(job_id, f"[ERROR] 子任务异常: {r!r}")
        await asyncio.sleep(0)
    done_msg = "[INFO] 检测完成（已取消）" if user_cancelled else "[INFO] 检测完成"
    log.info(done_msg)
    _append_job_log(job_id, done_msg)


async def run_manual_check_job(job_id: str) -> None:
    """
    仅接收 job_id：在任务内查库，保证日志连续写入 CHECK_JOBS。
    不在此设置 done；由路由层 Task.add_done_callback 统一收尾。
    """
    print("🔥🔥🔥 任务执行:", job_id, flush=True)
    log.info("检测任务开始: %s", job_id)
    try:
        print(f"检测任务开始: {job_id}", file=sys.stderr, flush=True)
    except Exception:
        pass
    _append_job_log(job_id, "[INFO] 开始检测代理...")
    CHECK_JOBS[job_id]["logs"].append("[DEBUG] 开始读取代理")
    proxy_ids = await asyncio.to_thread(fetch_pending_proxy_ids_sync)
    CHECK_JOBS[job_id]["logs"].append("[DEBUG] 读取完成")
    _append_job_log(job_id, f"[INFO] 共 {len(proxy_ids)} 个代理")
    if not proxy_ids:
        _append_job_log(job_id, "[INFO] 没有待检测条目（可能已在排队期间全部变为 ok），结束")
        return
    await run_checks_for_ids(proxy_ids, job_id=job_id)


def get_check_job(job_id: str) -> dict[str, Any] | None:
    return CHECK_JOBS.get(job_id)
