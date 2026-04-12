"""代理出口 IP / 国家检测（异步、限流）；不改变绑定与任务逻辑。"""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import quote

import httpx
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
CHECK_TIMEOUT = 8.0
MAX_CONCURRENT = 5

_check_semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# job_id -> {"logs": list[str], "done": bool}
CHECK_JOBS: dict[str, dict[str, Any]] = {}


def _append_job_log(job_id: str | None, line: str) -> None:
    if job_id and job_id in CHECK_JOBS:
        CHECK_JOBS[job_id]["logs"].append(line)


def _proxy_url_for_httpx(host: str, port: int, username: str | None, password: str | None) -> str:
    u = quote(username or "", safe="")
    p = quote(password or "", safe="")
    if username is not None or password is not None:
        return f"http://{u}:{p}@{host}:{port}"
    return f"http://{host}:{port}"


async def check_proxy(proxy_id: int, job_id: str | None = None) -> None:
    """
    对单条代理做出口检测并写库。异常吞掉并标记 dead，不向外抛。
    """
    db = SessionLocal()
    try:
        row = db.query(Proxy).filter(Proxy.id == proxy_id).first()
        if row is None:
            return
        await _check_proxy_session(row, db, job_id)
    except Exception as exc:
        log.exception("proxy check unexpected proxy_id=%s: %s", proxy_id, exc)
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
        _append_job_log(job_id, f"[ERROR] #{proxy_id} 检测异常: {exc}")
    finally:
        db.close()


async def _check_proxy_session(proxy: Proxy, db: Session, job_id: str | None) -> None:
    async with _check_semaphore:
        proxy_url = _proxy_url_for_httpx(
            proxy.host, int(proxy.port), proxy.username, proxy.password
        )
        label = f"#{proxy.id} {proxy.host}:{proxy.port}"
        try:
            async with httpx.AsyncClient(
                proxy=proxy_url,
                timeout=CHECK_TIMEOUT,
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
            msg = f"[ERROR] {label} 代理连接失败: {exc}"
            log.info(msg)
            _append_job_log(job_id, msg)
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
            ok_msg = f"[SUCCESS] {ip or '?'} → {country or '?'}"
            log.info("%s (%s)", ok_msg, label)
            _append_job_log(job_id, ok_msg)
        except Exception as exc:
            proxy.proxy_status = "dead"
            proxy.proxy_ip = None
            proxy.proxy_country = None
            proxy.proxy_city = None
            proxy.proxy_country_code = None
            db.add(proxy)
            db.commit()
            msg = f"[ERROR] {label} 解析失败: {exc}"
            log.info(msg)
            _append_job_log(job_id, msg)


async def run_checks_for_ids(proxy_ids: list[int], job_id: str | None = None) -> None:
    if not proxy_ids:
        return
    info = f"[INFO] 开始检测代理，共 {len(proxy_ids)} 条（并发上限 {MAX_CONCURRENT}）"
    log.info(info)
    _append_job_log(job_id, info)
    await asyncio.gather(*(check_proxy(pid, job_id) for pid in proxy_ids))
    done_msg = "[INFO] 本轮检测结束"
    log.info(done_msg)
    _append_job_log(job_id, done_msg)


async def run_manual_check_job(job_id: str, proxy_ids: list[int]) -> None:
    try:
        await run_checks_for_ids(proxy_ids, job_id=job_id)
    finally:
        if job_id in CHECK_JOBS:
            CHECK_JOBS[job_id]["done"] = True


def get_check_job(job_id: str) -> dict[str, Any] | None:
    return CHECK_JOBS.get(job_id)
