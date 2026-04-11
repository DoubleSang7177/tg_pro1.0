from __future__ import annotations

import asyncio
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pyrogram import Client
from pyrogram.errors import FloodWait

from cn_time import cn_hms
from database import SessionLocal
from logger import get_logger
from models import AccountFile, Group, Proxy, Setting
from services.account_status import (
    ST_DAILY_LIMITED,
    ST_NORMAL,
    ST_RISK_SUSPECTED,
    is_risk_suspected,
    recover_and_normalize,
)
from services.task_progress import progress_append, progress_highlight_publish
from services.task_run_control import task_run_should_continue

log = get_logger("telegram_service")
API_ID = int(os.getenv("TELEGRAM_API_ID", "20954937"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "d5a748cfdb420593307b5265c1864ba3")
TELEGRAM_START_TIMEOUT = float(os.getenv("TELEGRAM_START_TIMEOUT", "45"))
TELEGRAM_START_HEARTBEAT_SEC = max(5.0, float(os.getenv("TELEGRAM_START_HEARTBEAT_SEC", "8")))
TELEGRAM_ENSURE_GROUP_TIMEOUT = float(os.getenv("TELEGRAM_ENSURE_GROUP_TIMEOUT", "120"))
TELEGRAM_CLIENT_STOP_TIMEOUT = float(os.getenv("TELEGRAM_CLIENT_STOP_TIMEOUT", "15"))
# wait_for(client.start) 在部分代理环境下取消协程可能长时间不结束，此处超时后 stop() 强拆
TELEGRAM_START_ABORT_STOP_SEC = float(os.getenv("TELEGRAM_START_ABORT_STOP_SEC", "12"))
TELEGRAM_START_TASK_DRAIN_SEC = float(os.getenv("TELEGRAM_START_TASK_DRAIN_SEC", "10"))
LOGIN_FAIL_DELAY_MIN_SEC = int(os.getenv("LOGIN_FAIL_DELAY_MIN_SEC", "30"))
LOGIN_FAIL_DELAY_MAX_SEC = int(os.getenv("LOGIN_FAIL_DELAY_MAX_SEC", "60"))


def _env_float_bounded(key: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(os.getenv(key, str(default)))
    except ValueError:
        v = default
    return max(lo, min(hi, v))


def _env_int_bounded(key: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(os.getenv(key, str(default)))
    except ValueError:
        v = default
    return max(lo, min(hi, v))


# 单次登录 wait_for 超时（秒），限制在 5–8，避免单号长时间卡住
TELEGRAM_LOGIN_ATTEMPT_TIMEOUT = _env_float_bounded("TELEGRAM_LOGIN_ATTEMPT_TIMEOUT", 7.0, 5.0, 8.0)
# 每账号最多尝试次数，限制在 2–3
TELEGRAM_LOGIN_MAX_RETRIES = _env_int_bounded("TELEGRAM_LOGIN_MAX_RETRIES", 3, 2, 3)

GROUP_METADATA_SYNC_KEY = "group_metadata_last_sync"
GROUP_METADATA_STALE_SECONDS = int(os.getenv("GROUP_METADATA_STALE_SECONDS", str(24 * 3600)))


async def _client_start_with_hard_timeout(client: Client, timeout: float) -> None:
    """
    不用单独的 wait_for(start)：Pyrogram 在代理卡住时，取消 start 可能迟迟不完，
    表现为超过「上限 N 秒」仍不切号。超时后 cancel + stop() 尽量释放连接，再抛出 TimeoutError。
    """
    task = asyncio.create_task(client.start())
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        if task.done():
            if task.cancelled():
                raise asyncio.TimeoutError
            exc = task.exception()
            if exc is not None:
                raise exc
            return
        task.cancel()
        try:
            await asyncio.wait_for(client.stop(), timeout=TELEGRAM_START_ABORT_STOP_SEC)
        except Exception:
            log.debug("client.stop after login deadline failed", exc_info=True)
        try:
            await asyncio.wait_for(task, timeout=TELEGRAM_START_TASK_DRAIN_SEC)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        raise asyncio.TimeoutError
    finally:
        if not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=TELEGRAM_START_TASK_DRAIN_SEC)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass


async def _pyrogram_start_with_task_timeout(
    client: Client,
    timeout: float,
    tl,
    phone_label: str,
    attempt_no: int,
    max_attempts: int,
) -> bool:
    """
    代理下 client.start() 可能长时间不 yield；用 asyncio.wait_for(create_task(start)) 卡硬超时，
    超时后 cancel task 并 stop，避免单账号拖死队列。
    """
    task = asyncio.create_task(client.start())
    try:
        await asyncio.wait_for(task, timeout=timeout)
        return True
    except asyncio.TimeoutError:
        log.error(
            "登录超时 phone=%s attempt=%s/%s timeout=%s",
            phone_label,
            attempt_no,
            max_attempts,
            timeout,
        )
        tl(
            f"[WARN] 登录超时（第{attempt_no}/{max_attempts}次，wait_for {timeout:.1f}s）"
            f" · {phone_label or '—'}",
        )
        if not task.done():
            task.cancel()
        try:
            await asyncio.wait_for(task, timeout=TELEGRAM_START_TASK_DRAIN_SEC)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        except Exception:
            pass
        return False


async def _single_login_attempt(
    account: AccountFile,
    session_name: str,
    proxy_dict: dict[str, Any] | None,
    proxy_label: str,
    attempt_timeout: float,
    attempt_no: int,
    max_attempts: int,
    tl,
) -> tuple[bool, Client | None, str | None]:
    """单次登录：成功返回 (True, client, None)，失败返回 (False, None, err)。"""
    login_cap = max(1, int(round(attempt_timeout)))
    phone = account.phone or "—"
    tl(
        f"[INFO] 登录中（第{attempt_no}/{max_attempts}次），超时 {login_cap}s，经由 {proxy_label} · {phone}",
    )
    client = Client(
        name=session_name,
        api_id=API_ID,
        api_hash=API_HASH,
        phone_number=account.phone,
        proxy=proxy_dict,
        no_updates=True,
    )
    try:
        success = await _pyrogram_start_with_task_timeout(
            client,
            attempt_timeout,
            tl,
            account.phone or "",
            attempt_no,
            max_attempts,
        )
        if success:
            tl(f"[INFO] 登录成功（第{attempt_no}/{max_attempts}次）· {phone}")
            return True, client, None
        try:
            await asyncio.wait_for(client.stop(), timeout=TELEGRAM_CLIENT_STOP_TIMEOUT)
        except Exception:
            pass
        tl(f"[WARN] 登录失败（第{attempt_no}/{max_attempts}次）· 原因：超时 · {phone}")
        return False, None, "timeout"
    except Exception as exc:
        tl(
            f"[ERROR] 登录失败（第{attempt_no}/{max_attempts}次）· {phone} · {str(exc)[:280]}",
        )
        try:
            await asyncio.wait_for(client.stop(), timeout=TELEGRAM_CLIENT_STOP_TIMEOUT)
        except Exception:
            pass
        return False, None, str(exc)


def _task_log(logs: list[str], message: str, progress_job_id: str | None = None) -> None:
    ts = cn_hms()
    line = f"[{ts}] {message}"
    logs.append(line)
    if progress_job_id:
        progress_append(progress_job_id, line)


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _classify_failure_reason(exc: Exception) -> str:
    msg = str(exc).lower()
    if "user_deactivated" in msg or "deactivated" in msg:
        return "account_auth_failed"
    if "username_not_occupied" in msg or "not occupied" in msg:
        return "user_issue"
    if "chat_member_add_failed" in msg:
        return "user_issue"
    if "peer_flood" in msg or "floodwait" in msg or "limited" in msg:
        return "account_limited"
    if "group" in msg and ("limited" in msg or "forbidden" in msg):
        return "group_limited"
    if "user" in msg and ("privacy" in msg or "invalid" in msg):
        return "user_issue"
    if "session" in msg or "login failed" in msg or "登录失败" in msg:
        return "account_auth_failed"
    return "unknown_error"


def _phone_digits(phone: str | None) -> str:
    return "".join(ch for ch in str(phone or "") if ch.isdigit())


def _resolve_session_name(account: AccountFile) -> str:
    account_dir = Path(account.saved_path)
    digits = _phone_digits(account.phone)
    candidates = [
        account_dir / f"{digits}.session",
        account_dir / f"{account.phone}.session",
    ]
    for p in candidates:
        if p.exists():
            return str(p.with_suffix(""))
    fallback = next(iter(account_dir.glob("*.session")), None)
    if fallback:
        return str(fallback.with_suffix(""))
    return str(account_dir / digits)


def _build_proxy(proxy_row: Proxy | None, proxy_type: str | None) -> dict[str, Any] | None:
    if not proxy_row:
        return None
    if (proxy_type or "").lower() in {"direct", ""}:
        return None
    return {
        "scheme": "socks5",
        "hostname": proxy_row.host,
        "port": int(proxy_row.port),
        "username": proxy_row.username,
        "password": proxy_row.password,
    }


async def _ensure_in_group(client: Client, group_username: str) -> tuple[bool, Any]:
    group = await client.get_chat(group_username)
    me = await client.get_me()
    try:
        await client.get_chat_member(group.id, me.id)
        return True, group
    except Exception:
        pass
    try:
        await client.join_chat(group_username)
        await asyncio.sleep(2)
        await client.get_chat_member(group.id, me.id)
        return True, group
    except Exception:
        return False, group


def _normalize_chat_identifier(config_username: str) -> str:
    u = (config_username or "").strip()
    if u.startswith("@"):
        return u[1:]
    return u


def _metadata_sync_recent(db, force: bool) -> bool:
    if force:
        return False
    row = db.query(Setting).filter(Setting.key == GROUP_METADATA_SYNC_KEY).first()
    if not row or not row.value:
        return False
    try:
        raw = row.value.replace("Z", "+00:00") if row.value.endswith("Z") else row.value
        last = datetime.fromisoformat(raw)
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        else:
            last = last.astimezone(timezone.utc)
        return (datetime.now(timezone.utc) - last).total_seconds() < GROUP_METADATA_STALE_SECONDS
    except Exception:
        return False


async def sync_groups_metadata(owner_id: int | None, force: bool, db) -> dict[str, Any]:
    """
    使用一个可用账号登录 Telegram，拉取所有目标群组的标题、公开用户名与人数并写入数据库。
    默认 24 小时内只同步一次（可用 force 跳过）。
    """
    if _metadata_sync_recent(db, force):
        return {"ok": True, "skipped": True, "reason": "recently_synced", "logs": []}

    query = db.query(AccountFile).order_by(AccountFile.id.desc())
    if owner_id is not None:
        query = query.filter(AccountFile.owner_id == owner_id)
    account_rows = query.all()
    now_utc = datetime.now(timezone.utc)
    runnable: list[AccountFile] = []
    for row in account_rows:
        recover_and_normalize(row, now_utc)
        if row.status == ST_RISK_SUSPECTED:
            continue
        if row.status == ST_DAILY_LIMITED:
            lu = row.limited_until
            if lu and lu.tzinfo is None:
                lu = lu.replace(tzinfo=timezone.utc)
            elif lu:
                lu = lu.astimezone(timezone.utc)
            if lu and now_utc < lu:
                continue
        if row.status != ST_NORMAL:
            continue
        runnable.append(row)

    if not runnable:
        return {"ok": False, "skipped": False, "message": "无可用账号", "logs": ["没有可用于同步的活跃账号"]}

    group_rows = db.query(Group).order_by(Group.id.asc()).all()
    if not group_rows:
        return {"ok": True, "skipped": False, "logs": ["数据库中无群组记录"], "updated": 0}

    logs: list[str] = []
    client: Client | None = None
    used_phone: str | None = None

    def _mlog(msg: str) -> None:
        logs.append(msg)

    for account in runnable:
        proxy_obj = db.query(Proxy).filter(Proxy.id == account.proxy_id).first() if account.proxy_id else None
        proxy_dict = _build_proxy(proxy_obj, account.proxy_type)
        session_name = _resolve_session_name(account)
        if proxy_dict:
            proxy_label = f"{proxy_dict.get('hostname', '?')}:{proxy_dict.get('port', '?')}"
        else:
            proxy_label = "直连"
        login_ok = False
        max_att = TELEGRAM_LOGIN_MAX_RETRIES
        for attempt in range(1, max_att + 1):
            ok, client, _ = await _single_login_attempt(
                account,
                session_name,
                proxy_dict,
                proxy_label,
                TELEGRAM_LOGIN_ATTEMPT_TIMEOUT,
                attempt,
                max_att,
                _mlog,
            )
            if ok:
                account.login_fail_count = 0
                account.status = ST_NORMAL
                account.limited_until = None
                db.add(account)
                used_phone = account.phone or ""
                logs.append(f"[INFO] 使用账号 {account.phone} 同步群组元数据（第{attempt}次尝试成功）")
                login_ok = True
                break
            account.login_fail_count = (account.login_fail_count or 0) + 1
            account.last_login_fail_at = datetime.now(timezone.utc)
            db.add(account)
        if not login_ok:
            if is_risk_suspected(account):
                account.status = ST_RISK_SUSPECTED
                tag = "疑似风控"
            else:
                account.status = ST_DAILY_LIMITED
                account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
                tag = "当日受限"
            logs.append(
                f"[WARN] 账号 {account.phone} 登录失败（已达 {max_att} 次），标记为{tag}",
            )
            logs.append(f"[INFO] 跳过账号 {account.phone}（同步元数据）")
            logs.append("[INFO] 切换账号")
            logs.append("[INFO] 切换下一个账号（同步元数据）")
            db.add(account)
            client = None
            db.commit()
            continue
        db.commit()
        break

    if not client:
        return {"ok": False, "skipped": False, "message": "无法登录任何账号", "logs": logs}

    updated = 0
    try:
        for g in group_rows:
            ident = _normalize_chat_identifier(g.username)
            try:
                chat = await client.get_chat(ident)
                title = (getattr(chat, "title", None) or "").strip() or g.username
                pub = getattr(chat, "username", None)
                if pub:
                    pub = str(pub).strip() or None
                mc = getattr(chat, "members_count", None)
                g.title = title[:255]
                g.public_username = pub[:255] if pub else None
                g.members_count = int(mc) if mc is not None else (g.members_count or 0)
                db.add(g)
                updated += 1
                logs.append(f"已更新 {g.username} → 名称={title!r} 公开@{pub or '-'} 人数={g.members_count}")
            except Exception as exc:
                logs.append(f"群组 {g.username} 获取失败: {exc}")
        marker = db.query(Setting).filter(Setting.key == GROUP_METADATA_SYNC_KEY).first()
        ts = datetime.now(timezone.utc).isoformat()
        if marker:
            marker.value = ts
            db.add(marker)
        else:
            db.add(Setting(key=GROUP_METADATA_SYNC_KEY, value=ts))
        db.commit()
    finally:
        if client:
            try:
                await client.stop()
            except Exception:
                pass

    return {"ok": True, "skipped": False, "updated": updated, "logs": logs, "account": used_phone}


async def _sleep_while_running(total_sec: float, *, step: float = 1.0) -> bool:
    """分片 sleep；若 RUNNING 为 False 则返回 False。"""
    deadline = time.monotonic() + max(0.0, float(total_sec))
    while time.monotonic() < deadline:
        if not task_run_should_continue():
            return False
        remain = deadline - time.monotonic()
        if remain <= 0:
            break
        await asyncio.sleep(min(float(step), remain))
    return True


async def _is_user_in_group(client: Client, group_id: int, username: str, user_id: int | None) -> bool:
    if user_id:
        try:
            await client.get_chat_member(group_id, user_id)
            return True
        except Exception:
            pass
    try:
        await client.get_chat_member(group_id, username)
        return True
    except Exception:
        return False


async def run_task(config: dict[str, Any]) -> dict[str, Any]:
    """
    Telegram 拉人任务统一入口。
    通过 config 动态接收参数，避免依赖任何全局变量。
    """
    groups = config.get("groups", [])
    users = config.get("users", [])
    owner_id = config.get("owner_id")

    if not isinstance(groups, list) or not groups:
        raise ValueError("groups 必须是非空列表")
    if not isinstance(users, list) or not users:
        raise ValueError("users 必须是非空列表")

    log.info(
        "run_task executing groups=%s users_count=%s owner_id=%s",
        groups,
        len(users),
        owner_id,
    )
    db = SessionLocal()
    updated = {"active": 0, "limited": 0, "banned": 0}
    now_utc = datetime.now(timezone.utc)
    process_logs: list[str] = []
    summary = {"success": 0, "skipped": 0, "failed": 0}
    highlight_active: str | None = None
    highlight_previous: str | None = None
    highlight_connecting: str | None = None
    progress_job_id: str | None = config.get("progress_job_id")
    user_stopped = False

    def tl(msg: str) -> None:
        _task_log(process_logs, msg, progress_job_id)

    def th_pub() -> None:
        if progress_job_id:
            progress_highlight_publish(
                progress_job_id,
                active_phone=highlight_active,
                previous_phone=highlight_previous,
                connecting_phone=highlight_connecting,
            )

    async def _fail_wait_then_detail(detail: str, *, wait_msg: str = "上一账号已不可用，等待 {sec}s 后输出详情…") -> None:
        """仅用于已成功登录后触发的受限/换号（如 account_limited），降低同代理连点。登录/握手超时须立即打日志并切号。"""
        lo = min(LOGIN_FAIL_DELAY_MIN_SEC, LOGIN_FAIL_DELAY_MAX_SEC)
        hi = max(LOGIN_FAIL_DELAY_MIN_SEC, LOGIN_FAIL_DELAY_MAX_SEC)
        sec = random.randint(lo, hi)
        tl(wait_msg.format(sec=sec))
        await asyncio.sleep(sec)
        tl(detail)

    try:
        th_pub()
        # 与 /accounts 列表一致：id 降序，先使用左侧「账号队列」最上方的账号
        query = db.query(AccountFile).order_by(AccountFile.id.desc())
        if owner_id is not None:
            query = query.filter(AccountFile.owner_id == owner_id)
        account_rows = query.all()
        group_rows = db.query(Group).filter(Group.username.in_(groups)).all()
        group_map = {g.username: g for g in group_rows}
        available_groups: list[str] = []
        for group_name in groups:
            group = group_map.get(group_name)
            if not group:
                continue
            disabled_u = _as_utc(group.disabled_until)
            if disabled_u and now_utc < disabled_u:
                continue
            if group.today_added >= group.daily_limit:
                group.disabled_until = now_utc + timedelta(hours=12)
                group.status = "limited"
                db.add(group)
                continue
            available_groups.append(group_name)

        if not available_groups:
            raise ValueError("当前无可用目标群组（可能达到单日上限或处于禁用期）")

        runnable_accounts: list[AccountFile] = []
        for row in account_rows:
            recover_and_normalize(row, now_utc)
            if row.status == ST_RISK_SUSPECTED:
                updated["banned"] += 1
                continue
            row_lu = _as_utc(row.limited_until)
            if row.status == ST_DAILY_LIMITED and row_lu and now_utc < row_lu:
                updated["limited"] += 1
                continue
            if row.status != ST_NORMAL:
                continue
            runnable_accounts.append(row)

        if not runnable_accounts:
            raise ValueError("当前没有可执行的账号（账号受限或已封禁）")

        default_group = available_groups[0]
        already_in_group_users = set(config.get("already_in_group_users", []))
        account_idx = 0
        user_idx = 0

        while user_idx < len(users):
            if not task_run_should_continue():
                user_stopped = True
                tl("任务已中断")
                tl("已停止")
                break
            if account_idx >= len(runnable_accounts):
                for rest in users[user_idx:]:
                    summary["failed"] += 1
                    tl(f"用户 {rest} 失败: 无可用账号")
                break

            account = runnable_accounts[account_idx]
            now_utc = datetime.now(timezone.utc)
            recover_and_normalize(account, now_utc)
            acc_lu = _as_utc(account.limited_until)
            if account.status == ST_DAILY_LIMITED and acc_lu and now_utc < acc_lu:
                account_idx += 1
                continue
            if account.status == ST_RISK_SUSPECTED:
                account_idx += 1
                continue
            if account.status != ST_NORMAL:
                account_idx += 1
                continue
            if (account.today_used_count or 0) >= 3:
                account.status = ST_DAILY_LIMITED
                account.limited_until = now_utc + timedelta(hours=12)
                db.add(account)
                tl(f"账号 {account.phone} 达到当日阈值，标记为 daily_limited，切换下一个账号")
                db.commit()
                account_idx += 1
                continue

            proxy_obj = db.query(Proxy).filter(Proxy.id == account.proxy_id).first() if account.proxy_id else None
            proxy_dict = _build_proxy(proxy_obj, account.proxy_type)
            session_name = _resolve_session_name(account)
            client: Client | None = None
            group = None
            try:
                if proxy_dict:
                    proxy_label = f"{proxy_dict.get('hostname', '?')}:{proxy_dict.get('port', '?')}"
                else:
                    proxy_label = "直连"
                highlight_connecting = account.phone
                th_pub()
                attempt_cap = int(round(TELEGRAM_LOGIN_ATTEMPT_TIMEOUT))
                max_login_attempts = TELEGRAM_LOGIN_MAX_RETRIES
                tl(
                    f"账号 {account.phone} 经 {proxy_label} 登录：单次 wait_for 超时 {attempt_cap}s，"
                    f"最多 {max_login_attempts} 次；超限跳过该号。",
                )
                login_ok = False
                for attempt in range(1, max_login_attempts + 1):
                    if not task_run_should_continue():
                        user_stopped = True
                        tl("任务已中断")
                        tl("已停止")
                        break
                    ok, client, _ = await _single_login_attempt(
                        account,
                        session_name,
                        proxy_dict,
                        proxy_label,
                        TELEGRAM_LOGIN_ATTEMPT_TIMEOUT,
                        attempt,
                        max_login_attempts,
                        tl,
                    )
                    if ok:
                        account.login_fail_count = 0
                        account.status = ST_NORMAL
                        account.limited_until = None
                        db.add(account)
                        login_ok = True
                        break
                    account.login_fail_count = (account.login_fail_count or 0) + 1
                    account.last_login_fail_at = datetime.now(timezone.utc)
                    account.error_count = (account.error_count or 0) + 1
                    db.add(account)

                if user_stopped:
                    highlight_connecting = None
                    th_pub()
                    if client:
                        try:
                            await asyncio.wait_for(client.stop(), timeout=TELEGRAM_CLIENT_STOP_TIMEOUT)
                        except Exception:
                            pass
                    break

                if not login_ok:
                    highlight_connecting = None
                    if is_risk_suspected(account):
                        account.status = ST_RISK_SUSPECTED
                        account.limited_until = None
                        tag = "疑似风控"
                    else:
                        account.status = ST_DAILY_LIMITED
                        account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
                        tag = "当日受限"
                    tl(
                        f"[WARN] 账号 {account.phone} 登录失败（已达 {max_login_attempts} 次），标记为{tag}",
                    )
                    tl(f"[INFO] 跳过账号 {account.phone}，继续队列下一账号")
                    tl("[INFO] 切换账号")
                    tl("[INFO] 切换下一个账号")
                    account.last_used_time = datetime.now(timezone.utc)
                    highlight_previous = account.phone
                    highlight_active = None
                    db.add(account)
                    th_pub()
                    db.commit()
                    client = None

                if login_ok:
                    highlight_connecting = None
                    th_pub()
                    tl(f"账号 {account.phone} Telegram 会话已建立，正在校验目标群组并尝试入群…")
                    if not task_run_should_continue():
                        user_stopped = True
                        tl("任务已中断")
                        tl("已停止")
                    else:
                        try:
                            ok_in_group, group = await asyncio.wait_for(
                                _ensure_in_group(client, default_group),
                                timeout=TELEGRAM_ENSURE_GROUP_TIMEOUT,
                            )
                        except asyncio.TimeoutError:
                            highlight_connecting = None
                            account.error_count = (account.error_count or 0) + 1
                            account.last_used_time = datetime.now(timezone.utc)
                            db.add(account)
                            highlight_previous = account.phone
                            highlight_active = None
                            th_pub()
                            tl(
                                f"账号 {account.phone} 登录失败：校验/加入群组超过 {int(TELEGRAM_ENSURE_GROUP_TIMEOUT)}s，"
                                f"切换下一个账号",
                            )
                            raise
                        if not ok_in_group:
                            raise RuntimeError("group limited: join failed")

                        highlight_active = account.phone
                        th_pub()
                        tl(f"账号 {account.phone} 登录成功，开始处理用户队列")
                        switch_account = False

                        while user_idx < len(users):
                            if not task_run_should_continue():
                                user_stopped = True
                                tl("任务已中断")
                                tl("已停止")
                                break
                            target_user = users[user_idx]
                            i = user_idx + 1
                            skip_short_sleep = False
                            if target_user in already_in_group_users:
                                summary["skipped"] += 1
                                tl(f"[{i}/{len(users)}] 用户 {target_user} 已在群里，跳过")
                                user_idx += 1
                                continue

                            tl(f"[{i}/{len(users)}] 使用账号 {account.phone} 处理用户 {target_user}")
                            try:
                                if not task_run_should_continue():
                                    user_stopped = True
                                    tl("任务已中断")
                                    tl("已停止")
                                    break
                                user_obj = await client.get_users(target_user)
                                if await _is_user_in_group(client, group.id, target_user, getattr(user_obj, "id", None)):
                                    summary["skipped"] += 1
                                    tl(f"用户 {target_user} 已在群组 {default_group}，跳过")
                                    user_idx += 1
                                    continue

                                if not task_run_should_continue():
                                    user_stopped = True
                                    tl("任务已中断")
                                    tl("已停止")
                                    break
                                await client.add_chat_members(group.id, [user_obj.id])
                                summary["success"] += 1
                                account.status = ST_NORMAL
                                account.today_count = (account.today_count or 0) + 1
                                account.today_used_count = (account.today_used_count or 0) + 1
                                account.last_used_time = datetime.now(timezone.utc)

                                group_row = group_map.get(default_group)
                                if group_row:
                                    group_row.failed_streak = 0
                                    group_row.status = "normal"
                                    group_row.total_added = (group_row.total_added or 0) + 1
                                    group_row.today_added = (group_row.today_added or 0) + 1
                                    if group_row.today_added >= group_row.daily_limit:
                                        group_row.disabled_until = datetime.now(timezone.utc) + timedelta(hours=12)
                                        group_row.status = "limited"
                                        tl(f"群组 {default_group} 达到每日上限，12小时禁用")
                                    db.add(group_row)

                                db.add(account)
                                db.commit()
                                user_idx += 1
                                tl(f"用户 {target_user} 拉入群组 {default_group} 成功，开始执行 60 秒间隔（结束后再处理下一名）")
                                t_wait = time.monotonic()
                                if not await _sleep_while_running(60.0):
                                    user_stopped = True
                                    tl("任务已中断")
                                    tl("已停止")
                                    break
                                waited = int(time.monotonic() - t_wait)
                                tl(f"60 秒间隔结束（实际等待 {waited}s），继续处理队列")
                                skip_short_sleep = True
                                if (account.today_used_count or 0) >= 3:
                                    account.status = ST_DAILY_LIMITED
                                    account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
                                    db.add(account)
                                    highlight_connecting = None
                                    highlight_previous = account.phone
                                    highlight_active = None
                                    th_pub()
                                    tl(f"账号 {account.phone} 达到当日阈值，标记为 daily_limited，切换下一个账号")
                                    switch_account = True
                                    break
                            except Exception as exc:
                                reason = _classify_failure_reason(exc)
                                account.error_count = (account.error_count or 0) + 1
                                db.add(account)
                                if reason in {"account_limited", "account_auth_failed"}:
                                    account.last_used_time = datetime.now(timezone.utc)
                                    account.status = (
                                        ST_DAILY_LIMITED if reason == "account_limited" else ST_RISK_SUSPECTED
                                    )
                                    if reason == "account_limited":
                                        account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
                                    else:
                                        account.limited_until = None
                                    highlight_connecting = None
                                    highlight_previous = account.phone
                                    highlight_active = None
                                    db.add(account)
                                    th_pub()
                                    await _fail_wait_then_detail(
                                        f"账号 {account.phone} 失败: {reason}，切换下一个账号",
                                        wait_msg="上一账号已不可用，等待 {sec}s 后再连下一账号…",
                                    )
                                    switch_account = True
                                    break
                                if reason == "group_limited":
                                    group_row = group_map.get(default_group)
                                    if group_row:
                                        group_row.failed_streak = (group_row.failed_streak or 0) + 1
                                        if group_row.failed_streak >= 3:
                                            group_row.status = "limited"
                                            group_row.disabled_until = datetime.now(timezone.utc) + timedelta(hours=12)
                                        db.add(group_row)
                                    summary["failed"] += 1
                                    tl(f"用户 {target_user} 失败: 目标群组受限")
                                    user_idx += 1
                                elif isinstance(exc, FloodWait):
                                    summary["failed"] += 1
                                    tl(f"用户 {target_user} 失败: FloodWait {exc.value}s")
                                    user_idx += 1
                                elif reason == "user_issue":
                                    summary["failed"] += 1
                                    tl(f"用户 {target_user} 失败: 用户本身问题")
                                    user_idx += 1
                                else:
                                    summary["failed"] += 1
                                    tl(f"用户 {target_user} 失败: {reason} {exc}")
                                    user_idx += 1
                            finally:
                                if not skip_short_sleep:
                                    if not await _sleep_while_running(float(random.randint(1, 2))):
                                        user_stopped = True
                                        tl("任务已中断")
                                        tl("已停止")

                        if not switch_account and user_idx >= len(users):
                            tl(f"账号 {account.phone} 用户队列处理完成")

                    if user_stopped:
                        break
            except asyncio.TimeoutError:
                pass
            except Exception as exc:
                highlight_connecting = None
                reason = _classify_failure_reason(exc)
                account.error_count = (account.error_count or 0) + 1
                if reason in {"account_limited", "account_auth_failed"}:
                    account.last_used_time = datetime.now(timezone.utc)
                    account.status = ST_DAILY_LIMITED if reason == "account_limited" else ST_RISK_SUSPECTED
                    if reason == "account_limited":
                        account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
                    else:
                        account.limited_until = None
                highlight_previous = account.phone
                highlight_active = None
                if reason in {"account_limited", "account_auth_failed"}:
                    detail = f"账号 {account.phone} 登录/入群失败: {reason}，切换下一个账号"
                else:
                    detail = f"账号 {account.phone} 登录/入群失败: {reason} {exc}，切换下一个账号"
                db.add(account)
                th_pub()
                tl(detail)
            finally:
                if client:
                    try:
                        await asyncio.wait_for(client.stop(), timeout=TELEGRAM_CLIENT_STOP_TIMEOUT)
                    except (asyncio.TimeoutError, Exception):
                        pass
                try:
                    db.commit()
                except Exception:
                    log.exception("run_task incremental commit failed")
            account_idx += 1

        db.commit()
    finally:
        db.close()

    log.info("run_task finished groups=%s updated=%s stopped=%s", groups, updated, user_stopped)
    for line in process_logs:
        log.info("task-step: %s", line)
    final_status = "stopped" if user_stopped else "completed"
    return {
        "status": final_status,
        "stopped": user_stopped,
        "groups": available_groups,
        "users_count": len(users),
        "accounts_path": "auto_scan",
        "updated": updated,
        "summary": summary,
        "logs": process_logs[-100:],
        "highlight": {
            "active_phone": highlight_active,
            "previous_phone": highlight_previous,
        },
    }
