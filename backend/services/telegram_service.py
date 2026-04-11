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
from services.task_progress import progress_append, progress_highlight_publish

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


def _classify_account_status(exc: Exception) -> str:
    msg = str(exc).lower()
    if "peer_flood" in msg or "floodwait" in msg:
        return "limited"
    if "session" in msg or "登录失败" in msg or "login failed" in msg:
        return "banned"
    return "active"


def _recover_account_status(account: AccountFile, now_utc: datetime) -> None:
    limited_until = account.limited_until
    if limited_until and limited_until.tzinfo is None:
        limited_until = limited_until.replace(tzinfo=timezone.utc)
    elif limited_until:
        limited_until = limited_until.astimezone(timezone.utc)

    if account.status == "limited_today" and limited_until and now_utc >= limited_until:
        account.status = "active"
        account.limited_until = None

    if account.status == "limited_today" and limited_until and now_utc - limited_until > timedelta(days=3):
        account.status = "banned"


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
        _recover_account_status(row, now_utc)
        if row.status == "banned":
            continue
        lu = row.limited_until
        if lu and lu.tzinfo is None:
            lu = lu.replace(tzinfo=timezone.utc)
        elif lu:
            lu = lu.astimezone(timezone.utc)
        if row.status == "limited_today" and lu and now_utc < lu:
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

    for account in runnable:
        proxy_obj = db.query(Proxy).filter(Proxy.id == account.proxy_id).first() if account.proxy_id else None
        proxy_dict = _build_proxy(proxy_obj, account.proxy_type)
        session_name = _resolve_session_name(account)
        try:
            client = Client(
                name=session_name,
                api_id=API_ID,
                api_hash=API_HASH,
                phone_number=account.phone,
                proxy=proxy_dict,
            )
            await client.start()
            used_phone = account.phone or ""
            logs.append(f"使用账号 {account.phone} 同步群组元数据")
            break
        except Exception as exc:
            logs.append(f"账号 {account.phone} 登录失败: {exc}")
            if client:
                try:
                    await client.stop()
                except Exception:
                    pass
            client = None

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
            _recover_account_status(row, now_utc)
            if row.status == "banned":
                updated["banned"] += 1
                continue
            row_lu = _as_utc(row.limited_until)
            if row.status == "limited_today" and row_lu and now_utc < row_lu:
                updated["limited"] += 1
                continue
            runnable_accounts.append(row)

        if not runnable_accounts:
            raise ValueError("当前没有可执行的账号（账号受限或已封禁）")

        default_group = available_groups[0]
        already_in_group_users = set(config.get("already_in_group_users", []))
        account_idx = 0
        user_idx = 0

        while user_idx < len(users):
            if account_idx >= len(runnable_accounts):
                for rest in users[user_idx:]:
                    summary["failed"] += 1
                    tl(f"用户 {rest} 失败: 无可用账号")
                break

            account = runnable_accounts[account_idx]
            now_utc = datetime.now(timezone.utc)
            acc_lu = _as_utc(account.limited_until)
            if account.status == "limited_today" and acc_lu and now_utc < acc_lu:
                account_idx += 1
                continue
            if account.status in {"limited_long", "banned"}:
                account_idx += 1
                continue
            if (account.today_used_count or 0) >= 3:
                account.status = "limited_today"
                account.limited_until = now_utc + timedelta(hours=12)
                db.add(account)
                tl(f"账号 {account.phone} 达到当日阈值，标记 limited_today，切换下一个账号")
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
                login_cap = int(TELEGRAM_START_TIMEOUT)
                tl(
                    f"账号 {account.phone} 经 {proxy_label} 发起 Telegram 登录（client.start，硬超时 {login_cap}s）；"
                    f"此阶段在等与服务器握手，不是拉人成功后的 60 秒间隔。",
                )
                client = Client(
                    name=session_name,
                    api_id=API_ID,
                    api_hash=API_HASH,
                    phone_number=account.phone,
                    proxy=proxy_dict,
                    no_updates=True,
                )

                t_login_start = time.monotonic()

                async def _login_heartbeat() -> None:
                    while True:
                        await asyncio.sleep(TELEGRAM_START_HEARTBEAT_SEC)
                        elapsed = int(time.monotonic() - t_login_start)
                        remain = max(0, login_cap - elapsed)
                        tl(
                            f"账号 {account.phone} 仍在登录中（经 {proxy_label}）："
                            f"已约 {elapsed}s / 上限 {login_cap}s，预计最多再等约 {remain}s；"
                            f"超时将放弃本号并换下一个。",
                        )

                # finally 必须在 except 里长 sleep 之前执行，否则会先睡 30～60s 才停心跳，日志仍刷「仍在登录」且无失败提示
                login_timed_out = False
                hb = asyncio.create_task(_login_heartbeat())
                try:
                    await _client_start_with_hard_timeout(client, TELEGRAM_START_TIMEOUT)
                except asyncio.TimeoutError:
                    login_timed_out = True
                finally:
                    hb.cancel()
                    try:
                        await hb
                    except asyncio.CancelledError:
                        pass
                if login_timed_out:
                    highlight_connecting = None
                    account.error_count = (account.error_count or 0) + 1
                    account.last_used_time = datetime.now(timezone.utc)
                    db.add(account)
                    highlight_previous = account.phone
                    highlight_active = None
                    th_pub()
                    tl(
                        f"账号 {account.phone} 登录失败：client.start 超过 {login_cap}s 未完成（代理或链路卡住），"
                        f"切换下一个账号",
                    )
                    raise asyncio.TimeoutError
                highlight_connecting = None
                th_pub()
                tl(f"账号 {account.phone} Telegram 会话已建立，正在校验目标群组并尝试入群…")
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
                        user_obj = await client.get_users(target_user)
                        if await _is_user_in_group(client, group.id, target_user, getattr(user_obj, "id", None)):
                            summary["skipped"] += 1
                            tl(f"用户 {target_user} 已在群组 {default_group}，跳过")
                            user_idx += 1
                            continue

                        await client.add_chat_members(group.id, [user_obj.id])
                        summary["success"] += 1
                        account.status = "active"
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
                        await asyncio.sleep(60)
                        waited = int(time.monotonic() - t_wait)
                        tl(f"60 秒间隔结束（实际等待 {waited}s），继续处理队列")
                        skip_short_sleep = True
                        if (account.today_used_count or 0) >= 3:
                            account.status = "limited_today"
                            account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
                            db.add(account)
                            highlight_connecting = None
                            highlight_previous = account.phone
                            highlight_active = None
                            th_pub()
                            tl(f"账号 {account.phone} 达到当日阈值，标记 limited_today，切换下一个账号")
                            switch_account = True
                            break
                    except Exception as exc:
                        reason = _classify_failure_reason(exc)
                        account.error_count = (account.error_count or 0) + 1
                        db.add(account)
                        if reason in {"account_limited", "account_auth_failed"}:
                            account.last_used_time = datetime.now(timezone.utc)
                            account.status = "limited_today" if reason == "account_limited" else "limited_long"
                            if reason == "account_limited":
                                account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
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
                            await asyncio.sleep(random.randint(1, 2))

                if not switch_account and user_idx >= len(users):
                    tl(f"账号 {account.phone} 用户队列处理完成")
            except asyncio.TimeoutError:
                pass
            except Exception as exc:
                highlight_connecting = None
                reason = _classify_failure_reason(exc)
                account.error_count = (account.error_count or 0) + 1
                if reason in {"account_limited", "account_auth_failed"}:
                    account.last_used_time = datetime.now(timezone.utc)
                    account.status = "limited_today" if reason == "account_limited" else "limited_long"
                    if reason == "account_limited":
                        account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
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

    log.info("run_task finished groups=%s updated=%s", groups, updated)
    for line in process_logs:
        log.info("task-step: %s", line)
    return {
        "status": "running",
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
