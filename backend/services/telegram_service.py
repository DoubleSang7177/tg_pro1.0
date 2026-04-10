from __future__ import annotations

import asyncio
import os
import random
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pyrogram import Client
from pyrogram.errors import FloodWait

from database import SessionLocal
from logger import get_logger
from models import AccountFile, Group, Proxy

log = get_logger("telegram_service")
API_ID = int(os.getenv("TELEGRAM_API_ID", "20954937"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "d5a748cfdb420593307b5265c1864ba3")


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
    try:
        query = db.query(AccountFile).order_by(AccountFile.id.asc())
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
            if group.disabled_until and now_utc < group.disabled_until:
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
            if row.status == "limited_today" and row.limited_until and now_utc < row.limited_until:
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
                    process_logs.append(f"用户 {rest} 失败: 无可用账号")
                break

            account = runnable_accounts[account_idx]
            now_utc = datetime.now(timezone.utc)
            if account.status == "limited_today" and account.limited_until and now_utc < account.limited_until:
                account_idx += 1
                continue
            if account.status in {"limited_long", "banned"}:
                account_idx += 1
                continue
            if (account.today_used_count or 0) >= 3:
                account.status = "limited_today"
                account.limited_until = now_utc + timedelta(hours=12)
                db.add(account)
                process_logs.append(f"账号 {account.phone} 达到当日阈值，标记 limited_today，切换下一个账号")
                account_idx += 1
                continue

            proxy_obj = db.query(Proxy).filter(Proxy.id == account.proxy_id).first() if account.proxy_id else None
            proxy_dict = _build_proxy(proxy_obj, account.proxy_type)
            session_name = _resolve_session_name(account)
            client: Client | None = None
            group = None
            try:
                client = Client(
                    name=session_name,
                    api_id=API_ID,
                    api_hash=API_HASH,
                    phone_number=account.phone,
                    proxy=proxy_dict,
                )
                await client.start()
                ok_in_group, group = await _ensure_in_group(client, default_group)
                if not ok_in_group:
                    raise RuntimeError("group limited: join failed")

                process_logs.append(f"账号 {account.phone} 登录成功，开始处理用户队列")
                switch_account = False

                while user_idx < len(users):
                    target_user = users[user_idx]
                    i = user_idx + 1
                    if target_user in already_in_group_users:
                        summary["skipped"] += 1
                        process_logs.append(f"[{i}/{len(users)}] 用户 {target_user} 已在群里，跳过")
                        user_idx += 1
                        continue

                    process_logs.append(f"[{i}/{len(users)}] 使用账号 {account.phone} 处理用户 {target_user}")
                    try:
                        user_obj = await client.get_users(target_user)
                        if await _is_user_in_group(client, group.id, target_user, getattr(user_obj, "id", None)):
                            summary["skipped"] += 1
                            process_logs.append(f"用户 {target_user} 已在群组 {default_group}，跳过")
                            user_idx += 1
                            continue

                        await client.add_chat_members(group.id, [user_obj.id])
                        await asyncio.sleep(2)
                        summary["success"] += 1
                        process_logs.append(f"用户 {target_user} 拉入群组 {default_group} 成功")
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
                                process_logs.append(f"群组 {default_group} 达到每日上限，12小时禁用")
                            db.add(group_row)

                        db.add(account)
                        user_idx += 1
                        if (account.today_used_count or 0) >= 3:
                            account.status = "limited_today"
                            account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
                            db.add(account)
                            process_logs.append(f"账号 {account.phone} 达到当日阈值，标记 limited_today，切换下一个账号")
                            switch_account = True
                            break
                    except Exception as exc:
                        reason = _classify_failure_reason(exc)
                        account.error_count = (account.error_count or 0) + 1
                        db.add(account)
                        if reason in {"account_limited", "account_auth_failed"}:
                            account.status = "limited_today" if reason == "account_limited" else "limited_long"
                            if reason == "account_limited":
                                account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
                            db.add(account)
                            process_logs.append(f"账号 {account.phone} 失败: {reason}，切换下一个账号")
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
                            process_logs.append(f"用户 {target_user} 失败: 目标群组受限")
                            user_idx += 1
                        elif isinstance(exc, FloodWait):
                            summary["failed"] += 1
                            process_logs.append(f"用户 {target_user} 失败: FloodWait {exc.value}s")
                            user_idx += 1
                        elif reason == "user_issue":
                            summary["failed"] += 1
                            process_logs.append(f"用户 {target_user} 失败: 用户本身问题")
                            user_idx += 1
                        else:
                            summary["failed"] += 1
                            process_logs.append(f"用户 {target_user} 失败: {reason} {exc}")
                            user_idx += 1
                    finally:
                        await asyncio.sleep(random.randint(1, 2))

                if not switch_account and user_idx >= len(users):
                    process_logs.append(f"账号 {account.phone} 用户队列处理完成")
            except Exception as exc:
                reason = _classify_failure_reason(exc)
                account.error_count = (account.error_count or 0) + 1
                if reason in {"account_limited", "account_auth_failed"}:
                    account.status = "limited_today" if reason == "account_limited" else "limited_long"
                    if reason == "account_limited":
                        account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
                    process_logs.append(f"账号 {account.phone} 登录/入群失败: {reason}，切换下一个账号")
                else:
                    process_logs.append(f"账号 {account.phone} 登录/入群失败: {reason} {exc}，切换下一个账号")
                db.add(account)
            finally:
                if client:
                    try:
                        await client.stop()
                    except Exception:
                        pass
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
    }
