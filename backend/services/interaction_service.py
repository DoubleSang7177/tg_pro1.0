from __future__ import annotations

import asyncio
import random
from datetime import datetime, timedelta, timezone

from pyrogram.errors import FloodWait, PeerFlood

from database import SessionLocal
from logger import get_logger
from models import AccountFile, InteractionTask, Proxy
from services.account_status import ST_DAILY_LIMITED, ST_NORMAL, recover_and_normalize
from services.telegram_service import (
    TELEGRAM_CLIENT_STOP_TIMEOUT,
    TELEGRAM_ENSURE_GROUP_TIMEOUT,
    TELEGRAM_LOGIN_ATTEMPT_TIMEOUT,
    TELEGRAM_LOGIN_MAX_RETRIES,
    _build_proxy,
    _ensure_in_group,
    _normalize_chat_identifier,
    _resolve_session_name,
    _single_login_attempt,
)

log = get_logger("interaction_service")

ENGAGEMENT_REACTIONS = ["❤️", "👍", "🔥", "🎉", "💯"]
GROUP_GAP_MIN_SEC = 5
GROUP_GAP_MAX_SEC = 15
_FLUSH_EVERY = 12


def _msg_utc(msg_date: datetime) -> datetime:
    if msg_date.tzinfo is None:
        return msg_date.replace(tzinfo=timezone.utc)
    return msg_date.astimezone(timezone.utc)


def run_interaction_task_sync(task_id: int) -> None:
    try:
        asyncio.run(_run_interaction_task_async(task_id))
    except Exception:
        log.exception("interaction task crashed task_id=%s", task_id)
        db = SessionLocal()
        try:
            row = db.query(InteractionTask).filter(InteractionTask.id == task_id).first()
            if row and row.status == "running":
                row.status = "failed"
                db.add(row)
                db.commit()
        finally:
            db.close()


def _flush_task_buffers(
    db,
    task_id: int,
    succ_buf: int,
    fail_buf: int,
) -> tuple[int, int]:
    if succ_buf == 0 and fail_buf == 0:
        return 0, 0
    row = db.query(InteractionTask).filter(InteractionTask.id == task_id).first()
    if row:
        row.success_count = (row.success_count or 0) + succ_buf
        row.fail_count = (row.fail_count or 0) + fail_buf
        db.add(row)
        db.commit()
    return 0, 0


async def _run_interaction_task_async(task_id: int) -> None:
    db = SessionLocal()
    succ_buf = 0
    fail_buf = 0
    try:
        task_row = db.query(InteractionTask).filter(InteractionTask.id == task_id).first()
        if task_row is None:
            return
        task_row.status = "running"
        db.add(task_row)
        db.commit()

        group_names = list(task_row.target_groups or [])
        account_ids = list(task_row.account_ids or [])
        scan_limit = max(10, min(int(task_row.scan_limit or 300), 5000))

        rows = db.query(AccountFile).filter(AccountFile.id.in_(account_ids)).all()
        id_map = {a.id: a for a in rows}
        accounts_ordered = [id_map[i] for i in account_ids if i in id_map]

        now_utc = datetime.now(timezone.utc)
        day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

        def tl(msg: str) -> None:
            log.info("interaction tid=%s %s", task_id, msg)

        for account in accounts_ordered:
            recover_and_normalize(account, datetime.now(timezone.utc))
            if account.status not in (ST_NORMAL, ST_DAILY_LIMITED):
                continue

            proxy_obj = db.query(Proxy).filter(Proxy.id == account.proxy_id).first() if account.proxy_id else None
            proxy_dict = _build_proxy(proxy_obj, account.proxy_type)
            session_name = _resolve_session_name(account)
            proxy_label = (
                f"{proxy_dict.get('hostname', '?')}:{proxy_dict.get('port', '?')}" if proxy_dict else "直连"
            )
            client = None
            login_ok = False
            for attempt in range(1, TELEGRAM_LOGIN_MAX_RETRIES + 1):
                ok, client, _ = await _single_login_attempt(
                    account,
                    session_name,
                    proxy_dict,
                    proxy_label,
                    TELEGRAM_LOGIN_ATTEMPT_TIMEOUT,
                    attempt,
                    tl,
                )
                if ok:
                    login_ok = True
                    break
                db.refresh(account)

            if not login_ok:
                tl(f"[WARN] 跳过账号 {account.phone}（登录失败）")
                if client:
                    try:
                        await asyncio.wait_for(client.stop(), timeout=TELEGRAM_CLIENT_STOP_TIMEOUT)
                    except Exception:
                        pass
                continue

            peer_flood_break = False
            try:
                for gname in group_names:
                    if peer_flood_break:
                        break
                    ident = _normalize_chat_identifier(gname)
                    try:
                        ok_join, chat = await asyncio.wait_for(
                            _ensure_in_group(client, ident),
                            timeout=TELEGRAM_ENSURE_GROUP_TIMEOUT,
                        )
                    except Exception as exc:
                        tl(f"[WARN] 群组 {ident} 入群/校验失败，跳过: {str(exc)[:500]}")
                        fail_buf += 1
                        if fail_buf >= _FLUSH_EVERY:
                            succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
                        continue
                    if not ok_join:
                        tl(f"[WARN] 群组 {ident} 未加入，跳过")
                        fail_buf += 1
                        if fail_buf >= _FLUSH_EVERY:
                            succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
                        continue

                    chat_id = chat.id
                    try:
                        async for msg in client.get_chat_history(chat_id, limit=scan_limit):
                            if msg.id is None:
                                continue
                            if msg.date and _msg_utc(msg.date) < day_start:
                                break
                            try:
                                emoji = random.choice(ENGAGEMENT_REACTIONS)
                                await client.send_reaction(chat_id, msg.id, emoji=emoji)
                                succ_buf += 1
                                if succ_buf >= _FLUSH_EVERY:
                                    succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
                                await asyncio.sleep(random.uniform(0.2, 0.9))
                            except PeerFlood:
                                succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
                                account.status = ST_DAILY_LIMITED
                                account.limited_until = datetime.now(timezone.utc) + timedelta(hours=12)
                                account.last_used_time = datetime.now(timezone.utc)
                                db.add(account)
                                db.commit()
                                tl(f"[WARN] PEER_FLOOD 账号 {account.phone}，已标记当日受限")
                                peer_flood_break = True
                                break
                            except FloodWait as fw:
                                await asyncio.sleep(min(int(fw.value), 120))
                            except Exception:
                                fail_buf += 1
                                if fail_buf >= _FLUSH_EVERY:
                                    succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
                    except Exception as exc:
                        tl(f"[WARN] 群组 {ident} 拉取历史/反应异常，跳过: {str(exc)[:500]}")
                        fail_buf += 1
                        if fail_buf >= _FLUSH_EVERY:
                            succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)

                    if not peer_flood_break:
                        await asyncio.sleep(random.randint(GROUP_GAP_MIN_SEC, GROUP_GAP_MAX_SEC))
            finally:
                if client:
                    try:
                        await asyncio.wait_for(client.stop(), timeout=TELEGRAM_CLIENT_STOP_TIMEOUT)
                    except Exception:
                        pass

        succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
        task_row = db.query(InteractionTask).filter(InteractionTask.id == task_id).first()
        if task_row:
            task_row.status = "completed"
            db.add(task_row)
            db.commit()
    except Exception:
        log.exception("interaction task error task_id=%s", task_id)
        try:
            db.rollback()
        except Exception:
            pass
        succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
        task_row = db.query(InteractionTask).filter(InteractionTask.id == task_id).first()
        if task_row:
            task_row.status = "failed"
            db.add(task_row)
            db.commit()
    finally:
        db.close()
