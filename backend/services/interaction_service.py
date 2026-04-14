from __future__ import annotations

import asyncio
import random
import time as time_mod
from datetime import datetime, timezone

from pyrogram.errors import FloodWait, PeerFlood

from database import SessionLocal
from logger import get_logger
from models import AccountFile, Group, InteractionTask, Proxy
from services.account_activity_log import record_account_activity as _iag_activity
from services.account_status import (
    ST_DAILY_LIMITED,
    ST_NORMAL,
    login_fail_reason_cn,
    mark_daily_limited,
    mark_risk_login_failed,
    recover_and_normalize,
)
from services.interaction_live_log import append as live_append
from services.interaction_live_log import finalize as live_finalize
from services.task_run_control import clear_interaction_job, task_run_should_continue, task_run_stop
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


def _interaction_activity(owner_id: int | None, phone: str | None, *, action: str, status: str, level: str) -> None:
    try:
        _iag_activity(int(owner_id or 0), phone, action=action, status=status, level=level)
    except Exception:
        log.debug("interaction activity log failed", exc_info=True)

ENGAGEMENT_REACTIONS = ["❤️", "👍", "🔥", "🎉", "💯"]
GROUP_GAP_MIN_SEC = 5
GROUP_GAP_MAX_SEC = 15
_FLUSH_EVERY = 12
# 每群只拉 1 条最新消息，避免 get_chat_history 大批量阻塞
GROUP_HISTORY_LIMIT = 1
GROUP_FETCH_TIMEOUT_SEC = 5.0
GROUP_REACTION_TIMEOUT_SEC = 5.0
ROUND_GAP_MIN_SEC = 8
ROUND_GAP_MAX_SEC = 20


def _mask_phone(phone: str | None) -> str:
    p = (phone or "").strip()
    if not p:
        return "—"
    if len(p) <= 5:
        return "****"
    return p[:5] + "****"


def _msg_utc(msg_date: datetime) -> datetime:
    if msg_date.tzinfo is None:
        return msg_date.replace(tzinfo=timezone.utc)
    return msg_date.astimezone(timezone.utc)


async def _fetch_latest_message(client, chat_id: int, *, limit: int = 1):
    """async for 只取首条即返回，避免长时间迭代。"""
    async for m in client.get_chat_history(chat_id, limit=limit):
        return m
    return None


async def _sleep_if_running(total_sec: float, *, step: float = 1.0) -> bool:
    deadline = time_mod.monotonic() + max(0.0, float(total_sec))
    while time_mod.monotonic() < deadline:
        if not task_run_should_continue():
            return False
        remain = deadline - time_mod.monotonic()
        if remain <= 0:
            break
        await asyncio.sleep(min(float(step), remain))
    return True


def run_interaction_task_sync(task_id: int, job_id: str | None = None) -> None:
    try:
        asyncio.run(_run_interaction_task_async(task_id, job_id))
    except Exception:
        log.exception("interaction task crashed task_id=%s", task_id)
        live_finalize(job_id, "failed")
        db = SessionLocal()
        try:
            row = db.query(InteractionTask).filter(InteractionTask.id == task_id).first()
            if row and row.status == "running":
                row.status = "failed"
                db.add(row)
                db.commit()
        finally:
            db.close()
    finally:
        clear_interaction_job()
        task_run_stop()


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


async def _run_interaction_task_async(task_id: int, job_id: str | None = None) -> None:
    db = SessionLocal()
    succ_buf = 0
    fail_buf = 0
    try:
        task_row = db.query(InteractionTask).filter(InteractionTask.id == task_id).first()
        if task_row is None:
            live_finalize(job_id, "failed")
            return
        task_row.status = "running"
        db.add(task_row)
        db.commit()

        group_names = list(task_row.target_groups or [])
        account_ids = list(task_row.account_ids or [])

        group_titles: dict[str, str] = {}
        if group_names:
            for gr in db.query(Group).filter(Group.username.in_(group_names)).all():
                group_titles[gr.username] = (gr.title or gr.username).strip() or gr.username

        rows = db.query(AccountFile).filter(AccountFile.id.in_(account_ids)).all()
        id_map = {a.id: a for a in rows}
        accounts_ordered = [id_map[i] for i in account_ids if i in id_map]

        now_utc = datetime.now(timezone.utc)
        day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        n_acc = len(accounts_ordered)
        n_grp = len(group_names)

        def emit(
            level: str,
            account: str,
            group: str,
            emoji: str = "",
            message: str = "",
            *,
            layer: str = "",
            progress: str = "",
        ) -> None:
            live_append(
                job_id,
                level=level,
                account=account,
                group=group,
                emoji=emoji,
                message=message,
                layer=layer,
                progress=progress,
            )

        emit("success", "—", "SYSTEM", "▶", "任务已启动", layer="system", progress="—")

        user_stopped = False

        def mark_interrupted(progress: str = "—") -> None:
            nonlocal user_stopped
            user_stopped = True
            emit("warn", "—", "SYSTEM", "⏹", "任务已中断", layer="system", progress=progress)
            emit("warn", "—", "SYSTEM", "✓", "已停止", layer="system", progress=progress)

        def tl(msg: str) -> None:
            log.info("interaction tid=%s %s", task_id, msg)

        last_interacted_msg_id: dict[str, int] = {}
        round_idx = 0
        while task_run_should_continue():
            round_idx += 1
            emit("success", "—", "SYSTEM", "🔁", f"开始第 {round_idx} 轮互动", layer="system", progress="—")
            for acc_i, account in enumerate(accounts_ordered, start=1):
                pr_acc = f"轮 {round_idx} · 账号 {acc_i}/{n_acc}" if n_acc else f"轮 {round_idx} · 账号 —"
                if not task_run_should_continue():
                    mark_interrupted(pr_acc)
                    break
                recover_and_normalize(account, datetime.now(timezone.utc))
                if account.status not in (ST_NORMAL, ST_DAILY_LIMITED):
                    emit(
                        "warn",
                        _mask_phone(account.phone),
                        "—",
                        "⏭",
                        "跳过 · 状态不可用",
                        layer="account",
                        progress=pr_acc,
                    )
                    continue

                if acc_i > 1:
                    emit("success", "—", "SYSTEM", "🔁", "切换账号", layer="system", progress=pr_acc)

                emit("success", _mask_phone(account.phone), "—", "📱", "开始账号", layer="account", progress=pr_acc)
                max_login = TELEGRAM_LOGIN_MAX_RETRIES
                proxy_obj = db.query(Proxy).filter(Proxy.id == account.proxy_id).first() if account.proxy_id else None
                proxy_dict = _build_proxy(proxy_obj, account.proxy_type)
                session_name = _resolve_session_name(account)
                proxy_label = (
                    f"{proxy_dict.get('hostname', '?')}:{proxy_dict.get('port', '?')}" if proxy_dict else "直连"
                )
                client = None
                login_ok = False
                last_login_err: str | None = None

                for attempt in range(1, max_login + 1):
                    if not task_run_should_continue():
                        mark_interrupted(pr_acc)
                        break
                    emit(
                        "info",
                        _mask_phone(account.phone),
                        "—",
                        "⏳",
                        f"登录中（第{attempt}/{max_login}次）",
                        layer="account",
                        progress=pr_acc,
                    )
                    ok, client, err = await _single_login_attempt(
                        account,
                        session_name,
                        proxy_dict,
                        proxy_label,
                        TELEGRAM_LOGIN_ATTEMPT_TIMEOUT,
                        attempt,
                        max_login,
                        tl,
                    )
                    if ok:
                        login_ok = True
                        _interaction_activity(
                            task_row.owner_id,
                            account.phone,
                            action="登录",
                            status="互动·成功",
                            level="success",
                        )
                        break
                    last_login_err = err
                    db.refresh(account)

                if not login_ok:
                    mark_risk_login_failed(
                        account,
                        datetime.now(timezone.utc),
                        logger=log,
                        task_notify=tl,
                        status_reason_cn=login_fail_reason_cn(last_login_err),
                    )
                    db.add(account)
                    db.commit()
                    _interaction_activity(
                        task_row.owner_id,
                        account.phone,
                        action="登录",
                        status=login_fail_reason_cn(last_login_err),
                        level="error",
                    )
                    emit("warn", _mask_phone(account.phone), "—", "✕", "登录失败", layer="account", progress=pr_acc)
                    emit("success", "—", "SYSTEM", "📋", "账号完成", layer="system", progress=pr_acc)
                    if client:
                        try:
                            await asyncio.wait_for(client.stop(), timeout=TELEGRAM_CLIENT_STOP_TIMEOUT)
                        except Exception:
                            pass
                    continue

                emit("success", _mask_phone(account.phone), "—", "✓", "登录成功", layer="account", progress=pr_acc)
                peer_flood_break = False
                try:
                    for grp_i, gname in enumerate(group_names, start=1):
                        if peer_flood_break:
                            break
                        pr_grp = f"轮 {round_idx} · 账号 {acc_i}/{n_acc} · 群 {grp_i}/{n_grp}" if n_grp else pr_acc
                        if not task_run_should_continue():
                            mark_interrupted(pr_grp)
                            break
                        ident = _normalize_chat_identifier(gname)
                        g_label = group_titles.get(ident, ident)
                        try:
                            ok_join, chat = await asyncio.wait_for(
                                _ensure_in_group(client, ident),
                                timeout=TELEGRAM_ENSURE_GROUP_TIMEOUT,
                            )
                        except Exception:
                            fail_buf += 1
                            if fail_buf >= _FLUSH_EVERY:
                                succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
                            continue
                        if not ok_join:
                            fail_buf += 1
                            if fail_buf >= _FLUSH_EVERY:
                                succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
                            continue

                        chat_id = chat.id
                        msg = await _fetch_latest_message(client, chat_id, limit=GROUP_HISTORY_LIMIT)
                        if msg is None or getattr(msg, "id", None) is None:
                            continue
                        if msg.date and _msg_utc(msg.date) < day_start:
                            continue

                        cursor_key = str(ident)
                        if int(msg.id) <= int(last_interacted_msg_id.get(cursor_key, 0)):
                            emit(
                                "warn",
                                _mask_phone(account.phone),
                                g_label,
                                "↺",
                                "无新消息 · 跳过",
                                layer="group",
                                progress=pr_grp,
                            )
                            continue

                        emoji = random.choice(ENGAGEMENT_REACTIONS)
                        emit(
                            "info",
                            _mask_phone(account.phone),
                            g_label,
                            emoji,
                            "发送互动",
                            layer="group",
                            progress=pr_grp,
                        )
                        try:
                            await asyncio.wait_for(
                                client.send_reaction(chat_id, msg.id, emoji=emoji),
                                timeout=GROUP_REACTION_TIMEOUT_SEC,
                            )
                            succ_buf += 1
                            last_interacted_msg_id[cursor_key] = int(msg.id)
                            emit(
                                "success",
                                _mask_phone(account.phone),
                                g_label,
                                emoji,
                                "互动已送达",
                                layer="group",
                                progress=pr_grp,
                            )
                            if succ_buf >= _FLUSH_EVERY:
                                succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
                        except PeerFlood:
                            succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
                            mark_daily_limited(account, datetime.now(timezone.utc), logger=log, task_notify=tl)
                            db.add(account)
                            db.commit()
                            peer_flood_break = True
                        except Exception:
                            fail_buf += 1
                            if fail_buf >= _FLUSH_EVERY:
                                succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
                        if not peer_flood_break:
                            if not await _sleep_if_running(float(random.randint(GROUP_GAP_MIN_SEC, GROUP_GAP_MAX_SEC))):
                                mark_interrupted(pr_grp)
                                break
                finally:
                    if client:
                        try:
                            await asyncio.wait_for(client.stop(), timeout=TELEGRAM_CLIENT_STOP_TIMEOUT)
                        except Exception:
                            pass

                if user_stopped:
                    break
                emit("success", "—", "SYSTEM", "📋", "账号完成", layer="system", progress=pr_acc)

            if user_stopped:
                break
            emit("success", "—", "SYSTEM", "✅", f"第 {round_idx} 轮完成，等待下一轮新消息", layer="system", progress="—")
            if not await _sleep_if_running(float(random.randint(ROUND_GAP_MIN_SEC, ROUND_GAP_MAX_SEC))):
                mark_interrupted("—")
                break

        succ_buf, fail_buf = _flush_task_buffers(db, task_id, succ_buf, fail_buf)
        task_row = db.query(InteractionTask).filter(InteractionTask.id == task_id).first()
        if task_row:
            task_row.status = "stopped" if user_stopped else "completed"
            db.add(task_row)
            db.commit()
        if user_stopped:
            live_finalize(job_id, "stopped")
        else:
            emit("success", "—", "SYSTEM", "🏁", "任务结束", layer="system", progress="—")
            live_finalize(job_id, "completed")
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
        live_append(
            job_id,
            level="error",
            account="—",
            group="SYSTEM",
            emoji="✕",
            message="任务异常中断",
            layer="system",
            progress="—",
        )
        live_finalize(job_id, "failed")
    finally:
        db.close()
