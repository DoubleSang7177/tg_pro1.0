"""账号生命周期：normal / daily_limited / cooldown / risk_suspected / banned(已封号)，兼容旧库 status 字符串。"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from collections.abc import Callable
from typing import Any, Protocol

from models import AccountFile

ST_NORMAL = "normal"
ST_DAILY_LIMITED = "daily_limited"
ST_COOLDOWN = "cooldown"
ST_RISK_SUSPECTED = "risk_suspected"
ST_BANNED = "banned"

NOTE_LONG_TERM = "long_term"

LONG_TERM_COOLDOWN_DAYS = int(os.getenv("ACCOUNT_LONG_TERM_COOLDOWN_DAYS", "3"))
INVITE_FAIL_STREAK_DAYS = int(os.getenv("ACCOUNT_INVITE_FAIL_STREAK_DAYS", "3"))
COOLDOWN_CYCLES_BEFORE_RISK = int(os.getenv("ACCOUNT_COOLDOWN_CYCLES_BEFORE_RISK", "3"))
RECENT_SIDEBAR_SECONDS = int(os.getenv("ACCOUNT_RECENT_SIDEBAR_SECONDS", "60"))

_LEGACY_TO_NEW = {
    "active": ST_NORMAL,
    "normal": ST_NORMAL,
    "limited_today": ST_DAILY_LIMITED,
    "daily_limited": ST_DAILY_LIMITED,
    "cooldown": ST_COOLDOWN,
    "limited_long": ST_RISK_SUSPECTED,
    "banned": ST_BANNED,
    "risk_suspected": ST_RISK_SUSPECTED,
}


class LoggerLike(Protocol):
    def info(self, msg: str, *args: Any) -> None: ...


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_stored_status(raw: str | None) -> str:
    s = (raw or ST_NORMAL).strip().lower()
    if s == ST_BANNED:
        return ST_BANNED
    if s == ST_COOLDOWN:
        return ST_COOLDOWN
    return _LEGACY_TO_NEW.get(s, ST_NORMAL)


def status_log_phone(phone: str | None) -> str:
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    return f"+{digits}" if digits else "+—"


def format_status_log_line(phone: str | None, code: str, reason_cn: str) -> str:
    return f"[STATUS] {status_log_phone(phone)} → {code}（{reason_cn}）"


def login_fail_reason_cn(last_err: str | None) -> str:
    """登录最终失败时 [STATUS] 括号内说明（与 Telegram 错误对齐）。"""
    e = (last_err or "").lower()
    if "user_deactivated" in e or ("401" in e and "deactiv" in e):
        return "账号已注销或冻结"
    return "登录失败"


def emit_status_line(
    phone: str | None,
    code: str,
    reason_cn: str,
    *,
    logger: LoggerLike | None = None,
    task_notify: Callable[[str], None] | None = None,
) -> None:
    """写入应用日志；可选同步到任务进度流（如 run_task 的 tl）。"""
    line = format_status_log_line(phone, code, reason_cn)
    if logger is not None:
        logger.info("%s", line)
    if task_notify is not None:
        task_notify(line)


def log_account_status(logger: LoggerLike | None, phone: str | None, code: str, reason_cn: str) -> None:
    emit_status_line(phone, code, reason_cn, logger=logger, task_notify=None)


def touch_status_change(account: AccountFile, now_utc: datetime) -> None:
    account.status_changed_at = now_utc


def _broadcast_account(account: AccountFile) -> None:
    try:
        from services.account_realtime import schedule_account_broadcast

        schedule_account_broadcast(account)
    except Exception:
        pass


def recover_daily_limited_if_expired(account: AccountFile, now_utc: datetime) -> None:
    if account.status != ST_DAILY_LIMITED:
        return
    lu = _as_utc(account.limited_until)
    if lu is None:
        return
    if now_utc >= lu:
        account.status = ST_NORMAL
        account.limited_until = None
        touch_status_change(account, now_utc)


def recover_cooldown_if_expired(account: AccountFile, now_utc: datetime) -> None:
    if account.status != ST_COOLDOWN:
        return
    lu = _as_utc(account.limited_until)
    if lu is None:
        return
    if now_utc >= lu:
        account.status = ST_NORMAL
        account.limited_until = None
        account.status_note = None
        account.cooldown_completed_count = (account.cooldown_completed_count or 0) + 1
        touch_status_change(account, now_utc)


def recover_and_normalize(account: AccountFile, now_utc: datetime) -> None:
    account.status = normalize_stored_status(account.status)
    if account.status == ST_BANNED:
        return
    recover_cooldown_if_expired(account, now_utc)
    recover_daily_limited_if_expired(account, now_utc)


def mark_telegram_banned(
    account: AccountFile,
    now_utc: datetime,
    *,
    logger: LoggerLike | None = None,
    task_notify: Callable[[str], None] | None = None,
) -> None:
    """Telegram 明确封号（如 PHONE_NUMBER_BANNED），不可自动恢复为 active。"""
    account.status = ST_BANNED
    account.limited_until = None
    account.status_note = None
    account.last_used_time = now_utc
    touch_status_change(account, now_utc)
    phone_disp = status_log_phone(account.phone)
    err_line = f"[ERROR] {phone_disp} 已封号（Telegram banned）"
    if logger is not None:
        logger.error("%s", err_line)
    if task_notify is not None:
        task_notify(err_line)
    _broadcast_account(account)


def mark_daily_limited(
    account: AccountFile,
    now_utc: datetime,
    *,
    hours: int = 12,
    logger: LoggerLike | None = None,
    task_notify: Callable[[str], None] | None = None,
) -> None:
    account.status = ST_DAILY_LIMITED
    account.limited_until = now_utc + timedelta(hours=hours)
    account.last_used_time = now_utc
    touch_status_change(account, now_utc)
    emit_status_line(account.phone, "DAILY", "当日受限", logger=logger, task_notify=task_notify)
    _broadcast_account(account)


def mark_risk_login_failed(
    account: AccountFile,
    now_utc: datetime,
    *,
    logger: LoggerLike | None = None,
    task_notify: Callable[[str], None] | None = None,
    status_reason_cn: str = "登录失败",
) -> None:
    account.status = ST_RISK_SUSPECTED
    account.limited_until = None
    account.status_note = None
    account.last_used_time = now_utc
    touch_status_change(account, now_utc)
    emit_status_line(account.phone, "RISK", status_reason_cn, logger=logger, task_notify=task_notify)
    _broadcast_account(account)


def mark_risk_session_or_auth(
    account: AccountFile,
    now_utc: datetime,
    *,
    logger: LoggerLike | None = None,
    task_notify: Callable[[str], None] | None = None,
) -> None:
    account.status = ST_RISK_SUSPECTED
    account.limited_until = None
    account.status_note = None
    account.last_used_time = now_utc
    touch_status_change(account, now_utc)
    emit_status_line(account.phone, "RISK", "会话/鉴权失效", logger=logger, task_notify=task_notify)
    _broadcast_account(account)


def mark_risk_after_cooldown_cycles(
    account: AccountFile,
    now_utc: datetime,
    *,
    logger: LoggerLike | None = None,
    task_notify: Callable[[str], None] | None = None,
) -> None:
    account.status = ST_RISK_SUSPECTED
    account.limited_until = None
    account.status_note = None
    account.invite_fail_streak_days = 0
    touch_status_change(account, now_utc)
    emit_status_line(account.phone, "RISK", "多次冷却后仍失败", logger=logger, task_notify=task_notify)
    _broadcast_account(account)


def enter_long_term_cooldown(
    account: AccountFile,
    now_utc: datetime,
    *,
    logger: LoggerLike | None = None,
    task_notify: Callable[[str], None] | None = None,
) -> None:
    account.status = ST_COOLDOWN
    account.limited_until = now_utc + timedelta(days=LONG_TERM_COOLDOWN_DAYS)
    account.status_note = NOTE_LONG_TERM
    account.invite_fail_streak_days = 0
    account.last_used_time = now_utc
    touch_status_change(account, now_utc)
    emit_status_line(account.phone, "LONG_TERM", "长期受限", logger=logger, task_notify=task_notify)
    _broadcast_account(account)


def lifecycle_ui_labels(status: str, status_note: str | None) -> tuple[str, str]:
    """(主标签, 副标签) 用于控制台三栏与侧栏 echo。"""
    st = (status or ST_NORMAL).lower()
    if st == ST_NORMAL:
        return "ACTIVE", "可用"
    if st == ST_DAILY_LIMITED:
        return "LIMITED · DAILY", "当日受限"
    if st == ST_COOLDOWN:
        if (status_note or "").strip() == NOTE_LONG_TERM:
            return "LIMITED · LONG_TERM", "冷却中"
        return "COOLDOWN", "冷却中"
    if st == ST_RISK_SUSPECTED:
        return "RISK", "疑似风控"
    if st == ST_BANNED:
        return "BANNED", "已封号"
    return "ACTIVE", "可用"


def process_daily_invite_streaks(
    accounts: list[AccountFile],
    now_utc: datetime,
    *,
    logger: LoggerLike | None = None,
) -> None:
    """在清零今日计数之前调用：仅对当前为 normal 的账号累计连续失败日，并触发长期冷却或风控。"""
    for account in accounts:
        recover_and_normalize(account, now_utc)
        if account.status != ST_NORMAL:
            continue
        tries = account.invite_try_today or 0
        succ = account.today_count or 0
        if tries > 0 and succ == 0:
            account.invite_fail_streak_days = (account.invite_fail_streak_days or 0) + 1
        elif succ > 0:
            account.invite_fail_streak_days = 0
        if (account.invite_fail_streak_days or 0) >= INVITE_FAIL_STREAK_DAYS:
            completed = account.cooldown_completed_count or 0
            if completed >= COOLDOWN_CYCLES_BEFORE_RISK:
                mark_risk_after_cooldown_cycles(account, now_utc, logger=logger)
            else:
                enter_long_term_cooldown(account, now_utc, logger=logger)
