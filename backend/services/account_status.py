"""账号业务状态：normal / daily_limited / risk_suspected，并兼容旧库中的 active 等取值。"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from models import AccountFile

ST_NORMAL = "normal"
ST_DAILY_LIMITED = "daily_limited"
ST_RISK_SUSPECTED = "risk_suspected"

# 连续登录失败累计阈值（简版「多日」判断，后续可按日历优化）
RISK_LOGIN_FAIL_THRESHOLD = int(os.getenv("TELEGRAM_RISK_LOGIN_FAIL_THRESHOLD", "9"))

_LEGACY_TO_NEW = {
    "active": ST_NORMAL,
    "normal": ST_NORMAL,
    "limited_today": ST_DAILY_LIMITED,
    "daily_limited": ST_DAILY_LIMITED,
    "limited_long": ST_RISK_SUSPECTED,
    "banned": ST_RISK_SUSPECTED,
    "risk_suspected": ST_RISK_SUSPECTED,
}


def normalize_stored_status(raw: str | None) -> str:
    s = (raw or ST_NORMAL).strip().lower()
    return _LEGACY_TO_NEW.get(s, ST_NORMAL)


def is_risk_suspected(account: AccountFile) -> bool:
    """第一版：login_fail_count >= 9 视为疑似风控。"""
    return (account.login_fail_count or 0) >= RISK_LOGIN_FAIL_THRESHOLD


def recover_account_if_expired(account: AccountFile, now_utc: datetime) -> None:
    """daily_limited 且 limited_until 已过 → 恢复 normal（与原先 12h limited_until 一致）。"""
    if account.status != ST_DAILY_LIMITED:
        return
    lu = account.limited_until
    if lu is None:
        return
    if lu.tzinfo is None:
        lu = lu.replace(tzinfo=timezone.utc)
    else:
        lu = lu.astimezone(timezone.utc)
    if now_utc >= lu:
        account.status = ST_NORMAL
        account.limited_until = None


def recover_and_normalize(account: AccountFile, now_utc: datetime) -> None:
    """读库后统一旧 status 字符串，并尝试解除已过期的当日受限。"""
    account.status = normalize_stored_status(account.status)
    recover_account_if_expired(account, now_utc)
