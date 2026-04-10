from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from database import SessionLocal
from logger import get_logger
from models import AccountFile, Group

log = get_logger("telegram_service")


def _classify_account_status(exc: Exception) -> str:
    msg = str(exc).lower()
    if "peer_flood" in msg or "floodwait" in msg:
        return "limited"
    if "session" in msg or "登录失败" in msg or "login failed" in msg:
        return "banned"
    return "active"


def _recover_account_status(account: AccountFile, now_utc: datetime) -> None:
    if account.status == "limited_today" and account.limited_until and now_utc >= account.limited_until:
        account.status = "active"
        account.limited_until = None

    if account.status == "limited_today" and account.limited_until and now_utc - account.limited_until > timedelta(days=3):
        account.status = "banned"


async def run_task(config: dict[str, Any]) -> dict[str, Any]:
    """
    Telegram 拉人任务统一入口。
    通过 config 动态接收参数，避免依赖任何全局变量。
    """
    groups = config.get("groups", [])
    users = config.get("users", [])
    accounts_path = str(config.get("accounts_path", "")).strip()

    if not isinstance(groups, list) or not groups:
        raise ValueError("groups 必须是非空列表")
    if not isinstance(users, list) or not users:
        raise ValueError("users 必须是非空列表")
    if not accounts_path:
        raise ValueError("accounts_path 不能为空")

    if not Path(accounts_path).exists():
        raise ValueError(f"accounts_path 不存在: {accounts_path}")

    log.info(
        "run_task executing groups=%s users_count=%s accounts_path=%s",
        groups,
        len(users),
        accounts_path,
    )
    db = SessionLocal()
    updated = {"active": 0, "limited": 0, "banned": 0}
    now_utc = datetime.now(timezone.utc)
    try:
        query = db.query(AccountFile).filter(AccountFile.saved_path.like(f"{accounts_path}%")).order_by(AccountFile.id.asc())
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

        for row in account_rows:
            _recover_account_status(row, now_utc)
            if row.status == "banned":
                updated["banned"] += 1
                continue
            if row.status == "limited_today" and row.limited_until and now_utc < row.limited_until:
                updated["limited"] += 1
                continue
            try:
                # TODO: 在这里接入真实 Telegram 拉新逻辑。
                # 支持用 config["simulate_errors"] 模拟运行时异常，便于前端调试状态变化。
                simulated = (config.get("simulate_errors") or {}).get(row.phone)
                if simulated:
                    raise RuntimeError(simulated)
                row.status = "active"
                row.today_count = (row.today_count or 0) + len(users)
                row.today_used_count = (row.today_used_count or 0) + 1
                row.last_used_time = now_utc
                if row.today_used_count > 3:
                    row.status = "limited_today"
                    row.limited_until = now_utc + timedelta(hours=12)
                updated["active"] += 1
                for group_name in available_groups:
                    group = group_map.get(group_name)
                    if group:
                        group.failed_streak = 0
                        group.status = "normal"
                        group.total_added = (group.total_added or 0) + len(users)
                        group.today_added = (group.today_added or 0) + len(users)
                        if group.today_added >= group.daily_limit:
                            group.disabled_until = now_utc + timedelta(hours=12)
                            group.status = "limited"
                        db.add(group)
            except Exception as exc:
                row.error_count = (row.error_count or 0) + 1
                detected = _classify_account_status(exc)
                if detected == "limited":
                    row.status = "limited_today"
                    row.limited_until = now_utc + timedelta(hours=12)
                else:
                    row.status = detected
                updated[row.status] = updated.get(row.status, 0) + 1
                log.warning("account status updated phone=%s status=%s error=%s", row.phone, row.status, exc)
                for group_name in available_groups:
                    group = group_map.get(group_name)
                    if group:
                        group.failed_streak = (group.failed_streak or 0) + 1
                        if group.failed_streak >= 3:
                            group.status = "limited"
                        db.add(group)
            db.add(row)
        db.commit()
    finally:
        db.close()

    log.info("run_task finished groups=%s updated=%s", groups, updated)
    return {
        "status": "running",
        "groups": available_groups,
        "users_count": len(users),
        "accounts_path": accounts_path,
        "updated": updated,
    }
