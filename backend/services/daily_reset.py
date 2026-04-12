from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from logger import get_logger
from models import AccountFile, Group, Setting
from services.account_status import process_daily_invite_streaks


RESET_KEY = "daily_reset_date"
_log = get_logger("daily_reset")


def perform_daily_reset_if_needed(db: Session) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    marker = db.query(Setting).filter(Setting.key == RESET_KEY).first()
    if marker and marker.value == today:
        return

    now_utc = datetime.now(timezone.utc)
    accounts = db.query(AccountFile).all()
    process_daily_invite_streaks(accounts, now_utc, logger=_log)

    groups = db.query(Group).all()
    for g in groups:
        g.yesterday_added = g.today_added or 0
        g.today_added = 0
        g.failed_streak = 0
        db.add(g)

    for a in accounts:
        a.today_used_count = 0
        a.today_count = 0
        a.invite_try_today = 0
        db.add(a)

    if marker is None:
        marker = Setting(key=RESET_KEY, value=today)
    else:
        marker.value = today
    db.add(marker)
    db.commit()
