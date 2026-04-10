from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from models import AccountFile, Group, Setting


RESET_KEY = "daily_reset_date"


def perform_daily_reset_if_needed(db: Session) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    marker = db.query(Setting).filter(Setting.key == RESET_KEY).first()
    if marker and marker.value == today:
        return

    groups = db.query(Group).all()
    for g in groups:
        g.yesterday_added = g.today_added or 0
        g.today_added = 0
        g.failed_streak = 0
        db.add(g)

    accounts = db.query(AccountFile).all()
    for a in accounts:
        a.today_used_count = 0
        a.today_count = 0
        db.add(a)

    if marker is None:
        marker = Setting(key=RESET_KEY, value=today)
    else:
        marker.value = today
    db.add(marker)
    db.commit()
