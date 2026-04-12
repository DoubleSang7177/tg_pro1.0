from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker


DATABASE_URL = "sqlite:///./app.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _ensure_group_columns() -> None:
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(groups)")).fetchall()
        col_names = {r[1] for r in rows}
        if "public_username" not in col_names:
            conn.execute(text("ALTER TABLE groups ADD COLUMN public_username VARCHAR(255)"))


def _ensure_account_file_columns() -> None:
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(account_files)")).fetchall()
        col_names = {r[1] for r in rows}
        if "login_fail_count" not in col_names:
            conn.execute(
                text("ALTER TABLE account_files ADD COLUMN login_fail_count INTEGER NOT NULL DEFAULT 0")
            )
        if "last_login_fail_at" not in col_names:
            conn.execute(text("ALTER TABLE account_files ADD COLUMN last_login_fail_at DATETIME"))
        if "status_changed_at" not in col_names:
            conn.execute(text("ALTER TABLE account_files ADD COLUMN status_changed_at DATETIME"))
        if "invite_try_today" not in col_names:
            conn.execute(
                text("ALTER TABLE account_files ADD COLUMN invite_try_today INTEGER NOT NULL DEFAULT 0")
            )
        if "invite_fail_streak_days" not in col_names:
            conn.execute(
                text(
                    "ALTER TABLE account_files ADD COLUMN invite_fail_streak_days INTEGER NOT NULL DEFAULT 0"
                )
            )
        if "cooldown_completed_count" not in col_names:
            conn.execute(
                text(
                    "ALTER TABLE account_files ADD COLUMN cooldown_completed_count INTEGER NOT NULL DEFAULT 0"
                )
            )
        if "status_note" not in col_names:
            conn.execute(text("ALTER TABLE account_files ADD COLUMN status_note VARCHAR(32)"))
        conn.execute(
            text("UPDATE account_files SET status = 'normal' WHERE status IN ('active', '') OR status IS NULL")
        )
        conn.execute(text("UPDATE account_files SET status = 'daily_limited' WHERE status = 'limited_today'"))
        conn.execute(
            text(
                "UPDATE account_files SET status = 'risk_suspected' "
                "WHERE status IN ('limited_long', 'banned')"
            )
        )


def _ensure_copy_bots_session_name() -> None:
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(copy_bots)")).fetchall()
        if not rows:
            return
        col_names = {r[1] for r in rows}
        if "session_name" not in col_names:
            conn.execute(text("ALTER TABLE copy_bots ADD COLUMN session_name VARCHAR(128)"))


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_group_columns()
    _ensure_account_file_columns()
    _ensure_copy_bots_session_name()
