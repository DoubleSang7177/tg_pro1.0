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
        if "last_update" not in col_names:
            conn.execute(text("ALTER TABLE account_files ADD COLUMN last_update DATETIME"))
        conn.execute(
            text("UPDATE account_files SET status = 'normal' WHERE status IN ('active', '') OR status IS NULL")
        )
        conn.execute(text("UPDATE account_files SET status = 'daily_limited' WHERE status = 'limited_today'"))
        conn.execute(
            text(
                "UPDATE account_files SET status = 'risk_suspected' "
                "WHERE status IN ('limited_long')"
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


def _ensure_users_avatar_url() -> None:
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(users)")).fetchall()
        if not rows:
            return
        col_names = {r[1] for r in rows}
        if "avatar_url" not in col_names:
            conn.execute(text("ALTER TABLE users ADD COLUMN avatar_url VARCHAR(512)"))


def _ensure_proxies_check_columns() -> None:
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(proxies)")).fetchall()
        if not rows:
            return
        col_names = {r[1] for r in rows}
        if "proxy_ip" not in col_names:
            conn.execute(text("ALTER TABLE proxies ADD COLUMN proxy_ip VARCHAR(64)"))
        if "proxy_country" not in col_names:
            conn.execute(text("ALTER TABLE proxies ADD COLUMN proxy_country VARCHAR(128)"))
        if "proxy_city" not in col_names:
            conn.execute(text("ALTER TABLE proxies ADD COLUMN proxy_city VARCHAR(128)"))
        if "proxy_country_code" not in col_names:
            conn.execute(text("ALTER TABLE proxies ADD COLUMN proxy_country_code VARCHAR(4)"))
        if "proxy_status" not in col_names:
            conn.execute(
                text("ALTER TABLE proxies ADD COLUMN proxy_status VARCHAR(16) NOT NULL DEFAULT 'unknown'")
            )


def _ensure_copy_tasks_owner_id() -> None:
    """旧库 copy_tasks 可能无 owner_id，补列并默认归到用户 id=1（通常为 admin）。"""
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(copy_tasks)")).fetchall()
        if not rows:
            return
        col_names = {r[1] for r in rows}
        if "owner_id" not in col_names:
            conn.execute(text("ALTER TABLE copy_tasks ADD COLUMN owner_id INTEGER NOT NULL DEFAULT 1"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_copy_tasks_owner_id ON copy_tasks (owner_id)"))
        if "listener_id" not in col_names:
            conn.execute(text("ALTER TABLE copy_tasks ADD COLUMN listener_id INTEGER"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_copy_tasks_listener_id ON copy_tasks (listener_id)"))


def _ensure_interaction_target_groups_columns() -> None:
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(interaction_target_groups)")).fetchall()
        if not rows:
            return
        col_names = {r[1] for r in rows}
        if "remark" not in col_names:
            conn.execute(text("ALTER TABLE interaction_target_groups ADD COLUMN remark VARCHAR(255)"))


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_group_columns()
    _ensure_account_file_columns()
    _ensure_copy_bots_session_name()
    _ensure_users_avatar_url()
    _ensure_copy_tasks_owner_id()
    _ensure_proxies_check_columns()
    _ensure_interaction_target_groups_columns()
