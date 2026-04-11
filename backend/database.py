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


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_group_columns()
