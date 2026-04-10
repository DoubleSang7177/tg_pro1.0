from sqlalchemy import Column, DateTime, ForeignKey, Integer, Text, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(128), nullable=False)
    role = Column(String(20), nullable=False, default="user")
    token = Column(String(128), nullable=True, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    tasks = relationship("TaskRecord", back_populates="owner")


class AccountFile(Base):
    __tablename__ = "account_files"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    phone = Column(String(50), nullable=True)
    filename = Column(String(255), nullable=False)
    saved_path = Column(String(500), nullable=False)
    status = Column(String(20), nullable=False, default="active")
    today_count = Column(Integer, nullable=False, default=0)
    error_count = Column(Integer, nullable=False, default=0)
    today_used_count = Column(Integer, nullable=False, default=0)
    last_used_time = Column(DateTime(timezone=True), nullable=True)
    limited_until = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class TaskRecord(Base):
    __tablename__ = "task_records"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    group_name = Column(String(255), nullable=False)
    users_text = Column(Text, nullable=False)
    accounts_path = Column(String(500), nullable=False)
    status = Column(String(50), nullable=False, default="pending")
    result_text = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    owner = relationship("User", back_populates="tasks")


class Setting(Base):
    __tablename__ = "settings"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(100), unique=True, nullable=False, index=True)
    value = Column(Text, nullable=False)


class Group(Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(255), unique=True, nullable=False, index=True)
    title = Column(String(255), nullable=False)
    members_count = Column(Integer, default=0, nullable=False)
    total_added = Column(Integer, default=0, nullable=False)
    today_added = Column(Integer, default=0, nullable=False)
    yesterday_added = Column(Integer, default=0, nullable=False)
    yesterday_left = Column(Integer, default=0, nullable=False)
    status = Column(String(20), default="normal", nullable=False)
    failed_streak = Column(Integer, default=0, nullable=False)
    daily_limit = Column(Integer, default=30, nullable=False)
    disabled_until = Column(DateTime(timezone=True), nullable=True)


class Proxy(Base):
    __tablename__ = "proxies"

    id = Column(Integer, primary_key=True, index=True)
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    username = Column(String(255), nullable=True)
    password = Column(String(255), nullable=True)
    status = Column(String(20), nullable=False, default="idle")
    assigned_account_id = Column(Integer, ForeignKey("account_files.id"), unique=True, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
