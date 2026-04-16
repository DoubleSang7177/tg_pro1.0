from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, JSON, Text, String, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

try:
    from database import Base
except ModuleNotFoundError:
    from backend.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(128), nullable=False)
    role = Column(String(20), nullable=False, default="user")
    token = Column(String(128), nullable=True, unique=True)
    # 相对 URL，如 /uploads/avatars/1.jpg，前端拼接 BASE_URL
    avatar_url = Column(String(512), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    tasks = relationship("TaskRecord", back_populates="owner")


class AccountFile(Base):
    __tablename__ = "account_files"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    phone = Column(String(50), nullable=True)
    proxy_id = Column(Integer, ForeignKey("proxies.id"), nullable=True)
    proxy_type = Column(String(20), nullable=False, default="direct")
    filename = Column(String(255), nullable=False)
    saved_path = Column(String(500), nullable=False)
    status = Column(String(32), nullable=False, default="normal")
    today_count = Column(Integer, nullable=False, default=0)
    error_count = Column(Integer, nullable=False, default=0)
    today_used_count = Column(Integer, nullable=False, default=0)
    last_used_time = Column(DateTime(timezone=True), nullable=True)
    limited_until = Column(DateTime(timezone=True), nullable=True)
    login_fail_count = Column(Integer, nullable=False, default=0)
    last_login_fail_at = Column(DateTime(timezone=True), nullable=True)
    # 生命周期：状态变更时间（左侧队列 60s 提示）、拉人尝试/连续失败日、长期冷却轮次、冷却来源标记
    status_changed_at = Column(DateTime(timezone=True), nullable=True)
    invite_try_today = Column(Integer, nullable=False, default=0)
    invite_fail_streak_days = Column(Integer, nullable=False, default=0)
    cooldown_completed_count = Column(Integer, nullable=False, default=0)
    status_note = Column(String(32), nullable=True)
    # 最近一次账号状态推送/同步时间（与 WebSocket 事件、列表 API 对齐）
    last_update = Column(DateTime(timezone=True), nullable=True)
    # 注册来源与养号状态（注册模块）
    source_type = Column(String(20), nullable=False, default="upload")  # upload / register / scan
    register_status = Column(String(20), nullable=False, default="none")  # none / pending / success / failed
    warmup_status = Column(String(20), nullable=False, default="ready")  # none / warming / ready
    warmup_start_at = Column(DateTime(timezone=True), nullable=True)
    ready_at = Column(DateTime(timezone=True), nullable=True)
    # 注册账号的 Telethon session 文件绝对路径；仅保存路径，不保存内容
    session_path = Column(String(500), nullable=True)
    # 可选：账号二步验证密码（仅在接码注册时用户主动填写后保存）
    twofa_password = Column(String(256), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class AccountFactory(Base):
    """实验账号生产池（与正式账号系统隔离）。"""

    __tablename__ = "accounts_factory"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    phone = Column(String(50), nullable=False, index=True)
    country = Column(String(32), nullable=False, default="ID")
    status = Column(String(20), nullable=False, default="NEW")  # NEW / WARMING / READY / FAILED / BANNED
    session_path = Column(String(500), nullable=True)
    warmup_until = Column(DateTime(timezone=True), nullable=True)
    fail_reason = Column(Text, nullable=True)
    source = Column(String(20), nullable=False, default="factory")
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
    # Telegram 公开用户名（不含 @），用于展示为 @xxx；无公开链接的群组为空
    public_username = Column(String(255), nullable=True)
    members_count = Column(Integer, default=0, nullable=False)
    total_added = Column(Integer, default=0, nullable=False)
    today_added = Column(Integer, default=0, nullable=False)
    yesterday_added = Column(Integer, default=0, nullable=False)
    yesterday_left = Column(Integer, default=0, nullable=False)
    status = Column(String(20), default="normal", nullable=False)
    failed_streak = Column(Integer, default=0, nullable=False)
    daily_limit = Column(Integer, default=30, nullable=False)
    disabled_until = Column(DateTime(timezone=True), nullable=True)


class InteractionTargetGroup(Base):
    """群组互动专用目标群组库（与增长目标群组隔离）。"""

    __tablename__ = "interaction_target_groups"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(255), unique=True, nullable=False, index=True)
    title = Column(String(255), nullable=False)
    remark = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


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
    # 出口检测（与 status 闲置/已分配/失效 无关；不影响绑定与任务逻辑）
    proxy_ip = Column(String(64), nullable=True)
    proxy_country = Column(String(128), nullable=True)
    proxy_city = Column(String(128), nullable=True)
    proxy_country_code = Column(String(4), nullable=True)
    proxy_status = Column(String(16), nullable=False, default="unknown")
    usage_type = Column(String(20), nullable=False, default="unknown")


class AccountPath(Base):
    __tablename__ = "account_paths"

    id = Column(Integer, primary_key=True, index=True)
    path = Column(String(500), unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ScraperAccount(Base):
    """Telethon 采集账号（单例）：与账号池、代理池无关"""

    __tablename__ = "scraper_account"

    id = Column(Integer, primary_key=True, index=True)
    phone = Column(String(32), nullable=False, unique=True)
    session_file = Column(String(500), nullable=False)
    proxy_id = Column(Integer, ForeignKey("proxies.id"), nullable=True)
    status = Column(String(20), nullable=False, default="active")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class ScraperTask(Base):
    """用户采集任务记录（结果文件可重复下载）"""

    __tablename__ = "scraper_tasks"

    id = Column(Integer, primary_key=True, index=True)
    group_link = Column(String(512), nullable=False)
    group_name = Column(String(512), nullable=False, default="")
    result_file = Column(String(500), nullable=False, default="")
    user_count = Column(Integer, nullable=False, default=0)
    download_count = Column(Integer, nullable=False, default=0)
    status = Column(String(20), nullable=False, default="running")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class InteractionTask(Base):
    """群组互动：多群随机表情反应任务"""

    __tablename__ = "interaction_tasks"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    # 目标群组 username 列表（与 groups 表一致）
    target_groups = Column(JSON, nullable=False)
    account_ids = Column(JSON, nullable=False)
    status = Column(String(32), nullable=False, default="pending")
    success_count = Column(Integer, nullable=False, default=0)
    fail_count = Column(Integer, nullable=False, default=0)
    # 断点记忆：key=account_id:group_ident, value=last_msg_id
    cursor_map = Column(JSON, nullable=False, default=dict)
    # 已完成到的轮次（用于重启后延续轮次计数与进度感知）
    round_idx = Column(Integer, nullable=False, default=0)
    scan_limit = Column(Integer, nullable=False, default=300)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class UserFilterTask(Base):
    __tablename__ = "user_filter_tasks"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String(255), nullable=False, default="")
    source_group_id = Column(String(255), nullable=False, default="")
    source_task_id = Column(Integer, ForeignKey("scraper_tasks.id"), nullable=True, index=True)
    status = Column(String(20), nullable=False, default="idle")  # idle / running / finished / stopped / failed
    total_users = Column(Integer, nullable=False, default=0)
    processed_users = Column(Integer, nullable=False, default=0)
    success_count = Column(Integer, nullable=False, default=0)
    fail_count = Column(Integer, nullable=False, default=0)
    real_verify_enabled = Column(Integer, nullable=False, default=0)
    real_verify_ratio = Column(Float, nullable=False, default=0.1)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class UserFilterResult(Base):
    __tablename__ = "user_filter_results"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("user_filter_tasks.id"), nullable=False, index=True)
    user_id = Column(String(64), nullable=False, default="")
    username = Column(String(255), nullable=True)
    phone = Column(String(64), nullable=True)
    can_invite = Column(Integer, nullable=False, default=0)
    fail_reason = Column(String(255), nullable=True)
    probe_account_id = Column(Integer, ForeignKey("filter_accounts.id"), nullable=True, index=True)
    verified_by_real = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class FilterAccount(Base):
    __tablename__ = "filter_accounts"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    type = Column(String(16), nullable=False, default="probe")  # probe / real
    phone = Column(String(32), nullable=False)
    api_id = Column(Integer, nullable=True)
    api_hash = Column(String(128), nullable=True)
    session_path = Column(String(500), nullable=False)
    status = Column(String(20), nullable=False, default="active")  # active / banned / idle
    last_used_at = Column(DateTime(timezone=True), nullable=True)
    proxy_id = Column(Integer, ForeignKey("proxies.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class CopyBot(Base):
    """Copy 转发：机器人库（Telethon session 监听 + 同 session MTProto 转发）"""

    __tablename__ = "copy_bots"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    api_id = Column(Integer, nullable=False)
    api_hash = Column(String(64), nullable=False)
    bot_token = Column(String(256), nullable=False)
    # Telethon 会话文件名（不含路径），对应文件 backend/sessions/{session_name}.session
    session_name = Column(String(128), nullable=True)
    status = Column(String(16), nullable=False, default="active")  # active / error
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    tasks = relationship("CopyTask", back_populates="bot")


class CopyTask(Base):
    """Copy 转发任务"""

    __tablename__ = "copy_tasks"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    source_channel = Column(String(255), nullable=False)
    target_channel = Column(String(255), nullable=False)
    bot_id = Column(Integer, ForeignKey("copy_bots.id"), nullable=False, index=True)
    listener_id = Column(Integer, ForeignKey("copy_listener_accounts.id"), nullable=True, index=True)
    status = Column(String(20), nullable=False, default="idle")  # idle / starting / running / paused / error
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    total_forwarded = Column(Integer, nullable=False, default=0)
    today_forwarded = Column(Integer, nullable=False, default=0)
    stats_utc_date = Column(String(10), nullable=True)  # YYYY-MM-DD 用于重置今日计数
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    bot = relationship("CopyBot", back_populates="tasks")
    owner = relationship("User", foreign_keys=[owner_id])
    forwards = relationship("ForwardRecord", back_populates="task", cascade="all, delete-orphan")


class CopyListenerAccount(Base):
    """Copy 监听账号池：只监听 source，不做发送。"""

    __tablename__ = "copy_listener_accounts"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    api_id = Column(Integer, nullable=False)
    api_hash = Column(String(64), nullable=False)
    phone = Column(String(32), nullable=False, unique=True)
    session_name = Column(String(128), nullable=False, unique=True)
    proxy_id = Column(Integer, ForeignKey("proxies.id"), nullable=True)
    status = Column(String(20), nullable=False, default="active")  # active / disconnected / error
    enabled = Column(Integer, nullable=False, default=1)
    last_error = Column(Text, nullable=True)
    last_seen_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ForwardRecord(Base):
    """已转发消息去重"""

    __tablename__ = "forward_records"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("copy_tasks.id", ondelete="CASCADE"), nullable=False, index=True)
    message_hash = Column(String(128), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    task = relationship("CopyTask", back_populates="forwards")

    __table_args__ = (UniqueConstraint("task_id", "message_hash", name="uq_forward_task_hash"),)
