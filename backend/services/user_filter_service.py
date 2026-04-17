from __future__ import annotations

import asyncio
import csv
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from cn_time import cn_hm
from database import SessionLocal
from logger import get_logger
from models import FilterAccount, ScraperTask, UserFilterResult, UserFilterTask
from settings import TELEGRAM_API_HASH, TELEGRAM_API_ID
from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.functions.channels import GetParticipantRequest, InviteToChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import ChannelParticipantAdmin, ChannelParticipantCreator

log = get_logger("user_filter_service")

BACKEND_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = BACKEND_ROOT / "data" / "user_filter" / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

_MAX_LIVE_LINES = 300
_job_lock = threading.Lock()
_jobs: dict[str, dict] = {}


def _append_log(job_id: str | None, level: str, module: str, message: str) -> None:
    if not job_id:
        return
    with _job_lock:
        row = _jobs.get(job_id)
        if not row:
            return
        logs = row["logs"]
        logs.append({"t": cn_hm(), "level": level, "module": module, "message": message})
        if len(logs) > _MAX_LIVE_LINES:
            del logs[: len(logs) - _MAX_LIVE_LINES]


def init_live(job_id: str, owner_id: int, task_id: int) -> None:
    with _job_lock:
        _jobs[job_id] = {
            "owner_id": owner_id,
            "task_id": task_id,
            "status": "running",
            "logs": [],
            "stop": False,
        }


def live_snapshot(job_id: str) -> dict | None:
    with _job_lock:
        row = _jobs.get(job_id)
        if not row:
            return None
        return {
            "owner_id": row["owner_id"],
            "task_id": row["task_id"],
            "status": row["status"],
            "logs": list(row["logs"]),
        }


def request_stop(job_id: str | None = None, task_id: int | None = None) -> bool:
    with _job_lock:
        matched = False
        for jid, row in _jobs.items():
            if job_id and jid != job_id:
                continue
            if task_id is not None and int(row["task_id"]) != int(task_id):
                continue
            row["stop"] = True
            matched = True
        return matched


def _job_should_stop(job_id: str | None) -> bool:
    if not job_id:
        return False
    with _job_lock:
        row = _jobs.get(job_id)
        return bool(row and row.get("stop"))


def _job_finalize(job_id: str | None, status: str) -> None:
    if not job_id:
        return
    with _job_lock:
        row = _jobs.get(job_id)
        if row:
            row["status"] = status


def _mask_phone(phone: str | None) -> str:
    p = (phone or "").strip()
    if len(p) < 8:
        return "****"
    prefix = "+" + p[1:4] if p.startswith("+") and len(p) >= 5 else p[:3]
    return f"{prefix}**{p[-4:]}"


def _load_scraper_usernames(source_task_id: int) -> list[str]:
    db = SessionLocal()
    try:
        row = db.query(ScraperTask).filter(ScraperTask.id == source_task_id).first()
        if row is None or not row.result_file:
            return []
        p = Path(str(row.result_file)).resolve()
        if not p.is_file():
            return []
        vals: list[str] = []
        for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            x = line.strip()
            if not x:
                continue
            vals.append(x.split(",", 1)[0].strip())
        return vals
    finally:
        db.close()


def _camel_to_upper_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).upper()


def _error_code(exc: Exception) -> str:
    if isinstance(exc, RPCError):
        if getattr(exc, "message", None):
            return str(exc.message).strip().upper()
        name = exc.__class__.__name__
        if name.endswith("Error"):
            name = name[:-5]
        return _camel_to_upper_snake(name)
    return exc.__class__.__name__.upper()


def _classify_invite_error(exc: Exception) -> tuple[bool, str]:
    """
    返回: (can_invite, reason)
    - True: 归类为可用用户
    - False: 归类为不可用用户
    """
    code = _error_code(exc)
    msg = str(exc or "").strip()
    low = msg.lower()

    # 用户隐私限制：不可用用户
    if (
        code == "USER_PRIVACY_RESTRICTED"
        or "user_privacy_restricted" in low
        or "restricts adding them to groups" in low
        or "send invite link instead" in low
    ):
        return False, "USER_PRIVACY_RESTRICTED"

    # 账号限制（互关限制）：可用用户
    if (
        code == "USER_NOT_MUTUAL_CONTACT"
        or "user_not_mutual_contact" in low
        or "you can only add mutual contacts" in low
    ):
        return True, "USER_NOT_MUTUAL_CONTACT"

    # 其余错误默认按不可用处理，并保留错误码
    return False, code


def _session_base_from_path(raw: str | None) -> str:
    p = Path(str(raw or "").strip())
    if not p.is_absolute():
        p = (BACKEND_ROOT / p).resolve()
    if p.suffix == ".session":
        p = p.with_suffix("")
    return str(p)


async def _build_probe_clients(task: UserFilterTask, db, job_id: str | None) -> list[dict]:
    return await _build_filter_clients(task, db, job_id, acc_type="probe", required=True)


async def _build_filter_clients(
    task: UserFilterTask,
    db,
    job_id: str | None,
    *,
    acc_type: str,
    required: bool,
) -> list[dict]:
    rows = (
        db.query(FilterAccount)
        .filter(
            FilterAccount.owner_id == task.owner_id,
            FilterAccount.type == acc_type,
            FilterAccount.status.in_(["active", "idle"]),
        )
        .order_by(FilterAccount.id.asc())
        .all()
    )
    if not rows:
        if required:
            raise ValueError(f"筛选账号池为空：请先添加至少一个 {acc_type} 账号")
        _append_log(job_id, "warn", "RUNNER", f"未配置 {acc_type} 账号，已跳过该账号池登录")
        return []

    usable: list[dict] = []
    for row in rows:
        api_id = int(row.api_id or TELEGRAM_API_ID)
        api_hash = str(row.api_hash or TELEGRAM_API_HASH).strip()
        if not api_hash:
            _append_log(job_id, "warn", acc_type.upper(), f"账号 {_mask_phone(row.phone)} 缺少 API_HASH，已跳过")
            continue
        client = TelegramClient(_session_base_from_path(row.session_path), api_id, api_hash)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                _append_log(job_id, "warn", acc_type.upper(), f"账号 {_mask_phone(row.phone)} 未授权，已跳过")
                await client.disconnect()
                continue
            me = await client.get_me()
            if bool(getattr(me, "bot", False)):
                _append_log(job_id, "warn", acc_type.upper(), f"账号 {_mask_phone(row.phone)} 是 bot，已跳过")
                await client.disconnect()
                continue
            usable.append({"row": row, "client": client, "channel": None, "acc_type": acc_type})
        except Exception as exc:
            _append_log(job_id, "warn", acc_type.upper(), f"账号 {_mask_phone(row.phone)} 初始化失败: {_error_code(exc)}")
            try:
                await client.disconnect()
            except Exception:
                pass
    if not usable:
        if required:
            raise ValueError(f"没有可用的用户会话账号（{acc_type}）可执行筛选")
        _append_log(job_id, "warn", "RUNNER", f"未找到可用的 {acc_type} 账号会话，已跳过该账号池登录")
    return usable


def _extract_invite_hash(source: str) -> str:
    src = str(source or "").strip()
    low = src.lower()
    if "t.me/+" in low:
        return src.split("t.me/+", 1)[1].split("?", 1)[0].strip().lstrip("+")
    if "joinchat/" in low:
        return src.split("joinchat/", 1)[1].split("?", 1)[0].strip().lstrip("+")
    return ""


async def _ensure_joined_test_group(ctx: dict, test_group: str, job_id: str | None) -> None:
    row = ctx["row"]
    client = ctx["client"]
    acc_type = str(ctx.get("acc_type") or "probe").upper()
    phone_mask = _mask_phone(row.phone)

    try:
        ent = await client.get_entity(test_group)
    except Exception as exc:
        _append_log(job_id, "warn", acc_type, f"账号 {phone_mask} 获取测试群失败，尝试入群: {_error_code(exc)}")
        ent = None

    if ent is not None:
        try:
            await client(JoinChannelRequest(ent))
            _append_log(job_id, "info", acc_type, f"账号 {phone_mask} 已尝试加入测试群")
        except Exception:
            pass
    else:
        invite_hash = _extract_invite_hash(test_group)
        if invite_hash:
            try:
                await client(ImportChatInviteRequest(invite_hash))
                _append_log(job_id, "info", acc_type, f"账号 {phone_mask} 已通过邀请链接加入测试群")
            except Exception as exc:
                _append_log(job_id, "warn", acc_type, f"账号 {phone_mask} 通过邀请链接入群失败: {_error_code(exc)}")

    try:
        ent = await client.get_entity(test_group)
        await client(GetParticipantRequest(channel=ent, participant="me"))
    except Exception as exc:
        raise ValueError(f"账号 {phone_mask} 不在测试群中，请先将账号加入测试群: {_error_code(exc)}") from exc
    ctx["channel"] = ent


async def _has_admin_invite_permission(ctx: dict) -> bool:
    client = ctx["client"]
    channel = ctx.get("channel")
    if channel is None:
        return False
    try:
        perms = await client.get_permissions(channel, "me")
        is_creator = bool(getattr(perms, "is_creator", False))
        is_admin = bool(getattr(perms, "is_admin", False))
        invite_users = getattr(perms, "invite_users", None)
        if is_creator:
            return True
        if is_admin:
            return True if invite_users is None else bool(invite_users)
    except Exception:
        pass
    try:
        resp = await client(GetParticipantRequest(channel=channel, participant="me"))
        p = getattr(resp, "participant", None)
        if isinstance(p, (ChannelParticipantCreator, ChannelParticipantAdmin)):
            rights = getattr(p, "admin_rights", None)
            if rights is None:
                return True
            iv = getattr(rights, "invite_users", None)
            return True if iv is None else bool(iv)
    except Exception:
        return False
    return False


async def _ensure_group_ready(
    all_ctx: list[dict],
    test_group: str,
    job_id: str | None,
) -> None:
    for ctx in all_ctx:
        await _ensure_joined_test_group(ctx, test_group, job_id)

    warned = False
    while True:
        waiting = []
        for ctx in all_ctx:
            ok = await _has_admin_invite_permission(ctx)
            if not ok:
                waiting.append(ctx)
        if not waiting:
            _append_log(job_id, "success", "RUNNER", "测试群管理员权限检查通过，开始执行筛选")
            return
        if _job_should_stop(job_id):
            raise ValueError("任务在等待测试群管理员权限期间被停止")
        if not warned:
            _append_log(
                job_id,
                "warn",
                "RUNNER",
                "检测到账号缺少测试群管理员邀请权限，请在测试群给探测号/真实号管理员权限后继续",
            )
            warned = True
        wait_desc = ", ".join(
            f"{str(ctx.get('acc_type') or 'probe')}:{_mask_phone(ctx['row'].phone)}" for ctx in waiting
        )
        _append_log(job_id, "info", "RUNNER", f"等待管理员权限中: {wait_desc}")
        await asyncio.sleep(5.0)


async def _disconnect_all(ctx_rows: list[dict]) -> None:
    for ctx in ctx_rows:
        try:
            await ctx["client"].disconnect()
        except Exception:
            pass


async def _run_task_async(task_id: int, job_id: str | None = None) -> None:
    db = SessionLocal()
    probe_ctx: list[dict] = []
    real_ctx: list[dict] = []
    try:
        task = db.query(UserFilterTask).filter(UserFilterTask.id == task_id).first()
        if task is None:
            _job_finalize(job_id, "failed")
            return
        task.status = "running"
        task.last_error = None
        db.add(task)
        db.commit()

        usernames = _load_scraper_usernames(int(task.source_task_id or 0))
        task.total_users = len(usernames)
        task.processed_users = 0
        task.success_count = 0
        task.fail_count = 0
        db.add(task)
        db.commit()

        _append_log(job_id, "success", "RUNNER", f"任务启动，待筛选用户 {len(usernames)}")
        _append_log(job_id, "info", "RUNNER", f"测试群组: {str(getattr(task, 'test_group', '') or '').strip()}")

        db.query(UserFilterResult).filter(UserFilterResult.task_id == task.id).delete()
        db.commit()

        test_group = str(getattr(task, "test_group", "") or "").strip()
        if not test_group:
            raise ValueError("测试群组不能为空")
        probe_ctx = await _build_probe_clients(task, db, job_id)
        real_ctx = await _build_filter_clients(task, db, job_id, acc_type="real", required=False)
        await _ensure_group_ready([*probe_ctx, *real_ctx], test_group, job_id)

        for idx, username in enumerate(usernames, start=1):
            if _job_should_stop(job_id):
                task.status = "stopped"
                db.add(task)
                db.commit()
                _append_log(job_id, "warn", "RUNNER", "收到停止指令，任务暂停")
                _job_finalize(job_id, "stopped")
                return

            ctx = probe_ctx[(idx - 1) % len(probe_ctx)]
            probe = ctx["row"]
            client = ctx["client"]
            channel = ctx["channel"]
            user_ref = str(username or "").strip()
            if not user_ref:
                continue

            can_invite = False
            reason = None
            _append_log(job_id, "info", "INVITE", f"[INVITE] user={user_ref}")
            try:
                await client(InviteToChannelRequest(channel=channel, users=[user_ref]))
                can_invite = True
                _append_log(job_id, "success", "RESULT", f"[RESULT] user={user_ref} -> LINK_ONLY ❌（不可用）")
            except Exception as exc:
                can_invite, reason = _classify_invite_error(exc)
                if reason == "USER_NOT_MUTUAL_CONTACT":
                    _append_log(job_id, "warn", "ERROR", "[ERROR] USER_NOT_MUTUAL_CONTACT -> 标记：可用用户（账号限制）")
                elif reason == "USER_PRIVACY_RESTRICTED":
                    _append_log(job_id, "warn", "ERROR", "[ERROR] USER_PRIVACY_RESTRICTED -> 标记：不可用用户")
                else:
                    _append_log(job_id, "warn", "ERROR", f"[ERROR] {reason}")
                _append_log(
                    job_id,
                    "info",
                    "RESULT",
                    f"[RESULT] user={user_ref} -> {'DIRECT_INVITE ✅（可用）' if can_invite else 'LINK_ONLY ❌（不可用）'}",
                )

            db.add(
                UserFilterResult(
                    task_id=task.id,
                    user_id="",
                    username=user_ref,
                    phone="",
                    can_invite=1 if can_invite else 0,
                    fail_reason=reason if not can_invite else None,
                    probe_account_id=probe.id,
                    verified_by_real=0,
                )
            )
            probe.last_used_at = datetime.now(timezone.utc)
            db.add(probe)

            task.processed_users = idx
            if can_invite:
                task.success_count = (task.success_count or 0) + 1
            else:
                task.fail_count = (task.fail_count or 0) + 1
            db.add(task)
            db.commit()
            await asyncio.sleep(1.0)

        task.status = "finished"
        db.add(task)
        db.commit()
        _append_log(job_id, "success", "RUNNER", "任务完成")
        _job_finalize(job_id, "completed")
    except Exception as exc:
        log.exception("run user filter task failed task_id=%s", task_id)
        try:
            task = db.query(UserFilterTask).filter(UserFilterTask.id == task_id).first()
            if task:
                task.status = "failed"
                task.last_error = str(exc)[:500]
                db.add(task)
                db.commit()
        except Exception:
            db.rollback()
        _append_log(job_id, "error", "RUNNER", f"任务失败: {str(exc)[:180]}")
        _job_finalize(job_id, "failed")
    finally:
        await _disconnect_all([*probe_ctx, *real_ctx])
        db.close()


def run_task_sync(task_id: int, job_id: str | None = None) -> None:
    asyncio.run(_run_task_async(task_id, job_id))


def spawn_task(task_id: int, job_id: str) -> None:
    t = threading.Thread(target=run_task_sync, args=(task_id, job_id), daemon=True, name=f"user-filter-{task_id}")
    t.start()


def export_results_csv(task_id: int, *, filter_mode: str = "all") -> Path:
    db = SessionLocal()
    try:
        mode = str(filter_mode or "all").lower()
        q = db.query(UserFilterResult).filter(UserFilterResult.task_id == task_id).order_by(UserFilterResult.id.asc())
        if mode == "link_only":
            q = q.filter(UserFilterResult.can_invite == 1)
        elif mode == "direct_invitable":
            q = q.filter(UserFilterResult.can_invite == 0)
        rows = q.all()
        out = RESULTS_DIR / f"user_filter_task_{task_id}_{mode}.csv"
        with out.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["user_id", "username", "can_invite", "reason"])
            for r in rows:
                writer.writerow([r.user_id or "", r.username or "", 1 if r.can_invite else 0, r.fail_reason or ""])
        return out
    finally:
        db.close()
