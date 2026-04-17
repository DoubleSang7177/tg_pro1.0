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
_LINK_ONLY_TEXT_HINTS = (
    "restricts adding them to groups",
    "send invite link instead",
    "send invite link",
    "send an invite link",
)


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


def _normalize_reason(reason: str | None) -> str:
    return str(reason or "").strip().upper()


def _is_bot_username(username: str | None) -> bool:
    u = str(username or "").strip().lstrip("@").lower()
    if not u:
        return False
    return u.endswith("bot") or u.endswith("_bot")


def _resolve_second_check_status(final_status: str, checked: bool) -> str:
    if checked:
        return "checked"
    return "pending" if final_status == "unknown" else "checked"


def _resolve_final_status_from_invite(exc: Exception | None, response_text: str | None) -> tuple[str, str | None]:
    """
    邀请判定统一入口：隐私/互关语义优先；仅 NULL（无有效错误码）与 FLOOD 归为 unknown 以便换号复检；
    其余异常按「可用用户」处理为 direct_invitable。
    返回: (final_status, reason)
    """
    text = str(response_text or "").lower()
    err = str(exc or "").lower()
    combined = f"{text} {err}"

    # 唯一不可用（用户隐私限制）
    if any(x in combined for x in _LINK_ONLY_TEXT_HINTS) or "user_privacy_restricted" in combined:
        return "link_only", "USER_PRIVACY_RESTRICTED"

    # 可用（账号限制）
    if "user_not_mutual_contact" in combined or "you can only add mutual contacts" in combined:
        return "direct_invitable", "USER_NOT_MUTUAL_CONTACT"

    # 按保守口径：Invite 成功但无法精确区分时，统一按 link_only 处理
    if exc is None:
        return "link_only", "INVITE_SUCCESS_UNCERTAIN"

    code_n = _normalize_reason(_error_code(exc))
    # FLOOD：不确定，换账号复检
    if code_n.startswith("FLOOD") or "FLOOD" in code_n or "flood" in err:
        return "unknown", code_n or "FLOOD"

    # NULL / 无有效错误码：不确定
    if not code_n or code_n in ("NULL", "NONE") or "null" in err:
        return "unknown", code_n or "NULL"

    # 其它异常：标记为可用用户
    return "direct_invitable", code_n


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


async def _run_secondary_filter(
    *,
    task: UserFilterTask,
    db,
    job_id: str | None,
    real_ctx: list[dict],
) -> None:
    enabled = bool(getattr(task, "real_verify_enabled", 0))
    if not enabled:
        return
    unknown_rows = (
        db.query(UserFilterResult)
        .filter(UserFilterResult.task_id == task.id, UserFilterResult.final_status == "unknown")
        .order_by(UserFilterResult.id.asc())
        .all()
    )
    if not unknown_rows:
        _append_log(job_id, "info", "SECOND_CHECK", "无 unknown 用户，跳过二次复检")
        return
    if not real_ctx:
        _append_log(job_id, "warn", "SECOND_CHECK", "未配置可用真实账号，unknown 用户保留为未复检")
        return

    # 按需求：unknown 永远全量复检（忽略 real_verify_ratio）
    sample_count = len(unknown_rows)
    sample_rows = unknown_rows
    _append_log(
        job_id,
        "info",
        "SECOND_CHECK",
        f"启动二次复检：unknown={len(unknown_rows)}，抽样={len(sample_rows)}，ratio=1.00（强制全量）",
    )

    for idx, row in enumerate(sample_rows, start=1):
        if _job_should_stop(job_id):
            _append_log(job_id, "warn", "SECOND_CHECK", "收到停止指令，二次复检中止")
            return
        user_ref = str(row.username or "").strip()
        if not user_ref:
            row.second_check_status = "checked"
            db.add(row)
            db.commit()
            continue
        final_status = "unknown"
        reason = row.fail_reason
        used_rounds = 0
        start_idx = (idx - 1) % len(real_ctx)
        ordered_ctx = [real_ctx[(start_idx + i) % len(real_ctx)] for i in range(len(real_ctx))]
        for ctx in ordered_ctx:
            if _job_should_stop(job_id):
                _append_log(job_id, "warn", "SECOND_CHECK", "收到停止指令，二次复检中止")
                return
            client = ctx["client"]
            channel = ctx["channel"]
            real_acc = ctx["row"]
            used_rounds += 1
            _append_log(
                job_id,
                "info",
                "SECOND_CHECK",
                f"[ROUND_CHECK] user={user_ref} account={_mask_phone(getattr(real_acc, 'phone', None))}",
            )
            try:
                resp = await client(InviteToChannelRequest(channel=channel, users=[user_ref]))
                _append_log(
                    job_id,
                    "info",
                    "SECOND_CHECK",
                    f"[INVITE_RAW] user={user_ref} ok response={str(resp)[:180]}",
                )
                final_status, reason = _resolve_final_status_from_invite(None, None)
            except Exception as exc:
                _append_log(
                    job_id,
                    "warn",
                    "SECOND_CHECK",
                    f"[INVITE_RAW] user={user_ref} err_code={_error_code(exc)} err={str(exc)[:220]}",
                )
                final_status, reason = _resolve_final_status_from_invite(exc, None)

            real_acc.last_used_at = datetime.now(timezone.utc)
            db.add(real_acc)
            await asyncio.sleep(1.5)

            if final_status == "unknown":
                _append_log(
                    job_id,
                    "warn",
                    "SECOND_CHECK",
                    f"[RESULT] unknown（NULL/FLOOD，换真实号重试） account_round={used_rounds}",
                )
                continue
            break

        row.final_status = final_status
        row.fail_reason = reason
        row.verified_by_real = 1
        row.real_check_rounds = used_rounds
        row.second_check_status = _resolve_second_check_status(final_status, checked=True)
        db.add(row)
        if final_status == "direct_invitable":
            _append_log(job_id, "success", "SECOND_CHECK", f"[RESULT] direct_invitable（第{used_rounds}个账号命中）")
        elif final_status == "link_only":
            _append_log(job_id, "warn", "SECOND_CHECK", "[RESULT] link_only（隐私限制）")
        else:
            _append_log(job_id, "warn", "SECOND_CHECK", "[RESULT] unknown（全部真实账号复检后仍不确定）")
        db.commit()

    # 未进入抽样的 unknown 用户保留 pending
    for row in unknown_rows[sample_count:]:
        row.second_check_status = "pending"
        row.real_check_rounds = 0
        db.add(row)
    db.commit()


async def _run_task_async(
    task_id: int,
    job_id: str | None = None,
    *,
    run_scope: str = "full",
    defer_job_finalize: bool = False,
) -> None:
    db = SessionLocal()
    probe_ctx: list[dict] = []
    real_ctx: list[dict] = []
    try:
        task = db.query(UserFilterTask).filter(UserFilterTask.id == task_id).first()
        if task is None:
            _job_finalize(job_id, "failed")
            return
        scope = str(run_scope or "full").strip().lower()
        if scope not in ("full", "unknown_only"):
            scope = "full"

        task.status = "running"
        task.last_error = None
        db.add(task)
        db.commit()
        _append_log(job_id, "info", "RUNNER", f"runner 已启动 task_id={task.id} scope={scope}")

        if scope == "unknown_only":
            res_rows = (
                db.query(UserFilterResult)
                .filter(UserFilterResult.task_id == task.id, UserFilterResult.final_status == "unknown")
                .order_by(UserFilterResult.id.asc())
                .all()
            )
            seen_u: set[str] = set()
            usernames: list[str] = []
            for r in res_rows:
                u = str(r.username or "").strip()
                if not u or u in seen_u:
                    continue
                seen_u.add(u)
                usernames.append(u)
            if not usernames:
                if defer_job_finalize:
                    _append_log(job_id, "warn", "RUNNER", f"task_id={task.id} 无不确定用户，已跳过")
                    task.status = "finished"
                    db.add(task)
                    db.commit()
                    return
                _append_log(job_id, "warn", "RUNNER", f"task_id={task.id} 无不确定用户，已结束")
                task.status = "finished"
                db.add(task)
                db.commit()
                _job_finalize(job_id, "completed")
                return
            db.query(UserFilterResult).filter(
                UserFilterResult.task_id == task.id,
                UserFilterResult.final_status == "unknown",
            ).delete(synchronize_session=False)
            db.commit()
        else:
            usernames = _load_scraper_usernames(int(task.source_task_id or 0))
            db.query(UserFilterResult).filter(UserFilterResult.task_id == task.id).delete()
            db.commit()

        task.total_users = len(usernames)
        task.processed_users = 0
        task.success_count = 0
        task.fail_count = 0
        db.add(task)
        db.commit()

        if scope == "unknown_only":
            _append_log(job_id, "success", "RUNNER", f"仅重筛不确定用户，待处理 {len(usernames)} 人")
        else:
            _append_log(job_id, "success", "RUNNER", f"任务启动，待筛选用户 {len(usernames)}")
        _append_log(job_id, "info", "RUNNER", f"测试群组: {str(getattr(task, 'test_group', '') or '').strip()}")

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

            if _is_bot_username(user_ref):
                _append_log(job_id, "warn", "RUNNER", f"[BOT_FILTER] user={user_ref} 命中 bot 后缀，已直接过滤")
                db.add(
                    UserFilterResult(
                        task_id=task.id,
                        user_id="",
                        username=user_ref,
                        phone="",
                        fail_reason="BOT_USERNAME_FILTERED",
                        probe_account_id=probe.id,
                        verified_by_real=0,
                        second_check_status="checked",
                        final_status="link_only",
                        real_check_rounds=0,
                    )
                )
                task.processed_users = idx
                task.fail_count = (task.fail_count or 0) + 1
                db.add(task)
                db.commit()
                continue

            reason = None
            final_status = "unknown"
            _append_log(job_id, "info", "RUNNER", f"[ROUND_CHECK] user={user_ref} account={_mask_phone(getattr(probe, 'phone', None))}")
            _append_log(job_id, "info", "INVITE", f"[INVITE] user={user_ref}")
            try:
                resp = await client(InviteToChannelRequest(channel=channel, users=[user_ref]))
                _append_log(
                    job_id,
                    "info",
                    "INVITE",
                    f"[INVITE_RAW] user={user_ref} ok response={str(resp)[:180]}",
                )
                final_status, reason = _resolve_final_status_from_invite(None, None)
                if final_status == "link_only":
                    _append_log(job_id, "warn", "RESULT", f"[RESULT] user={user_ref} -> LINK_ONLY（success但按保守策略）")
                else:
                    _append_log(job_id, "success", "RESULT", f"[RESULT] user={user_ref} -> DIRECT_INVITABLE ✅")
            except Exception as exc:
                _append_log(
                    job_id,
                    "warn",
                    "INVITE",
                    f"[INVITE_RAW] user={user_ref} err_code={_error_code(exc)} err={str(exc)[:220]}",
                )
                final_status, reason = _resolve_final_status_from_invite(exc, None)
                if final_status == "direct_invitable":
                    if reason == "USER_NOT_MUTUAL_CONTACT":
                        _append_log(
                            job_id,
                            "warn",
                            "ERROR",
                            "[ERROR] USER_NOT_MUTUAL_CONTACT -> 标记：可用用户（账号限制）",
                        )
                    else:
                        _append_log(
                            job_id,
                            "info",
                            "ERROR",
                            f"[ERROR] {reason or _error_code(exc)} -> 标记：可用用户（非 NULL/FLOOD）",
                        )
                elif final_status == "link_only":
                    _append_log(job_id, "warn", "ERROR", "[ERROR] USER_PRIVACY_RESTRICTED -> 标记：不可用用户")
                else:
                    _append_log(job_id, "warn", "ERROR", f"[ERROR] {reason or _error_code(exc)}")
                _append_log(
                    job_id,
                    "info",
                    "RESULT",
                    f"[RESULT] user={user_ref} -> {final_status}",
                )

            second_check_status = _resolve_second_check_status(final_status, checked=False)
            db.add(
                UserFilterResult(
                    task_id=task.id,
                    user_id="",
                    username=user_ref,
                    phone="",
                    fail_reason=reason,
                    probe_account_id=probe.id,
                    verified_by_real=0,
                    second_check_status=second_check_status,
                    final_status=final_status,
                )
            )
            probe.last_used_at = datetime.now(timezone.utc)
            db.add(probe)

            task.processed_users = idx
            if final_status == "direct_invitable":
                task.success_count = (task.success_count or 0) + 1
            else:
                task.fail_count = (task.fail_count or 0) + 1
            db.add(task)
            db.commit()
            await asyncio.sleep(1.0)

        await _run_secondary_filter(task=task, db=db, job_id=job_id, real_ctx=real_ctx)
        task.status = "finished"
        db.add(task)
        db.commit()
        _append_log(job_id, "success", "RUNNER", "任务完成")
        if not defer_job_finalize:
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


def run_task_sync(
    task_id: int,
    job_id: str | None = None,
    *,
    run_scope: str = "full",
    defer_job_finalize: bool = False,
) -> None:
    asyncio.run(_run_task_async(task_id, job_id, run_scope=run_scope, defer_job_finalize=defer_job_finalize))


def spawn_task(task_id: int, job_id: str, *, run_scope: str = "full") -> None:
    t = threading.Thread(
        target=run_task_sync,
        args=(task_id, job_id),
        kwargs={"run_scope": run_scope},
        daemon=True,
        name=f"user-filter-{task_id}",
    )
    t.start()


def run_unknown_refilter_chain_sync(task_ids: list[int], job_id: str | None) -> None:
    """同一 live 会话内依次对多个任务执行 unknown_only（由最后一个任务在内层 finalize）。"""
    ids = [int(x) for x in task_ids if int(x) > 0]
    if not ids:
        _job_finalize(job_id, "failed")
        return
    n = len(ids)
    for i, tid in enumerate(ids):
        defer = i < n - 1
        if n > 1:
            _append_log(job_id, "info", "RUNNER", f"── 不确定重筛 {i + 1}/{n}：task_id={tid} ──")
        asyncio.run(_run_task_async(tid, job_id, run_scope="unknown_only", defer_job_finalize=defer))


def spawn_unknown_refilter_chain(task_ids: list[int], job_id: str) -> None:
    t = threading.Thread(
        target=run_unknown_refilter_chain_sync,
        args=(task_ids, job_id),
        daemon=True,
        name="user-filter-unknown-chain",
    )
    t.start()


def export_results_csv(task_id: int, *, filter_mode: str = "all") -> Path:
    db = SessionLocal()
    try:
        mode = str(filter_mode or "all").lower()
        q = db.query(UserFilterResult).filter(UserFilterResult.task_id == task_id).order_by(UserFilterResult.id.asc())
        if mode == "link_only":
            q = q.filter(UserFilterResult.final_status == "link_only")
        elif mode == "direct_invitable":
            q = q.filter(UserFilterResult.final_status == "direct_invitable")
        elif mode == "unknown":
            q = q.filter(UserFilterResult.final_status == "unknown")
        rows = q.all()
        out = RESULTS_DIR / f"user_filter_task_{task_id}_{mode}.csv"
        with out.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["user_id", "username", "reason", "final_status", "second_check_status"])
            for r in rows:
                writer.writerow(
                    [
                        r.user_id or "",
                        r.username or "",
                        r.fail_reason or "",
                        r.final_status or "",
                        r.second_check_status or "",
                    ]
                )
        return out
    finally:
        db.close()
