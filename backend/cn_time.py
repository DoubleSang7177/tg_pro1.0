"""用户可见日志时间统一为东八区。"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

_CN = ZoneInfo("Asia/Shanghai")


def cn_hms() -> str:
    return datetime.now(_CN).strftime("%H:%M:%S")


def cn_hm() -> str:
    """东八区时:分（终端行首展示）。"""
    return datetime.now(_CN).strftime("%H:%M")
