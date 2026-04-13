from __future__ import annotations

import os


def _to_int(name: str, default: str) -> int:
    raw = os.getenv(name, default).strip()
    try:
        return int(raw)
    except Exception as exc:
        raise RuntimeError(f"{name} 必须为整数，当前值={raw!r}") from exc


TELEGRAM_API_ID = _to_int("TELEGRAM_API_ID", "28591489")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "427fe982437cbe21aec340e6d80a10a4").strip()

if not TELEGRAM_API_HASH:
    raise RuntimeError("TELEGRAM_API_HASH 不能为空")
