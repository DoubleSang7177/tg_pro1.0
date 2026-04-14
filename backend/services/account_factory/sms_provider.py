from __future__ import annotations


STRATEGIES = {"low_cost", "balanced", "high_success"}


def normalize_strategy(strategy: str | None) -> str:
    s = str(strategy or "balanced").strip().lower()
    return s if s in STRATEGIES else "balanced"


def pick_provider_name(strategy: str | None, countries: list[str] | None = None) -> str:
    s = normalize_strategy(strategy)
    if s == "low_cost":
        return "sms-cheap-pool"
    if s == "high_success":
        return "sms-premium-pool"
    return "sms-balanced-pool"

