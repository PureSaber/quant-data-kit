"""Symbol normalization helpers."""

from __future__ import annotations


def normalize_symbol(symbol: str) -> str:
    return str(symbol).strip().zfill(6)


def to_market_symbol(symbol: str) -> str:
    code = normalize_symbol(symbol)
    if code.startswith(("5", "6", "9")):
        return f"sh{code}"
    return f"sz{code}"
