"""Canonical daily-price contract shared by provider adapters."""

from __future__ import annotations

import math

import pandas as pd

from quant_data_kit.providers._symbols import normalize_symbol

PRICE_COLUMNS = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "name",
    "industry",
    "provider",
    "source",
    "volume_unit",
    "source_volume_unit",
    "amount_unit",
    "source_amount_unit",
    "adjustment",
]


def adjustment_name(adjust: str) -> str:
    if adjust not in {"", "qfq", "hfq"}:
        raise ValueError("adjust must be '', 'qfq' or 'hfq'")
    return adjust or "none"


def canonicalize_prices(
    frame: pd.DataFrame,
    *,
    symbol: str,
    provider: str,
    source: str,
    adjust: str,
    source_volume_unit: str,
    source_amount_unit: str,
    volume_multiplier: float = 1.0,
    amount_multiplier: float = 1.0,
) -> pd.DataFrame:
    required = {"date", "open", "high", "low", "close", "volume"}
    if not required.issubset(frame.columns):
        raise ValueError(
            f"{provider} price response is incomplete; missing {sorted(required - set(frame.columns))}"
        )
    result = frame.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.normalize()
    for column in ("open", "high", "low", "close", "volume"):
        result[column] = pd.to_numeric(result[column], errors="coerce")
    if result[["date", "open", "high", "low", "close", "volume"]].isna().any().any():
        raise ValueError(f"{provider} price response contains invalid required values")
    result["volume"] *= volume_multiplier
    if "amount" in result:
        result["amount"] = pd.to_numeric(result["amount"], errors="coerce") * amount_multiplier
    else:
        result["amount"] = math.nan
    result["symbol"] = normalize_symbol(symbol)
    result["provider"] = provider
    result["source"] = source
    result["volume_unit"] = "share"
    result["source_volume_unit"] = source_volume_unit
    result["amount_unit"] = "CNY"
    result["source_amount_unit"] = source_amount_unit
    result["adjustment"] = adjustment_name(adjust)
    for column in ("name", "industry"):
        if column not in result:
            result[column] = None
    return result[PRICE_COLUMNS].sort_values("date").reset_index(drop=True)
