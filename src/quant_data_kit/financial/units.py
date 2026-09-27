"""Normalize vendor trading units without combining adjusted and raw shares."""

import numpy as np
import pandas as pd


def normalize_trading_units(
    frame: pd.DataFrame,
    *,
    volume_unit: str,
    amount_unit: str,
    currency: str,
    share_basis: str,
    lot_size: int | None = None,
) -> pd.DataFrame:
    """Produce shares and currency units; a lot multiplier must be explicitly supplied.

    ``free_float_shares`` must already use the same raw share basis as volume.
    Split-normalized historical volume must not be divided by raw historical float.
    The caller supplies historical/PIT float, never a current float backfill.
    """
    if share_basis != "raw" or volume_unit not in {"shares", "lots"}:
        raise ValueError("explicit raw shares/lots required")
    if amount_unit not in {"currency", "thousand_currency", "ten_thousand_currency"}:
        raise ValueError("unsupported traded amount unit")
    if not isinstance(currency, str) or len(currency) != 3 or not currency.isupper():
        raise ValueError("explicit three-letter currency required")
    if volume_unit == "lots" and (type(lot_size) is not int or lot_size <= 0):
        raise ValueError("lot volume requires an explicit positive lot_size")
    if "volume" not in frame or "amount" not in frame:
        raise ValueError("volume and actual traded amount are required")
    out = frame.copy()
    for col in ("volume", "amount", "free_float_shares"):
        if col not in out:
            continue
        out[col] = pd.to_numeric(out[col], errors="raise")
        values = out[col].dropna()
        if not np.isfinite(values).all() or (values < 0).any():
            raise ValueError(f"invalid {col}")
        if col == "free_float_shares" and values.eq(0).any():
            raise ValueError("known free float must be positive")
    out["volume"] *= lot_size if volume_unit == "lots" else 1
    out["amount"] *= {"currency": 1, "thousand_currency": 1000, "ten_thousand_currency": 10000}[
        amount_unit
    ]
    out["volume_unit"], out["amount_unit"] = "shares", "currency"
    out["share_basis"], out["currency"] = "raw", currency
    out.attrs["financial_units_version"] = "raw-trading-units/1"
    return out
