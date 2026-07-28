"""Dataframe schema and quality validation."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from quant_data_kit.exceptions import ValidationError

PRICE_REQUIRED = ("symbol", "date", "open", "high", "low", "close", "volume")


def validate_price_frame(
    df: pd.DataFrame,
    *,
    required: Sequence[str] = PRICE_REQUIRED,
    max_missing_ratio: float = 0.05,
) -> dict[str, float | int]:
    """Validate OHLCV panel; return summary stats or raise ValidationError."""
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValidationError(f"Missing columns: {missing_cols}")

    if df.empty:
        raise ValidationError("Dataframe is empty")

    numeric_cols = [c for c in ("open", "high", "low", "close", "volume") if c in df.columns]
    missing_ratio = float(df[numeric_cols].isna().mean().mean()) if numeric_cols else 0.0
    if missing_ratio > max_missing_ratio:
        raise ValidationError(f"Missing ratio {missing_ratio:.2%} exceeds {max_missing_ratio:.2%}")

    dup = df.duplicated(subset=["symbol", "date"]).sum() if {"symbol", "date"}.issubset(df.columns) else 0
    if dup:
        raise ValidationError(f"Found {dup} duplicate symbol-date rows")

    return {
        "rows": len(df),
        "symbols": int(df["symbol"].nunique()) if "symbol" in df.columns else 0,
        "missing_ratio": round(missing_ratio, 6),
        "duplicates": int(dup),
    }
