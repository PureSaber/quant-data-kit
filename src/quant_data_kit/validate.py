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

    dup = (
        df.duplicated(subset=["symbol", "date"]).sum()
        if {"symbol", "date"}.issubset(df.columns)
        else 0
    )
    if dup:
        raise ValidationError(f"Found {dup} duplicate symbol-date rows")

    numeric = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    if (numeric[[c for c in ("open", "high", "low", "close") if c in numeric]] <= 0).any().any():
        raise ValidationError("OHLC prices must be positive")
    if "volume" in numeric and (numeric["volume"] < 0).any():
        raise ValidationError("Volume must be non-negative")
    if {"open", "high", "low", "close"}.issubset(numeric.columns):
        invalid_bar = (numeric["high"] < numeric[["open", "close", "low"]].max(axis=1)) | (
            numeric["low"] > numeric[["open", "close", "high"]].min(axis=1)
        )
        if invalid_bar.any():
            raise ValidationError(f"Found {int(invalid_bar.sum())} invalid OHLC bars")

    dates = pd.to_datetime(df["date"], errors="coerce")
    if dates.isna().any():
        raise ValidationError("Invalid dates in price frame")

    return {
        "rows": len(df),
        "symbols": int(df["symbol"].nunique()) if "symbol" in df.columns else 0,
        "missing_ratio": round(missing_ratio, 6),
        "duplicates": int(dup),
    }


def validate_calendar_coverage(
    df: pd.DataFrame,
    expected_dates: pd.DatetimeIndex,
    *,
    date_col: str = "date",
    symbol_col: str = "symbol",
    max_missing_ratio: float = 0.02,
) -> pd.DataFrame:
    """Return per-symbol calendar coverage and reject materially incomplete data."""
    if not {date_col, symbol_col}.issubset(df.columns):
        raise ValidationError(f"Calendar coverage requires {date_col},{symbol_col}")
    expected = pd.DatetimeIndex(pd.to_datetime(expected_dates)).normalize().unique()
    if expected.empty:
        raise ValidationError("Expected trading calendar is empty")
    rows: list[dict[str, float | int | str]] = []
    for symbol, group in df.groupby(symbol_col):
        observed = pd.DatetimeIndex(pd.to_datetime(group[date_col])).normalize().unique()
        missing = expected.difference(observed)
        ratio = len(missing) / len(expected)
        rows.append(
            {
                "symbol": str(symbol),
                "expected_days": len(expected),
                "observed_days": len(expected.intersection(observed)),
                "missing_days": len(missing),
                "missing_ratio": ratio,
            }
        )
    report = pd.DataFrame(rows)
    offenders = report[report["missing_ratio"] > max_missing_ratio]
    if not offenders.empty:
        raise ValidationError(
            f"Calendar coverage failed for {len(offenders)} symbols; "
            f"worst missing ratio={offenders['missing_ratio'].max():.2%}"
        )
    return report
