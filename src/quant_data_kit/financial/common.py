"""Strict boundary validation shared by financial data domains."""

from decimal import Decimal, InvalidOperation

import pandas as pd

from quant_data_kit.temporal_v2 import parse_timestamp_exact


def utc(value, field="timestamp") -> pd.Timestamp:
    result = parse_timestamp_exact(value, field=field)
    if pd.isna(result) or result.tzinfo is None:
        raise ValueError(f"{field} requires a nonmissing timezone-aware timestamp")
    return result.tz_convert("UTC")


def day(value) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if pd.isna(result) or result.tzinfo is not None or result != result.normalize():
        raise ValueError("expected a nonmissing plain calendar date")
    return result


def number(value, *, nonnegative=False) -> Decimal:
    if isinstance(value, bool):
        raise TypeError("boolean is not a financial number")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid financial number: {value!r}") from exc
    if not result.is_finite() or (nonnegative and result < 0):
        raise ValueError("financial numbers must be finite and respect sign constraints")
    return result


def table(frame, columns, *, text=(), timestamps=(), dates=(), unique=()):
    missing = set(columns) - set(frame.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")
    out = frame.copy(deep=True)
    for col in text:
        if out[col].map(lambda x: not isinstance(x, str) or not x.strip()).any():
            raise ValueError(f"{col} must contain nonempty strings")
    for col in timestamps:
        out[col] = pd.to_datetime(out[col].map(lambda x, field=col: utc(x, field)), utc=True)
    for col in dates:
        out[col] = pd.to_datetime(out[col].map(day))
    if unique and out.duplicated(list(unique)).any():
        raise ValueError(f"ambiguous duplicate facts: {list(unique)}")
    return out
