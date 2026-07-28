"""A-share trading calendar helpers."""

from __future__ import annotations

import pandas as pd


def trading_days_between(start: str, end: str, trade_dates: pd.DatetimeIndex | None = None) -> pd.DatetimeIndex:
    """Return trading days in [start, end]. Uses AKShare SSE calendar when trade_dates is None."""
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    if trade_dates is None:
        trade_dates = load_sse_trade_dates()
    mask = (trade_dates >= start_ts) & (trade_dates <= end_ts)
    return trade_dates[mask]


def load_sse_trade_dates() -> pd.DatetimeIndex:
    """Load SSE trading calendar via AKShare."""
    import akshare as ak

    df = ak.tool_trade_date_hist_sina()
    col = "trade_date" if "trade_date" in df.columns else df.columns[0]
    dates = pd.to_datetime(df[col]).sort_values()
    return pd.DatetimeIndex(dates)


def is_trading_day(day: str | pd.Timestamp, trade_dates: pd.DatetimeIndex | None = None) -> bool:
    ts = pd.Timestamp(day).normalize()
    if trade_dates is None:
        trade_dates = load_sse_trade_dates()
    return ts in set(trade_dates)
