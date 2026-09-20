"""Yahoo Finance daily-price adapter using the optional yfinance client."""

from __future__ import annotations

import pandas as pd

from quant_data_kit.providers._symbols import normalize_symbol
from quant_data_kit.providers.equity_prices._common import canonicalize_prices


def _ticker(symbol: str) -> str:
    code = normalize_symbol(symbol)
    return f"{code}.SS" if code.startswith(("5", "6", "9")) else f"{code}.SZ"


def _single_ticker_frame(frame: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if not isinstance(frame.columns, pd.MultiIndex):
        return frame
    for level in range(frame.columns.nlevels):
        if ticker in frame.columns.get_level_values(level):
            return frame.xs(ticker, axis=1, level=level)
    if any(len(frame.columns.get_level_values(level).unique()) == 1 for level in range(2)):
        level = next(
            level for level in range(2) if len(frame.columns.get_level_values(level).unique()) == 1
        )
        return frame.droplevel(level, axis=1)
    raise ValueError("Yahoo response contains an unexpected multi-ticker schema")


def fetch_prices(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    import yfinance as yf

    ticker = _ticker(symbol)
    frame = yf.download(
        ticker,
        start=pd.Timestamp(start).strftime("%Y-%m-%d"),
        end=(pd.Timestamp(end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
        auto_adjust=adjust == "qfq",
        back_adjust=adjust == "hfq",
        actions=False,
        progress=False,
        repair=False,
        threads=False,
    )
    frame = _single_ticker_frame(frame, ticker).reset_index()
    frame.columns = [str(column).strip().lower().replace(" ", "_") for column in frame.columns]
    return canonicalize_prices(
        frame,
        symbol=symbol,
        provider="yahoo",
        source="yfinance:yahoo",
        adjust=adjust,
        source_volume_unit="share",
        source_amount_unit="unavailable",
    )
