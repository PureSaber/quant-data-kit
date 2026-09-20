"""Tushare Pro daily-price adapter."""

from __future__ import annotations

import os

import pandas as pd

from quant_data_kit.providers._symbols import normalize_symbol
from quant_data_kit.providers.equity_prices._common import canonicalize_prices


def _ts_code(symbol: str) -> str:
    code = normalize_symbol(symbol)
    suffix = "SH" if code.startswith(("5", "6", "9")) else "SZ"
    return f"{code}.{suffix}"


def fetch_prices(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    import tushare as ts

    token = os.environ["TUSHARE_TOKEN"]
    client = ts.pro_api(token)
    arguments = {
        "ts_code": _ts_code(symbol),
        "start_date": pd.Timestamp(start).strftime("%Y%m%d"),
        "end_date": pd.Timestamp(end).strftime("%Y%m%d"),
    }
    if adjust:
        frame = ts.pro_bar(api=client, adj=adjust, **arguments)
    else:
        frame = client.daily(**arguments)
    if frame is None:
        raise RuntimeError("Tushare returned no price response")
    frame = frame.rename(columns={"trade_date": "date", "vol": "volume"})
    return canonicalize_prices(
        frame,
        symbol=symbol,
        provider="tushare",
        source="tushare:pro_bar" if adjust else "tushare:daily",
        adjust=adjust,
        source_volume_unit="lot_100_shares",
        source_amount_unit="thousand_CNY",
        volume_multiplier=100,
        amount_multiplier=1000,
    )
