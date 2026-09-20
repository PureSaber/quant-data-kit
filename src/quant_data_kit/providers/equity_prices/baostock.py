"""BaoStock daily-price adapter."""

from __future__ import annotations

import pandas as pd

from quant_data_kit.providers._symbols import normalize_symbol, to_market_symbol
from quant_data_kit.providers.equity_prices._common import canonicalize_prices


def fetch_prices(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    import baostock as bs

    login = bs.login()
    if str(login.error_code) != "0":
        raise RuntimeError("BaoStock login failed")
    try:
        result = bs.query_history_k_data_plus(
            to_market_symbol(symbol).replace("sh", "sh.").replace("sz", "sz."),
            "date,code,open,high,low,close,volume,amount",
            start_date=pd.Timestamp(start).strftime("%Y-%m-%d"),
            end_date=pd.Timestamp(end).strftime("%Y-%m-%d"),
            frequency="d",
            adjustflag={"": "3", "qfq": "2", "hfq": "1"}[adjust],
        )
        if str(result.error_code) != "0":
            raise RuntimeError("BaoStock price request failed")
        rows = []
        while result.next():
            rows.append(result.get_row_data())
        frame = pd.DataFrame(rows, columns=result.fields)
    finally:
        bs.logout()
    return canonicalize_prices(
        frame,
        symbol=normalize_symbol(symbol),
        provider="baostock",
        source="baostock:query_history_k_data_plus",
        adjust=adjust,
        source_volume_unit="share",
        source_amount_unit="CNY",
    )
