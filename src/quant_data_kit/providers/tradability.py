"""Capture current public ST/halt observations without fabricating historical states."""

from __future__ import annotations

import pandas as pd

from quant_data_kit.providers._network import configure_network
from quant_data_kit.providers._symbols import normalize_symbol


def normalize_tradability(symbols, st, halts, session, captured_at):
    if "代码" not in st or not {"代码", "停牌时间", "停牌截止时间"}.issubset(halts.columns):
        raise ValueError("ST/halt response is incomplete")
    at = pd.Timestamp(captured_at)
    day = pd.Timestamp(session).normalize()
    if at.tzinfo is None or day > at.tz_convert("Asia/Shanghai").tz_localize(None).normalize():
        raise ValueError("Invalid trading-status observation date")
    st_symbols = set(st["代码"].astype(str).map(normalize_symbol))
    rows = []
    for symbol in symbols:
        symbol = normalize_symbol(symbol)
        observations = halts[halts["代码"].astype(str).map(normalize_symbol) == symbol]
        state = "risk_warning" if symbol in st_symbols else "no_reported_restriction"
        for row in observations.to_dict("records"):
            start = pd.to_datetime(row["停牌时间"], errors="coerce")
            end = pd.to_datetime(row["停牌截止时间"], errors="coerce")
            if pd.isna(start):
                state = "unknown"
            elif start.normalize() <= day and (pd.isna(end) or end.normalize() >= day):
                state = "suspended"
        rows.append(
            {
                "symbol": symbol,
                "session": day,
                "status": state,
                "captured_at": at.isoformat(),
                "source": "akshare.eastmoney.current_st_and_halts",
                "scope": "current source observations only; no historical/delisting completeness claim",
            }
        )
    return pd.DataFrame(rows)


def fetch_tradability(symbols, session, captured_at):
    import akshare as ak

    configure_network()
    return normalize_tradability(
        symbols,
        ak.stock_zh_a_st_em(),
        ak.stock_tfp_em(date=pd.Timestamp(session).strftime("%Y%m%d")),
        session,
        captured_at,
    )
