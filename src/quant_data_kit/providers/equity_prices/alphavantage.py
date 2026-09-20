"""Alpha Vantage daily-price adapter."""

from __future__ import annotations

import os

import pandas as pd

from quant_data_kit.providers._symbols import normalize_symbol
from quant_data_kit.providers.equity_prices._common import canonicalize_prices


def _ticker(symbol: str) -> str:
    code = normalize_symbol(symbol)
    return f"{code}.SHH" if code.startswith(("5", "6", "9")) else f"{code}.SHZ"


def fetch_prices(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    import requests

    if adjust == "hfq":
        raise ValueError("Alpha Vantage does not expose a canonical backward-adjusted daily series")
    function = "TIME_SERIES_DAILY_ADJUSTED" if adjust else "TIME_SERIES_DAILY"
    response = requests.get(
        "https://www.alphavantage.co/query",
        params={
            "function": function,
            "symbol": _ticker(symbol),
            "outputsize": "full",
            "apikey": os.environ["ALPHAVANTAGE_API_KEY"],
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    series = payload.get("Time Series (Daily)")
    try:
        series_items = series.items()
    except AttributeError as exc:
        raise ValueError("Alpha Vantage daily series is unavailable for this request") from exc
    rows = []
    for date, values in series_items:
        close = float(values["4. close"])
        scale = float(values.get("5. adjusted close", close)) / close if adjust else 1.0
        rows.append(
            {
                "date": date,
                "open": float(values["1. open"]) * scale,
                "high": float(values["2. high"]) * scale,
                "low": float(values["3. low"]) * scale,
                "close": close * scale,
                "volume": values["6. volume"] if adjust else values["5. volume"],
            }
        )
    frame = pd.DataFrame(rows)
    dates = pd.to_datetime(frame["date"])
    frame = frame[(dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end))]
    return canonicalize_prices(
        frame,
        symbol=symbol,
        provider="alpha_vantage",
        source=f"alphavantage:{function.lower()}",
        adjust=adjust,
        source_volume_unit="share",
        source_amount_unit="unavailable",
    )
