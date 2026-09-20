"""Explicit AKShare endpoint adapters."""

from __future__ import annotations

import logging

import pandas as pd

from quant_data_kit.providers._network import configure_network
from quant_data_kit.providers._symbols import normalize_symbol, to_market_symbol
from quant_data_kit.providers.equity_prices._common import canonicalize_prices

logger = logging.getLogger(__name__)


def fetch_eastmoney(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    import akshare as ak

    configure_network()
    code = normalize_symbol(symbol)
    hist = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=pd.Timestamp(start).strftime("%Y%m%d"),
        end_date=pd.Timestamp(end).strftime("%Y%m%d"),
        adjust=adjust,
    ).rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
        }
    )
    return canonicalize_prices(
        hist,
        symbol=code,
        provider="akshare_eastmoney",
        source="akshare:eastmoney",
        adjust=adjust,
        source_volume_unit="lot_100_shares",
        source_amount_unit="CNY",
        volume_multiplier=100,
    )


def fetch_tencent(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    import akshare as ak

    configure_network()
    code = normalize_symbol(symbol)
    kwargs = {
        "symbol": to_market_symbol(code),
        "start_date": pd.Timestamp(start).strftime("%Y%m%d"),
        "end_date": pd.Timestamp(end).strftime("%Y%m%d"),
        "adjust": adjust,
    }
    hist = ak.stock_zh_a_hist_tx(**kwargs)
    source_unit = "share"
    multiplier = 1
    if "amount" in hist and len(hist):
        reference = hist if not adjust else ak.stock_zh_a_hist_tx(**{**kwargs, "adjust": ""})
        tail = reference.loc[pd.to_numeric(reference["volume"]) > 0].tail(5)
        vwap = pd.to_numeric(tail["amount"]) / pd.to_numeric(tail["volume"]).replace(
            0, float("nan")
        )
        low = pd.to_numeric(tail["low"]) * 0.95
        high = pd.to_numeric(tail["high"]) * 1.05
        shares_valid = vwap.between(low, high).all()
        lots_valid = (vwap / 100).between(low, high).all()
        if lots_valid and not shares_valid:
            source_unit = "lot_100_shares"
            multiplier = 100
        elif not shares_valid:
            raise ValueError("Tencent amount/price cannot verify a consistent share-volume unit")
    return canonicalize_prices(
        hist,
        symbol=code,
        provider="akshare_tencent",
        source="akshare:tencent",
        adjust=adjust,
        source_volume_unit=source_unit,
        source_amount_unit="CNY",
        volume_multiplier=multiplier,
    )


def fetch_auto(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    try:
        return fetch_eastmoney(symbol, start, end, adjust)
    except Exception:  # noqa: BLE001 - legacy compatibility path intentionally falls back
        logger.debug("Eastmoney failed for %s; using Tencent compatibility fallback", symbol)
        return fetch_tencent(symbol, start, end, adjust)
