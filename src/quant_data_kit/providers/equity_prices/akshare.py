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


def fetch_eastmoney_etf(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    """Fetch one exchange-traded fund through Eastmoney's ETF endpoint.

    ETF turnover volume is reported in board lots, just like the Eastmoney
    A-share endpoint, so the canonical frame records and converts that source
    unit explicitly.
    """
    import akshare as ak

    configure_network()
    code = normalize_symbol(symbol)
    hist = ak.fund_etf_hist_em(
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
        provider="akshare_eastmoney_etf",
        source="akshare:eastmoney:fund_etf_hist_em",
        adjust=adjust,
        source_volume_unit="lot_100_shares",
        source_amount_unit="CNY",
        volume_multiplier=100,
    )


def fetch_sina_etf(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    """Fetch Sina ETF bars and derive qfq from Sina's cumulative cash series.

    Sina exposes real unadjusted ETF bars and a separate cumulative dividend
    series. The qfq transformation is deterministic and cash-evidenced: every
    earlier OHLC value is multiplied by ``(previous_close-cash)/previous_close``
    at each distribution date. No dividend is inferred from a price jump.
    """
    import akshare as ak

    if adjust == "hfq":
        raise ValueError("akshare_sina_etf does not provide an evidenced hfq series")
    configure_network()
    code = normalize_symbol(symbol)
    market_symbol = to_market_symbol(code)
    hist = ak.fund_etf_hist_sina(symbol=market_symbol).rename(
        columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "amount": "amount",
        }
    )
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce").dt.normalize()
    for column in ("open", "high", "low", "close"):
        hist[column] = pd.to_numeric(hist[column], errors="coerce")
    hist = hist.sort_values("date").reset_index(drop=True)
    source = "akshare:sina:fund_etf_hist_sina"
    if adjust == "qfq":
        dividends = ak.fund_etf_dividend_sina(symbol=market_symbol).rename(
            columns={"日期": "date", "累计分红": "cumulative_cash"}
        )
        required = {"date", "cumulative_cash"}
        if not required.issubset(dividends):
            raise ValueError("Sina ETF cumulative-dividend schema changed")
        dividends["date"] = pd.to_datetime(dividends["date"], errors="coerce").dt.normalize()
        dividends["cumulative_cash"] = pd.to_numeric(dividends["cumulative_cash"], errors="coerce")
        dividends = dividends.sort_values("date").reset_index(drop=True)
        dividends["cash"] = dividends["cumulative_cash"].diff().fillna(dividends["cumulative_cash"])
        if dividends[["date", "cash"]].isna().any().any() or (dividends["cash"] < 0).any():
            raise ValueError("Sina ETF cumulative-dividend series is invalid")
        for event in dividends.itertuples():
            cash = float(event.cash)
            if cash == 0:
                continue
            previous = hist[hist["date"] < event.date]
            if previous.empty:
                continue
            previous_close = float(previous.iloc[-1]["close"])
            if cash >= previous_close:
                raise ValueError("Sina ETF dividend cash exceeds the previous close")
            factor = (previous_close - cash) / previous_close
            hist.loc[hist["date"] < event.date, ["open", "high", "low", "close"]] *= factor
        source += "+fund_etf_dividend_sina"
    start_day = pd.Timestamp(start).normalize()
    end_day = pd.Timestamp(end).normalize()
    hist = hist[hist["date"].between(start_day, end_day)]
    return canonicalize_prices(
        hist,
        symbol=code,
        provider="akshare_sina_etf",
        source=source,
        adjust=adjust,
        source_volume_unit="share",
        source_amount_unit="CNY",
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
