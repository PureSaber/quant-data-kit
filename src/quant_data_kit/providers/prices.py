"""Daily OHLCV fetch via AKShare."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

import pandas as pd

from quant_data_kit.providers._fetch import fetch_symbols_parallel, fetch_with_retries
from quant_data_kit.providers._network import configure_network
from quant_data_kit.providers._symbols import normalize_symbol, to_market_symbol

logger = logging.getLogger(__name__)

PRICE_COLUMNS = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "name",
    "industry",
    "source",
    "volume_unit",
    "source_volume_unit",
    "adjustment",
    "amount",
]


def _fetch_one_price(
    symbol: str,
    start: str,
    end: str,
    fetch_fn: Callable[[str, str, str], pd.DataFrame] | None,
    sleep_seconds: float,
    adjust: str = "qfq",
) -> pd.DataFrame:
    if fetch_fn is not None:
        hist = fetch_fn(symbol, start, end)
    else:
        import akshare as ak

        configure_network()
        code = normalize_symbol(symbol)
        try:
            hist = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start,
                end_date=end,
                adjust=adjust,
            )
            hist = hist.rename(
                columns={
                    "日期": "date",
                    "开盘": "open",
                    "最高": "high",
                    "最低": "low",
                    "收盘": "close",
                    "成交量": "volume",
                }
            )
            # Eastmoney reports lots; Tencent's AKShare adapter returns shares.
            hist["volume"] = pd.to_numeric(hist["volume"], errors="raise") * 100
            hist["source"] = "akshare:eastmoney"
            hist["source_volume_unit"] = "lot_100_shares"
        except Exception:  # noqa: BLE001
            logger.debug("Eastmoney price API failed for %s; using stock_zh_a_hist_tx", code)
            hist = ak.stock_zh_a_hist_tx(
                symbol=to_market_symbol(code),
                start_date=start,
                end_date=end,
                adjust=adjust,
            )
            hist["source"] = "akshare:tencent"
            hist["source_volume_unit"] = "share"
            # AKShare 1.18.88 exempts all sz000* symbols from its lot conversion,
            # although these are Shenzhen stocks here, not index requests.
            # Verify using turnover value / volume against the UNADJUSTED bar.
            # An old qfq/hfq price cannot be compared with actual traded value.
            # Use an unadjusted reference for that same response window.
            if "amount" in hist and len(hist):
                reference = (
                    hist
                    if not adjust
                    else ak.stock_zh_a_hist_tx(
                        symbol=to_market_symbol(code), start_date=start, end_date=end, adjust=""
                    )
                )
                tail = reference.loc[pd.to_numeric(reference["volume"]) > 0].tail(5)
                vwap = pd.to_numeric(tail["amount"]) / pd.to_numeric(tail["volume"]).replace(
                    0, float("nan")
                )
                low = pd.to_numeric(tail["low"]) * 0.95
                high = pd.to_numeric(tail["high"]) * 1.05
                shares_valid = vwap.between(low, high).all()
                lots_valid = (vwap / 100).between(low, high).all()
                if lots_valid and not shares_valid:
                    hist["volume"] = pd.to_numeric(hist["volume"]) * 100
                    hist["source_volume_unit"] = "lot_100_shares"
                elif not shares_valid:
                    raise ValueError(
                        "Tencent amount/price cannot verify a consistent share-volume unit"
                    )
        hist["volume_unit"] = "share"
        hist["adjustment"] = adjust or "none"
        hist["symbol"] = code
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    keep = [col for col in PRICE_COLUMNS if col in hist.columns]
    frame = hist[keep].copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    return frame


def fetch_daily_prices(
    symbols: list[str],
    start_date: str,
    end_date: str,
    fetch_fn: Callable[[str, str, str], pd.DataFrame] | None = None,
    sleep_seconds: float = 0.2,
    max_workers: int = 1,
    max_retries: int = 3,
    adjust: str = "qfq",
) -> pd.DataFrame:
    """Fetch daily bars; volume is shares, adjust='' preserves traded prices.

    Injected fetch_fn must already return canonical share-volume data.
    """
    if adjust not in {"", "qfq", "hfq"}:
        raise ValueError("adjust must be '', 'qfq' or 'hfq'")
    start = pd.Timestamp(start_date).strftime("%Y%m%d")
    end = pd.Timestamp(end_date).strftime("%Y%m%d")

    def _task(symbol: str) -> pd.DataFrame:
        return fetch_with_retries(
            lambda: _fetch_one_price(symbol, start, end, fetch_fn, sleep_seconds, adjust),
            max_retries=max_retries,
            sleep_seconds=sleep_seconds,
            error_message=f"Failed to fetch prices for {symbol}",
        )

    return fetch_symbols_parallel(
        symbols,
        _task,
        max_workers=max_workers,
        empty_columns=PRICE_COLUMNS,
        sort_columns=["symbol", "date"],
    )
