"""Daily OHLCV fetch via AKShare."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

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
]


def _fetch_one_price(
    symbol: str,
    start: str,
    end: str,
    fetch_fn: Callable[[str, str, str], pd.DataFrame] | None,
    sleep_seconds: float,
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
                adjust="qfq",
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
        except Exception:  # noqa: BLE001
            logger.debug("Eastmoney price API failed for %s; using stock_zh_a_hist_tx", code)
            hist = ak.stock_zh_a_hist_tx(
                symbol=to_market_symbol(code),
                start_date=start,
                end_date=end,
                adjust="qfq",
            )
            if "amount" in hist.columns and "volume" not in hist.columns:
                hist = hist.rename(columns={"amount": "volume"})
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
) -> pd.DataFrame:
    start = pd.Timestamp(start_date).strftime("%Y%m%d")
    end = pd.Timestamp(end_date).strftime("%Y%m%d")
    frames: list[pd.DataFrame] = []

    def _task(symbol: str) -> pd.DataFrame:
        last_error: Exception | None = None
        for _ in range(max_retries):
            try:
                return _fetch_one_price(symbol, start, end, fetch_fn, sleep_seconds)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                time.sleep(sleep_seconds * 2)
        raise RuntimeError(f"Failed to fetch prices for {symbol}") from last_error

    if max_workers <= 1:
        for symbol in symbols:
            frames.append(_task(symbol))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_task, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                frames.append(future.result())

    if not frames:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    prices = pd.concat(frames, ignore_index=True)
    return prices.sort_values(["symbol", "date"]).reset_index(drop=True)
