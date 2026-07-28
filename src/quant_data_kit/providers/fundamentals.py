"""Fundamental data fetch via AKShare."""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from quant_data_kit.providers._network import configure_network
from quant_data_kit.providers._symbols import normalize_symbol
from quant_data_kit.storage import parse_date

FUNDAMENTAL_COLUMNS = ["symbol", "date", "report_date", "market_cap", "pe_ratio", "pb_ratio"]
FUNDAMENTAL_RENAME = {
    "数据日期": "date",
    "总市值": "market_cap",
    "PE(TTM)": "pe_ratio",
    "市净率": "pb_ratio",
    "PB": "pb_ratio",
}


def _fetch_one_fundamental(
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    fetch_fn: Callable[[str], pd.DataFrame] | None,
    sleep_seconds: float,
) -> pd.DataFrame:
    if fetch_fn is not None:
        values = fetch_fn(symbol)
    else:
        import akshare as ak

        configure_network()
        values = ak.stock_value_em(symbol=normalize_symbol(symbol))
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    values = values.rename(columns=FUNDAMENTAL_RENAME)
    values["symbol"] = normalize_symbol(symbol)

    keep = [col for col in FUNDAMENTAL_COLUMNS if col in values.columns]
    frame = values[keep].copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    if "report_date" not in frame.columns:
        frame["report_date"] = frame["date"]
    else:
        frame["report_date"] = pd.to_datetime(frame["report_date"]).dt.normalize()
    return frame[(frame["date"] >= start) & (frame["date"] <= end)]


def fetch_fundamentals(
    symbols: list[str],
    start_date: str,
    end_date: str,
    fetch_fn: Callable[[str], pd.DataFrame] | None = None,
    sleep_seconds: float = 0.2,
    max_workers: int = 1,
    max_retries: int = 3,
) -> pd.DataFrame:
    start = parse_date(start_date)
    end = parse_date(end_date)
    frames: list[pd.DataFrame] = []

    def _task(symbol: str) -> pd.DataFrame:
        last_error: Exception | None = None
        for _ in range(max_retries):
            try:
                return _fetch_one_fundamental(symbol, start, end, fetch_fn, sleep_seconds)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                time.sleep(sleep_seconds * 2)
        raise RuntimeError(f"Failed to fetch fundamentals for {symbol}") from last_error

    if max_workers <= 1:
        for symbol in symbols:
            frames.append(_task(symbol))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_task, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                frames.append(future.result())

    if not frames:
        return pd.DataFrame(columns=FUNDAMENTAL_COLUMNS)

    fundamentals = pd.concat(frames, ignore_index=True)
    return fundamentals.sort_values(["symbol", "date"]).reset_index(drop=True)
