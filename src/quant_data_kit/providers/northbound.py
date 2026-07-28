"""Northbound (Stock Connect) holdings with disclosure lag."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from quant_data_kit.providers._network import configure_network
from quant_data_kit.providers._symbols import normalize_symbol

logger = logging.getLogger(__name__)

NORTHBOUND_COLUMNS = ["symbol", "date", "northbound_hold_ratio"]


def _fetch_one_northbound(
    symbol: str,
    fetch_fn: Callable[[str], pd.DataFrame] | None,
    sleep_seconds: float,
) -> pd.DataFrame:
    if fetch_fn is not None:
        hist = fetch_fn(symbol)
    else:
        import akshare as ak

        configure_network()
        code = normalize_symbol(symbol)
        try:
            hist = ak.stock_hsgt_individual_em(symbol=code)
        except Exception:  # noqa: BLE001
            logger.debug("stock_hsgt_individual_em failed for %s", code)
            return pd.DataFrame(columns=NORTHBOUND_COLUMNS)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        date_col = next((c for c in hist.columns if "日期" in c or c.lower() == "date"), None)
        ratio_col = next(
            (
                c
                for c in hist.columns
                if "持股数量占A股百分比" in c or "占流通A股" in c or "持股比例" in c
            ),
            None,
        )
        if date_col is None or ratio_col is None:
            return pd.DataFrame(columns=NORTHBOUND_COLUMNS)

        frame = pd.DataFrame(
            {
                "symbol": code,
                "date": pd.to_datetime(hist[date_col]).dt.normalize(),
                "northbound_hold_ratio": pd.to_numeric(hist[ratio_col], errors="coerce"),
            }
        )
        hist = frame

    hist = hist.copy()
    hist["symbol"] = normalize_symbol(symbol)
    hist["date"] = pd.to_datetime(hist["date"]).dt.normalize()
    # Disclosure lag: data published T reflects T-1 holdings.
    hist = hist.sort_values("date")
    hist["northbound_hold_ratio"] = hist["northbound_hold_ratio"].shift(1)
    return hist.dropna(subset=["northbound_hold_ratio"])


def fetch_northbound_holdings(
    symbols: list[str],
    fetch_fn: Callable[[str], pd.DataFrame] | None = None,
    sleep_seconds: float = 0.2,
    max_workers: int = 1,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    def _task(symbol: str) -> pd.DataFrame:
        try:
            return _fetch_one_northbound(symbol, fetch_fn, sleep_seconds)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Northbound fetch failed for %s: %s", symbol, exc)
            return pd.DataFrame(columns=NORTHBOUND_COLUMNS)

    if max_workers <= 1:
        for symbol in symbols:
            frames.append(_task(symbol))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_task, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                frames.append(future.result())

    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=NORTHBOUND_COLUMNS)

    result = pd.concat(frames, ignore_index=True)
    return result.sort_values(["symbol", "date"]).reset_index(drop=True)
