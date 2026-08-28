"""Benchmark index returns."""

from __future__ import annotations

import logging
from collections.abc import Callable

import pandas as pd

from quant_data_kit.providers._network import configure_network
from quant_data_kit.storage import parse_date

logger = logging.getLogger(__name__)

BENCHMARK_COLUMNS = ["date", "benchmark_return"]


def fetch_hs300_benchmark(
    start_date: str,
    end_date: str,
    fetch_fn: Callable[[str, str], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    start = parse_date(start_date).strftime("%Y%m%d")
    end = parse_date(end_date).strftime("%Y%m%d")

    if fetch_fn is not None:
        hist = fetch_fn(start, end)
    else:
        import akshare as ak

        configure_network()
        try:
            hist = ak.stock_zh_index_daily_em(symbol="sh000300")
        except Exception:  # noqa: BLE001
            logger.warning("Eastmoney index API failed; falling back to stock_zh_index_daily")
            hist = ak.stock_zh_index_daily(symbol="sh000300")
        hist["date"] = pd.to_datetime(hist["date"]).dt.normalize()

    hist = hist.sort_values("date")
    hist["benchmark_return"] = hist["close"].pct_change()
    hist = hist[(hist["date"] >= parse_date(start_date)) & (hist["date"] <= parse_date(end_date))]
    return hist[["date", "benchmark_return"]].dropna().reset_index(drop=True)
