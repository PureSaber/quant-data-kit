"""Canonical daily OHLCV retrieval through an explicit provider."""

from __future__ import annotations

import time
from collections.abc import Callable

import pandas as pd

from quant_data_kit.providers._fetch import fetch_symbols_parallel, fetch_with_retries
from quant_data_kit.providers._symbols import normalize_symbol
from quant_data_kit.providers.equity_prices._common import (
    PRICE_COLUMNS,
    adjustment_name,
    canonicalize_prices,
)
from quant_data_kit.providers.provider_registry import load_price_fetcher
from quant_data_kit.validate import validate_price_frame


def _injected_frame(
    frame: pd.DataFrame, symbol: str, adjust: str, provider: str = "injected"
) -> pd.DataFrame:
    frame = frame.copy()
    defaults = {
        "source": provider,
        "provider": provider,
        "volume_unit": "share",
        "source_volume_unit": "share",
        "amount_unit": "CNY",
        "source_amount_unit": "CNY",
        "adjustment": adjustment_name(adjust),
        "symbol": normalize_symbol(symbol),
        "amount": float("nan"),
        "name": None,
        "industry": None,
    }
    for column, value in defaults.items():
        if column not in frame:
            frame[column] = value
    return canonicalize_prices(
        frame,
        symbol=symbol,
        provider=str(frame["provider"].iloc[0]),
        source=str(frame["source"].iloc[0]),
        adjust=adjust,
        source_volume_unit=str(frame["source_volume_unit"].iloc[0]),
        source_amount_unit=str(frame["source_amount_unit"].iloc[0]),
    )


def fetch_daily_prices(
    symbols: list[str],
    start_date: str,
    end_date: str,
    fetch_fn: Callable[[str, str, str], pd.DataFrame] | None = None,
    sleep_seconds: float = 0.2,
    max_workers: int = 1,
    max_retries: int = 3,
    adjust: str = "qfq",
    provider: str = "akshare_auto",
) -> pd.DataFrame:
    """Fetch normalized daily bars from one explicitly selected provider.

    ``akshare_auto`` retains the historical Eastmoney-to-Tencent compatibility
    fallback. Frozen research bundles reject that provider and require one source.
    Injected ``fetch_fn`` values must already use share-volume units.
    """
    adjustment_name(adjust)
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if start > end:
        raise ValueError("start_date must not be after end_date")
    if max_retries < 1 or max_workers < 1 or sleep_seconds < 0:
        raise ValueError("retry, worker and sleep settings must be positive")

    if fetch_fn is None:
        spec, fetcher = load_price_fetcher(provider, adjust)
        if max_workers > spec.max_workers:
            raise ValueError(
                f"Provider {spec.name} permits at most {spec.max_workers} workers; got {max_workers}"
            )
    else:

        def fetcher(symbol: str, fetch_start: str, fetch_end: str, selected: str):
            frame = fetch_fn(
                symbol,
                pd.Timestamp(fetch_start).strftime("%Y%m%d"),
                pd.Timestamp(fetch_end).strftime("%Y%m%d"),
            )
            return _injected_frame(frame, symbol, selected)

    def task(symbol: str) -> pd.DataFrame:
        normalized = normalize_symbol(symbol)
        result = fetch_with_retries(
            lambda: fetcher(normalized, str(start.date()), str(end.date()), adjust),
            max_retries=max_retries,
            sleep_seconds=sleep_seconds,
            error_message=f"Failed to fetch {provider} prices for {normalized}",
        )
        if sleep_seconds:
            time.sleep(sleep_seconds)
        return result

    combined = fetch_symbols_parallel(
        [normalize_symbol(symbol) for symbol in symbols],
        task,
        max_workers=max_workers,
        empty_columns=PRICE_COLUMNS,
        sort_columns=["symbol", "date"],
    )
    if not combined.empty:
        validate_price_frame(combined)
        expected = {normalize_symbol(symbol) for symbol in symbols}
        if set(combined["symbol"]) != expected:
            raise ValueError("Provider response does not cover every requested symbol")
        if not combined["volume_unit"].eq("share").all():
            raise ValueError("Canonical price volume must be expressed in shares")
    return combined
