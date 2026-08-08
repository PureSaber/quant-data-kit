"""Shared fetch/retry helpers for AKShare providers."""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd


def fetch_with_retries(
    fetch_once: Callable[[], pd.DataFrame],
    *,
    max_retries: int,
    sleep_seconds: float,
    error_message: str,
) -> pd.DataFrame:
    last_error: Exception | None = None
    for _ in range(max_retries):
        try:
            return fetch_once()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(sleep_seconds * 2)
    raise RuntimeError(error_message) from last_error


def fetch_symbols_parallel(
    symbols: list[str],
    task_fn: Callable[[str], pd.DataFrame],
    *,
    max_workers: int,
    empty_columns: list[str],
    sort_columns: list[str],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if max_workers <= 1:
        for symbol in symbols:
            frames.append(task_fn(symbol))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(task_fn, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                frames.append(future.result())

    if not frames:
        return pd.DataFrame(columns=empty_columns)

    combined = pd.concat(frames, ignore_index=True)
    return combined.sort_values(sort_columns).reset_index(drop=True)
