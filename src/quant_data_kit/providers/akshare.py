"""AKShare data provider helpers (optional dependency)."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

import pandas as pd

logger = logging.getLogger(__name__)

PRICE_COLUMNS = ["symbol", "date", "open", "high", "low", "close", "volume", "name", "industry"]


def normalize_symbol(symbol: str) -> str:
    return str(symbol).strip().zfill(6)


def to_market_symbol(symbol: str) -> str:
    code = normalize_symbol(symbol)
    if code.startswith(("5", "6", "9")):
        return f"sh{code}"
    return f"sz{code}"


def configure_network() -> None:
    import os

    os.environ.setdefault("NO_PROXY", "*")
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(key, None)


def fetch_hs300_constituents(fetch_fn: Callable[[], pd.DataFrame] | None = None) -> list[str]:
    if fetch_fn is not None:
        df = fetch_fn()
    else:
        import akshare as ak

        configure_network()
        df = ak.index_stock_cons(symbol="000300")

    symbol_col = "品种代码" if "品种代码" in df.columns else df.columns[1]
    return [normalize_symbol(code) for code in df[symbol_col].tolist()]


def fetch_daily_prices(
    symbols: list[str],
    start_date: str,
    end_date: str,
    *,
    sleep_seconds: float = 0.2,
    max_retries: int = 3,
    fetch_one: Callable[[str, str, str], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """Fetch adjusted daily OHLCV for symbols between start_date and end_date."""
    frames: list[pd.DataFrame] = []
    for symbol in symbols:
        frame = _fetch_symbol_with_retry(
            symbol, start_date, end_date, sleep_seconds, max_retries, fetch_one
        )
        if not frame.empty:
            frames.append(frame)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    if not frames:
        return pd.DataFrame(columns=PRICE_COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    return out.sort_values(["symbol", "date"]).reset_index(drop=True)


def _fetch_symbol_with_retry(
    symbol: str,
    start_date: str,
    end_date: str,
    sleep_seconds: float,
    max_retries: int,
    fetch_one: Callable[[str, str, str], pd.DataFrame] | None,
) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            if fetch_one is not None:
                return fetch_one(symbol, start_date, end_date)
            return _fetch_symbol_akshare(symbol, start_date, end_date)
        except Exception as exc:  # noqa: BLE001 — retry wrapper
            last_error = exc
            logger.warning("Fetch %s failed (attempt %s): %s", symbol, attempt + 1, exc)
            time.sleep(sleep_seconds * (attempt + 1))
    if last_error:
        logger.error("Giving up on %s: %s", symbol, last_error)
    return pd.DataFrame(columns=PRICE_COLUMNS)


def _fetch_symbol_akshare(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    import akshare as ak

    configure_network()
    market = to_market_symbol(symbol)
    raw = ak.stock_zh_a_hist(
        symbol=normalize_symbol(symbol),
        period="daily",
        start_date=start_date.replace("-", ""),
        end_date=end_date.replace("-", ""),
        adjust="qfq",
    )
    if raw.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    rename = {
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
    }
    df = raw.rename(columns=rename)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["symbol"] = normalize_symbol(symbol)
    df["name"] = ""
    df["industry"] = ""
    _ = market  # reserved for future market metadata
    return df[[c for c in PRICE_COLUMNS if c in df.columns or c in ("name", "industry")]]
