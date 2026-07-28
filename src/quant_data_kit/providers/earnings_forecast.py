"""Earnings forecast fetch with point-in-time effective dates."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

import pandas as pd

from quant_data_kit.providers._network import configure_network
from quant_data_kit.providers._symbols import normalize_symbol
from quant_data_kit.storage import parse_date

logger = logging.getLogger(__name__)

FORECAST_TYPE_SCORES: dict[str, int] = {
    "预增": 2,
    "略增": 1,
    "续盈": 0,
    "扭亏": 1,
    "略减": -1,
    "预减": -2,
    "首亏": -3,
    "续亏": -3,
    "不确定": 0,
}

EARNINGS_COLUMNS = [
    "symbol",
    "report_period",
    "announce_date",
    "effective_date",
    "forecast_type",
    "forecast_score",
    "change_pct_low",
    "change_pct_high",
]


def _score_forecast_type(value: object) -> int:
    text = str(value).strip()
    for key, score in FORECAST_TYPE_SCORES.items():
        if key in text:
            return score
    return 0


def _quarter_end_dates(start_date: str, end_date: str) -> list[str]:
    start = parse_date(start_date)
    end = parse_date(end_date)
    dates: list[str] = []
    for year in range(start.year, end.year + 1):
        for month_day in ("0331", "0630", "0930", "1231"):
            dt = pd.Timestamp(f"{year}{month_day}")
            if start <= dt <= end:
                dates.append(f"{year}{month_day}")
    return dates


def fetch_earnings_forecasts(
    start_date: str,
    end_date: str,
    fetch_fn: Callable[[str], pd.DataFrame] | None = None,
    sleep_seconds: float = 0.2,
) -> pd.DataFrame:
    """Fetch quarterly earnings forecasts; effective_date is next business day after announce."""
    frames: list[pd.DataFrame] = []
    for period in _quarter_end_dates(start_date, end_date):
        if fetch_fn is not None:
            raw = fetch_fn(period)
        else:
            import akshare as ak

            configure_network()
            try:
                raw = ak.stock_yjyg_em(date=period)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed earnings forecast for %s: %s", period, exc)
                continue
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        if raw is None or raw.empty:
            continue

        frame = raw.copy()
        symbol_col = "股票代码" if "股票代码" in frame.columns else frame.columns[0]
        announce_col = next(
            (c for c in frame.columns if "公告" in c or c in {"公告日期", "公告时间"}),
            None,
        )
        type_col = next((c for c in frame.columns if "预告" in c or "类型" in c), None)
        low_col = next((c for c in frame.columns if "下限" in c or "最低" in c), None)
        high_col = next((c for c in frame.columns if "上限" in c or "最高" in c), None)

        if announce_col is None or type_col is None:
            logger.warning("Unexpected earnings forecast columns for %s", period)
            continue

        out = pd.DataFrame(
            {
                "symbol": frame[symbol_col].map(normalize_symbol),
                "report_period": period,
                "announce_date": pd.to_datetime(frame[announce_col]).dt.normalize(),
                "forecast_type": frame[type_col].astype(str),
                "change_pct_low": frame[low_col] if low_col else pd.NA,
                "change_pct_high": frame[high_col] if high_col else pd.NA,
            }
        )
        out["forecast_score"] = out["forecast_type"].map(_score_forecast_type)
        out["effective_date"] = out["announce_date"] + pd.offsets.BDay(1)
        frames.append(out)

    if not frames:
        return pd.DataFrame(columns=EARNINGS_COLUMNS)

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(["symbol", "effective_date"]).reset_index(drop=True)
    return result[EARNINGS_COLUMNS]
