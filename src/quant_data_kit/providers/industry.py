"""Industry index returns and symbol mapping."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

import pandas as pd

from quant_data_kit.providers._network import configure_network
from quant_data_kit.storage import parse_date

logger = logging.getLogger(__name__)

INDUSTRY_RETURN_COLUMNS = ["industry", "date", "industry_return"]


def fetch_industry_returns(
    industries: list[str],
    start_date: str,
    end_date: str,
    fetch_fn: Callable[[str, str, str], pd.DataFrame] | None = None,
    sleep_seconds: float = 0.2,
) -> pd.DataFrame:
    """Fetch daily returns for Eastmoney industry boards."""
    start = parse_date(start_date).strftime("%Y%m%d")
    end = parse_date(end_date).strftime("%Y%m%d")
    frames: list[pd.DataFrame] = []

    for industry in industries:
        if fetch_fn is not None:
            hist = fetch_fn(industry, start, end)
        else:
            import akshare as ak

            configure_network()
            try:
                hist = ak.stock_board_industry_hist_em(
                    symbol=industry,
                    period="daily",
                    start_date=start,
                    end_date=end,
                    adjust="",
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Industry fetch failed for %s: %s", industry, exc)
                continue
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

            date_col = "日期" if "日期" in hist.columns else "date"
            close_col = "收盘" if "收盘" in hist.columns else "close"
            hist = hist.rename(columns={date_col: "date", close_col: "close"})

        frame = hist.copy()
        frame["industry"] = industry
        frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
        frame["industry_return"] = frame["close"].pct_change()
        frames.append(frame[["industry", "date", "industry_return"]])

    if not frames:
        return pd.DataFrame(columns=INDUSTRY_RETURN_COLUMNS)

    result = pd.concat(frames, ignore_index=True)
    return result.dropna(subset=["industry_return"]).sort_values(["industry", "date"])
