"""HS300 universe and membership history."""

from __future__ import annotations

import logging
from collections.abc import Callable

import pandas as pd

from quant_data_kit.providers._network import configure_network
from quant_data_kit.providers._symbols import normalize_symbol
from quant_data_kit.storage import parse_date

logger = logging.getLogger(__name__)

UNIVERSE_COLUMNS = ["symbol", "date", "in_universe"]


def fetch_hs300_constituents(fetch_fn: Callable[[], pd.DataFrame] | None = None) -> list[str]:
    if fetch_fn is not None:
        df = fetch_fn()
    else:
        import akshare as ak

        configure_network()
        df = ak.index_stock_cons(symbol="000300")

    symbol_col = "品种代码" if "品种代码" in df.columns else df.columns[1]
    return [normalize_symbol(code) for code in df[symbol_col].tolist()]


def fetch_hs300_constituents_history(
    start_date: str,
    end_date: str,
    fetch_fn: Callable[[], pd.DataFrame] | None = None,
    current_symbols: list[str] | None = None,
) -> pd.DataFrame:
    if fetch_fn is not None:
        adjustments = fetch_fn()
    else:
        import akshare as ak

        configure_network()
        adjustments = ak.index_detail_hist_adjust_cni(symbol="000300")

    rename_map = {
        "日期": "date",
        "成分券代码": "symbol",
        "操作": "action",
    }
    events = adjustments.rename(columns={k: v for k, v in rename_map.items() if k in adjustments.columns})
    if "date" not in events.columns:
        for col in events.columns:
            if "日期" in str(col) or col.lower() == "date":
                events = events.rename(columns={col: "date"})
                break
    if "symbol" not in events.columns:
        for col in events.columns:
            if "代码" in str(col) or col.lower() == "symbol":
                events = events.rename(columns={col: "symbol"})
                break
    if "action" not in events.columns:
        for col in events.columns:
            if "操作" in str(col) or col.lower() == "action":
                events = events.rename(columns={col: "action"})
                break

    if events.empty or "date" not in events.columns:
        logger.warning("HS300 adjustment history unavailable; using current constituents only")
        current = current_symbols or fetch_hs300_constituents()
        all_dates = pd.date_range(parse_date(start_date), parse_date(end_date), freq="B")
        rows = [
            {"symbol": symbol, "date": date, "in_universe": 1}
            for date in all_dates
            for symbol in current
        ]
        return pd.DataFrame(rows)
    events["date"] = pd.to_datetime(events["date"]).dt.normalize()
    events["symbol"] = events["symbol"].map(normalize_symbol)
    events = events.sort_values("date")

    start = parse_date(start_date)
    end = parse_date(end_date)
    all_dates = pd.date_range(start, end, freq="B")

    active = set(current_symbols or fetch_hs300_constituents())
    for _, row in events.sort_values("date", ascending=False).iterrows():
        event_date = row["date"]
        if event_date > end or event_date <= start:
            continue
        symbol = row["symbol"]
        action = str(row["action"])
        if "纳入" in action or "进入" in action:
            active.discard(symbol)
        elif "剔除" in action or "退出" in action:
            active.add(symbol)

    rows: list[dict[str, object]] = []
    event_idx = 0
    event_list = events.reset_index(drop=True)

    for date in all_dates:
        while event_idx < len(event_list) and event_list.loc[event_idx, "date"] <= date:
            symbol = event_list.loc[event_idx, "symbol"]
            action = str(event_list.loc[event_idx, "action"])
            if event_list.loc[event_idx, "date"] > start:
                if "纳入" in action or "进入" in action:
                    active.add(symbol)
                elif "剔除" in action or "退出" in action:
                    active.discard(symbol)
            event_idx += 1
        for symbol in active:
            rows.append({"symbol": symbol, "date": date, "in_universe": 1})

    if not rows:
        for symbol in active:
            for date in all_dates:
                rows.append({"symbol": symbol, "date": date, "in_universe": 1})

    return pd.DataFrame(rows)
