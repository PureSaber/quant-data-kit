"""Panel merge helpers for alt-data and fundamentals."""

from __future__ import annotations

import pandas as pd


def merge_earnings_to_panel(panel: pd.DataFrame, forecasts: pd.DataFrame) -> pd.DataFrame:
    """Point-in-time merge of forecast_score onto daily panel."""
    if forecasts.empty:
        panel = panel.copy()
        panel["forecast_score"] = pd.NA
        return panel

    fund = forecasts[["symbol", "effective_date", "forecast_score"]].rename(
        columns={"effective_date": "asof_date"}
    )
    parts: list[pd.DataFrame] = []
    price_df = panel.copy()
    price_df["date"] = pd.to_datetime(price_df["date"]).dt.normalize()

    for symbol, price_group in price_df.groupby("symbol", sort=False):
        fund_group = fund[fund["symbol"] == symbol][["asof_date", "forecast_score"]].sort_values(
            "asof_date"
        )
        if fund_group.empty:
            part = price_group.copy()
            part["forecast_score"] = pd.NA
        else:
            part = pd.merge_asof(
                price_group.sort_values("date"),
                fund_group,
                left_on="date",
                right_on="asof_date",
                direction="backward",
            ).drop(columns=["asof_date"], errors="ignore")
        parts.append(part)

    merged = pd.concat(parts, ignore_index=True)
    return merged.sort_values(["date", "symbol"]).reset_index(drop=True)


def merge_northbound_to_panel(panel: pd.DataFrame, northbound: pd.DataFrame) -> pd.DataFrame:
    if northbound.empty:
        panel = panel.copy()
        panel["northbound_hold_ratio"] = pd.NA
        return panel

    merged = panel.merge(
        northbound[["symbol", "date", "northbound_hold_ratio"]],
        on=["symbol", "date"],
        how="left",
    )
    return merged.sort_values(["date", "symbol"]).reset_index(drop=True)


def add_industry_relative_strength(
    panel: pd.DataFrame,
    industry_returns: pd.DataFrame,
    benchmark_returns: pd.Series,
    window: int = 20,
) -> pd.DataFrame:
    """Map industry window-day excess return onto each symbol row."""
    if "industry" not in panel.columns or industry_returns.empty:
        panel = panel.copy()
        panel["industry_rs_20d"] = pd.NA
        return panel

    ind = industry_returns.copy()
    ind = ind.sort_values(["industry", "date"])
    ind["industry_mom"] = ind.groupby("industry")["industry_return"].transform(
        lambda s: (1 + s).rolling(window).apply(lambda x: x.prod() - 1, raw=True)
    )

    bench = benchmark_returns.sort_index()
    bench_mom = (1 + bench).rolling(window).apply(lambda x: x.prod() - 1, raw=True)
    bench_df = bench_mom.reset_index()
    bench_df.columns = ["date", "benchmark_mom"]

    ind = ind.merge(bench_df, on="date", how="left")
    ind["industry_rs_20d"] = ind["industry_mom"] - ind["benchmark_mom"]

    merged = panel.merge(
        ind[["industry", "date", "industry_rs_20d"]].drop_duplicates(),
        on=["industry", "date"],
        how="left",
    )
    return merged.sort_values(["date", "symbol"]).reset_index(drop=True)
