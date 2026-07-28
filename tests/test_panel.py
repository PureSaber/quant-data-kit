import pandas as pd

from quant_data_kit.panel import (
    add_industry_relative_strength,
    merge_earnings_to_panel,
    merge_northbound_to_panel,
)


def test_merge_earnings_to_panel() -> None:
    panel = pd.DataFrame(
        {
            "symbol": ["000001", "000001"],
            "date": pd.to_datetime(["2020-04-15", "2020-04-16"]),
            "close": [10.0, 10.1],
        }
    )
    forecasts = pd.DataFrame(
        {
            "symbol": ["000001"],
            "report_period": ["20200331"],
            "announce_date": pd.to_datetime(["2020-04-15"]),
            "effective_date": pd.to_datetime(["2020-04-16"]),
            "forecast_type": ["预增"],
            "forecast_score": [2],
            "change_pct_low": [50],
            "change_pct_high": [80],
        }
    )
    merged = merge_earnings_to_panel(panel, forecasts)
    assert merged.loc[1, "forecast_score"] == 2


def test_merge_northbound_to_panel() -> None:
    panel = pd.DataFrame(
        {
            "symbol": ["000001"],
            "date": pd.to_datetime(["2020-01-03"]),
            "close": [10.0],
        }
    )
    north = pd.DataFrame(
        {
            "symbol": ["000001"],
            "date": pd.to_datetime(["2020-01-03"]),
            "northbound_hold_ratio": [1.2],
        }
    )
    merged = merge_northbound_to_panel(panel, north)
    assert merged.loc[0, "northbound_hold_ratio"] == 1.2


def test_add_industry_relative_strength() -> None:
    panel = pd.DataFrame(
        {
            "symbol": ["000001"] * 25,
            "industry": ["银行"] * 25,
            "date": pd.date_range("2020-01-01", periods=25, freq="B"),
            "close": [10.0 + i * 0.1 for i in range(25)],
        }
    )
    industry_returns = pd.DataFrame(
        {
            "industry": ["银行"] * 25,
            "date": pd.date_range("2020-01-01", periods=25, freq="B"),
            "industry_return": [0.01] * 25,
        }
    )
    benchmark = pd.Series([0.005] * 25, index=panel["date"])
    merged = add_industry_relative_strength(panel, industry_returns, benchmark, window=5)
    assert "industry_rs_20d" in merged.columns
