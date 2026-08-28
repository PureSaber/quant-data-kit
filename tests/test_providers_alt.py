import pandas as pd

from quant_data_kit.providers.earnings_forecast import fetch_earnings_forecasts
from quant_data_kit.providers.industry import fetch_industry_returns
from quant_data_kit.providers.northbound import fetch_northbound_holdings


def test_fetch_earnings_forecast_mock() -> None:
    def mock_fetch(period: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "股票代码": ["000001"],
                "公告日期": ["2020-04-15"],
                "预告类型": ["预增"],
                "净利润变动幅度下限": [50],
                "净利润变动幅度上限": [80],
            }
        )

    df = fetch_earnings_forecasts("2020-01-01", "2020-12-31", fetch_fn=mock_fetch, sleep_seconds=0)
    assert len(df) >= 1
    assert df.loc[0, "forecast_score"] == 2


def test_fetch_northbound_mock() -> None:
    def mock_fetch(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "symbol": [symbol, symbol, symbol],
                "date": pd.to_datetime(["2020-01-02", "2020-01-03", "2020-01-06"]),
                "northbound_hold_ratio": [1.0, 1.2, 1.5],
            }
        )

    df = fetch_northbound_holdings(["000001"], fetch_fn=mock_fetch, sleep_seconds=0)
    assert len(df) == 2
    assert df.iloc[-1]["northbound_hold_ratio"] == 1.2


def test_fetch_industry_returns_mock() -> None:
    def mock_fetch(industry: str, start: str, end: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-02", "2020-01-03"]),
                "close": [100.0, 102.0],
            }
        )

    df = fetch_industry_returns(
        ["银行"], "2020-01-01", "2020-01-31", fetch_fn=mock_fetch, sleep_seconds=0
    )
    assert len(df) == 1
    assert df.iloc[0]["industry"] == "银行"
