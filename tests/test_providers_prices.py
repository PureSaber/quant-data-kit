import pandas as pd

from quant_data_kit.providers.prices import fetch_daily_prices


def test_fetch_daily_prices_mock() -> None:
    def mock_fetch(symbol: str, start: str, end: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "symbol": [symbol],
                "date": pd.to_datetime(["2020-01-02"]),
                "open": [1.0],
                "high": [1.1],
                "low": [0.9],
                "close": [1.05],
                "volume": [100],
            }
        )

    prices = fetch_daily_prices(["000001"], "2020-01-01", "2020-12-31", fetch_fn=mock_fetch)
    assert len(prices) == 1
    assert prices.loc[0, "symbol"] == "000001"
