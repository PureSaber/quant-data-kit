import sys
from types import SimpleNamespace

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


def test_live_primary_and_fallback_use_same_share_units(monkeypatch):
    east = pd.DataFrame(
        {
            "日期": ["2025-01-02"],
            "开盘": [10],
            "最高": [11],
            "最低": [9],
            "收盘": [10],
            "成交量": [10],
            "成交额": [10000],
        }
    )
    tencent = pd.DataFrame(
        {
            "date": ["2025-01-02"],
            "open": [10],
            "high": [11],
            "low": [9],
            "close": [10],
            "volume": [1000],
        }
    )
    requested = []

    def primary(**kw):
        requested.append(kw["adjust"])
        return east.copy()

    ak = SimpleNamespace(stock_zh_a_hist=primary, stock_zh_a_hist_tx=lambda **kw: tencent.copy())
    monkeypatch.setitem(sys.modules, "akshare", ak)
    a = fetch_daily_prices(["000001"], "2025-01-01", "2025-01-03", adjust="", sleep_seconds=0)

    def failed(**kw):
        raise ConnectionError("primary unavailable")

    ak.stock_zh_a_hist = failed
    b = fetch_daily_prices(["000001"], "2025-01-01", "2025-01-03", adjust="", sleep_seconds=0)
    assert a.volume.iloc[0] == b.volume.iloc[0] == 1000
    assert a.amount.iloc[0] == 10000
    assert a.volume_unit.iloc[0] == b.volume_unit.iloc[0] == "share"
    assert a.adjustment.iloc[0] == b.adjustment.iloc[0] == "none"
    assert requested == [""]


def test_shenzhen_lot_bug_is_verified_against_raw_prices_even_for_old_adjusted_data(monkeypatch):
    calls = []

    def tencent(**kw):
        calls.append(kw["adjust"])
        price = 10 if kw["adjust"] == "" else 5
        return pd.DataFrame(
            {
                "date": ["2020-01-02"],
                "open": [price],
                "high": [price],
                "low": [price],
                "close": [price],
                "volume": [10],
                "amount": [10000],
            }
        )

    def primary(**kw):
        raise ConnectionError("unavailable")

    monkeypatch.setitem(
        sys.modules, "akshare", SimpleNamespace(stock_zh_a_hist=primary, stock_zh_a_hist_tx=tencent)
    )
    result = fetch_daily_prices(
        ["000001"], "2020-01-01", "2020-01-03", adjust="qfq", sleep_seconds=0
    )
    assert result.volume.iloc[0] == 1000
    assert calls == ["qfq", ""]
    assert result.source_volume_unit.iloc[0] == "lot_100_shares"
