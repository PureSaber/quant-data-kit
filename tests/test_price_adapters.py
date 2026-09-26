import sys
from types import SimpleNamespace

import pandas as pd

from quant_data_kit.providers.equity_prices.akshare import fetch_sina_etf
from quant_data_kit.providers.equity_prices.alphavantage import fetch_prices as alpha_prices
from quant_data_kit.providers.equity_prices.baostock import fetch_prices as baostock_prices
from quant_data_kit.providers.equity_prices.tushare import fetch_prices as tushare_prices
from quant_data_kit.providers.equity_prices.yahoo import fetch_prices as yahoo_prices


def test_baostock_normalizes_symbol_units_and_adjustment(monkeypatch):
    class Result:
        error_code = "0"

        def __init__(self):
            self.done = False
            self.fields = [
                "date",
                "code",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
            ]

        def next(self):
            if self.done:
                return False
            self.done = True
            return True

        def get_row_data(self):
            return ["2026-01-02", "sh.600000", "10", "11", "9", "10.5", "200", "2100"]

    query = []
    fake = SimpleNamespace(
        login=lambda: SimpleNamespace(error_code="0"),
        logout=lambda: None,
        query_history_k_data_plus=lambda *args, **kwargs: query.append((args, kwargs)) or Result(),
    )
    monkeypatch.setitem(sys.modules, "baostock", fake)
    frame = baostock_prices("600000", "2026-01-01", "2026-01-03", "qfq")
    assert query[0][0][0] == "sh.600000"
    assert query[0][1]["adjustflag"] == "2"
    assert frame.loc[0, "volume"] == 200
    assert frame.loc[0, "provider"] == "baostock"
    assert frame.loc[0, "adjustment"] == "qfq"


def test_tushare_converts_lots_and_thousand_yuan(monkeypatch):
    raw = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20260102"],
            "open": [10],
            "high": [11],
            "low": [9],
            "close": [10.5],
            "vol": [2],
            "amount": [3],
        }
    )
    client = SimpleNamespace(daily=lambda **kwargs: raw.copy())
    fake = SimpleNamespace(pro_api=lambda token: client, pro_bar=lambda **kwargs: raw.copy())
    monkeypatch.setitem(sys.modules, "tushare", fake)
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-not-logged")
    frame = tushare_prices("000001", "2026-01-01", "2026-01-03", "")
    assert frame.loc[0, "volume"] == 200
    assert frame.loc[0, "amount"] == 3000
    assert frame.loc[0, "source_volume_unit"] == "lot_100_shares"


def test_yahoo_uses_exchange_symbol_and_normalizes_columns(monkeypatch):
    calls = []
    history = pd.DataFrame(
        {"Open": [10], "High": [11], "Low": [9], "Close": [10.5], "Volume": [200]},
        index=pd.DatetimeIndex(["2026-01-02"], name="Date"),
    )
    fake = SimpleNamespace(
        download=lambda ticker, **kwargs: calls.append((ticker, kwargs)) or history
    )
    monkeypatch.setitem(sys.modules, "yfinance", fake)
    frame = yahoo_prices("600000", "2026-01-01", "2026-01-03", "qfq")
    assert calls[0][0] == "600000.SS"
    assert calls[0][1]["auto_adjust"] is True
    assert frame.loc[0, "source"] == "yfinance:yahoo"


def test_alpha_vantage_filters_dates_and_scales_adjusted_ohlc(monkeypatch):
    payload = {
        "Time Series (Daily)": {
            "2026-01-02": {
                "1. open": "10",
                "2. high": "11",
                "3. low": "9",
                "4. close": "10",
                "5. adjusted close": "5",
                "6. volume": "200",
            },
            "2025-12-31": {
                "1. open": "8",
                "2. high": "9",
                "3. low": "7",
                "4. close": "8",
                "5. adjusted close": "4",
                "6. volume": "100",
            },
        }
    }

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return payload

    calls = []
    fake = SimpleNamespace(get=lambda *args, **kwargs: calls.append((args, kwargs)) or Response())
    monkeypatch.setitem(sys.modules, "requests", fake)
    monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "secret-not-logged")
    frame = alpha_prices("600000", "2026-01-01", "2026-01-03", "qfq")
    assert calls[0][1]["params"]["symbol"] == "600000.SHH"
    assert frame.loc[0, "open"] == 5
    assert frame.loc[0, "close"] == 5
    assert len(frame) == 1


def test_sina_etf_qfq_is_derived_from_separate_real_cash_series(monkeypatch):
    history = pd.DataFrame(
        {
            "date": pd.DatetimeIndex(["2026-01-15", "2026-01-16", "2026-01-19"]),
            "open": [10.0, 10.0, 9.0],
            "high": [10.0, 10.0, 9.0],
            "low": [10.0, 10.0, 9.0],
            "close": [10.0, 10.0, 9.0],
            "volume": [100, 100, 100],
            "amount": [1000, 1000, 900],
        }
    )
    dividends = pd.DataFrame(
        {"日期": pd.DatetimeIndex(["2026-01-19"]), "累计分红": [1.0]}
    )
    fake = SimpleNamespace(
        fund_etf_hist_sina=lambda **kwargs: history.copy(),
        fund_etf_dividend_sina=lambda **kwargs: dividends.copy(),
    )
    monkeypatch.setitem(sys.modules, "akshare", fake)
    adjusted = fetch_sina_etf("510300", "2026-01-15", "2026-01-19", "qfq")
    assert adjusted.close.tolist() == [9.0, 9.0, 9.0]
    assert adjusted.volume.tolist() == [100, 100, 100]
    assert adjusted.source_volume_unit.eq("share").all()
    assert adjusted.source.str.contains("fund_etf_dividend_sina").all()
