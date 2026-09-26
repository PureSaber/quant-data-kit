from pathlib import Path

import pandas as pd
import pytest

from quant_data_kit.hong_kong import hk_symbol, hk_yahoo_symbol, normalize_hk_bars


@pytest.mark.parametrize("raw", ["700", "00700", "HK.00700", "0700.HK"])
def test_explicit_hk_symbol_preserves_market(raw):
    assert hk_symbol(raw) == "00700"
    assert hk_yahoo_symbol(raw) == "0700.HK"


@pytest.mark.parametrize("raw", ["600000", "00700.SZ", "00000", "AAPL", 700])
def test_reject_foreign_or_ambiguous_symbols(raw):
    with pytest.raises((ValueError, TypeError)):
        hk_symbol(raw)


def bar():
    return pd.DataFrame(
        [
            {
                "date": "2025-07-02",
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 1000,
                "amount": 10500,
            }
        ]
    )


def test_hk_turnover_is_already_shares_and_hkd():
    out = normalize_hk_bars(bar(), "00700", "test")
    assert out.volume.iloc[0] == 1000
    assert out.amount.iloc[0] == 10500
    assert out.symbol.iloc[0] == "00700"
    assert out.currency.iloc[0] == "HKD"


@pytest.mark.parametrize("column,value", [("high", 2), ("volume", -1), ("close", float("nan"))])
def test_invalid_prices_fail(column, value):
    frame = bar()
    frame[column] = value
    with pytest.raises(ValueError):
        normalize_hk_bars(frame, "700", "test")


def test_duplicates_fail():
    with pytest.raises(ValueError, match="unique"):
        normalize_hk_bars(pd.concat([bar(), bar()]), "700", "test")


def test_snapshot_refuses_partial_and_modified_input(tmp_path: Path):
    import json

    from quant_data_kit.hong_kong import load_hk_snapshot

    (tmp_path / "manifest.json").write_text(json.dumps({"status": "failed"}))
    with pytest.raises(ValueError, match="complete"):
        load_hk_snapshot(tmp_path)


def test_capture_preserves_failed_source_and_requires_explicit_provider(tmp_path, monkeypatch):
    import json
    import sys
    from types import SimpleNamespace

    from quant_data_kit.hong_kong import capture_hk_snapshot, fetch_hk_bars

    bad = bar()
    bad["close"] = 8
    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(stock_hk_daily=lambda **kw: bad))
    monkeypatch.setattr("importlib.metadata.version", lambda package: "fixture")
    root = tmp_path / "failure"
    with pytest.raises(ValueError, match="low"):
        capture_hk_snapshot(root, ["00700"], "2025-07-01", "2025-07-03", provider="akshare_sina_hk")
    evidence = json.loads((root / "manifest.json").read_text())
    assert evidence["status"] == "failed"
    assert "raw-00700.csv" in evidence["files"]
    with pytest.raises(FileExistsError):
        capture_hk_snapshot(root, ["00700"], "2025-07-01", "2025-07-03", provider="akshare_sina_hk")
    with pytest.raises(ValueError, match="explicit"):
        fetch_hk_bars("00700", "2025-07-01", "2025-07-03", provider="auto")


def test_capture_roundtrip_and_hashes(tmp_path, monkeypatch):
    import sys
    from types import SimpleNamespace

    from quant_data_kit.hong_kong import capture_hk_snapshot, load_hk_snapshot

    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(stock_hk_daily=lambda **kw: bar()))
    monkeypatch.setattr("importlib.metadata.version", lambda package: "fixture")
    days = pd.to_datetime(["2025-07-02"])
    calendar = pd.DataFrame(
        {
            "date": days,
            "open": days.tz_localize("UTC") + pd.Timedelta(hours=1),
            "close": days.tz_localize("UTC") + pd.Timedelta(hours=8),
        }
    )
    monkeypatch.setattr("quant_data_kit.hong_kong.hk_calendar", lambda start, end: calendar)
    root = tmp_path / "complete"
    capture_hk_snapshot(root, ["00700"], "2025-07-01", "2025-07-03", provider="akshare_sina_hk")
    loaded, _, metadata = load_hk_snapshot(root)
    assert len(loaded) == 1
    assert metadata["corporate_actions_complete"] is False
    (root / "raw-00700.csv").write_text("changed")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_hk_snapshot(root)


def test_eastmoney_hk_explicit_endpoint_and_units(monkeypatch):
    import sys
    from types import SimpleNamespace

    from quant_data_kit.hong_kong import fetch_hk_bars

    source = bar().rename(
        columns={
            "date": "日期",
            "open": "开盘",
            "high": "最高",
            "low": "最低",
            "close": "收盘",
            "volume": "成交量",
            "amount": "成交额",
        }
    )
    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(stock_hk_hist=lambda **kw: source))
    out, _ = fetch_hk_bars("700", "2025-07-01", "2025-07-03", provider="akshare_eastmoney_hk")
    assert out.volume.iloc[0] == 1000
    assert out.amount_unit.iloc[0] == "HKD"
