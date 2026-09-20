import hashlib
import json

import pandas as pd
import pytest
import yaml

from quant_data_kit import research_inputs as inputs


def _prices(symbols, start, end, *, adjust, provider, **kwargs):
    rows = []
    for symbol in symbols:
        for date in pd.date_range("2026-01-02", "2026-01-05", freq="B"):
            rows.append(
                {
                    "symbol": symbol,
                    "date": date,
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5 if provider == "akshare_eastmoney" else 10.6,
                    "volume": 100.0,
                    "amount": 1050.0,
                    "name": None,
                    "industry": None,
                    "provider": provider,
                    "source": provider,
                    "volume_unit": "share",
                    "source_volume_unit": "share",
                    "amount_unit": "CNY",
                    "source_amount_unit": "CNY",
                    "adjustment": adjust or "none",
                }
            )
    return pd.DataFrame(rows)


def _policy():
    return {
        "schema_version": inputs.SCHEMA_VERSION,
        "prices": {
            "primary": "akshare_eastmoney",
            "shadows": ["yahoo"],
            "max_workers": 1,
            "max_retries": 1,
            "sleep_seconds": 0.0,
        },
        "benchmark": {"provider": "akshare_eastmoney"},
        "calendar": {"provider": "akshare_sina"},
        "corporate_actions": {"provider": "cninfo", "required": True},
        "trading_status": {"provider": "akshare_eastmoney", "required": False},
    }


def test_policy_forbids_implicit_fallback(tmp_path):
    path = tmp_path / "policy.yaml"
    payload = _policy()
    payload["prices"]["primary"] = "akshare_auto"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="fallback providers are forbidden"):
        inputs.load_policy(path)


def test_bundle_is_atomic_hashed_and_keeps_shadows_separate(tmp_path, monkeypatch):
    monkeypatch.setattr(inputs, "fetch_daily_prices", _prices)
    monkeypatch.setattr(
        inputs,
        "fetch_hs300_benchmark",
        lambda *args, **kwargs: pd.DataFrame(
            {"date": pd.date_range("2026-01-02", "2026-01-05", freq="B"), "benchmark_return": [0.0, 0.01]}
        ),
    )
    monkeypatch.setattr(
        inputs,
        "load_sse_trade_dates",
        lambda **kwargs: pd.DatetimeIndex(["2026-01-02", "2026-01-05", "2026-01-06"]),
    )
    monkeypatch.setattr(inputs, "fetch_corporate_actions", lambda *args: pd.DataFrame())
    monkeypatch.setattr(
        inputs,
        "fetch_tradability",
        lambda symbols, session, captured_at: pd.DataFrame(
            {
                "symbol": symbols,
                "session": pd.Timestamp(session),
                "status": "no_reported_restriction",
                "captured_at": captured_at,
                "source": "test",
            }
        ),
    )
    output = tmp_path / "bundle"
    manifest = inputs.build_research_inputs(
        _policy(),
        output,
        symbols=["000001"],
        start="2026-01-01",
        end="2026-01-05",
        captured_at="2026-01-05T09:00:00+00:00",
    )
    assert output.is_dir()
    assert manifest["files"]["raw"]["provider"] == "akshare_eastmoney"
    assert manifest["shadows"]["yahoo"]["status"] == "available"
    assert "yahoo" not in pd.read_parquet(output / "raw.parquet")["provider"].unique()
    raw_path = output / manifest["files"]["raw"]["file"]
    assert hashlib.sha256(raw_path.read_bytes()).hexdigest() == manifest["files"]["raw"]["sha256"]
    assert json.loads((output / "manifest.json").read_text()) == manifest
    with pytest.raises(FileExistsError):
        inputs.build_research_inputs(
            _policy(),
            output,
            symbols=["000001"],
            start="2026-01-01",
            end="2026-01-05",
            captured_at="2026-01-05T09:00:00+00:00",
        )
