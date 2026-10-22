import hashlib

import pandas as pd
import pytest

from quant_data_kit.etf_history import audit_etf_history


def panel():
    dates = pd.bdate_range("2022-08-24", periods=5)
    raw = pd.DataFrame(
        [
            {
                "symbol": "510500",
                "date": day,
                "open": 7.0,
                "high": 7.1,
                "low": 6.9,
                "close": 7.0,
                "volume": 1000.0,
                "amount": 7000.0,
                "adjustment": "none",
                "source": "vendor_a",
            }
            for day in dates[:-1]
        ]
    )
    comparison = raw.assign(source="vendor_b")
    actions = pd.DataFrame(columns=["symbol", "ex_date", "cash_per_share", "share_ratio"])
    return dates, raw, comparison, actions


def test_zero_turnover_and_cross_vendor_conflicts_are_not_suspensions():
    dates, raw, other, actions = panel()
    raw.loc[1, ["volume", "amount"]] = 0
    report = audit_etf_history(
        raw,
        other,
        dates,
        actions,
        symbols=["510500"],
        start=str(dates[0].date()),
        end=str(dates[-2].date()),
    )
    assert not report["admitted"]
    assert {r["kind"] for r in report["blocking_issues"]} == {
        "invalid_traded_bar",
        "provider_conflict",
        "historical_master_missing",
    }
    assert report["checks"]["close"]["conflicts"] == 0
    assert report["synthetic_bars"] == 0


def test_evidenced_nontrading_explains_gap_without_filling_a_bar(tmp_path):
    dates, raw, other, actions = panel()
    source = b"synthetic full-session suspension announcement"
    (tmp_path / "source.txt").write_bytes(source)
    event = {
        "symbol": "510500",
        "date": str(dates[1].date()),
        "status": "full_session_suspension",
        "known_at": "2022-08-24T00:00:00Z",
        "source": "synthetic-issuer",
        "file": "source.txt",
        "sha256": hashlib.sha256(source).hexdigest(),
    }
    kw = {
        "symbols": ["510500"],
        "start": str(dates[0].date()),
        "end": str(dates[-2].date()),
        "nontrading": [event],
        "evidence_root": tmp_path,
    }
    report = audit_etf_history(raw.drop(1), other.drop(1), dates, actions, **kw)
    assert report["evidenced_nontrading_sessions"] == 1
    assert report["blocking_issues"] == [{"kind": "historical_master_missing"}]
    assert not report["admitted"]
    with pytest.raises(ValueError, match="contradicts"):
        audit_etf_history(raw, other, dates, actions, **kw)
    event["known_at"] = "2022-08-26T00:00:00Z"
    with pytest.raises(ValueError, match="late"):
        audit_etf_history(raw.drop(1), other.drop(1), dates, actions, **kw)


def test_unknown_missing_bar_remains_in_inventory():
    dates, raw, other, actions = panel()
    report = audit_etf_history(
        raw.drop(1),
        other.drop(1),
        dates,
        actions,
        symbols=["510500"],
        start=str(dates[0].date()),
        end=str(dates[-2].date()),
    )
    assert sum(r["kind"] == "unexplained_missing_session" for r in report["blocking_issues"]) == 2
