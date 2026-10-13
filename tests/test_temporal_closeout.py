import pandas as pd
import pytest

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.providers.earnings_forecast import fetch_earnings_forecasts
from quant_data_kit.temporal import point_in_time_join


def test_conflicting_same_time_facts_are_rejected_in_both_orders():
    obs = pd.DataFrame({"date": ["2026-01-10"], "symbol": ["A"]})
    facts = pd.DataFrame({"available_at": ["2026-01-01"] * 2, "symbol": ["A"] * 2, "pe": [10, 20]})
    for value in (facts, facts.iloc[::-1]):
        with pytest.raises(ValidationError, match="Conflicting"):
            point_in_time_join(obs, value)
    facts["pe"] = 10
    assert point_in_time_join(obs, facts)["pe"].tolist() == [10]


def test_announcement_window_queries_previous_and_forward_report_periods():
    called = []

    def fetch(period):
        called.append(period)
        if period not in {"20251231", "20260331"}:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "股票代码": ["000001"] * 3,
                "公告日期": ["2025-12-31", "2026-01-15", "2026-02-16"],
                "预告类型": ["预增"] * 3,
            }
        )

    result = fetch_earnings_forecasts("2026-01-01", "2026-02-15", fetch_fn=fetch)
    assert {"20251231", "20260331"} <= set(called)
    assert len(result) == 2
    assert set(result["announce_date"]) == {pd.Timestamp("2026-01-15")}
    with pytest.raises(ValueError, match="start_date"):
        fetch_earnings_forecasts("2026-02-15", "2026-01-01", fetch_fn=fetch)


def test_explicit_report_periods_allow_old_revisions_and_deduplicate_calls():
    calls = []

    def fetch(period):
        calls.append(period)
        return pd.DataFrame()

    fetch_earnings_forecasts(
        "2026-01-01", "2026-02-15", fetch_fn=fetch, report_periods=["20231231", "20231231"]
    )
    assert calls == ["20231231"]
    with pytest.raises(ValueError, match="quarter ends"):
        fetch_earnings_forecasts(
            "2026-01-01", "2026-02-15", fetch_fn=fetch, report_periods=["20230101"]
        )
