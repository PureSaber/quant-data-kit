from __future__ import annotations

import pandas as pd
import pytest

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.providers.universe import fetch_hs300_constituents_history


def test_historical_universe_refuses_survivorship_fallback() -> None:
    with pytest.raises(ValidationError, match="survivorship bias"):
        fetch_hs300_constituents_history(
            "2025-01-01",
            "2025-01-31",
            fetch_fn=lambda: pd.DataFrame(),
            current_symbols=["000001"],
            trade_dates=pd.date_range("2025-01-01", "2025-01-31", freq="B"),
        )


def test_historical_universe_allows_explicit_current_fallback() -> None:
    result = fetch_hs300_constituents_history(
        "2025-01-01",
        "2025-01-03",
        fetch_fn=lambda: pd.DataFrame(),
        current_symbols=["000001"],
        trade_dates=pd.date_range("2025-01-01", "2025-01-03", freq="B"),
        allow_current_fallback=True,
    )
    assert result["symbol"].unique().tolist() == ["000001"]
    assert len(result) == 3


def test_adjustments_after_query_end_are_undone_and_effective_boundary_is_inclusive():
    events = pd.DataFrame(
        {"date": ["2025-06-02"] * 2, "symbol": ["000001", "000002"], "action": ["剔除", "纳入"]}
    )

    def fetch(start, end):
        return fetch_hs300_constituents_history(
            start,
            end,
            fetch_fn=lambda: events,
            current_symbols=["000002"],
            trade_dates=pd.bdate_range(start, end),
        )

    assert set(fetch("2024-01-01", "2024-01-05").symbol) == {"000001"}
    assert set(fetch("2025-06-02", "2025-06-03").symbol) == {"000002"}
    spanning = fetch("2025-05-30", "2025-06-03")
    assert set(spanning.loc[spanning.date == "2025-05-30", "symbol"]) == {"000001"}
    assert set(spanning.loc[spanning.date == "2025-06-02", "symbol"]) == {"000002"}


def test_future_announced_adjustment_is_not_undone_from_current_snapshot():
    events = pd.DataFrame(
        {"date": ["2025-06-02"] * 2, "symbol": ["000001", "000002"], "action": ["剔除", "纳入"]}
    )
    result = fetch_hs300_constituents_history(
        "2025-01-02",
        "2025-01-03",
        fetch_fn=lambda: events,
        current_symbols=["000001"],
        current_as_of="2025-01-03",
        trade_dates=pd.bdate_range("2025-01-02", "2025-01-03"),
    )
    assert set(result.symbol) == {"000001"}
