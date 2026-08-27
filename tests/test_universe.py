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
