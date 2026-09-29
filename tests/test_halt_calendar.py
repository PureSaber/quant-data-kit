import pandas as pd
import pytest

from quant_data_kit.providers.halt_calendar import freeze_halt_calendar, is_tradable, lookup


def _frame():
    return pd.DataFrame(
        [
            {"symbol": "600036", "session": "2026-07-10", "status": "suspended"},
            {"symbol": "600036", "session": "2026-07-11", "status": "tradable"},
            {"symbol": "000001", "session": "2026-07-10", "status": "risk_warning"},
        ]
    )


def test_calendar_is_hashed_and_uncovered_sessions_fail():
    first = freeze_halt_calendar(_frame(), source="exchange.halt.history")
    second = freeze_halt_calendar(_frame().iloc[::-1], source="exchange.halt.history")
    assert first["sha256"] == second["sha256"]
    assert is_tradable(first, "600036", "2026-07-10") is False
    assert is_tradable(first, "600036", "2026-07-11") is True
    assert lookup(first, "000001", "2026-07-10") == "risk_warning"
    with pytest.raises(ValueError, match="not covered"):
        is_tradable(first, "600036", "2026-07-12")


def test_current_snapshot_cannot_be_frozen_as_history():
    with pytest.raises(ValueError, match="current"):
        freeze_halt_calendar(_frame(), source="akshare.eastmoney.current_st_and_halts")


def test_tampered_calendar_is_rejected():
    calendar = freeze_halt_calendar(_frame(), source="exchange.halt.history")
    calendar["observations"][1]["status"] = "tradable"
    with pytest.raises(ValueError, match="SHA-256"):
        lookup(calendar, "600036", "2026-07-10")


@pytest.mark.parametrize("symbol", [None, float("nan"), " "])
def test_missing_symbol_is_rejected(symbol):
    frame = _frame()
    frame.loc[0, "symbol"] = symbol
    with pytest.raises(ValueError, match="empty symbol"):
        freeze_halt_calendar(frame, source="exchange.halt.history")
