import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from quant_data_kit.providers import corporate_actions as actions


def test_outside_window_does_not_require_unrelated_announcement(monkeypatch):
    dividends = pd.DataFrame(
        [
            {
                "权益登记日": "2021-03-12",
                "除息日": "2021-03-15",
                "分红发放日": "2021-03-18",
                "每10份分红": "每10份派现金1元",
            },
            {
                "权益登记日": "2026-01-16",
                "除息日": "2026-01-19",
                "分红发放日": "2026-01-27",
                "每10份分红": "每10份派现金1.23元",
            },
        ]
    )
    announcements = pd.DataFrame(
        [{"公告日期": "2026-01-12", "公告标题": "分红公告", "报告ID": "AN-real"}]
    )
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            fund_open_fund_info_em=lambda **_: dividends,
            fund_announcement_dividend_em=lambda **_: announcements,
        ),
    )
    monkeypatch.setattr(actions, "configure_network", lambda: None)
    selected = actions.fetch_etf_corporate_actions(
        ["510300"], "2026-01-22T00:00:00Z", start="2025-01-01", end="2026-01-22"
    )
    assert len(selected) == 1 and selected.iloc[0].cash_per_share == "0.123"
    with pytest.raises(ValueError, match="prior announcement"):
        actions.fetch_etf_corporate_actions(["510300"], "2026-01-22T00:00:00Z")
    dividends.loc[0, "除息日"] = None
    with pytest.raises(ValueError, match="unknown ex-date"):
        actions.fetch_etf_corporate_actions(["510300"], "2026-01-22T00:00:00Z", start="2025-01-01")
