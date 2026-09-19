import pandas as pd
import pytest

from quant_data_kit.providers.corporate_actions import normalize_actions


def sample():
    return pd.DataFrame(
        [
            {
                "实施方案公告日期": "2026-07-01",
                "股权登记日": "2026-07-09",
                "除权日": "2026-07-10",
                "派息日": "2026-07-10",
                "股份到账日": None,
                "送股比例": None,
                "转增比例": None,
                "派息比例": 10.03,
                "实施方案分红说明": "10派10.03元(含税)",
            }
        ]
    )


def test_per_ten_units_and_unknown_dates_are_preserved():
    frame = normalize_actions(sample(), "600036", "2026-09-19T00:00:00Z")
    assert frame.iloc[0].cash_per_share == "1.003"
    assert frame.iloc[0].share_ratio == "1"
    assert pd.isna(frame.iloc[0].shares_available_date)
    assert "10.03" in frame.iloc[0].source_record
    assert (
        normalize_actions(sample(), "600036", "2026-09-20T00:00:00Z").iloc[0].event_id
        == frame.iloc[0].event_id
    )


def test_incomplete_and_conflicting_actions_fail_closed():
    raw = sample()
    raw.loc[0, "派息比例"] = None
    with pytest.raises(ValueError, match="Missing"):
        normalize_actions(raw, "600036", "now")
    with pytest.raises(ValueError, match="Duplicate"):
        normalize_actions(pd.concat([sample(), sample()]), "600036", "now")
    with pytest.raises(ValueError, match="schema"):
        normalize_actions(pd.DataFrame(), "600036", "now")


def test_separate_ordinary_and_special_cash_distributions_are_both_retained():
    ordinary = sample().assign(分红类型="年度分红")
    special = sample().assign(分红类型="特别分红", 派息比例=2, 实施方案分红说明="10派2元(含税)")
    result = normalize_actions(pd.concat([ordinary, special]), "600036", "2026-09-19T00:00:00Z")
    assert len(result) == 1 and result.iloc[0].cash_per_share == "1.203"
    assert "特别分红" in result.iloc[0].source_record
