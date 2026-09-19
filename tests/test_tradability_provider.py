import pandas as pd
import pytest

from quant_data_kit.providers.tradability import normalize_tradability


def test_current_status_does_not_invent_historical_membership():
    st = pd.DataFrame({"代码": ["000001"]})
    halts = pd.DataFrame({"代码": ["600036"], "停牌时间": ["2026-09-18"], "停牌截止时间": [None]})
    result = normalize_tradability(
        ["000001", "600036", "000333"], st, halts, "2026-09-18", "2026-09-19T01:00:00Z"
    )
    assert list(result.status) == ["risk_warning", "suspended", "no_reported_restriction"]
    assert result.scope.str.contains("no historical").all()
    with pytest.raises(ValueError):
        normalize_tradability(
            ["000001"], pd.DataFrame(), halts, "2026-09-18", "2026-09-19T01:00:00Z"
        )
