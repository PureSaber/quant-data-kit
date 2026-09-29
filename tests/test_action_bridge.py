from decimal import Decimal

import pandas as pd
import pytest

from quant_data_kit.providers.action_bridge import distribution_terms, explicit_terms
from quant_data_kit.providers.corporate_actions import normalize_actions


def _cash_row():
    raw = pd.DataFrame(
        [
            {
                "实施方案公告日期": "2026-07-01",
                "股权登记日": "2026-07-09",
                "除权日": "2026-07-10",
                "派息日": "2026-07-12",
                "股份到账日": None,
                "送股比例": None,
                "转增比例": None,
                "派息比例": 10,
                "实施方案分红说明": "10派10元(含税)",
            }
        ]
    )
    return normalize_actions(raw, "600036", "2026-09-19T00:00:00Z").iloc[0]


def test_cash_distribution_becomes_entitlement_and_payment():
    terms = distribution_terms(_cash_row(), instrument_id="600036.SH")
    assert [item.kind for item in terms] == ["dividend_entitlement", "dividend_payment"]
    assert terms[0].cash_per_unit == "1"
    assert terms[0].entitlement_date == "2026-07-09"
    assert terms[1].effective_at.startswith("2026-07-12")
    assert all(term.available_at == "2026-09-19T00:00:00Z" for term in terms)


def test_bonus_shares_become_a_split_and_deferred_delivery_is_refused():
    raw = pd.DataFrame(
        [
            {
                "实施方案公告日期": "2026-07-01",
                "股权登记日": "2026-07-09",
                "除权日": "2026-07-10",
                "派息日": None,
                "股份到账日": "2026-07-10",
                "送股比例": 5,
                "转增比例": None,
                "派息比例": None,
                "实施方案分红说明": "10送5股",
            }
        ]
    )
    row = normalize_actions(raw, "600036", "2026-09-19T00:00:00Z").iloc[0]
    terms = distribution_terms(row, instrument_id="600036.SH")
    assert len(terms) == 1 and terms[0].kind == "split"
    assert Decimal(terms[0].ratio) == Decimal("1.5")
    assert terms[0].available_at == "2026-09-19T00:00:00Z"
    late = row.copy()
    late["shares_available_date"] = pd.Timestamp("2026-07-20")
    with pytest.raises(ValueError, match="receivables"):
        distribution_terms(late, instrument_id="600036.SH")


def test_rights_and_mergers_require_an_explicit_record():
    with pytest.raises(ValueError, match="explicit"):
        explicit_terms({"kind": "split"})
    terms = explicit_terms(
        {
            "event_id": "rights-1",
            "instrument_id": "600036.SH",
            "kind": "rights_distribution",
            "effective_at": "2026-07-10T01:00:00Z",
            "available_at": "2026-07-10T01:00:00Z",
            "currency": "CNY",
            "source": "prospectus",
            "evidence_id": "doc-1",
            "ratio": "0.3",
            "target_id": "600036.SH.RIGHT",
            "cost_fraction": "0",
            "target_mark": "1",
            "parent_mark": "10",
        }
    )
    assert terms.kind == "rights_distribution"


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1"])
def test_invalid_cash_amount_is_rejected(value):
    row = _cash_row().copy()
    row["cash_per_share"] = value
    with pytest.raises(ValueError, match="distribution amounts"):
        distribution_terms(row, instrument_id="600036.SH")
