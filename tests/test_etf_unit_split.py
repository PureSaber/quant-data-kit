import hashlib
from decimal import Decimal

import pandas as pd
import pytest

from quant_data_kit import FixedPoint
from quant_data_kit.etf_events import adjust_etf_prices, normalize_etf_splits
from quant_data_kit.research_dataset import _validate_adjustments
from quant_data_kit.share_entitlements import ShareEntitlement


def fixture(tmp_path):
    source = b"synthetic issuer: ratio 1.14539; round up per holder"
    (tmp_path / "issuer.txt").write_bytes(source)
    item = {
        "event_id": "split",
        "symbol": "510500",
        "announced_date": "2022-08-23",
        "record_date": "2022-08-26",
        "ex_date": "2022-08-29",
        "credit_at": "2022-08-26T16:00:00+08:00",
        "known_at": "2022-08-24T00:00:00+08:00",
        "sellable_date": "2022-08-29",
        "ratio": "1.14539",
        "fractional_rule": "ceil",
        "source": "synthetic-issuer",
        "file": "issuer.txt",
        "sha256": hashlib.sha256(source).hexdigest(),
    }
    rows = []
    for day, price in zip(pd.bdate_range("2022-08-24", periods=5), [7.2, 7.2, 7.2, 6.3, 6.3]):
        rows.append(
            {
                "symbol": "510500",
                "date": day,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1000000,
                "amount": price * 1000000,
                "adjustment": "none",
                "source": "synthetic-raw",
                "composite_score": 1.0,
            }
        )
    return item, pd.DataFrame(rows)


def test_exact_ratio_adjustment_and_evidence_tamper(tmp_path):
    item, raw = fixture(tmp_path)
    actions = normalize_etf_splits(
        [item], evidence_root=tmp_path, captured_at="2026-10-22T00:00:00Z"
    )
    adjusted = adjust_etf_prices(raw, actions)
    assert adjusted.iloc[0].close == pytest.approx(7.2 / 1.14539)
    assert raw.iloc[0].close == 7.2
    assert _validate_adjustments(raw, adjusted, actions)["passed"]
    with pytest.raises(ValueError, match="identity"):
        normalize_etf_splits(
            [{**item, "sha256": "a" * 64}],
            evidence_root=tmp_path,
            captured_at="2026-10-22T00:00:00Z",
        )


def test_issuer_rounding_not_vendor_precision_or_sensitivity():
    from tests.test_share_entitlements import terms

    value = terms(
        eligible_quantity=FixedPoint(100000, 0),
        ordinary_additional_ratio=FixedPoint(14539, 5),
        evidence_status="rule_determined",
        fractional_rule="ceil",
    )
    assert value.resolved_quantity() == Decimal(14539)
    small = terms(
        eligible_quantity=FixedPoint(100, 0),
        ordinary_additional_ratio=FixedPoint(14539, 5),
        evidence_status="rule_determined",
        fractional_rule="ceil",
    )
    assert small.resolved_quantity() == 15
    assert ShareEntitlement.from_payload(small.payload()) == small
