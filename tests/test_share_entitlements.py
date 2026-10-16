import json
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from quant_data_kit import CorporateActionEvent, FixedPoint, market_event_payload
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.schemas_v2 import (
    CORPORATE_ACTION_EVENT_SCHEMA_ID,
    get_arrow_schema,
    validate_arrow_table,
    validate_json_record,
)
from quant_data_kit.share_entitlements import EntitlementUnresolved, ShareEntitlement


def terms(**kwargs):
    data = {
        "entitlement_id": "action",
        "account_id": "account",
        "record_date": date(2024, 7, 4),
        "eligible_quantity": FixedPoint(100, 0),
        "ordinary_additional_ratio": FixedPoint(400595, 6),
        "known_at": datetime(2024, 7, 1, tzinfo=timezone.utc),
        "credit_at": datetime(2024, 7, 5, tzinfo=timezone.utc),
        "sellable_date": date(2024, 7, 5),
        "evidence_source": "fixture:verified-announcement",
        "evidence_sha256": "f" * 64,
        "fractional_rule": "largest_fraction",
    }
    return ShareEntitlement(**(data | kwargs))


def test_json_arrow_and_dataclass_agree_and_preserve_exact_entitlement():
    item = terms(evidence_status="confirmed", confirmed_quantity=FixedPoint(41, 0))
    event = CorporateActionEvent(
        event_id="action:account",
        instrument_id="600763",
        event_time=item.credit_at,
        received_at=item.credit_at,
        available_at=item.credit_at,
        source=item.evidence_source,
        trading_day=date(2024, 7, 5),
        session_id="day",
        sequence=0,
        action_type="share_credit",
        effective_date=item.record_date,
        share_entitlement=item,
    )
    payload = market_event_payload(event)
    golden = Path(__file__).parent / "golden" / "v2" / "share_credit.json"
    assert payload == json.loads(golden.read_text(encoding="utf8"))
    validate_json_record(CORPORATE_ACTION_EVENT_SCHEMA_ID, payload)
    # Arrow common time/date fields are normalized in this test; nested evidence
    # retains ISO strings, which makes round-tripping independent of local TZ.
    row = {
        **payload,
        "event_time": event.event_time,
        "received_at": event.received_at,
        "available_at": event.available_at,
        "trading_day": event.trading_day,
        "effective_date": event.effective_date,
    }
    table = pa.Table.from_pylist([row], schema=get_arrow_schema(CORPORATE_ACTION_EVENT_SCHEMA_ID))
    validate_arrow_table(CORPORATE_ACTION_EVENT_SCHEMA_ID, table)
    legacy = table.drop(["share_entitlement"])
    validate_arrow_table(CORPORATE_ACTION_EVENT_SCHEMA_ID, legacy)
    with pytest.raises(ValidationError, match="schema mismatch"):
        validate_arrow_table(CORPORATE_ACTION_EVENT_SCHEMA_ID, legacy.drop(["ratio"]))
    decoded = ShareEntitlement.from_payload(table.to_pylist()[0]["share_entitlement"])
    assert decoded == item and decoded.resolved_quantity() == 41


def test_fractional_ambiguity_requires_evidence_or_explicit_permitted_branch():
    with pytest.raises(EntitlementUnresolved):
        terms().resolved_quantity()
    assert terms(evidence_status="sensitivity_floor").resolved_quantity() == 40
    assert terms(evidence_status="sensitivity_ceil").resolved_quantity() == 41
    with pytest.raises(ValidationError, match="floor-only"):
        terms(evidence_status="sensitivity_ceil", fractional_rule="floor")
    with pytest.raises(ValidationError, match="only confirmed"):
        terms(confirmed_quantity=FixedPoint(40, 0))
