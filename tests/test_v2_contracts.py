from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd
import pytest

from quant_data_kit import (
    AggressorSide,
    AssetClass,
    BookAction,
    BookDeltaEvent,
    BookLevel,
    BookSide,
    BookSnapshotEvent,
    CorporateActionEvent,
    FixedPoint,
    FundingRateEvent,
    InstrumentSpec,
    MarginMode,
    MarketClock,
    MarkPriceEvent,
    QuoteEvent,
    SessionPhase,
    TradeEvent,
    TradingSession,
    dataclass_payload,
    ensure_utc_datetime,
    market_event_payload,
    point_in_time_join_bitemporal,
    validate_json_record,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.schemas_v2 import (
    BOOK_DELTA_EVENT_SCHEMA_ID,
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    CORPORATE_ACTION_EVENT_SCHEMA_ID,
    FUNDING_RATE_EVENT_SCHEMA_ID,
    INSTRUMENT_SPEC_SCHEMA_ID,
    MARK_PRICE_EVENT_SCHEMA_ID,
    QUOTE_EVENT_SCHEMA_ID,
    TRADE_EVENT_SCHEMA_ID,
    get_arrow_schema,
)

UTC = timezone.utc
T0 = datetime(2026, 1, 2, 1, 0, tzinfo=UTC)


def fp(value: str, scale: int = 2) -> FixedPoint:
    return FixedPoint.from_decimal(Decimal(value), scale)


def event_base(sequence: int | None = 1) -> dict:
    return {
        "event_id": f"event-{sequence}",
        "instrument_id": "crypto:binance:BTCUSDT",
        "event_time": T0,
        "received_at": T0 + timedelta(milliseconds=1),
        "available_at": T0 + timedelta(milliseconds=2),
        "source": "binance",
        "trading_day": date(2026, 1, 2),
        "session_id": "binance-24x7-20260102",
        "sequence": sequence,
    }


def test_fixed_point_is_exact_and_utc_is_strict() -> None:
    assert fp("12.34").to_decimal() == Decimal("12.34")
    with pytest.raises(ValidationError, match="not exact"):
        FixedPoint.from_decimal("12.345", 2)
    with pytest.raises(ValidationError, match="timezone-aware UTC"):
        ensure_utc_datetime(datetime(2026, 1, 1), field="event_time")
    with pytest.raises(ValidationError, match="must use UTC"):
        ensure_utc_datetime(
            datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=8))),
            field="event_time",
        )


def test_instrument_spec_matches_json_and_arrow_contracts() -> None:
    spec = InstrumentSpec(
        instrument_id="crypto:binance:BTCUSDT-PERP",
        asset_class=AssetClass.CRYPTO,
        product_type="linear_perpetual",
        venue="BINANCE",
        native_symbol="BTCUSDT",
        base_currency="BTC",
        quote_currency="USDT",
        settlement_currency="USDT",
        price_tick=fp("0.10"),
        quantity_step=FixedPoint.from_decimal("0.001", 3),
        contract_multiplier=FixedPoint.from_decimal("1", 0),
        calendar_id="crypto-24x7-v1",
        margin_mode=MarginMode.CROSS,
        inverse=False,
        effective_from=T0,
        available_at=T0,
    )
    payload = dataclass_payload(spec)
    validate_json_record(INSTRUMENT_SPEC_SCHEMA_ID, payload)
    assert list(payload) == get_arrow_schema(INSTRUMENT_SPEC_SCHEMA_ID).names
    with pytest.raises(TypeError):
        spec.metadata["mutate"] = "blocked"


def test_market_clock_uses_explicit_trading_day() -> None:
    session = TradingSession(
        session_id="night-1",
        calendar_id="cn-futures-v1",
        venue="SHFE",
        trading_day=date(2026, 1, 5),
        phase=SessionPhase.CONTINUOUS,
        opens_at=datetime(2026, 1, 4, 13, 0, tzinfo=UTC),
        closes_at=datetime(2026, 1, 4, 15, 30, tzinfo=UTC),
        available_at=datetime(2025, 12, 1, tzinfo=UTC),
    )
    clock = MarketClock("cn-futures-v1", [session])
    assert clock.trading_day_at(datetime(2026, 1, 4, 14, 0, tzinfo=UTC)) == date(
        2026, 1, 5
    )
    with pytest.raises(ValidationError, match="overlap"):
        MarketClock("cn-futures-v1", [session, session])


def test_trade_and_quote_events_enforce_causal_time() -> None:
    trade = TradeEvent(
        **event_base(),
        price=fp("100.00"),
        quantity=fp("2.00"),
        aggressor_side=AggressorSide.BUY,
    )
    validate_json_record(TRADE_EVENT_SCHEMA_ID, market_event_payload(trade))
    quote = QuoteEvent(
        **event_base(2),
        bid_price=fp("99.99"),
        bid_quantity=fp("1.00"),
        ask_price=fp("100.01"),
        ask_quantity=fp("1.50"),
    )
    validate_json_record(QUOTE_EVENT_SCHEMA_ID, market_event_payload(quote))
    invalid = event_base(3)
    invalid["available_at"] = T0
    with pytest.raises(ValidationError, match="available_at"):
        TradeEvent(
            **invalid,
            price=fp("100.00"),
            quantity=fp("1.00"),
        )


def test_l2_snapshot_and_delta_are_strictly_sequenced() -> None:
    snapshot = BookSnapshotEvent(
        **event_base(100),
        bids=(BookLevel(fp("99.99"), fp("2")), BookLevel(fp("99.98"), fp("3"))),
        asks=(BookLevel(fp("100.01"), fp("2")), BookLevel(fp("100.02"), fp("4"))),
    )
    validate_json_record(
        BOOK_SNAPSHOT_EVENT_SCHEMA_ID, market_event_payload(snapshot)
    )
    delta = BookDeltaEvent(
        **event_base(101),
        side=BookSide.BID,
        action=BookAction.UPSERT,
        price=fp("100.00"),
        quantity=fp("1.00"),
        previous_sequence=100,
    )
    validate_json_record(BOOK_DELTA_EVENT_SCHEMA_ID, market_event_payload(delta))
    with pytest.raises(ValidationError, match="precede"):
        BookDeltaEvent(
            **event_base(101),
            side=BookSide.BID,
            action=BookAction.DELETE,
            price=fp("100.00"),
            quantity=fp("0.00"),
            previous_sequence=101,
        )


def test_asset_specific_events_match_registered_schemas() -> None:
    funding = FundingRateEvent(
        **event_base(10),
        rate=0.0001,
        interval_start=T0 - timedelta(hours=8),
        interval_end=T0,
    )
    mark = MarkPriceEvent(**event_base(11), price=fp("100.05"))
    action = CorporateActionEvent(
        **event_base(12),
        action_type="cash_dividend",
        effective_date=date(2026, 1, 2),
        cash_amount=fp("0.50"),
        currency="CNY",
    )
    for schema_id, event in (
        (FUNDING_RATE_EVENT_SCHEMA_ID, funding),
        (MARK_PRICE_EVENT_SCHEMA_ID, mark),
        (CORPORATE_ACTION_EVENT_SCHEMA_ID, action),
    ):
        validate_json_record(schema_id, market_event_payload(event))


def test_bitemporal_join_never_uses_future_knowledge() -> None:
    observations = pd.DataFrame(
        {
            "instrument_id": ["asset-1", "asset-1"],
            "observation_time": [T0, T0 + timedelta(days=2)],
            "as_of": [T0, T0 + timedelta(days=2)],
        }
    )
    facts = pd.DataFrame(
        {
            "instrument_id": ["asset-1"],
            "effective_from": [T0 - timedelta(days=10)],
            "effective_to": [pd.NaT],
            "available_at": [T0 + timedelta(days=1)],
            "superseded_at": [pd.NaT],
            "value": [7],
        }
    )
    joined = point_in_time_join_bitemporal(
        observations, facts, fact_columns=["value"]
    )
    assert pd.isna(joined.loc[0, "value"])
    assert joined.loc[1, "value"] == 7
