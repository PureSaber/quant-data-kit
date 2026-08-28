from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from quant_data_kit import (
    AssetClass,
    BarEvent,
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
    QuoteEvent,
    SessionPhase,
    StatusEvent,
    SymbolMapping,
    TradeEvent,
    TradingSession,
    dataclass_payload,
    market_event_payload,
)
from quant_data_kit.exceptions import ValidationError

UTC = timezone.utc
NOW = datetime(2026, 1, 2, 1, 0, tzinfo=UTC)


def fp(value: str, scale: int = 2) -> FixedPoint:
    return FixedPoint.from_decimal(Decimal(value), scale)


def event_base(sequence: int | None = 1) -> dict:
    return {
        "event_id": "branch-event",
        "instrument_id": "crypto:binance:BTCUSDT",
        "event_time": NOW,
        "received_at": NOW,
        "available_at": NOW,
        "source": "binance",
        "trading_day": date(2026, 1, 2),
        "session_id": "binance-24x7-20260102",
        "sequence": sequence,
    }


def test_market_event_base_and_simple_event_negative_contracts() -> None:
    with pytest.raises(ValidationError, match="event_id"):
        TradeEvent(**(event_base() | {"event_id": " "}), price=fp("1"), quantity=fp("1"))
    with pytest.raises(ValidationError, match="received_at"):
        TradeEvent(
            **(event_base() | {"received_at": NOW - timedelta(seconds=1)}),
            price=fp("1"),
            quantity=fp("1"),
        )
    with pytest.raises(ValidationError, match="available_at"):
        TradeEvent(
            **(event_base() | {"available_at": NOW - timedelta(seconds=1)}),
            price=fp("1"),
            quantity=fp("1"),
        )
    with pytest.raises(ValidationError, match="trading_day"):
        TradeEvent(
            **(event_base() | {"trading_day": NOW}),
            price=fp("1"),
            quantity=fp("1"),
        )
    for invalid_sequence in (True, "1", -1):
        with pytest.raises(ValidationError, match="sequence"):
            TradeEvent(**event_base(invalid_sequence), price=fp("1"), quantity=fp("1"))
    with pytest.raises(ValidationError, match="aggressor_side"):
        TradeEvent(**event_base(), price=fp("1"), quantity=fp("1"), aggressor_side="buy")
    with pytest.raises(ValidationError, match="positive FixedPoint"):
        TradeEvent(**event_base(), price=FixedPoint(0, 2), quantity=fp("1"))
    with pytest.raises(ValidationError, match="positive FixedPoint"):
        TradeEvent(**event_base(), price=fp("1"), quantity="1")

    with pytest.raises(ValidationError, match="order_count"):
        BookLevel(fp("1"), fp("1"), order_count=True)
    with pytest.raises(ValidationError, match="non-negative"):
        BookLevel(fp("1"), FixedPoint(-1, 2))

    with pytest.raises(ValidationError, match="one scale"):
        QuoteEvent(
            **event_base(),
            bid_price=fp("1", 2),
            bid_quantity=fp("1"),
            ask_price=fp("2", 3),
            ask_quantity=fp("1"),
        )
    with pytest.raises(ValidationError, match="crossed"):
        QuoteEvent(
            **event_base(),
            bid_price=fp("2"),
            bid_quantity=fp("1"),
            ask_price=fp("1"),
            ask_quantity=fp("1"),
        )


def _bar(**changes):
    values = {
        **event_base(None),
        "event_time": NOW,
        "bar_start": NOW - timedelta(minutes=1),
        "bar_end": NOW,
        "open_price": fp("2"),
        "high_price": fp("3"),
        "low_price": fp("1"),
        "close_price": fp("2"),
        "volume": fp("5"),
        "is_complete": True,
    }
    values.update(changes)
    return BarEvent(**values)


def test_bar_contract_rejects_each_structural_violation() -> None:
    assert market_event_payload(_bar())["event_type"] == "bar"
    cases = (
        ({"bar_end": NOW - timedelta(minutes=1)}, "bar_end"),
        ({"event_time": NOW + timedelta(seconds=1)}, "event_time"),
        ({"open_price": fp("1", 3)}, "one price scale"),
        ({"high_price": fp("1")}, "high_price"),
        ({"low_price": fp("3")}, "low_price"),
        ({"volume": FixedPoint(-1, 2)}, "non-negative"),
        ({"is_complete": 1}, "is_complete"),
    )
    for changes, message in cases:
        with pytest.raises(ValidationError, match=message):
            _bar(**changes)


def _snapshot(**changes):
    values = {
        **event_base(10),
        "bids": (BookLevel(fp("99"), fp("1")), BookLevel(fp("98"), fp("1"))),
        "asks": (BookLevel(fp("101"), fp("1")), BookLevel(fp("102"), fp("1"))),
    }
    values.update(changes)
    return BookSnapshotEvent(**values)


def test_book_snapshot_and_delta_negative_contracts() -> None:
    snapshot_cases = (
        ({"sequence": None}, "sequence"),
        ({"bids": ()}, "contain bids"),
        ({"bids": ("bad",)}, "BookLevel"),
        ({"bids": (BookLevel(fp("99", 3), fp("1")),)}, "one scale"),
        (
            {"bids": (BookLevel(fp("98"), fp("1")), BookLevel(fp("99"), fp("1")))},
            "bids",
        ),
        (
            {"asks": (BookLevel(fp("102"), fp("1")), BookLevel(fp("101"), fp("1")))},
            "asks",
        ),
        (
            {"bids": (BookLevel(fp("99"), fp("1")), BookLevel(fp("99"), fp("2")))},
            "duplicate",
        ),
        ({"bids": (BookLevel(fp("101"), fp("1")),)}, "locked or crossed"),
    )
    for changes, message in snapshot_cases:
        with pytest.raises(ValidationError, match=message):
            _snapshot(**changes)

    delta_values = {
        **event_base(11),
        "side": BookSide.BID,
        "action": BookAction.UPSERT,
        "price": fp("100"),
        "quantity": fp("1"),
        "previous_sequence": 10,
    }
    cases = (
        ({"sequence": None}, "sequence"),
        ({"previous_sequence": True}, "previous_sequence"),
        ({"previous_sequence": 11}, "precede"),
        ({"side": "bid"}, "BookSide"),
        ({"action": "upsert"}, "BookAction"),
        ({"action": BookAction.DELETE}, "quantity must be zero"),
        ({"quantity": FixedPoint(0, 2)}, "quantity must be positive"),
    )
    for changes, message in cases:
        with pytest.raises(ValidationError, match=message):
            BookDeltaEvent(**(delta_values | changes))


def test_funding_corporate_action_status_and_payload_branches() -> None:
    for rate, message in ((True, "numeric"), (float("inf"), "finite")):
        with pytest.raises(ValidationError, match=message):
            FundingRateEvent(
                **event_base(),
                rate=rate,
                interval_start=NOW - timedelta(hours=8),
                interval_end=NOW,
            )
    with pytest.raises(ValidationError, match="interval"):
        FundingRateEvent(
            **event_base(),
            rate=0.1,
            interval_start=NOW,
            interval_end=NOW,
        )

    action_base = {
        **event_base(),
        "action_type": "split",
        "effective_date": date(2026, 1, 2),
    }
    action_cases = (
        ({"effective_date": NOW, "ratio": fp("1")}, "effective_date"),
        ({}, "requires ratio"),
        ({"ratio": FixedPoint(-1, 2)}, "non-negative"),
        ({"cash_amount": FixedPoint(-1, 2), "currency": "USD"}, "non-negative"),
        ({"cash_amount": fp("1"), "currency": None}, "currency"),
        ({"ratio": fp("1"), "currency": "USD"}, "requires cash_amount"),
    )
    for changes, message in action_cases:
        with pytest.raises(ValidationError, match=message):
            CorporateActionEvent(**(action_base | changes))
    ratio_action = CorporateActionEvent(**action_base, ratio=fp("2"))
    assert market_event_payload(ratio_action)["cash_amount"] is None

    status = StatusEvent(**event_base(), status="open", reason="scheduled")
    assert market_event_payload(status)["status"] == "open"
    with pytest.raises(ValidationError, match="reason"):
        StatusEvent(**event_base(), status="open", reason=1)
    with pytest.raises(TypeError, match="Unsupported market event"):
        market_event_payload(SimpleNamespace(event_type="unknown", **event_base()))


def instrument_spec(**changes) -> InstrumentSpec:
    values = {
        "instrument_id": "crypto:binance:BTCUSDT",
        "asset_class": AssetClass.CRYPTO,
        "product_type": "spot",
        "venue": "BINANCE",
        "native_symbol": "BTCUSDT",
        "base_currency": "BTC",
        "quote_currency": "USDT",
        "settlement_currency": "USDT",
        "price_tick": fp("0.01"),
        "quantity_step": fp("0.01"),
        "contract_multiplier": fp("1"),
        "calendar_id": "crypto-24x7-v1",
        "margin_mode": MarginMode.CASH,
        "inverse": False,
        "effective_from": NOW,
        "effective_to": NOW + timedelta(days=1),
        "available_at": NOW,
        "superseded_at": NOW + timedelta(days=2),
        "underlying_id": "crypto:bitcoin",
        "expiry_date": date(2026, 12, 31),
        "metadata": {"tier": "certified"},
    }
    values.update(changes)
    return InstrumentSpec(**values)


def test_reference_contract_negative_branches_and_serializers() -> None:
    spec = instrument_spec()
    assert dataclass_payload(spec)["effective_to"].endswith("Z")
    cases = (
        ({"instrument_id": ""}, "instrument_id"),
        ({"asset_class": "crypto"}, "asset_class"),
        ({"margin_mode": "cash"}, "margin_mode"),
        ({"inverse": 0}, "inverse"),
        ({"price_tick": "0.01"}, "price_tick"),
        ({"quantity_step": FixedPoint(0, 2)}, "quantity_step"),
        ({"effective_to": NOW}, "effective_to"),
        ({"superseded_at": NOW}, "superseded_at"),
        ({"metadata": []}, "metadata"),
        ({"metadata": {"tier": 1}}, "metadata"),
    )
    for changes, message in cases:
        with pytest.raises(ValidationError, match=message):
            instrument_spec(**changes)

    mapping = SymbolMapping(
        source="binance",
        provider_symbol="BTCUSDT",
        instrument_id=spec.instrument_id,
        effective_from=NOW,
        effective_to=NOW + timedelta(days=1),
        available_at=NOW,
        superseded_at=NOW + timedelta(days=2),
    )
    assert dataclass_payload(mapping)["provider_symbol"] == "BTCUSDT"
    with pytest.raises(ValidationError, match="effective_to"):
        replace(mapping, effective_to=NOW)
    with pytest.raises(ValidationError, match="superseded_at"):
        replace(mapping, superseded_at=NOW)

    session = TradingSession(
        session_id="session-1",
        calendar_id="calendar-1",
        venue="BINANCE",
        trading_day=date(2026, 1, 2),
        phase=SessionPhase.CONTINUOUS,
        opens_at=NOW,
        closes_at=NOW + timedelta(hours=1),
        available_at=NOW,
        superseded_at=NOW + timedelta(days=1),
    )
    assert dataclass_payload(session)["phase"] == "continuous"
    for changes, message in (
        ({"trading_day": NOW}, "trading_day"),
        ({"phase": "continuous"}, "phase"),
        ({"closes_at": NOW}, "closes_at"),
        ({"superseded_at": NOW}, "superseded_at"),
    ):
        with pytest.raises(ValidationError, match=message):
            replace(session, **changes)
    with pytest.raises(TypeError, match="Unsupported public contract"):
        dataclass_payload(object())
