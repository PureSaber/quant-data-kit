"""Discriminated cross-asset market-event contracts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, ClassVar, TypeAlias

from quant_data_kit.domain_v2 import fixed_point_payload
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint
from quant_data_kit.temporal_v2 import ensure_utc_datetime


class AggressorSide(str, Enum):
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class BookSide(str, Enum):
    BID = "bid"
    ASK = "ask"


class BookAction(str, Enum):
    UPSERT = "upsert"
    DELETE = "delete"


def _required_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty string")
    return value.strip()


def _positive(value: FixedPoint, field_name: str) -> None:
    if not isinstance(value, FixedPoint) or not value.is_positive():
        raise ValidationError(f"{field_name} must be a positive FixedPoint")


def _non_negative(value: FixedPoint, field_name: str) -> None:
    if not isinstance(value, FixedPoint) or not value.is_non_negative():
        raise ValidationError(f"{field_name} must be a non-negative FixedPoint")


@dataclass(frozen=True, kw_only=True)
class _MarketEventBase:
    event_id: str
    instrument_id: str
    event_time: datetime
    received_at: datetime
    available_at: datetime
    source: str
    trading_day: date
    session_id: str
    sequence: int
    event_type: ClassVar[str]

    def __post_init__(self) -> None:
        for field_name in ("event_id", "instrument_id", "source", "session_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        event_time = ensure_utc_datetime(self.event_time, field="event_time")
        received_at = ensure_utc_datetime(self.received_at, field="received_at")
        available_at = ensure_utc_datetime(self.available_at, field="available_at")
        if received_at < event_time:
            raise ValidationError("received_at must not be earlier than event_time")
        if available_at < received_at:
            raise ValidationError("available_at must not be earlier than received_at")
        if not isinstance(self.trading_day, date) or isinstance(self.trading_day, datetime):
            raise ValidationError("trading_day must be a date")
        if (
            isinstance(self.sequence, bool)
            or not isinstance(self.sequence, int)
            or self.sequence < 0
        ):
            raise ValidationError("sequence must be a non-negative integer")
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "available_at", available_at)


@dataclass(frozen=True)
class BookLevel:
    price: FixedPoint
    quantity: FixedPoint
    order_count: int | None = None

    def __post_init__(self) -> None:
        _positive(self.price, "price")
        _non_negative(self.quantity, "quantity")
        if self.order_count is not None and (
            isinstance(self.order_count, bool)
            or not isinstance(self.order_count, int)
            or self.order_count < 0
        ):
            raise ValidationError("order_count must be a non-negative integer or null")


@dataclass(frozen=True, kw_only=True)
class QuoteEvent(_MarketEventBase):
    event_type: ClassVar[str] = "quote"
    bid_price: FixedPoint
    bid_quantity: FixedPoint
    ask_price: FixedPoint
    ask_quantity: FixedPoint

    def __post_init__(self) -> None:
        super().__post_init__()
        _positive(self.bid_price, "bid_price")
        _positive(self.ask_price, "ask_price")
        _non_negative(self.bid_quantity, "bid_quantity")
        _non_negative(self.ask_quantity, "ask_quantity")
        if self.bid_price.scale != self.ask_price.scale:
            raise ValidationError("bid and ask prices must use one scale")
        if self.bid_price.units > self.ask_price.units:
            raise ValidationError("quote is crossed")


@dataclass(frozen=True, kw_only=True)
class TradeEvent(_MarketEventBase):
    event_type: ClassVar[str] = "trade"
    price: FixedPoint
    quantity: FixedPoint
    aggressor_side: AggressorSide = AggressorSide.UNKNOWN

    def __post_init__(self) -> None:
        super().__post_init__()
        _positive(self.price, "price")
        _positive(self.quantity, "quantity")
        if not isinstance(self.aggressor_side, AggressorSide):
            raise ValidationError("aggressor_side must be an AggressorSide")


@dataclass(frozen=True, kw_only=True)
class BarEvent(_MarketEventBase):
    event_type: ClassVar[str] = "bar"
    bar_start: datetime
    bar_end: datetime
    open_price: FixedPoint
    high_price: FixedPoint
    low_price: FixedPoint
    close_price: FixedPoint
    volume: FixedPoint
    is_complete: bool

    def __post_init__(self) -> None:
        super().__post_init__()
        bar_start = ensure_utc_datetime(self.bar_start, field="bar_start")
        bar_end = ensure_utc_datetime(self.bar_end, field="bar_end")
        if bar_end <= bar_start:
            raise ValidationError("bar_end must be later than bar_start")
        if self.event_time != bar_end:
            raise ValidationError("BarEvent event_time must equal bar_end")
        for field_name in ("open_price", "high_price", "low_price", "close_price"):
            _positive(getattr(self, field_name), field_name)
        _non_negative(self.volume, "volume")
        if (
            len(
                {
                    self.open_price.scale,
                    self.high_price.scale,
                    self.low_price.scale,
                    self.close_price.scale,
                }
            )
            != 1
        ):
            raise ValidationError("bar OHLC values must use one price scale")
        if self.high_price.units < max(
            self.open_price.units, self.low_price.units, self.close_price.units
        ):
            raise ValidationError("high_price is below another OHLC value")
        if self.low_price.units > min(
            self.open_price.units, self.high_price.units, self.close_price.units
        ):
            raise ValidationError("low_price is above another OHLC value")
        if not isinstance(self.is_complete, bool):
            raise ValidationError("is_complete must be boolean")
        object.__setattr__(self, "bar_start", bar_start)
        object.__setattr__(self, "bar_end", bar_end)


@dataclass(frozen=True, kw_only=True)
class BookSnapshotEvent(_MarketEventBase):
    event_type: ClassVar[str] = "book_snapshot"
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.bids or not self.asks:
            raise ValidationError("book snapshot must contain bids and asks")
        if any(not isinstance(level, BookLevel) for level in (*self.bids, *self.asks)):
            raise ValidationError("book snapshot levels must be BookLevel values")
        scales = {level.price.scale for level in (*self.bids, *self.asks)}
        if len(scales) != 1:
            raise ValidationError("book prices must use one scale")
        bid_units = [level.price.units for level in self.bids]
        ask_units = [level.price.units for level in self.asks]
        if bid_units != sorted(bid_units, reverse=True):
            raise ValidationError("book bids must be strictly ordered best first")
        if ask_units != sorted(ask_units):
            raise ValidationError("book asks must be strictly ordered best first")
        if len(set(bid_units)) != len(bid_units) or len(set(ask_units)) != len(ask_units):
            raise ValidationError("book snapshot contains duplicate price levels")
        if bid_units[0] >= ask_units[0]:
            raise ValidationError("book snapshot is locked or crossed")


@dataclass(frozen=True, kw_only=True)
class BookDeltaEvent(_MarketEventBase):
    event_type: ClassVar[str] = "book_delta"
    side: BookSide
    action: BookAction
    price: FixedPoint
    quantity: FixedPoint
    previous_sequence: int

    def __post_init__(self) -> None:
        super().__post_init__()
        if not isinstance(self.previous_sequence, int) or isinstance(self.previous_sequence, bool):
            raise ValidationError("previous_sequence must be an integer")
        if self.previous_sequence < 0 or self.previous_sequence >= self.sequence:
            raise ValidationError("previous_sequence must precede sequence")
        if not isinstance(self.side, BookSide):
            raise ValidationError("side must be a BookSide")
        if not isinstance(self.action, BookAction):
            raise ValidationError("action must be a BookAction")
        _positive(self.price, "price")
        _non_negative(self.quantity, "quantity")
        if self.action is BookAction.DELETE and self.quantity.units != 0:
            raise ValidationError("delete delta quantity must be zero")
        if self.action is BookAction.UPSERT and self.quantity.units == 0:
            raise ValidationError("upsert delta quantity must be positive")


@dataclass(frozen=True, kw_only=True)
class FundingRateEvent(_MarketEventBase):
    event_type: ClassVar[str] = "funding_rate"
    rate: float
    interval_start: datetime
    interval_end: datetime

    def __post_init__(self) -> None:
        super().__post_init__()
        if isinstance(self.rate, bool) or not isinstance(self.rate, (int, float)):
            raise ValidationError("funding rate must be numeric")
        if not math.isfinite(float(self.rate)):
            raise ValidationError("funding rate must be finite")
        interval_start = ensure_utc_datetime(self.interval_start, field="interval_start")
        interval_end = ensure_utc_datetime(self.interval_end, field="interval_end")
        if interval_end <= interval_start:
            raise ValidationError("funding interval must be positive")
        object.__setattr__(self, "rate", float(self.rate))
        object.__setattr__(self, "interval_start", interval_start)
        object.__setattr__(self, "interval_end", interval_end)


@dataclass(frozen=True, kw_only=True)
class MarkPriceEvent(_MarketEventBase):
    event_type: ClassVar[str] = "mark_price"
    price: FixedPoint

    def __post_init__(self) -> None:
        super().__post_init__()
        _positive(self.price, "price")


@dataclass(frozen=True, kw_only=True)
class CorporateActionEvent(_MarketEventBase):
    event_type: ClassVar[str] = "corporate_action"
    action_type: str
    effective_date: date
    ratio: FixedPoint | None = None
    cash_amount: FixedPoint | None = None
    currency: str | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "action_type", _required_text(self.action_type, "action_type"))
        if not isinstance(self.effective_date, date) or isinstance(self.effective_date, datetime):
            raise ValidationError("effective_date must be a date")
        if self.ratio is None and self.cash_amount is None:
            raise ValidationError("corporate action requires ratio or cash_amount")
        if self.ratio is not None:
            _non_negative(self.ratio, "ratio")
        if self.cash_amount is not None:
            _non_negative(self.cash_amount, "cash_amount")
            object.__setattr__(self, "currency", _required_text(self.currency, "currency"))
        elif self.currency is not None:
            raise ValidationError("currency requires cash_amount")


@dataclass(frozen=True, kw_only=True)
class StatusEvent(_MarketEventBase):
    event_type: ClassVar[str] = "status"
    status: str
    reason: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "status", _required_text(self.status, "status"))
        if not isinstance(self.reason, str):
            raise ValidationError("reason must be a string")


MarketEvent: TypeAlias = (
    QuoteEvent
    | TradeEvent
    | BarEvent
    | BookSnapshotEvent
    | BookDeltaEvent
    | FundingRateEvent
    | MarkPriceEvent
    | CorporateActionEvent
    | StatusEvent
)


def _time(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _base_payload(event: MarketEvent) -> dict[str, Any]:
    return {
        "event_type": event.event_type,
        "event_id": event.event_id,
        "instrument_id": event.instrument_id,
        "event_time": _time(event.event_time),
        "received_at": _time(event.received_at),
        "available_at": _time(event.available_at),
        "source": event.source,
        "trading_day": event.trading_day.isoformat(),
        "session_id": event.session_id,
        "sequence": event.sequence,
    }


def _level_payload(level: BookLevel) -> dict[str, Any]:
    return {
        "price": fixed_point_payload(level.price),
        "quantity": fixed_point_payload(level.quantity),
        "order_count": level.order_count,
    }


def market_event_payload(event: MarketEvent) -> dict[str, Any]:
    payload = _base_payload(event)
    if isinstance(event, QuoteEvent):
        payload.update(
            bid_price=fixed_point_payload(event.bid_price),
            bid_quantity=fixed_point_payload(event.bid_quantity),
            ask_price=fixed_point_payload(event.ask_price),
            ask_quantity=fixed_point_payload(event.ask_quantity),
        )
    elif isinstance(event, TradeEvent):
        payload.update(
            price=fixed_point_payload(event.price),
            quantity=fixed_point_payload(event.quantity),
            aggressor_side=event.aggressor_side.value,
        )
    elif isinstance(event, BarEvent):
        payload.update(
            bar_start=_time(event.bar_start),
            bar_end=_time(event.bar_end),
            open_price=fixed_point_payload(event.open_price),
            high_price=fixed_point_payload(event.high_price),
            low_price=fixed_point_payload(event.low_price),
            close_price=fixed_point_payload(event.close_price),
            volume=fixed_point_payload(event.volume),
            is_complete=event.is_complete,
        )
    elif isinstance(event, BookSnapshotEvent):
        payload.update(
            bids=[_level_payload(level) for level in event.bids],
            asks=[_level_payload(level) for level in event.asks],
        )
    elif isinstance(event, BookDeltaEvent):
        payload.update(
            side=event.side.value,
            action=event.action.value,
            price=fixed_point_payload(event.price),
            quantity=fixed_point_payload(event.quantity),
            previous_sequence=event.previous_sequence,
        )
    elif isinstance(event, FundingRateEvent):
        payload.update(
            rate=event.rate,
            interval_start=_time(event.interval_start),
            interval_end=_time(event.interval_end),
        )
    elif isinstance(event, MarkPriceEvent):
        payload.update(price=fixed_point_payload(event.price))
    elif isinstance(event, CorporateActionEvent):
        payload.update(
            action_type=event.action_type,
            effective_date=event.effective_date.isoformat(),
            ratio=fixed_point_payload(event.ratio) if event.ratio else None,
            cash_amount=(fixed_point_payload(event.cash_amount) if event.cash_amount else None),
            currency=event.currency,
        )
    elif isinstance(event, StatusEvent):
        payload.update(status=event.status, reason=event.reason)
    else:
        raise TypeError(f"Unsupported market event: {type(event).__name__}")
    return payload
