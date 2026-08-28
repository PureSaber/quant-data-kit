"""Cross-asset reference-data contracts for schema v2."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from types import MappingProxyType
from typing import Any

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint
from quant_data_kit.temporal_v2 import ensure_utc_datetime


class AssetClass(str, Enum):
    CASH = "cash"
    EQUITY = "equity"
    ETF = "etf"
    FUND = "fund"
    FUTURE = "future"
    OPTION = "option"
    BOND = "bond"
    FX = "fx"
    CRYPTO = "crypto"
    INDEX = "index"
    OTHER = "other"


class MarginMode(str, Enum):
    NONE = "none"
    CASH = "cash"
    CROSS = "cross"
    ISOLATED = "isolated"
    PORTFOLIO = "portfolio"


class SessionPhase(str, Enum):
    PREOPEN = "preopen"
    AUCTION = "auction"
    CONTINUOUS = "continuous"
    BREAK = "break"
    CLOSE = "close"
    AFTER_HOURS = "after_hours"


def _required_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_text(value: str | None, field_name: str) -> str | None:
    return _required_text(value, field_name) if value is not None else None


def _validate_knowledge_interval(
    available_at: datetime,
    superseded_at: datetime | None,
) -> tuple[datetime, datetime | None]:
    available = ensure_utc_datetime(available_at, field="available_at")
    superseded = (
        ensure_utc_datetime(superseded_at, field="superseded_at")
        if superseded_at is not None
        else None
    )
    if superseded is not None and superseded <= available:
        raise ValidationError("superseded_at must be later than available_at")
    return available, superseded


@dataclass(frozen=True)
class InstrumentSpec:
    instrument_id: str
    asset_class: AssetClass
    product_type: str
    venue: str
    native_symbol: str
    settlement_currency: str
    price_tick: FixedPoint
    quantity_step: FixedPoint
    contract_multiplier: FixedPoint
    calendar_id: str
    effective_from: datetime
    available_at: datetime
    base_currency: str | None = None
    quote_currency: str | None = None
    margin_mode: MarginMode = MarginMode.NONE
    inverse: bool = False
    effective_to: datetime | None = None
    superseded_at: datetime | None = None
    underlying_id: str | None = None
    expiry_date: date | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in (
            "instrument_id",
            "product_type",
            "venue",
            "native_symbol",
            "settlement_currency",
            "calendar_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        if not isinstance(self.asset_class, AssetClass):
            raise ValidationError("asset_class must be an AssetClass")
        if not isinstance(self.margin_mode, MarginMode):
            raise ValidationError("margin_mode must be a MarginMode")
        if not isinstance(self.inverse, bool):
            raise ValidationError("inverse must be boolean")
        for field_name in ("price_tick", "quantity_step", "contract_multiplier"):
            value = getattr(self, field_name)
            if not isinstance(value, FixedPoint) or not value.is_positive():
                raise ValidationError(f"{field_name} must be a positive FixedPoint")
        effective_from = ensure_utc_datetime(self.effective_from, field="effective_from")
        effective_to = (
            ensure_utc_datetime(self.effective_to, field="effective_to")
            if self.effective_to is not None
            else None
        )
        if effective_to is not None and effective_to <= effective_from:
            raise ValidationError("effective_to must be later than effective_from")
        available, superseded = _validate_knowledge_interval(
            self.available_at, self.superseded_at
        )
        if not isinstance(self.metadata, Mapping) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in self.metadata.items()
        ):
            raise ValidationError("metadata must be a string-to-string mapping")
        object.__setattr__(
            self, "base_currency", _optional_text(self.base_currency, "base_currency")
        )
        object.__setattr__(
            self, "quote_currency", _optional_text(self.quote_currency, "quote_currency")
        )
        object.__setattr__(
            self, "underlying_id", _optional_text(self.underlying_id, "underlying_id")
        )
        object.__setattr__(self, "effective_from", effective_from)
        object.__setattr__(self, "effective_to", effective_to)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "superseded_at", superseded)
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True)
class SymbolMapping:
    source: str
    provider_symbol: str
    instrument_id: str
    effective_from: datetime
    available_at: datetime
    effective_to: datetime | None = None
    superseded_at: datetime | None = None

    def __post_init__(self) -> None:
        for field_name in ("source", "provider_symbol", "instrument_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        effective_from = ensure_utc_datetime(self.effective_from, field="effective_from")
        effective_to = (
            ensure_utc_datetime(self.effective_to, field="effective_to")
            if self.effective_to is not None
            else None
        )
        if effective_to is not None and effective_to <= effective_from:
            raise ValidationError("effective_to must be later than effective_from")
        available, superseded = _validate_knowledge_interval(
            self.available_at, self.superseded_at
        )
        object.__setattr__(self, "effective_from", effective_from)
        object.__setattr__(self, "effective_to", effective_to)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "superseded_at", superseded)


@dataclass(frozen=True)
class TradingSession:
    session_id: str
    calendar_id: str
    venue: str
    trading_day: date
    phase: SessionPhase
    opens_at: datetime
    closes_at: datetime
    available_at: datetime
    superseded_at: datetime | None = None

    def __post_init__(self) -> None:
        for field_name in ("session_id", "calendar_id", "venue"):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        if not isinstance(self.trading_day, date) or isinstance(self.trading_day, datetime):
            raise ValidationError("trading_day must be a date")
        if not isinstance(self.phase, SessionPhase):
            raise ValidationError("phase must be a SessionPhase")
        opens_at = ensure_utc_datetime(self.opens_at, field="opens_at")
        closes_at = ensure_utc_datetime(self.closes_at, field="closes_at")
        if closes_at <= opens_at:
            raise ValidationError("closes_at must be later than opens_at")
        available, superseded = _validate_knowledge_interval(
            self.available_at, self.superseded_at
        )
        object.__setattr__(self, "opens_at", opens_at)
        object.__setattr__(self, "closes_at", closes_at)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "superseded_at", superseded)


def dataclass_payload(value: Any) -> dict[str, Any]:
    """Serialize public reference values without losing fixed-point semantics."""
    if isinstance(value, InstrumentSpec):
        return {
            "instrument_id": value.instrument_id,
            "asset_class": value.asset_class.value,
            "product_type": value.product_type,
            "venue": value.venue,
            "native_symbol": value.native_symbol,
            "base_currency": value.base_currency,
            "quote_currency": value.quote_currency,
            "settlement_currency": value.settlement_currency,
            "price_tick": fixed_point_payload(value.price_tick),
            "quantity_step": fixed_point_payload(value.quantity_step),
            "contract_multiplier": fixed_point_payload(value.contract_multiplier),
            "calendar_id": value.calendar_id,
            "margin_mode": value.margin_mode.value,
            "inverse": value.inverse,
            "effective_from": _time(value.effective_from),
            "effective_to": _optional_time(value.effective_to),
            "available_at": _time(value.available_at),
            "superseded_at": _optional_time(value.superseded_at),
            "underlying_id": value.underlying_id,
            "expiry_date": value.expiry_date.isoformat() if value.expiry_date else None,
            "metadata": dict(value.metadata),
        }
    if isinstance(value, SymbolMapping):
        return {
            "source": value.source,
            "provider_symbol": value.provider_symbol,
            "instrument_id": value.instrument_id,
            "effective_from": _time(value.effective_from),
            "effective_to": _optional_time(value.effective_to),
            "available_at": _time(value.available_at),
            "superseded_at": _optional_time(value.superseded_at),
        }
    if isinstance(value, TradingSession):
        return {
            "session_id": value.session_id,
            "calendar_id": value.calendar_id,
            "venue": value.venue,
            "trading_day": value.trading_day.isoformat(),
            "phase": value.phase.value,
            "opens_at": _time(value.opens_at),
            "closes_at": _time(value.closes_at),
            "available_at": _time(value.available_at),
            "superseded_at": _optional_time(value.superseded_at),
        }
    raise TypeError(f"Unsupported public contract type: {type(value).__name__}")


def fixed_point_payload(value: FixedPoint) -> dict[str, int]:
    return {"units": value.units, "scale": value.scale}


def _time(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _optional_time(value: datetime | None) -> str | None:
    return _time(value) if value is not None else None
