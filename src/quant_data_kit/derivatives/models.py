"""Strict, timezone-aware derivative research records; decimal strings on disk."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def decimal(value, name="value") -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"{name}: a finite decimal is required")  # noqa: TRY004 - input validation
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{name}: invalid decimal") from exc
    if not result.is_finite() or abs(result) > Decimal("1e18"):
        raise ValueError(f"{name}: finite decimal within 1e18 required")
    return result


def utc(value) -> datetime:
    try:
        result = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
    except ValueError as exc:
        raise ValueError("an ISO timestamp with timezone is required") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("timezone-naive timestamp is not allowed")
    return result.astimezone(timezone.utc)


def text(value, name):
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 200
        or any(ord(c) < 32 for c in value)
    ):
        raise ValueError(f"{name}: nonempty text of at most 200 characters required")
    return value


def record(cls, raw):
    if not isinstance(raw, dict) or set(raw) - {f.name for f in fields(cls)}:
        raise ValueError(f"unknown fields in {cls.__name__}")
    try:
        return cls(**raw)
    except TypeError as exc:
        raise ValueError(f"missing or invalid fields in {cls.__name__}") from exc


def payload(item):
    return {
        k: v.isoformat()
        if isinstance(v, (datetime, date))
        else str(v)
        if isinstance(v, Decimal)
        else v
        for k, v in asdict(item).items()
    }


@dataclass(frozen=True)
class Contract:
    instrument_id: str
    symbol: str
    product: str
    venue: str
    currency: str
    kind: str
    multiplier: Decimal
    tick: Decimal
    listed_at: datetime
    last_trade_at: datetime
    expiry: datetime
    known_at: datetime
    timezone: str
    settlement: str
    underlying: str = ""
    underlying_kind: str = "spot"
    option_right: str | None = None
    strike: Decimal | None = None
    exercise_style: str | None = None
    initial_margin: Decimal | None = None
    maintenance_margin: Decimal | None = None
    rules_source: str = "user-declared research assumptions"

    def __post_init__(self):
        for name in ("instrument_id", "symbol", "product", "venue", "currency", "rules_source"):
            text(getattr(self, name), name)
        if not re.fullmatch("[A-Z]{3}", self.currency):
            raise ValueError("currency must be a three-letter ISO code")
        if self.kind not in {"future", "option"} or self.settlement not in {"cash", "physical"}:
            raise ValueError("supported kind: future/option; settlement: cash/physical")
        if self.underlying_kind not in {"spot", "future"}:
            raise ValueError("underlying_kind must be spot or future")
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, TypeError) as exc:
            raise ValueError("unknown contract timezone") from exc
        for name in ("listed_at", "last_trade_at", "expiry", "known_at"):
            object.__setattr__(self, name, utc(getattr(self, name)))
        if not self.listed_at <= self.last_trade_at <= self.expiry:
            raise ValueError("contract lifecycle dates are inconsistent")
        for name in ("multiplier", "tick"):
            value = decimal(getattr(self, name), name)
            if value <= 0:
                raise ValueError(f"{name} must be positive")
            object.__setattr__(self, name, value)
        for name in ("initial_margin", "maintenance_margin"):
            value = getattr(self, name)
            if value is not None:
                value = decimal(value, name)
                if value < 0:
                    raise ValueError("margin cannot be negative")
                object.__setattr__(self, name, value)
        if (self.initial_margin is None) != (self.maintenance_margin is None):
            raise ValueError("initial and maintenance margin must be provided together")
        if self.initial_margin is not None and self.maintenance_margin > self.initial_margin:
            raise ValueError("maintenance margin exceeds initial margin")
        if self.kind == "option":
            text(self.underlying, "underlying")
            if self.option_right not in {"call", "put"} or self.exercise_style not in {
                "european",
                "american",
            }:
                raise ValueError("option needs call/put and european/american exercise style")
            strike = decimal(self.strike, "strike")
            if strike <= 0:
                raise ValueError("positive strike required for supported vanilla models")
            object.__setattr__(self, "strike", strike)
        elif any(v is not None for v in (self.option_right, self.strike, self.exercise_style)):
            raise ValueError("futures cannot contain option-only fields")


@dataclass(frozen=True)
class Quote:
    instrument_id: str
    at: datetime
    available_at: datetime
    session: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    open_interest: Decimal | None = None
    settlement: Decimal | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    underlying_price: Decimal | None = None

    def __post_init__(self):
        text(self.instrument_id, "instrument_id")
        for name in ("at", "available_at"):
            object.__setattr__(self, name, utc(getattr(self, name)))
        if self.available_at < self.at:
            raise ValueError("quote availability precedes observation")
        if date.fromisoformat(self.session).isoformat() != self.session:
            raise ValueError("session must be ISO date")
        for name in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
            "settlement",
            "bid",
            "ask",
            "underlying_price",
        ):
            value = getattr(self, name)
            if value is None and name in {"open", "high", "low", "close", "volume"}:
                raise ValueError(f"{name}: value is required")
            if value is not None:
                object.__setattr__(self, name, decimal(value, name))
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError("inconsistent OHLC prices")
        if self.volume < 0 or (self.open_interest is not None and self.open_interest < 0):
            raise ValueError("volume/open interest cannot be negative")
        if (self.bid is None) != (self.ask is None) or (
            self.bid is not None and self.bid > self.ask
        ):
            raise ValueError("both bid and ask required, with bid <= ask")
        if self.underlying_price is not None and self.underlying_price <= 0:
            raise ValueError("positive underlying required by supported option models")
