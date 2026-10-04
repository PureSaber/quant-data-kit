"""Point-in-time FX facts with explicit observation and knowledge boundaries."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from .common import utc
from .dividends import _currency, _decimal_text, _keys, _mapping, _text, _timestamp
from .reconciliation import canonical

PIT_FX_SCHEMA_ID = "puresaber.pit-fx/1"


@dataclass(frozen=True)
class PitFxRate:
    event_id: str
    base_currency: str
    quote_currency: str
    rate_text: str
    rate_convention: str
    observed_at: str
    available_at: str
    captured_at: str
    source: str
    evidence_id: str

    def __post_init__(self) -> None:
        event = _text(self.event_id, "event_id")
        base = _currency(self.base_currency, "base_currency")
        quote = _currency(self.quote_currency, "quote_currency")
        if base == quote:
            raise ValueError(
                "same-currency valuation uses identity and must not carry a market quote"
            )
        rate = _decimal_text(self.rate_text, "rate_text", positive=True)
        if self.rate_convention != "quote_per_base":
            raise ValueError("rate_convention must be 'quote_per_base'")
        observed = _timestamp(self.observed_at, "observed_at")
        available = _timestamp(self.available_at, "available_at")
        captured = _timestamp(self.captured_at, "captured_at")
        if utc(observed) > utc(available):
            raise ValueError("FX cannot be available before it was observed")
        if utc(available) > utc(captured):
            raise ValueError("FX available_at cannot follow captured_at")
        object.__setattr__(self, "event_id", event)
        object.__setattr__(self, "base_currency", base)
        object.__setattr__(self, "quote_currency", quote)
        object.__setattr__(self, "rate_text", rate)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "captured_at", captured)
        object.__setattr__(self, "source", _text(self.source, "source"))
        object.__setattr__(self, "evidence_id", _text(self.evidence_id, "evidence_id"))

    @property
    def rate(self) -> Decimal:
        return Decimal(self.rate_text)

    def to_dict(self) -> dict[str, str]:
        return {
            "schema": PIT_FX_SCHEMA_ID,
            "event_id": self.event_id,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "rate_text": self.rate_text,
            "rate_convention": self.rate_convention,
            "observed_at": self.observed_at,
            "available_at": self.available_at,
            "captured_at": self.captured_at,
            "source": self.source,
            "evidence_id": self.evidence_id,
        }

    def to_json(self) -> str:
        return canonical(self.to_dict()).decode("utf-8")

    def fingerprint(self) -> str:
        return hashlib.sha256(canonical(self.to_dict())).hexdigest()

    @classmethod
    def from_dict(cls, value: object) -> PitFxRate:
        value = _mapping(value, "PIT FX rate")
        _keys(
            value,
            required={
                "schema",
                "event_id",
                "base_currency",
                "quote_currency",
                "rate_text",
                "rate_convention",
                "observed_at",
                "available_at",
                "captured_at",
                "source",
                "evidence_id",
            },
            context="PIT FX rate",
        )
        if value["schema"] != PIT_FX_SCHEMA_ID:
            raise ValueError("unsupported PIT FX schema")
        return cls(**{key: value[key] for key in value if key != "schema"})

    @classmethod
    def from_json(cls, value: str | bytes) -> PitFxRate:
        if not isinstance(value, (str, bytes)):
            raise TypeError("serialized PIT FX must be str or bytes")
        try:
            payload = json.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError("invalid PIT FX JSON") from exc
        return cls.from_dict(payload)


@dataclass(frozen=True)
class FxAsOf:
    base_currency: str
    quote_currency: str
    rate: Decimal
    identity: bool
    event_id: str | None
    observed_at: str | None
    available_at: str | None


def select_pit_fx(
    records: Iterable[PitFxRate],
    *,
    base_currency: str,
    quote_currency: str,
    cutoff: str,
) -> FxAsOf:
    """Select one unambiguous quote known by cutoff; never invert or use a future fixing."""

    base = _currency(base_currency, "base_currency")
    quote = _currency(quote_currency, "quote_currency")
    at = _timestamp(cutoff, "cutoff")
    if base == quote:
        return FxAsOf(base, quote, Decimal(1), True, None, None, None)

    unique: dict[str, PitFxRate] = {}
    for record in records:
        if not isinstance(record, PitFxRate):
            raise TypeError("records must contain PitFxRate values")
        prior = unique.get(record.event_id)
        if prior is not None and prior.fingerprint() != record.fingerprint():
            raise ValueError("FX event_id was reused with conflicting facts")
        unique[record.event_id] = record

    eligible = [
        record
        for record in unique.values()
        if record.base_currency == base
        and record.quote_currency == quote
        and utc(record.observed_at) <= utc(at)
        and utc(record.available_at) <= utc(at)
    ]
    if not eligible:
        raise ValueError(f"missing PIT FX for {base}/{quote} at cutoff")
    latest_observed = max(utc(record.observed_at) for record in eligible)
    latest = [record for record in eligible if utc(record.observed_at) == latest_observed]
    if len(latest) != 1:
        raise ValueError(f"ambiguous PIT FX for {base}/{quote} at cutoff")
    selected = latest[0]
    return FxAsOf(
        base,
        quote,
        selected.rate,
        False,
        selected.event_id,
        selected.observed_at,
        selected.available_at,
    )


def fx_rate_asof(
    records: Iterable[PitFxRate],
    *,
    base_currency: str,
    quote_currency: str,
    cutoff: str,
) -> Decimal:
    return select_pit_fx(
        records,
        base_currency=base_currency,
        quote_currency=quote_currency,
        cutoff=cutoff,
    ).rate
