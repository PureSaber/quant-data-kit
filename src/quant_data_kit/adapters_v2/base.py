"""Provider-adapter primitives for strict v2 fixture normalization."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint
from quant_data_kit.schemas_v2 import validate_event_stream

_SEQUENCE_FACTOR = 1_000_000


@dataclass(frozen=True)
class AdapterInstrument:
    instrument_id: str
    price_scale: int
    quantity_scale: int


@dataclass(frozen=True)
class AdapterContext:
    provider: str
    venue: str
    instruments: Mapping[str, AdapterInstrument]
    session_kind: str = "24x7"

    def instrument(self, provider_symbol: str) -> AdapterInstrument:
        try:
            return self.instruments[provider_symbol]
        except KeyError as exc:
            raise ValidationError(
                f"No stable instrument mapping for {self.provider}:{provider_symbol}"
            ) from exc

    def session_id(self, provider_symbol: str, event_time: datetime) -> str:
        instrument = self.instrument(provider_symbol)
        if self.session_kind == "24x7":
            return f"{self.provider}-24x7-{instrument.instrument_id}"
        return f"{self.venue}-{event_time.date().isoformat()}-{instrument.instrument_id}"


class ProviderAdapter(Protocol):
    certification_status: str

    def adapt(self, message: Mapping[str, Any]) -> list[dict[str, Any]]: ...


def utc_from_milliseconds(value: int | str, field_name: str) -> datetime:
    if isinstance(value, bool):
        raise ValidationError(f"{field_name} must be epoch milliseconds")
    try:
        parsed = datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError) as exc:
        raise ValidationError(f"{field_name} must be epoch milliseconds") from exc
    return parsed


def utc_from_text(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError(f"{field_name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValidationError(f"{field_name} must be UTC")
    return parsed.astimezone(timezone.utc)


def fixed(value: str | int | Decimal, scale: int, field_name: str) -> FixedPoint:
    try:
        result = FixedPoint.from_decimal(value, scale)
    except (ArithmeticError, ValueError, ValidationError) as exc:
        raise ValidationError(f"{field_name} is invalid at scale {scale}: {value!r}") from exc
    return result


def event_identity(
    context: AdapterContext,
    provider_symbol: str,
    *,
    event_time: datetime,
    event_id: str,
    received_at: datetime,
    sequence: int | None,
    trading_day: date | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    if received_at < event_time:
        raise ValidationError("provider received_at precedes event_time")
    return {
        "event_id": event_id,
        "instrument_id": context.instrument(provider_symbol).instrument_id,
        "event_time": event_time,
        "received_at": received_at,
        "available_at": received_at,
        "source": context.provider,
        "trading_day": trading_day or event_time.date(),
        "session_id": session_id or context.session_id(provider_symbol, event_time),
        "sequence": sequence,
    }


class BookSequenceNormalizer:
    """Expand one provider update into stable per-level v2 sequences without losing continuity."""

    def __init__(self) -> None:
        self._provider_sequence: dict[str, int] = {}
        self._emitted_sequence: dict[str, int] = {}

    @contextmanager
    def transaction(self) -> Iterable[None]:
        """Roll sequence state back when any downstream conversion or validation fails."""
        provider_before = dict(self._provider_sequence)
        emitted_before = dict(self._emitted_sequence)
        try:
            yield
        except Exception:
            self._provider_sequence = provider_before
            self._emitted_sequence = emitted_before
            raise

    def snapshot(self, provider_symbol: str, provider_sequence: int) -> int:
        previous = self._provider_sequence.get(provider_symbol)
        if previous is not None and provider_sequence <= previous:
            raise ValidationError("provider BookSnapshot sequence did not advance")
        emitted = provider_sequence * _SEQUENCE_FACTOR
        self._provider_sequence[provider_symbol] = provider_sequence
        self._emitted_sequence[provider_symbol] = emitted
        return emitted

    def delta(
        self,
        provider_symbol: str,
        *,
        provider_previous_sequence: int,
        provider_sequence: int,
        level_count: int,
    ) -> tuple[tuple[int, int], ...]:
        if provider_symbol not in self._provider_sequence:
            raise ValidationError("provider BookDelta arrived before BookSnapshot")
        expected_provider = self._provider_sequence[provider_symbol]
        if provider_previous_sequence != expected_provider:
            raise ValidationError(
                f"provider BookDelta gap: expected previous={expected_provider}, "
                f"actual={provider_previous_sequence}"
            )
        if provider_sequence <= provider_previous_sequence:
            raise ValidationError("provider BookDelta sequence did not advance")
        if not 0 < level_count < _SEQUENCE_FACTOR:
            raise ValidationError("provider BookDelta level count is unsupported")
        previous_emitted = self._emitted_sequence[provider_symbol]
        pairs: list[tuple[int, int]] = []
        for index in range(1, level_count + 1):
            emitted = provider_sequence * _SEQUENCE_FACTOR + index
            pairs.append((previous_emitted, emitted))
            previous_emitted = emitted
        self._provider_sequence[provider_symbol] = provider_sequence
        self._emitted_sequence[provider_symbol] = previous_emitted
        return tuple(pairs)


def adapt_fixture_messages(
    adapter: ProviderAdapter,
    messages: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records = [record for message in messages for record in adapter.adapt(message)]
    validate_event_stream(records)
    return records
