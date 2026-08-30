"""Provider-specific live book synchronization above the frozen v2 adapters."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from quant_data_kit.adapters_v2 import (
    AdapterContext,
    AdapterInstrument,
    BinanceFixtureAdapter,
    OKXFixtureAdapter,
)
from quant_data_kit.capture_v2.models import (
    MarketKind,
    RawFrame,
    StreamConfig,
    SymbolMappingResolver,
)
from quant_data_kit.exceptions import ValidationError


class ResyncRequired(ValidationError):
    """A transport or sequence discontinuity that invalidates the local book."""


@dataclass(frozen=True)
class Observation:
    event: str
    reason: str
    details: Mapping[str, Any]


@dataclass(frozen=True)
class NormalizationOutcome:
    records: tuple[dict[str, Any], ...] = ()
    observations: tuple[Observation, ...] = ()
    snapshot_admitted: bool = False


def _json_object(payload: bytes, provider: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ResyncRequired(f"{provider} message is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ResyncRequired(f"{provider} message must be a JSON object")
    if provider == "binance" and isinstance(value.get("data"), dict):
        value = dict(value["data"])
    return value


def _received_milliseconds(received_at: datetime) -> int:
    """Round upward so Normalized available_at is never earlier than local receipt."""
    return math.ceil(received_at.timestamp() * 1000)


def _provider_event_milliseconds(message: Mapping[str, Any], *fields: str) -> int:
    for field in fields:
        value = message.get(field)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError) as exc:
                raise ResyncRequired(f"provider event time {field} must be milliseconds") from exc
    raise ResyncRequired(f"provider event time is missing; expected one of {fields}")


def _utc_event_datetime(milliseconds: int) -> datetime:
    from datetime import timezone

    try:
        return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)
    except (OSError, OverflowError, ValueError) as exc:
        raise ResyncRequired("provider event time is outside supported range") from exc


def _price(value: Any, field_name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ResyncRequired(f"{field_name} is not a decimal price") from exc
    if not result.is_finite() or result <= 0:
        raise ResyncRequired(f"{field_name} must be a finite positive price")
    return result


def _quantity(value: Any, field_name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ResyncRequired(f"{field_name} is not a decimal quantity") from exc
    if not result.is_finite() or result < 0:
        raise ResyncRequired(f"{field_name} must be a finite non-negative quantity")
    return result


class _TrackedBook:
    def __init__(self) -> None:
        self.bids: dict[Decimal, str] = {}
        self.asks: dict[Decimal, str] = {}

    def snapshot(self, bids: list[Any], asks: list[Any]) -> None:
        staged_bids = self._snapshot_side(bids, "bids")
        staged_asks = self._snapshot_side(asks, "asks")
        if staged_bids and staged_asks and max(staged_bids) >= min(staged_asks):
            raise ResyncRequired("provider snapshot is crossed or locked")
        self.bids, self.asks = staged_bids, staged_asks

    @staticmethod
    def _snapshot_side(levels: list[Any], field_name: str) -> dict[Decimal, str]:
        result: dict[Decimal, str] = {}
        for index, item in enumerate(levels):
            if not isinstance(item, list) or len(item) < 2:
                raise ResyncRequired(f"{field_name}[{index}] must be a price/quantity level")
            price = _price(item[0], f"{field_name}[{index}].price")
            quantity = _quantity(item[1], f"{field_name}[{index}].quantity")
            if quantity == 0:
                raise ResyncRequired("provider snapshot cannot contain zero quantity")
            if price in result:
                raise ResyncRequired(f"{field_name} contains duplicate prices")
            result[price] = str(item[1])
        return result

    def filter_delta(
        self,
        bids: list[Any],
        asks: list[Any],
    ) -> tuple[
        list[Any], list[Any], tuple[Observation, ...], dict[Decimal, str], dict[Decimal, str]
    ]:
        staged_bids = dict(self.bids)
        staged_asks = dict(self.asks)
        observations: list[Observation] = []
        kept_bids = self._filter_side(bids, staged_bids, "bid", observations)
        kept_asks = self._filter_side(asks, staged_asks, "ask", observations)
        if staged_bids and staged_asks and max(staged_bids) >= min(staged_asks):
            raise ResyncRequired("provider delta would cross or lock the local book")
        return kept_bids, kept_asks, tuple(observations), staged_bids, staged_asks

    @staticmethod
    def _filter_side(
        levels: list[Any],
        staged: dict[Decimal, str],
        side: str,
        observations: list[Observation],
    ) -> list[Any]:
        kept: list[Any] = []
        seen: set[Decimal] = set()
        for index, item in enumerate(levels):
            if not isinstance(item, list) or len(item) < 2:
                raise ResyncRequired(f"{side}[{index}] must be a price/quantity level")
            price = _price(item[0], f"{side}[{index}].price")
            quantity = _quantity(item[1], f"{side}[{index}].quantity")
            if price in seen:
                raise ResyncRequired(f"{side} delta contains duplicate prices")
            seen.add(price)
            if quantity == 0:
                if price not in staged:
                    observations.append(
                        Observation(
                            event="absent_level_delete",
                            reason="provider documents this as a normal no-op",
                            details={"side": side, "price": str(item[0])},
                        )
                    )
                    continue
                staged.pop(price)
            else:
                staged[price] = str(item[1])
            kept.append(item)
        return kept

    def commit(self, bids: dict[Decimal, str], asks: dict[Decimal, str]) -> None:
        self.bids, self.asks = bids, asks


class BinanceBookSynchronizer:
    """Bridge buffered Binance WS updates to an HTTPS snapshot and strict v2 events."""

    def __init__(self, stream: StreamConfig, mappings: SymbolMappingResolver) -> None:
        if stream.provider.value != "binance":
            raise ValidationError("BinanceBookSynchronizer requires a Binance stream")
        self.stream = stream
        self.mappings = mappings
        self.adapter = BinanceFixtureAdapter(
            AdapterContext(
                provider="binance",
                venue=stream.venue,
                instruments={
                    stream.native_symbol: AdapterInstrument(
                        stream.instrument_id, stream.price_scale, stream.quantity_scale
                    )
                },
            ),
            event_id_namespace=stream.instrument_id,
        )
        self.book = _TrackedBook()
        self._snapshot_id: int | None = None
        self._last_update_id: int | None = None
        self._bridged = False

    @property
    def live(self) -> bool:
        return self._bridged

    def admit_snapshot(self, frame: RawFrame) -> NormalizationOutcome:
        message = _json_object(frame.payload, "binance")
        try:
            snapshot_id = int(message["lastUpdateId"])
            bids, asks = list(message["bids"]), list(message["asks"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ResyncRequired("Binance REST snapshot is malformed") from exc
        self.book.snapshot(bids, asks)
        received_ms = _received_milliseconds(frame.received_at)
        event_time = frame.event_time or frame.received_at
        event_ms = _received_milliseconds(event_time)
        self._assert_mapping(event_time, frame.received_at)
        canonical = {
            "e": "depthSnapshot",
            "s": self.stream.native_symbol,
            "E": event_ms,
            "T": event_ms,
            "received_at": received_ms,
            "lastUpdateId": snapshot_id,
            "bids": bids,
            "asks": asks,
        }
        records = self.adapter.adapt(canonical)
        self._snapshot_id = snapshot_id
        self._last_update_id = snapshot_id
        self._bridged = False
        return NormalizationOutcome(records=tuple(records), snapshot_admitted=True)

    def admit_update(self, frame: RawFrame) -> NormalizationOutcome:
        if self._snapshot_id is None or self._last_update_id is None:
            raise ResyncRequired("Binance update arrived before REST snapshot admission")
        message = _json_object(frame.payload, "binance")
        if message.get("e") != "depthUpdate" or str(message.get("s")) != self.stream.native_symbol:
            raise ResyncRequired("Binance stream delivered an unexpected event or symbol")
        try:
            first_update = int(message["U"])
            final_update = int(message["u"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ResyncRequired("Binance update IDs are malformed") from exc
        if final_update < first_update:
            raise ResyncRequired("Binance update range is reversed")
        if not self._bridged:
            if final_update <= self._snapshot_id:
                return NormalizationOutcome(
                    observations=(
                        Observation(
                            event="stale_buffered_update",
                            reason="u<=lastUpdateId",
                            details={
                                "U": first_update,
                                "u": final_update,
                                "lastUpdateId": self._snapshot_id,
                            },
                        ),
                    )
                )
            if not first_update <= self._snapshot_id <= final_update:
                raise ResyncRequired(
                    "Binance buffered stream did not bridge REST lastUpdateId: "
                    f"U={first_update}, lastUpdateId={self._snapshot_id}, u={final_update}"
                )
            provider_previous = self._snapshot_id
        else:
            if final_update <= self._last_update_id:
                return NormalizationOutcome(
                    observations=(
                        Observation(
                            event="duplicate_or_old_update",
                            reason="u<=previous_u",
                            details={"u": final_update, "previous_u": self._last_update_id},
                        ),
                    )
                )
            if self.stream.market is MarketKind.USDT_PERPETUAL:
                try:
                    previous_update = int(message["pu"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise ResyncRequired("Binance USD-M update is missing pu") from exc
                if previous_update != self._last_update_id:
                    raise ResyncRequired(
                        "Binance USD-M pu chain broke: "
                        f"expected={self._last_update_id}, actual={previous_update}"
                    )
            elif not first_update <= self._last_update_id + 1 <= final_update:
                raise ResyncRequired(
                    "Binance Spot update IDs are discontinuous: "
                    f"U={first_update}, previous_u={self._last_update_id}, u={final_update}"
                )
            provider_previous = self._last_update_id
        event_ms = _provider_event_milliseconds(message, "T", "E")
        event_time = _utc_event_datetime(event_ms)
        self._assert_mapping(event_time, frame.received_at)
        bids = list(message.get("b", []))
        asks = list(message.get("a", []))
        kept_bids, kept_asks, observations, staged_bids, staged_asks = self.book.filter_delta(
            bids, asks
        )
        canonical = deepcopy(message)
        canonical.update(
            {
                "e": "depthUpdate",
                "s": self.stream.native_symbol,
                "pu": provider_previous,
                "b": kept_bids,
                "a": kept_asks,
                "received_at": _received_milliseconds(frame.received_at),
            }
        )
        try:
            records = self.adapter.adapt(canonical)
        except ValidationError as exc:
            raise ResyncRequired(f"Binance Normalized bridge rejected update: {exc}") from exc
        self.book.commit(staged_bids, staged_asks)
        self._last_update_id = final_update
        self._bridged = True
        return NormalizationOutcome(records=tuple(records), observations=observations)

    def _assert_mapping(self, event_time: datetime, known_at: datetime) -> None:
        try:
            instrument_id = self.mappings.resolve(
                self.stream.mapping_source,
                self.stream.native_symbol,
                event_time=event_time,
                known_at=known_at,
            )
        except ValidationError as exc:
            raise ResyncRequired(f"Binance SymbolMapping resolution failed: {exc}") from exc
        if instrument_id != self.stream.instrument_id:
            raise ResyncRequired("Binance SymbolMapping disagrees with stream instrument_id")


class OKXBookSynchronizer:
    """Apply current OKX books semantics: sequence continuity, no JSON checksum proof."""

    def __init__(self, stream: StreamConfig, mappings: SymbolMappingResolver) -> None:
        if stream.provider.value != "okx":
            raise ValidationError("OKXBookSynchronizer requires an OKX stream")
        self.stream = stream
        self.mappings = mappings
        self.adapter = OKXFixtureAdapter(
            AdapterContext(
                provider="okx",
                venue=stream.venue,
                instruments={
                    stream.native_symbol: AdapterInstrument(
                        stream.instrument_id, stream.price_scale, stream.quantity_scale
                    )
                },
            )
        )
        self.book = _TrackedBook()
        self._last_sequence: int | None = None

    @property
    def live(self) -> bool:
        return self._last_sequence is not None

    def admit_message(self, frame: RawFrame) -> NormalizationOutcome:
        envelope = _json_object(frame.payload, "okx")
        if "event" in envelope:
            event = str(envelope.get("event"))
            if event == "error":
                raise ResyncRequired(
                    f"OKX subscription error: {envelope.get('code')}:{envelope.get('msg')}"
                )
            return NormalizationOutcome(
                observations=(
                    Observation(
                        event="subscription_control",
                        reason=event,
                        details={"code": envelope.get("code"), "msg": envelope.get("msg")},
                    ),
                )
            )
        arg = envelope.get("arg")
        data = envelope.get("data")
        action = str(envelope.get("action", ""))
        if not isinstance(arg, dict) or not isinstance(data, list):
            raise ResyncRequired("OKX books envelope is malformed")
        if (
            arg.get("channel") != self.stream.channel
            or arg.get("instId") != self.stream.native_symbol
        ):
            raise ResyncRequired("OKX stream delivered an unexpected channel or symbol")
        all_records: list[dict[str, Any]] = []
        observations: list[Observation] = []
        snapshot_admitted = False
        for item in data:
            if not isinstance(item, dict):
                raise ResyncRequired("OKX books data item must be an object")
            outcome = self._admit_item(action, item, frame)
            all_records.extend(outcome.records)
            observations.extend(outcome.observations)
            snapshot_admitted = snapshot_admitted or outcome.snapshot_admitted
        return NormalizationOutcome(tuple(all_records), tuple(observations), snapshot_admitted)

    def _admit_item(
        self,
        action: str,
        item: Mapping[str, Any],
        frame: RawFrame,
    ) -> NormalizationOutcome:
        if action not in {"snapshot", "update"}:
            raise ResyncRequired(f"OKX books action is unsupported: {action!r}")
        try:
            sequence = int(item["seqId"])
            previous = int(item.get("prevSeqId", -1))
            event_ms = int(item["ts"])
            bids, asks = list(item.get("bids", [])), list(item.get("asks", []))
        except (KeyError, TypeError, ValueError) as exc:
            raise ResyncRequired("OKX books sequence or timestamp is malformed") from exc
        event_time = _utc_event_datetime(event_ms)
        try:
            instrument_id = self.mappings.resolve(
                self.stream.mapping_source,
                self.stream.native_symbol,
                event_time=event_time,
                known_at=frame.received_at,
            )
        except ValidationError as exc:
            raise ResyncRequired(f"OKX SymbolMapping resolution failed: {exc}") from exc
        if instrument_id != self.stream.instrument_id:
            raise ResyncRequired("OKX SymbolMapping disagrees with stream instrument_id")
        canonical = {
            **dict(item),
            "channel": self.stream.channel,
            "instId": self.stream.native_symbol,
            "action": action,
            "received_at": _received_milliseconds(frame.received_at),
            "checksum": 0,
        }
        checksum_observation = ()
        if item.get("checksum") not in (None, 0, "0"):
            checksum_observation = (
                Observation(
                    event="deprecated_checksum_ignored",
                    reason="OKX 2026 JSON checksum is not an integrity proof",
                    details={"received_checksum": item.get("checksum")},
                ),
            )
        if action == "snapshot":
            self.book.snapshot(bids, asks)
            try:
                records = self.adapter.adapt(canonical)
            except ValidationError as exc:
                raise ResyncRequired(f"OKX Normalized bridge rejected snapshot: {exc}") from exc
            self._last_sequence = sequence
            return NormalizationOutcome(tuple(records), checksum_observation, True)
        if self._last_sequence is None:
            raise ResyncRequired("OKX update arrived before a fresh books snapshot")
        if sequence < previous:
            try:
                self.adapter.adapt(canonical)
            except ValidationError:
                pass
            self._last_sequence = None
            raise ResyncRequired("OKX maintenance sequence reset requires a fresh snapshot")
        if previous != self._last_sequence:
            raise ResyncRequired(
                "OKX prevSeqId chain broke: "
                f"expected={self._last_sequence}, actual={previous}, seqId={sequence}"
            )
        if sequence == previous:
            if bids or asks:
                raise ResyncRequired("OKX equal-sequence heartbeat contained book levels")
            try:
                records = self.adapter.adapt(canonical)
            except ValidationError as exc:
                raise ResyncRequired(f"OKX heartbeat was rejected: {exc}") from exc
            return NormalizationOutcome(
                tuple(records),
                checksum_observation
                + (
                    Observation(
                        event="book_heartbeat",
                        reason="empty asks/bids with seqId==prevSeqId",
                        details={"seqId": sequence},
                    ),
                ),
            )
        kept_bids, kept_asks, absent, staged_bids, staged_asks = self.book.filter_delta(bids, asks)
        canonical["bids"] = kept_bids
        canonical["asks"] = kept_asks
        try:
            records = self.adapter.adapt(canonical)
        except ValidationError as exc:
            raise ResyncRequired(f"OKX Normalized bridge rejected update: {exc}") from exc
        self.book.commit(staged_bids, staged_asks)
        self._last_sequence = sequence
        return NormalizationOutcome(tuple(records), checksum_observation + absent)
