"""Deterministic L2 reconstruction from strict v2 snapshots and deltas."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.schemas_v2 import (
    BOOK_DELTA_EVENT_SCHEMA_ID,
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    validate_json_record,
)


class L2ReplayError(ValidationError):
    """Raised when an L2 stream cannot be deterministically reconstructed."""


@dataclass(frozen=True)
class ReconstructedLevel:
    price_units: int
    price_scale: int
    quantity_units: int
    quantity_scale: int
    order_count: int | None


@dataclass(frozen=True)
class BookCheckpoint:
    source: str
    instrument_id: str
    session_id: str
    sequence: int
    event_time: str
    bids: tuple[ReconstructedLevel, ...]
    asks: tuple[ReconstructedLevel, ...]
    state_sha256: str


@dataclass(frozen=True)
class L2ReplayResult:
    final_checkpoint: BookCheckpoint
    checkpoints: tuple[BookCheckpoint, ...]


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _level(payload: Mapping[str, Any]) -> ReconstructedLevel:
    price = payload["price"]
    quantity = payload["quantity"]
    return ReconstructedLevel(
        price_units=int(price["units"]),
        price_scale=int(price["scale"]),
        quantity_units=int(quantity["units"]),
        quantity_scale=int(quantity["scale"]),
        order_count=payload.get("order_count"),
    )


class L2BookReconstructor:
    """Apply one stream serially while preserving state on rejected updates."""

    def __init__(self) -> None:
        self._source: str | None = None
        self._instrument_id: str | None = None
        self._session_id: str | None = None
        self._sequence: int | None = None
        self._event_time: str | None = None
        self._price_scale: int | None = None
        self._bids: dict[int, ReconstructedLevel] = {}
        self._asks: dict[int, ReconstructedLevel] = {}

    @property
    def initialized(self) -> bool:
        return self._sequence is not None

    @property
    def sequence(self) -> int | None:
        return self._sequence

    def apply(self, event: Mapping[str, Any]) -> BookCheckpoint:
        event_type = event.get("event_type")
        if event_type == "book_snapshot":
            return self.apply_snapshot(event)
        if event_type == "book_delta":
            return self.apply_delta(event)
        raise L2ReplayError(f"Unsupported L2 event_type: {event_type!r}")

    def apply_snapshot(self, event: Mapping[str, Any]) -> BookCheckpoint:
        self._apply_snapshot_state(event)
        return self.checkpoint()

    def _apply_snapshot_state(self, event: Mapping[str, Any]) -> None:
        payload = dict(event)
        validate_json_record(BOOK_SNAPSHOT_EVENT_SCHEMA_ID, payload)
        self._apply_validated_snapshot_state(payload)

    def apply_delta(self, event: Mapping[str, Any]) -> BookCheckpoint:
        self._apply_delta_state(event)
        return self.checkpoint()

    def _apply_delta_state(self, event: Mapping[str, Any]) -> None:
        payload = dict(event)
        validate_json_record(BOOK_DELTA_EVENT_SCHEMA_ID, payload)
        self._apply_validated_delta_state(payload)

    def _apply_without_checkpoint(self, event: Mapping[str, Any]) -> None:
        event_type = event.get("event_type")
        if event_type == "book_snapshot":
            self._apply_snapshot_state(event)
            return
        if event_type == "book_delta":
            self._apply_delta_state(event)
            return
        raise L2ReplayError(f"Unsupported L2 event_type: {event_type!r}")

    def _apply_validated_without_checkpoint(self, event: Mapping[str, Any]) -> None:
        """Apply an event whose frozen JSON schema and PIT semantics already passed."""
        event_type = event["event_type"]
        if event_type == "book_snapshot":
            self._apply_validated_snapshot_state(event)
            return
        if event_type == "book_delta":
            self._apply_validated_delta_state(event)
            return
        raise L2ReplayError(f"Unsupported L2 event_type: {event_type!r}")

    def _apply_validated_snapshot_state(self, payload: Mapping[str, Any]) -> None:
        sequence = int(payload["sequence"])
        if self._sequence is not None:
            self._assert_identity(payload)
            if sequence <= self._sequence:
                raise L2ReplayError(
                    f"Snapshot sequence must advance: current={self._sequence}, incoming={sequence}"
                )
        self._assert_time_advances(str(payload["event_time"]))
        bid_levels = [_level(item) for item in payload["bids"]]
        ask_levels = [_level(item) for item in payload["asks"]]
        bids = {item.price_units: item for item in bid_levels}
        asks = {item.price_units: item for item in ask_levels}
        if any(level.quantity_units <= 0 for level in (*bids.values(), *asks.values())):
            raise L2ReplayError("Snapshot cannot contain empty price levels")
        scales = {level.price_scale for level in (*bids.values(), *asks.values())}
        if len(scales) != 1:
            raise L2ReplayError("Snapshot price scales differ")
        self._assert_book_valid(bids, asks)
        self._source = str(payload["source"])
        self._instrument_id = str(payload["instrument_id"])
        self._session_id = str(payload["session_id"])
        self._sequence = sequence
        self._event_time = str(payload["event_time"])
        self._price_scale = next(iter(scales))
        self._bids = bids
        self._asks = asks

    def _apply_validated_delta_state(self, payload: Mapping[str, Any]) -> None:
        if not self.initialized:
            raise L2ReplayError("L2 replay must start from a BookSnapshot")
        self._assert_identity(payload)
        sequence = int(payload["sequence"])
        previous_sequence = int(payload["previous_sequence"])
        if previous_sequence != self._sequence:
            raise L2ReplayError(
                f"L2 sequence gap, duplicate, or out-of-order delta: "
                f"expected previous_sequence={self._sequence}, actual={previous_sequence}"
            )
        self._assert_time_advances(str(payload["event_time"]))
        level = _level(
            {
                "price": payload["price"],
                "quantity": payload["quantity"],
                "order_count": None,
            }
        )
        if level.price_scale != self._price_scale:
            raise L2ReplayError(
                f"L2 delta price scale changed: expected={self._price_scale}, "
                f"actual={level.price_scale}"
            )
        side = self._bids if payload["side"] == "bid" else self._asks
        previous_level = side.get(level.price_units)
        if payload["action"] == "delete":
            if previous_level is None:
                raise L2ReplayError("L2 delete references an absent price level")
            del side[level.price_units]
        else:
            side[level.price_units] = level
        try:
            self._assert_book_valid(self._bids, self._asks)
        except L2ReplayError:
            if previous_level is None:
                side.pop(level.price_units, None)
            else:
                side[level.price_units] = previous_level
            raise
        self._sequence = sequence
        self._event_time = str(payload["event_time"])

    def _apply_validated_uniform_upsert_batch(
        self,
        *,
        source: str,
        instrument_id: str,
        session_id: str,
        first_previous_sequence: int,
        final_sequence: int,
        first_event_time: str,
        final_event_time: str,
        side: str,
        price_units: int,
        price_scale: int,
        quantity_units: int,
        quantity_scale: int,
    ) -> None:
        """Apply a proven-uniform batch without weakening serial book semantics.

        The caller must already have vector-validated every row, strict sequence
        continuity, non-decreasing event time, and positive quantities. Because all
        updates target one price on one side and are upserts, only the final quantity
        changes retained state; checking the one price against the opposite book is
        equivalent to checking every intermediate update.
        """
        if not self.initialized:
            raise L2ReplayError("L2 replay must start from a BookSnapshot")
        identity = (source, instrument_id, session_id)
        expected = (self._source, self._instrument_id, self._session_id)
        if identity != expected:
            raise L2ReplayError(
                f"L2 stream identity changed: expected={expected}, actual={identity}"
            )
        if first_previous_sequence != self._sequence:
            raise L2ReplayError(
                "L2 sequence gap, duplicate, or out-of-order delta: "
                f"expected previous_sequence={self._sequence}, "
                f"actual={first_previous_sequence}"
            )
        if final_sequence <= first_previous_sequence:
            raise L2ReplayError("L2 delta batch sequence must strictly advance")
        self._assert_time_advances(first_event_time)
        self._assert_time_advances(final_event_time)
        if price_scale != self._price_scale:
            raise L2ReplayError(
                f"L2 delta price scale changed: expected={self._price_scale}, actual={price_scale}"
            )
        level = ReconstructedLevel(
            price_units=price_units,
            price_scale=price_scale,
            quantity_units=quantity_units,
            quantity_scale=quantity_scale,
            order_count=None,
        )
        levels = self._bids if side == "bid" else self._asks
        previous_level = levels.get(price_units)
        levels[price_units] = level
        try:
            self._assert_book_valid(self._bids, self._asks)
        except L2ReplayError:
            if previous_level is None:
                levels.pop(price_units, None)
            else:
                levels[price_units] = previous_level
            raise
        self._sequence = final_sequence
        self._event_time = final_event_time

    def _assert_identity(self, payload: Mapping[str, Any]) -> None:
        identity = (
            str(payload["source"]),
            str(payload["instrument_id"]),
            str(payload["session_id"]),
        )
        expected = (self._source, self._instrument_id, self._session_id)
        if identity != expected:
            raise L2ReplayError(
                f"L2 stream identity changed: expected={expected}, actual={identity}"
            )

    def _assert_time_advances(self, event_time: str) -> None:
        if self._event_time is None:
            return
        if event_time == self._event_time:
            return
        previous = _parsed_timestamp(self._event_time)
        current = _parsed_timestamp(event_time)
        if current < previous:
            raise L2ReplayError("L2 event_time moved backwards")

    @staticmethod
    def _assert_book_valid(
        bids: Mapping[int, ReconstructedLevel],
        asks: Mapping[int, ReconstructedLevel],
    ) -> None:
        if any(level.quantity_units <= 0 for level in (*bids.values(), *asks.values())):
            raise L2ReplayError("Reconstructed book contains an empty price level")
        if bids and asks and max(bids) >= min(asks):
            raise L2ReplayError("Reconstructed book is locked or crossed")

    def checkpoint(self) -> BookCheckpoint:
        if not self.initialized:
            raise L2ReplayError("Cannot checkpoint an uninitialized book")
        bids = tuple(self._bids[price] for price in sorted(self._bids, reverse=True))
        asks = tuple(self._asks[price] for price in sorted(self._asks))
        state = {
            "source": self._source,
            "instrument_id": self._instrument_id,
            "session_id": self._session_id,
            "sequence": self._sequence,
            "event_time": self._event_time,
            "bids": [asdict(item) for item in bids],
            "asks": [asdict(item) for item in asks],
        }
        return BookCheckpoint(
            source=str(self._source),
            instrument_id=str(self._instrument_id),
            session_id=str(self._session_id),
            sequence=int(self._sequence),
            event_time=str(self._event_time),
            bids=bids,
            asks=asks,
            state_sha256=hashlib.sha256(_canonical(state)).hexdigest(),
        )


@lru_cache(maxsize=4096)
def _parsed_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def replay_l2(
    events: Iterable[Mapping[str, Any]],
    *,
    expected_checkpoint_hashes: Mapping[int, str] | None = None,
    capture_all_checkpoints: bool = True,
) -> L2ReplayResult:
    records = [dict(event) for event in events]
    if not records:
        raise L2ReplayError("L2 replay requires at least one event")
    if records[0].get("event_type") != "book_snapshot":
        raise L2ReplayError("L2 replay must begin with a BookSnapshot")
    reconstructor = L2BookReconstructor()
    checkpoints: list[BookCheckpoint] = []
    expected = dict(expected_checkpoint_hashes or {})
    event_ids: set[str] = set()
    for index, record in enumerate(records):
        event_id = str(record.get("event_id"))
        if event_id in event_ids:
            raise L2ReplayError(f"Duplicate event_id in L2 stream: {event_id}")
        event_ids.add(event_id)
        reconstructor._apply_without_checkpoint(record)
        sequence = reconstructor.sequence
        should_checkpoint = (
            capture_all_checkpoints or sequence in expected or index == len(records) - 1
        )
        if should_checkpoint:
            checkpoint = reconstructor.checkpoint()
            expected_hash = expected.get(checkpoint.sequence)
            if expected_hash is not None and checkpoint.state_sha256 != expected_hash:
                raise L2ReplayError(
                    f"L2 checkpoint hash mismatch at sequence {checkpoint.sequence}: "
                    f"expected={expected_hash}, actual={checkpoint.state_sha256}"
                )
            checkpoints.append(checkpoint)
    missing_checkpoints = set(expected).difference(item.sequence for item in checkpoints)
    if missing_checkpoints:
        raise L2ReplayError(
            f"Expected L2 checkpoints were not reached: {sorted(missing_checkpoints)}"
        )
    return L2ReplayResult(final_checkpoint=checkpoints[-1], checkpoints=tuple(checkpoints))
