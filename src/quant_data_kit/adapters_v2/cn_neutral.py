"""Supplier-neutral, fixture-only domestic L2 adapter."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

from quant_data_kit.adapters_v2.base import (
    AdapterContext,
    BookSequenceNormalizer,
    event_identity,
    fixed,
    utc_from_text,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.market_events_v2 import (
    BookAction,
    BookDeltaEvent,
    BookLevel,
    BookSide,
    BookSnapshotEvent,
    market_event_payload,
)


class CNNeutralFixtureAdapter:
    certification_status = "fixture-certified-not-market-data-certified"

    def __init__(self, context: AdapterContext) -> None:
        if context.session_kind == "24x7":
            raise ValidationError("Domestic fixture adapter requires an exchange-session context")
        self.context = context
        self._book_sequences = BookSequenceNormalizer()

    def adapt(self, message: Mapping[str, Any]) -> list[dict[str, Any]]:
        if message.get("certification_scope") != "fixture-only":
            raise ValidationError("Domestic neutral M2 adapter accepts fixture-only data")
        kind = str(message.get("kind", ""))
        symbol = str(message.get("symbol", ""))
        instrument = self.context.instrument(symbol)
        event_time = utc_from_text(str(message["event_time"]), "event_time")
        received_at = utc_from_text(str(message["received_at"]), "received_at")
        trading_day = date.fromisoformat(str(message["trading_day"]))
        session_id = str(message["session_id"])
        if kind == "l2_snapshot":
            provider_sequence = int(message["sequence"])
            sequence = self._book_sequences.snapshot(symbol, provider_sequence)
            event = BookSnapshotEvent(
                **event_identity(
                    self.context,
                    symbol,
                    event_time=event_time,
                    received_at=received_at,
                    event_id=f"{self.context.provider}-snapshot-{symbol}-{provider_sequence}",
                    sequence=sequence,
                    trading_day=trading_day,
                    session_id=session_id,
                ),
                bids=tuple(self._level(item, instrument, "bid") for item in message["bids"]),
                asks=tuple(self._level(item, instrument, "ask") for item in message["asks"]),
            )
            return [market_event_payload(event)]
        if kind == "l2_delta":
            changes = message["changes"]
            pairs = self._book_sequences.delta(
                symbol,
                provider_previous_sequence=int(message["previous_sequence"]),
                provider_sequence=int(message["sequence"]),
                level_count=len(changes),
            )
            events: list[dict[str, Any]] = []
            for index, (change, (previous_sequence, sequence)) in enumerate(
                zip(changes, pairs, strict=True), start=1
            ):
                quantity = fixed(change["quantity"], instrument.quantity_scale, "quantity")
                event = BookDeltaEvent(
                    **event_identity(
                        self.context,
                        symbol,
                        event_time=event_time,
                        received_at=received_at,
                        event_id=(
                            f"{self.context.provider}-delta-{symbol}-{message['sequence']}-{index}"
                        ),
                        sequence=sequence,
                        trading_day=trading_day,
                        session_id=session_id,
                    ),
                    side=BookSide(str(change["side"])),
                    action=BookAction.DELETE if quantity.units == 0 else BookAction.UPSERT,
                    price=fixed(change["price"], instrument.price_scale, "price"),
                    quantity=quantity,
                    previous_sequence=previous_sequence,
                )
                events.append(market_event_payload(event))
            return events
        raise ValidationError(f"Unsupported domestic neutral fixture kind: {kind!r}")

    @staticmethod
    def _level(item: Mapping[str, Any], instrument: Any, side: str) -> BookLevel:
        return BookLevel(
            fixed(item["price"], instrument.price_scale, f"{side}.price"),
            fixed(item["quantity"], instrument.quantity_scale, f"{side}.quantity"),
            int(item["order_count"]) if item.get("order_count") is not None else None,
        )
