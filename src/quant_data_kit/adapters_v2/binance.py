"""Binance desensitized fixture adapter for v2 market events."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from quant_data_kit.adapters_v2.base import (
    AdapterContext,
    BookSequenceNormalizer,
    event_identity,
    fixed,
    utc_from_milliseconds,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.market_events_v2 import (
    AggressorSide,
    BookAction,
    BookDeltaEvent,
    BookLevel,
    BookSide,
    BookSnapshotEvent,
    FundingRateEvent,
    MarkPriceEvent,
    QuoteEvent,
    TradeEvent,
    market_event_payload,
)


class BinanceFixtureAdapter:
    certification_status = "fixture-certified"

    def __init__(self, context: AdapterContext) -> None:
        if context.provider != "binance":
            raise ValidationError("Binance adapter context provider must be binance")
        self.context = context
        self._book_sequences = BookSequenceNormalizer()

    def adapt(self, message: Mapping[str, Any]) -> list[dict[str, Any]]:
        kind = message.get("e")
        symbol = str(message.get("s", ""))
        instrument = self.context.instrument(symbol)
        event_time = utc_from_milliseconds(message.get("T", message.get("E")), "event_time")
        received_at = utc_from_milliseconds(message.get("received_at", message.get("E")), "received_at")
        if kind == "trade":
            if not isinstance(message["m"], bool):
                raise ValidationError("Binance trade maker flag m must be boolean")
            side = AggressorSide.SELL if bool(message["m"]) else AggressorSide.BUY
            event = TradeEvent(
                **event_identity(
                    self.context,
                    symbol,
                    event_time=event_time,
                    received_at=received_at,
                    event_id=f"binance-trade-{symbol}-{message['t']}",
                    sequence=None,
                ),
                price=fixed(message["p"], instrument.price_scale, "p"),
                quantity=fixed(message["q"], instrument.quantity_scale, "q"),
                aggressor_side=side,
            )
            return [market_event_payload(event)]
        if kind == "bookTicker":
            event = QuoteEvent(
                **event_identity(
                    self.context,
                    symbol,
                    event_time=event_time,
                    received_at=received_at,
                    event_id=f"binance-bbo-{symbol}-{message['u']}",
                    sequence=None,
                ),
                bid_price=fixed(message["b"], instrument.price_scale, "b"),
                bid_quantity=fixed(message["B"], instrument.quantity_scale, "B"),
                ask_price=fixed(message["a"], instrument.price_scale, "a"),
                ask_quantity=fixed(message["A"], instrument.quantity_scale, "A"),
            )
            return [market_event_payload(event)]
        if kind == "depthSnapshot":
            provider_sequence = int(message["lastUpdateId"])
            sequence = self._book_sequences.snapshot(symbol, provider_sequence)
            event = BookSnapshotEvent(
                **event_identity(
                    self.context,
                    symbol,
                    event_time=event_time,
                    received_at=received_at,
                    event_id=f"binance-book-snapshot-{symbol}-{provider_sequence}",
                    sequence=sequence,
                ),
                bids=tuple(
                    BookLevel(
                        fixed(item[0], instrument.price_scale, "bid.price"),
                        fixed(item[1], instrument.quantity_scale, "bid.quantity"),
                    )
                    for item in message["bids"]
                ),
                asks=tuple(
                    BookLevel(
                        fixed(item[0], instrument.price_scale, "ask.price"),
                        fixed(item[1], instrument.quantity_scale, "ask.quantity"),
                    )
                    for item in message["asks"]
                ),
            )
            return [market_event_payload(event)]
        if kind == "depthUpdate":
            provider_previous = int(message["pu"])
            provider_sequence = int(message["u"])
            first_sequence = int(message["U"])
            if not first_sequence <= provider_previous + 1 <= provider_sequence:
                raise ValidationError("Binance depthUpdate U/u range does not bridge prior sequence")
            changes = [(BookSide.BID, item) for item in message["b"]] + [
                (BookSide.ASK, item) for item in message["a"]
            ]
            pairs = self._book_sequences.delta(
                symbol,
                provider_previous_sequence=provider_previous,
                provider_sequence=provider_sequence,
                level_count=len(changes),
            )
            events: list[dict[str, Any]] = []
            for index, ((side, item), (previous_sequence, sequence)) in enumerate(
                zip(changes, pairs, strict=True), start=1
            ):
                quantity = fixed(item[1], instrument.quantity_scale, "depth.quantity")
                action = BookAction.DELETE if quantity.units == 0 else BookAction.UPSERT
                event = BookDeltaEvent(
                    **event_identity(
                        self.context,
                        symbol,
                        event_time=event_time,
                        received_at=received_at,
                        event_id=f"binance-book-delta-{symbol}-{message['u']}-{index}",
                        sequence=sequence,
                    ),
                    side=side,
                    action=action,
                    price=fixed(item[0], instrument.price_scale, "depth.price"),
                    quantity=quantity,
                    previous_sequence=previous_sequence,
                )
                events.append(market_event_payload(event))
            return events
        if kind == "fundingRate":
            event = FundingRateEvent(
                **event_identity(
                    self.context,
                    symbol,
                    event_time=event_time,
                    received_at=received_at,
                    event_id=f"binance-funding-{symbol}-{message['T']}",
                    sequence=None,
                ),
                rate=float(message["r"]),
                interval_start=utc_from_milliseconds(message["intervalStart"], "intervalStart"),
                interval_end=utc_from_milliseconds(message["intervalEnd"], "intervalEnd"),
            )
            return [market_event_payload(event)]
        if kind == "markPriceUpdate":
            event = MarkPriceEvent(
                **event_identity(
                    self.context,
                    symbol,
                    event_time=event_time,
                    received_at=received_at,
                    event_id=f"binance-mark-{symbol}-{message['E']}",
                    sequence=None,
                ),
                price=fixed(message["p"], instrument.price_scale, "p"),
            )
            return [market_event_payload(event)]
        raise ValidationError(f"Unsupported Binance fixture event: {kind!r}")
