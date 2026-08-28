"""OKX desensitized fixture adapter for v2 market events."""

from __future__ import annotations

import copy
import zlib
from collections.abc import Mapping
from contextlib import contextmanager
from decimal import Decimal
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


class OKXFixtureAdapter:
    certification_status = "fixture-certified"
    integrity_gate = "OKX signed CRC32 books checksum plus trusted Raw SHA-256"

    def __init__(self, context: AdapterContext) -> None:
        if context.provider != "okx":
            raise ValidationError("OKX adapter context provider must be okx")
        self.context = context
        self._book_sequences = BookSequenceNormalizer()
        self._books: dict[str, dict[str, dict[str, str]]] = {}

    @contextmanager
    def _book_transaction(self, symbol: str) -> Any:
        before = copy.deepcopy(self._books.get(symbol))
        with self._book_sequences.transaction():
            try:
                yield
            except Exception:
                if before is None:
                    self._books.pop(symbol, None)
                else:
                    self._books[symbol] = before
                raise

    @staticmethod
    def _signed_crc32(value: str) -> int:
        checksum = zlib.crc32(value.encode("utf-8"))
        return checksum if checksum < 2**31 else checksum - 2**32

    @classmethod
    def _checksum(cls, book: Mapping[str, Mapping[str, str]]) -> int:
        bids = sorted(book["bids"].items(), key=lambda item: Decimal(item[0]), reverse=True)[:25]
        asks = sorted(book["asks"].items(), key=lambda item: Decimal(item[0]))[:25]
        values: list[str] = []
        for index in range(max(len(bids), len(asks))):
            if index < len(bids):
                values.extend(bids[index])
            if index < len(asks):
                values.extend(asks[index])
        return cls._signed_crc32(":".join(values))

    @classmethod
    def _apply_book_message(
        cls,
        book: dict[str, dict[str, str]],
        message: Mapping[str, Any],
        *,
        replace: bool,
    ) -> None:
        if replace:
            book["bids"].clear()
            book["asks"].clear()
        for field in ("bids", "asks"):
            levels = list(message[field])
            prices = [str(item[0]) for item in levels]
            if len(prices) != len(set(prices)):
                raise ValidationError(f"OKX {field} contains duplicate price levels")
            for item in levels:
                price, quantity = str(item[0]), str(item[1])
                if Decimal(quantity) == 0:
                    book[field].pop(price, None)
                else:
                    book[field][price] = quantity
        if cls._checksum(book) != int(message["checksum"]):
            raise ValidationError("OKX books checksum mismatch")

    def adapt(self, message: Mapping[str, Any]) -> list[dict[str, Any]]:
        channel = str(message.get("channel", ""))
        symbol = str(message.get("instId", ""))
        instrument = self.context.instrument(symbol)
        event_time = utc_from_milliseconds(message["ts"], "ts")
        received_at = utc_from_milliseconds(message.get("received_at", message["ts"]), "received_at")
        if channel == "trades":
            if message["side"] not in {"buy", "sell"}:
                raise ValidationError("OKX trade side must be buy or sell")
            event = TradeEvent(
                **event_identity(
                    self.context,
                    symbol,
                    event_time=event_time,
                    received_at=received_at,
                    event_id=f"okx-trade-{symbol}-{message['tradeId']}",
                    sequence=None,
                ),
                price=fixed(message["px"], instrument.price_scale, "px"),
                quantity=fixed(message["sz"], instrument.quantity_scale, "sz"),
                aggressor_side=(
                    AggressorSide.BUY if message["side"] == "buy" else AggressorSide.SELL
                ),
            )
            return [market_event_payload(event)]
        if channel == "bbo-tbt":
            event = QuoteEvent(
                **event_identity(
                    self.context,
                    symbol,
                    event_time=event_time,
                    received_at=received_at,
                    event_id=f"okx-bbo-{symbol}-{message['seqId']}",
                    sequence=None,
                ),
                bid_price=fixed(message["bidPx"], instrument.price_scale, "bidPx"),
                bid_quantity=fixed(message["bidSz"], instrument.quantity_scale, "bidSz"),
                ask_price=fixed(message["askPx"], instrument.price_scale, "askPx"),
                ask_quantity=fixed(message["askSz"], instrument.quantity_scale, "askSz"),
            )
            return [market_event_payload(event)]
        if channel == "books" and message["action"] == "snapshot":
            with self._book_transaction(symbol):
                book = self._books.setdefault(symbol, {"bids": {}, "asks": {}})
                self._apply_book_message(book, message, replace=True)
                provider_sequence = int(message["seqId"])
                sequence = self._book_sequences.snapshot(symbol, provider_sequence)
                event = BookSnapshotEvent(
                    **event_identity(
                        self.context,
                        symbol,
                        event_time=event_time,
                        received_at=received_at,
                        event_id=f"okx-book-snapshot-{symbol}-{provider_sequence}",
                        sequence=sequence,
                    ),
                    bids=tuple(
                        self._book_level(item, instrument, "bid") for item in message["bids"]
                    ),
                    asks=tuple(
                        self._book_level(item, instrument, "ask") for item in message["asks"]
                    ),
                )
                return [market_event_payload(event)]
        if channel == "books" and message["action"] == "update":
            with self._book_transaction(symbol):
                if symbol not in self._books:
                    raise ValidationError("OKX BookDelta arrived before BookSnapshot")
                self._apply_book_message(self._books[symbol], message, replace=False)
                changes = [(BookSide.BID, item) for item in message["bids"]] + [
                    (BookSide.ASK, item) for item in message["asks"]
                ]
                pairs = self._book_sequences.delta(
                    symbol,
                    provider_previous_sequence=int(message["prevSeqId"]),
                    provider_sequence=int(message["seqId"]),
                    level_count=len(changes),
                )
                events: list[dict[str, Any]] = []
                for index, ((side, item), (previous_sequence, sequence)) in enumerate(
                    zip(changes, pairs, strict=True), start=1
                ):
                    quantity = fixed(item[1], instrument.quantity_scale, "books.quantity")
                    event = BookDeltaEvent(
                        **event_identity(
                            self.context,
                            symbol,
                            event_time=event_time,
                            received_at=received_at,
                            event_id=f"okx-book-delta-{symbol}-{message['seqId']}-{index}",
                            sequence=sequence,
                        ),
                        side=side,
                        action=BookAction.DELETE if quantity.units == 0 else BookAction.UPSERT,
                        price=fixed(item[0], instrument.price_scale, "books.price"),
                        quantity=quantity,
                        previous_sequence=previous_sequence,
                    )
                    events.append(market_event_payload(event))
                return events
        if channel == "funding-rate":
            event = FundingRateEvent(
                **event_identity(
                    self.context,
                    symbol,
                    event_time=event_time,
                    received_at=received_at,
                    event_id=f"okx-funding-{symbol}-{message['fundingTime']}",
                    sequence=None,
                ),
                rate=float(message["fundingRate"]),
                interval_start=utc_from_milliseconds(message["intervalStart"], "intervalStart"),
                interval_end=utc_from_milliseconds(message["fundingTime"], "fundingTime"),
            )
            return [market_event_payload(event)]
        if channel == "mark-price":
            event = MarkPriceEvent(
                **event_identity(
                    self.context,
                    symbol,
                    event_time=event_time,
                    received_at=received_at,
                    event_id=f"okx-mark-{symbol}-{message['ts']}",
                    sequence=None,
                ),
                price=fixed(message["markPx"], instrument.price_scale, "markPx"),
            )
            return [market_event_payload(event)]
        raise ValidationError(f"Unsupported OKX fixture channel/action: {channel!r}")

    @staticmethod
    def _book_level(item: list[str], instrument: Any, side: str) -> BookLevel:
        order_count = int(item[3]) if len(item) > 3 and item[3] else None
        return BookLevel(
            fixed(item[0], instrument.price_scale, f"{side}.price"),
            fixed(item[1], instrument.quantity_scale, f"{side}.quantity"),
            order_count,
        )
