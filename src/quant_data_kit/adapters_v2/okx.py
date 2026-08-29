"""OKX fixture adapter with explicit legacy and post-checksum sequence semantics."""

from __future__ import annotations

import copy
import zlib
from collections.abc import Iterable, Mapping
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
    certification_status = "legacy-fixture-certified-not-market-data-certified"
    integrity_gate = (
        "current live prerequisite: TLS plus strict seqId/prevSeqId continuity; "
        "checksum=0 is not an integrity signal; nonzero signed CRC32 is verified "
        "for historical fixtures only; trusted Raw SHA-256 remains mandatory"
    )

    def __init__(self, context: AdapterContext) -> None:
        if context.provider != "okx":
            raise ValidationError("OKX adapter context provider must be okx")
        self.context = context
        self._book_sequences = BookSequenceNormalizer()
        self._books: dict[str, dict[str, dict[str, str]]] = {}
        self._awaiting_snapshot: set[str] = set()

    @contextmanager
    def transaction(self) -> Iterable[None]:
        books_before = copy.deepcopy(self._books)
        awaiting_before = set(self._awaiting_snapshot)
        try:
            with self._book_sequences.transaction():
                yield
        except Exception:
            reset_during_transaction = self._awaiting_snapshot.difference(awaiting_before)
            self._books = books_before
            self._awaiting_snapshot = awaiting_before
            for symbol in reset_during_transaction:
                self._books.pop(symbol, None)
                self._book_sequences.reset(symbol)
                self._awaiting_snapshot.add(symbol)
            raise

    @contextmanager
    def _book_transaction(self, symbol: str) -> Any:
        before = copy.deepcopy(self._books.get(symbol))
        awaiting_before = symbol in self._awaiting_snapshot
        with self._book_sequences.transaction():
            try:
                yield
            except Exception:
                if before is None:
                    self._books.pop(symbol, None)
                else:
                    self._books[symbol] = before
                if awaiting_before:
                    self._awaiting_snapshot.add(symbol)
                else:
                    self._awaiting_snapshot.discard(symbol)
                raise

    def _require_fresh_snapshot(self, symbol: str) -> None:
        self._books.pop(symbol, None)
        self._book_sequences.reset(symbol)
        self._awaiting_snapshot.add(symbol)

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
        try:
            checksum = int(message["checksum"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationError("OKX books checksum field must be an integer") from exc
        if checksum != 0 and cls._checksum(book) != checksum:
            raise ValidationError("OKX legacy books CRC32 checksum mismatch")

    def adapt(self, message: Mapping[str, Any]) -> list[dict[str, Any]]:
        channel = str(message.get("channel", ""))
        symbol = str(message.get("instId", ""))
        instrument = self.context.instrument(symbol)
        event_time = utc_from_milliseconds(message["ts"], "ts")
        received_at = utc_from_milliseconds(
            message.get("received_at", message["ts"]), "received_at"
        )
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
                    sequence=int(message["tradeId"]),
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
                    sequence=int(message["seqId"]),
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
                self._awaiting_snapshot.discard(symbol)
                return [market_event_payload(event)]
        if channel == "books" and message["action"] == "update":
            provider_previous_sequence = int(message["prevSeqId"])
            provider_sequence = int(message["seqId"])
            if provider_sequence < provider_previous_sequence:
                self._require_fresh_snapshot(symbol)
                raise ValidationError(
                    "OKX maintenance sequence reset terminated admission; "
                    "a fresh BookSnapshot is required"
                )
            with self._book_transaction(symbol):
                if symbol in self._awaiting_snapshot or symbol not in self._books:
                    raise ValidationError("OKX BookDelta arrived before BookSnapshot")
                if provider_sequence == provider_previous_sequence:
                    if message["bids"] or message["asks"]:
                        raise ValidationError(
                            "OKX equal-sequence heartbeat must not contain book levels"
                        )
                    self._apply_book_message(self._books[symbol], message, replace=False)
                    self._book_sequences.heartbeat(symbol, provider_sequence)
                    return []
                self._apply_book_message(self._books[symbol], message, replace=False)
                changes = [(BookSide.BID, item) for item in message["bids"]] + [
                    (BookSide.ASK, item) for item in message["asks"]
                ]
                pairs = self._book_sequences.delta(
                    symbol,
                    provider_previous_sequence=provider_previous_sequence,
                    provider_sequence=provider_sequence,
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
                    sequence=int(message["fundingTime"]),
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
                    sequence=int(message["ts"]),
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
