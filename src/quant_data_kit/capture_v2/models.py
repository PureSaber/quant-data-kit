"""Frozen configuration and audit contracts for public Crypto L2 capture."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import random
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, Protocol

from quant_data_kit.domain_v2 import SymbolMapping
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.temporal_v2 import ensure_utc_datetime

UTC = timezone.utc
M7_PROVIDERS = ("binance", "okx")
M7_CAPABILITIES = (
    "btc-spot-l2",
    "btc-usdt-perpetual-l2",
    "eth-spot-l2",
    "eth-usdt-perpetual-l2",
)


class Provider(str, Enum):
    BINANCE = "binance"
    OKX = "okx"


class MarketKind(str, Enum):
    SPOT = "spot"
    USDT_PERPETUAL = "usdt_perpetual"


class CaptureState(str, Enum):
    CONNECTING = "CONNECTING"
    BUFFERING = "BUFFERING"
    SNAPSHOT_SYNC = "SNAPSHOT_SYNC"
    LIVE = "LIVE"
    RESYNC = "RESYNC"
    PAUSED = "PAUSED"


def utc_text(value: datetime, field_name: str = "timestamp") -> str:
    return ensure_utc_datetime(value, field=field_name).isoformat().replace("+00:00", "Z")


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


class Clock(Protocol):
    def now(self) -> datetime: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(tz=UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class MonotonicReceivedClock:
    """Keep receive timestamps UTC and strictly monotonic within one connection."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._last: datetime | None = None

    def now(self) -> tuple[datetime, datetime]:
        observed = ensure_utc_datetime(self._clock.now(), field="clock.now")
        received = observed
        if self._last is not None and received <= self._last:
            received = self._last + timedelta(microseconds=1)
        self._last = received
        return observed, received


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay_seconds: float = 0.5
    maximum_delay_seconds: float = 8.0
    jitter_fraction: float = 0.20

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValidationError("max_attempts must be positive")
        if self.base_delay_seconds <= 0 or self.maximum_delay_seconds <= 0:
            raise ValidationError("retry delays must be positive")
        if self.maximum_delay_seconds < self.base_delay_seconds:
            raise ValidationError("maximum_delay_seconds must not be below base delay")
        if not 0 <= self.jitter_fraction <= 1:
            raise ValidationError("jitter_fraction must be between zero and one")

    def delay(self, failed_attempt: int, jitter: Callable[[], float]) -> float:
        if failed_attempt < 1 or failed_attempt >= self.max_attempts:
            raise ValidationError("failed_attempt is outside the retryable range")
        sample = float(jitter())
        if not 0 <= sample <= 1:
            raise ValidationError("jitter source must return a value between zero and one")
        base = min(
            self.maximum_delay_seconds,
            self.base_delay_seconds * (2 ** (failed_attempt - 1)),
        )
        spread = base * self.jitter_fraction
        return max(0.0, base - spread + 2 * spread * sample)


def random_jitter() -> float:
    return random.SystemRandom().random()


@dataclass(frozen=True)
class SegmentRotation:
    max_messages: int = 2_000
    max_wire_bytes: int = 16 * 1024 * 1024
    max_age_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.max_messages < 2:
            raise ValidationError("Raw segments must batch at least two messages")
        if self.max_wire_bytes <= 0 or self.max_age_seconds <= 0:
            raise ValidationError("Raw segment rotation limits must be positive")


@dataclass(frozen=True)
class StreamConfig:
    stream_id: str
    provider: Provider
    market: MarketKind
    native_symbol: str
    instrument_id: str
    venue: str
    websocket_url: str
    channel: str
    price_scale: int = 8
    quantity_scale: int = 8
    rest_snapshot_url: str | None = None

    def __post_init__(self) -> None:
        for name in ("stream_id", "native_symbol", "instrument_id", "venue", "channel"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValidationError(f"{name} must be a non-empty string")
        if not self.websocket_url.startswith("wss://"):
            raise ValidationError("public market-data websocket_url must use TLS wss://")
        if self.provider is Provider.BINANCE:
            if not self.rest_snapshot_url or not self.rest_snapshot_url.startswith("https://"):
                raise ValidationError("Binance L2 requires an HTTPS REST snapshot endpoint")
        elif self.rest_snapshot_url is not None:
            raise ValidationError("OKX books snapshot must arrive on its public WebSocket channel")
        if self.price_scale < 0 or self.quantity_scale < 0:
            raise ValidationError("fixed-point scales must be non-negative")

    @property
    def capability(self) -> str:
        asset = "btc" if "BTC" in self.native_symbol.upper() else "eth"
        suffix = "spot-l2" if self.market is MarketKind.SPOT else "usdt-perpetual-l2"
        return f"{asset}-{suffix}"

    @property
    def mapping_source(self) -> str:
        """Disambiguate venues where spot and derivatives reuse the same native code."""
        return f"{self.provider.value}:{self.market.value}"


@dataclass(frozen=True)
class CaptureConfig:
    hot_root: Path
    archive_root: Path
    restore_root: Path
    collector_commit: str
    streams: tuple[StreamConfig, ...] = field(default_factory=tuple)
    rotation: SegmentRotation = field(default_factory=SegmentRotation)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    archive_reserve_bytes: int = 150 * 1024**3

    def __post_init__(self) -> None:
        for name in ("hot_root", "archive_root", "restore_root"):
            path = Path(getattr(self, name))
            if not path.is_absolute():
                raise ValidationError(f"{name} must be an explicit absolute path")
            object.__setattr__(self, name, path)
        if not self.collector_commit.strip():
            raise ValidationError("collector_commit must be explicit")
        if not self.streams:
            raise ValidationError("capture configuration must include streams")
        ids = [stream.stream_id for stream in self.streams]
        if len(ids) != len(set(ids)):
            raise ValidationError("stream_id values must be unique")
        if self.archive_reserve_bytes <= 0:
            raise ValidationError("archive_reserve_bytes must be positive")

    @property
    def providers(self) -> tuple[str, ...]:
        return tuple(sorted({stream.provider.value for stream in self.streams}))

    @property
    def capabilities(self) -> tuple[str, ...]:
        return tuple(sorted({stream.capability for stream in self.streams}))


def default_crypto_l2_streams() -> tuple[StreamConfig, ...]:
    streams: list[StreamConfig] = []
    for symbol, asset in (("BTCUSDT", "BTC-USDT"), ("ETHUSDT", "ETH-USDT")):
        lower = symbol.lower()
        streams.append(
            StreamConfig(
                stream_id=f"binance-spot-{lower}-l2",
                provider=Provider.BINANCE,
                market=MarketKind.SPOT,
                native_symbol=symbol,
                instrument_id=f"CRYPTO:BINANCE:{asset}:SPOT",
                venue="BINANCE",
                websocket_url=f"wss://stream.binance.com:9443/ws/{lower}@depth@100ms",
                channel="depth",
                rest_snapshot_url=(
                    f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=1000"
                ),
            )
        )
        streams.append(
            StreamConfig(
                stream_id=f"binance-usdt-perpetual-{lower}-l2",
                provider=Provider.BINANCE,
                market=MarketKind.USDT_PERPETUAL,
                native_symbol=symbol,
                instrument_id=f"CRYPTO:BINANCE:{asset}:PERP",
                venue="BINANCE",
                websocket_url=(
                    f"wss://fstream.binance.com/public/stream?streams={lower}@depth@100ms"
                ),
                channel="depth",
                rest_snapshot_url=(
                    f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"
                ),
            )
        )
    for symbol, asset, market in (
        ("BTC-USDT", "BTC-USDT", MarketKind.SPOT),
        ("ETH-USDT", "ETH-USDT", MarketKind.SPOT),
        ("BTC-USDT-SWAP", "BTC-USDT", MarketKind.USDT_PERPETUAL),
        ("ETH-USDT-SWAP", "ETH-USDT", MarketKind.USDT_PERPETUAL),
    ):
        market_name = "spot" if market is MarketKind.SPOT else "usdt-perpetual"
        product = "SPOT" if market is MarketKind.SPOT else "PERP"
        streams.append(
            StreamConfig(
                stream_id=f"okx-{market_name}-{symbol.lower()}-l2",
                provider=Provider.OKX,
                market=market,
                native_symbol=symbol,
                instrument_id=f"CRYPTO:OKX:{asset}:{product}",
                venue="OKX",
                websocket_url="wss://ws.okx.com:8443/ws/v5/public",
                channel="books",
            )
        )
    return tuple(sorted(streams, key=lambda item: item.stream_id))


def default_symbol_mappings(
    streams: Iterable[StreamConfig],
    *,
    available_at: datetime | None = None,
) -> tuple[SymbolMapping, ...]:
    known_at = available_at or datetime(2020, 1, 1, tzinfo=UTC)
    return tuple(
        SymbolMapping(
            source=stream.mapping_source,
            provider_symbol=stream.native_symbol,
            instrument_id=stream.instrument_id,
            effective_from=datetime(2017, 1, 1, tzinfo=UTC),
            available_at=known_at,
        )
        for stream in streams
    )


class SymbolMappingResolver:
    def __init__(self, mappings: Iterable[SymbolMapping]) -> None:
        grouped: dict[tuple[str, str], list[SymbolMapping]] = {}
        for mapping in mappings:
            grouped.setdefault((mapping.source, mapping.provider_symbol), []).append(mapping)
        self._mappings = MappingProxyType(
            {
                key: tuple(sorted(value, key=lambda item: item.effective_from))
                for key, value in grouped.items()
            }
        )

    def resolve(
        self,
        source: str,
        provider_symbol: str,
        *,
        event_time: datetime,
        known_at: datetime,
    ) -> str:
        event = ensure_utc_datetime(event_time, field="event_time")
        known = ensure_utc_datetime(known_at, field="known_at")
        candidates = self._mappings.get((source, provider_symbol), ())
        valid = [
            mapping
            for mapping in candidates
            if mapping.effective_from <= event
            and (mapping.effective_to is None or event < mapping.effective_to)
            and mapping.available_at <= known
            and (mapping.superseded_at is None or known < mapping.superseded_at)
        ]
        if len(valid) != 1:
            raise ValidationError(
                "SymbolMapping resolution must yield exactly one stable instrument_id: "
                f"source={source}, symbol={provider_symbol}, matches={len(valid)}"
            )
        return valid[0].instrument_id


@dataclass(frozen=True)
class RawFrame:
    frame_kind: str
    provider: str
    stream_id: str
    connection_id: str
    subscription: str
    transport: str
    tls_url: str
    received_at: datetime
    observed_at: datetime
    payload: bytes
    native_sequence: Mapping[str, int] = field(default_factory=dict)
    event_time: datetime | None = None
    collector_commit: str = ""

    def __post_init__(self) -> None:
        if self.transport not in {"wss", "https", "audit"}:
            raise ValidationError("Raw frame transport must be wss, https, or audit")
        if self.transport in {"wss", "https"} and not self.tls_url.startswith(
            f"{self.transport}://"
        ):
            raise ValidationError("Raw market frame URL does not match its TLS transport")
        received = ensure_utc_datetime(self.received_at, field="received_at")
        observed = ensure_utc_datetime(self.observed_at, field="observed_at")
        event = (
            ensure_utc_datetime(self.event_time, field="event_time")
            if self.event_time is not None
            else None
        )
        if not isinstance(self.payload, bytes):
            raise ValidationError("Raw frame payload must be exact bytes")
        if not self.collector_commit:
            raise ValidationError("Raw frame collector_commit must be explicit")
        sequences = {str(key): int(value) for key, value in self.native_sequence.items()}
        object.__setattr__(self, "received_at", received)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "event_time", event)
        object.__setattr__(self, "native_sequence", MappingProxyType(sequences))

    @property
    def raw_sha256(self) -> str:
        return hashlib.sha256(self.payload).hexdigest()

    def record(self) -> dict[str, Any]:
        return {
            "schema_version": "puresaber.raw-frame@1.0.0",
            "frame_kind": self.frame_kind,
            "provider": self.provider,
            "stream_id": self.stream_id,
            "connection_id": self.connection_id,
            "subscription": self.subscription,
            "transport": self.transport,
            "tls_url": self.tls_url,
            "event_time": utc_text(self.event_time, "event_time") if self.event_time else None,
            "received_at": utc_text(self.received_at, "received_at"),
            "observed_at": utc_text(self.observed_at, "observed_at"),
            "native_sequence": dict(self.native_sequence),
            "raw_byte_length": len(self.payload),
            "raw_sha256": self.raw_sha256,
            "payload_base64": base64.b64encode(self.payload).decode("ascii"),
            "collector_commit": self.collector_commit,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> RawFrame:
        if record.get("schema_version") != "puresaber.raw-frame@1.0.0":
            raise ValidationError("unsupported Raw frame schema")
        try:
            payload = base64.b64decode(str(record["payload_base64"]), validate=True)
            received = datetime.fromisoformat(str(record["received_at"]).replace("Z", "+00:00"))
            observed = datetime.fromisoformat(str(record["observed_at"]).replace("Z", "+00:00"))
            event_text = record.get("event_time")
            event = (
                datetime.fromisoformat(str(event_text).replace("Z", "+00:00"))
                if event_text
                else None
            )
            frame = cls(
                frame_kind=str(record["frame_kind"]),
                provider=str(record["provider"]),
                stream_id=str(record["stream_id"]),
                connection_id=str(record["connection_id"]),
                subscription=str(record["subscription"]),
                transport=str(record["transport"]),
                tls_url=str(record["tls_url"]),
                event_time=event,
                received_at=received,
                observed_at=observed,
                native_sequence=dict(record.get("native_sequence", {})),
                payload=payload,
                collector_commit=str(record["collector_commit"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationError("Raw frame record is malformed") from exc
        if frame.raw_sha256 != record.get("raw_sha256") or len(payload) != record.get(
            "raw_byte_length"
        ):
            raise ValidationError("Raw frame exact-byte integrity changed")
        return frame


@dataclass(frozen=True)
class AuditEvent:
    ordinal: int
    stream_id: str
    occurred_at: datetime
    event: str
    from_state: CaptureState | None
    to_state: CaptureState
    reason: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "occurred_at", ensure_utc_datetime(self.occurred_at, field="occurred_at")
        )
        object.__setattr__(self, "details", MappingProxyType(dict(self.details)))

    def payload(self) -> bytes:
        value = {
            "schema_version": "puresaber.capture-audit@1.0.0",
            "ordinal": self.ordinal,
            "stream_id": self.stream_id,
            "occurred_at": utc_text(self.occurred_at, "occurred_at"),
            "event": self.event,
            "from_state": self.from_state.value if self.from_state else None,
            "to_state": self.to_state.value,
            "reason": self.reason,
            "details": dict(self.details),
        }
        return canonical_json_bytes(value)


class CaptureStateMachine:
    _ALLOWED: ClassVar[dict[CaptureState, set[CaptureState]]] = {
        CaptureState.CONNECTING: {CaptureState.BUFFERING, CaptureState.PAUSED},
        CaptureState.BUFFERING: {
            CaptureState.SNAPSHOT_SYNC,
            CaptureState.RESYNC,
            CaptureState.PAUSED,
        },
        CaptureState.SNAPSHOT_SYNC: {
            CaptureState.LIVE,
            CaptureState.RESYNC,
            CaptureState.PAUSED,
        },
        CaptureState.LIVE: {CaptureState.RESYNC, CaptureState.PAUSED},
        CaptureState.RESYNC: {CaptureState.CONNECTING, CaptureState.PAUSED},
        CaptureState.PAUSED: set(),
    }

    def __init__(
        self,
        stream: StreamConfig,
        *,
        connection_id: str,
        collector_commit: str,
        clock: Clock,
        audit_sink: Callable[[RawFrame], None] | None = None,
        alert_sink: Callable[[str], None] | None = None,
    ) -> None:
        self.stream = stream
        self.connection_id = connection_id
        self.collector_commit = collector_commit
        self.clock = clock
        self.audit_sink = audit_sink
        self.alert_sink = alert_sink or (lambda _message: None)
        self.state = CaptureState.CONNECTING
        self._events: list[AuditEvent] = []
        self._record(None, CaptureState.CONNECTING, "state_initialized", {})

    @property
    def events(self) -> tuple[AuditEvent, ...]:
        return tuple(self._events)

    def transition(
        self,
        target: CaptureState,
        reason: str,
        details: Mapping[str, Any] | None = None,
    ) -> AuditEvent:
        source = self.state
        if target not in self._ALLOWED[source]:
            event = self._record(
                source,
                source,
                "illegal_transition",
                {"attempted_state": target.value, "reason": reason, **dict(details or {})},
            )
            self.alert_sink(
                f"ILLEGAL_CAPTURE_TRANSITION:{self.stream.stream_id}:{source.value}->{target.value}"
            )
            raise ValidationError(
                f"illegal capture state transition: {source.value}->{target.value}; "
                f"audit_ordinal={event.ordinal}"
            )
        self.state = target
        return self._record(source, target, reason, dict(details or {}))

    def audit(
        self, event: str, reason: str, details: Mapping[str, Any] | None = None
    ) -> AuditEvent:
        return self._record(
            self.state, self.state, event, {"reason": reason, **dict(details or {})}
        )

    def _record(
        self,
        source: CaptureState | None,
        target: CaptureState,
        reason: str,
        details: Mapping[str, Any],
    ) -> AuditEvent:
        occurred = ensure_utc_datetime(self.clock.now(), field="audit clock")
        event = AuditEvent(
            ordinal=len(self._events) + 1,
            stream_id=self.stream.stream_id,
            occurred_at=occurred,
            event="state_transition" if source != target else reason,
            from_state=source,
            to_state=target,
            reason=reason,
            details=details,
        )
        self._events.append(event)
        if self.audit_sink is not None:
            try:
                self.audit_sink(
                    RawFrame(
                        frame_kind="audit",
                        provider=self.stream.provider.value,
                        stream_id=self.stream.stream_id,
                        connection_id=self.connection_id,
                        subscription=self.stream.channel,
                        transport="audit",
                        tls_url="audit://local",
                        received_at=occurred,
                        observed_at=occurred,
                        payload=event.payload(),
                        native_sequence={"audit_ordinal": event.ordinal},
                        collector_commit=self.collector_commit,
                    )
                )
            except Exception as exc:  # noqa: BLE001 - audit persistence must not mask state
                self.alert_sink(
                    f"AUDIT_PERSISTENCE_FAILED:{self.stream.stream_id}:{type(exc).__name__}:{exc}"
                )
        return event


def assert_m7_scope(streams: Iterable[StreamConfig]) -> None:
    values = tuple(streams)
    providers = tuple(sorted({item.provider.value for item in values}))
    capabilities = tuple(sorted({item.capability for item in values}))
    if len(values) != 8:
        raise ValidationError(
            f"M7 Crypto certification requires exactly 8 streams, got {len(values)}"
        )
    if providers != M7_PROVIDERS:
        raise ValidationError(f"M7 providers must be {list(M7_PROVIDERS)}, got {list(providers)}")
    missing = sorted(set(M7_CAPABILITIES).difference(capabilities))
    if missing:
        raise ValidationError(f"M7 capabilities are missing: {missing}")
