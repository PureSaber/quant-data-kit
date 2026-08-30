"""Fail-closed public Crypto L2 collector orchestration."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Executor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from quant_data_kit.capture_v2.epoch import NormalizedEpochJournal, NormalizedEpochReceipt
from quant_data_kit.capture_v2.models import (
    CaptureConfig,
    CaptureState,
    CaptureStateMachine,
    Clock,
    MonotonicReceivedClock,
    Provider,
    RawFrame,
    StreamConfig,
    SymbolMappingResolver,
    SystemClock,
    assert_m7_scope,
    canonical_json_bytes,
    default_symbol_mappings,
    random_jitter,
    utc_text,
)
from quant_data_kit.capture_v2.storage import (
    ArchivePreflightReceipt,
    CapturePausedError,
    CaptureStorageGuard,
    DurableAuditStore,
    LocalArchiveController,
    RawSegmentWriter,
    SegmentArchiveReceipt,
    _atomic_immutable_write,
)
from quant_data_kit.capture_v2.synchronizers import (
    BinanceBookSynchronizer,
    NormalizationOutcome,
    OKXBookSynchronizer,
    ResyncRequired,
)
from quant_data_kit.capture_v2.transport import (
    HttpClient,
    UrllibHttpClient,
    WebSocketConnection,
    WebSocketConnector,
    WebsocketsConnector,
)
from quant_data_kit.data_lake import StoragePolicy
from quant_data_kit.exceptions import ValidationError

UTC = timezone.utc
_DEFAULT_POLICY = StoragePolicy()


@dataclass(frozen=True)
class CaptureStreamReport:
    stream_id: str
    provider: str
    capability: str
    outcome: str
    final_state: str
    websocket_messages: int
    raw_segments: int
    archived_segments: int
    normalized_epochs: int
    accepted_rows: int
    quarantined_rows: int
    resyncs: int
    duplicate_or_old_updates: int
    absent_level_deletes: int
    heartbeats: int
    first_received_at: str | None
    last_received_at: str | None
    audit_events: int
    audit_failures: int
    audit_chain_sha256: str | None
    audit_references: tuple[str, ...]
    errors: tuple[str, ...]
    epoch_receipts: tuple[str, ...]
    epoch_aborts: tuple[str, ...]
    archive_receipts: tuple[str, ...]
    pending_raw_messages: int
    pending_raw_segments: int


@dataclass(frozen=True)
class CaptureRunReport:
    schema_version: str
    run_id: str
    started_at: str
    ended_at: str
    status: str
    collector_commit: str
    providers: tuple[str, ...]
    capabilities: tuple[str, ...]
    streams: tuple[CaptureStreamReport, ...]
    archive_preflight_receipt: str | None
    audit_reference_sha256: str
    continuous_days: int
    market_data_certified: bool
    long_running_capture_started: bool
    report_sha256: str
    report_path: str | None


def _json_object(payload: bytes, provider: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ResyncRequired(f"{provider} message is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ResyncRequired(f"{provider} message must be a JSON object")
    if provider == "binance" and isinstance(value.get("data"), dict):
        return dict(value["data"])
    return value


def _frame_metadata(provider: Provider, payload: bytes) -> tuple[datetime | None, dict[str, int]]:
    message = _json_object(payload, provider.value)
    if provider is Provider.BINANCE:
        event_ms = message.get("T", message.get("E"))
        sequences = {
            key: int(message[key]) for key in ("U", "u", "pu") if message.get(key) is not None
        }
    else:
        data = message.get("data")
        item = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else {}
        event_ms = item.get("ts")
        sequences = {
            key: int(item[key]) for key in ("seqId", "prevSeqId") if item.get(key) is not None
        }
    if event_ms is None:
        return None, sequences
    try:
        event_time = datetime.fromtimestamp(int(event_ms) / 1000, tz=UTC)
    except (OSError, TypeError, ValueError) as exc:
        raise ResyncRequired(f"{provider.value} event timestamp is invalid") from exc
    return event_time, sequences


class CaptureStreamRunner:
    def __init__(
        self,
        config: CaptureConfig,
        stream: StreamConfig,
        *,
        storage_guard: CaptureStorageGuard,
        archive: LocalArchiveController,
        mappings: SymbolMappingResolver,
        http: HttpClient,
        websockets: WebSocketConnector,
        clock: Clock,
        jitter: Callable[[], float],
        alert_sink: Callable[[str], None],
        policy: StoragePolicy = _DEFAULT_POLICY,
        receive_timeout_seconds: float = 30.0,
        monotonic: Callable[[], float] = time.monotonic,
        normalization_executor: Executor | None = None,
    ) -> None:
        self.config = config
        self.stream = stream
        self.storage_guard = storage_guard
        self.archive = archive
        self.mappings = mappings
        self.http = http
        self.websockets = websockets
        self.clock = clock
        self.jitter = jitter
        self.alert_sink = alert_sink
        self.policy = policy
        self.receive_timeout_seconds = receive_timeout_seconds
        self.monotonic = monotonic
        self.normalization_executor = normalization_executor
        self.connection_id = f"conn-{uuid.uuid4().hex}"
        self._audit_failures = 0
        self.segment_writer = RawSegmentWriter(
            config.hot_root,
            stream.stream_id,
            stream.provider.value,
            collector_commit=config.collector_commit,
            rotation=config.rotation,
            storage_guard=storage_guard,
            policy=policy,
        )
        self.audit_store = DurableAuditStore(
            config.hot_root,
            stream_id=stream.stream_id,
            connection_id=self.connection_id,
            collector_commit=config.collector_commit,
            storage_guard=storage_guard,
        )
        self.state = CaptureStateMachine(
            stream,
            connection_id=self.connection_id,
            collector_commit=config.collector_commit,
            clock=clock,
            audit_sink=self.audit_store.append,
            alert_sink=self._alert,
        )
        self._received_clock = MonotonicReceivedClock(clock)
        self._journal: NormalizedEpochJournal | None = None
        self._epoch_receipts: list[NormalizedEpochReceipt] = []
        self._archive_receipts: list[SegmentArchiveReceipt] = []
        self._epoch_aborts: list[str] = []
        self._errors: list[str] = []
        self._websocket_messages = 0
        self._first_received_at: datetime | None = None
        self._last_received_at: datetime | None = None
        self._resyncs = 0
        self._observations: dict[str, int] = {}
        self._outcome = "RUNNING"
        self._probe_message_limit: int | None = None
        self._probe_deadline: float | None = None
        self._last_report: CaptureStreamReport | None = None

    async def run(self, *, maximum_websocket_messages: int | None) -> CaptureStreamReport:
        failed_attempts = 0
        try:
            while self.state.state is not CaptureState.PAUSED:
                if self.state.state is CaptureState.RESYNC:
                    self.state.transition(CaptureState.CONNECTING, "bounded_reconnect")
                    self._drain_segments()
                await self._run_connection(maximum_websocket_messages)
                if maximum_websocket_messages is not None:
                    self._outcome = "BOUNDED_COMPLETE"
                    await self._terminal_cleanup("bounded_probe_complete")
                    break
                raise ValidationError("long-running connection returned without a stop cause")
        except asyncio.CancelledError:
            await self._handle_cancellation()
            raise
        except Exception as exc:  # noqa: BLE001 - feed failures share one retry boundary
            while True:
                failed_attempts += 1
                self._errors.append(f"{type(exc).__name__}: {exc}")
                try:
                    self.state.audit(
                        "capture_failure",
                        str(exc),
                        {"attempt": failed_attempts, "exception": type(exc).__name__},
                    )
                except Exception as audit_exc:  # noqa: BLE001 - state remains fail-closed
                    self._errors.append(f"{type(audit_exc).__name__}: {audit_exc}")
                    self._outcome = "FAILED"
                    await self._terminal_cleanup("audit_persistence_failure", abort_reason=str(exc))
                    break
                try:
                    if self.state.state not in {CaptureState.CONNECTING, CaptureState.RESYNC}:
                        self.state.transition(
                            CaptureState.RESYNC,
                            "connection_or_sequence_failure",
                            {"exception": type(exc).__name__, "message": str(exc)},
                        )
                        self._resyncs += 1
                    self.segment_writer.flush()
                    self._drain_segments()
                    await asyncio.to_thread(self._finalize_epoch, str(exc))
                except Exception as cleanup_exc:  # noqa: BLE001 - cleanup failures always pause
                    self._errors.append(f"{type(cleanup_exc).__name__}: {cleanup_exc}")
                    self._outcome = "FAILED"
                    await self._terminal_cleanup("failure_cleanup_failed", abort_reason=str(exc))
                    break
                if failed_attempts >= self.config.retry.max_attempts:
                    self._outcome = "FAILED"
                    await self._terminal_cleanup("retry_budget_exhausted", abort_reason=str(exc))
                    break
                delay = self.config.retry.delay(failed_attempts, self.jitter)
                self.state.audit(
                    "retry_scheduled", "bounded_exponential_backoff", {"seconds": delay}
                )
                try:
                    await self.clock.sleep(delay)
                except asyncio.CancelledError:
                    await self._handle_cancellation()
                    raise
                try:
                    if self.state.state is CaptureState.RESYNC:
                        self.state.transition(CaptureState.CONNECTING, "bounded_reconnect")
                        self._drain_segments()
                    await self._run_connection(maximum_websocket_messages)
                    if maximum_websocket_messages is not None:
                        self._outcome = "BOUNDED_COMPLETE"
                        await self._terminal_cleanup("bounded_probe_complete")
                        break
                except asyncio.CancelledError:
                    await self._handle_cancellation()
                    raise
                except Exception as retry_exc:  # noqa: BLE001 - retry shares the same boundary
                    exc = retry_exc
                    continue
                raise ValidationError("long-running connection returned without a stop cause")
        self._last_report = self._report()
        return self._last_report

    async def _handle_cancellation(self) -> None:
        self._outcome = "CANCELLED"
        message = "CancelledError: capture task cancelled"
        if message not in self._errors:
            self._errors.append(message)
        await self._terminal_cleanup("capture_cancelled", abort_reason="capture_cancelled")
        self._last_report = self._report()

    async def _run_connection(self, maximum_websocket_messages: int | None) -> None:
        self._probe_message_limit = maximum_websocket_messages
        self._probe_deadline = (
            self.monotonic() + self.config.durability.probe_timeout_seconds
            if maximum_websocket_messages is not None
            else None
        )
        connection = await self.websockets.connect(
            self.stream.websocket_url,
            timeout_seconds=self.receive_timeout_seconds,
        )
        try:
            self.state.transition(CaptureState.BUFFERING, "tls_websocket_connected")
            self._drain_segments()
            self._new_epoch()
            if self.stream.provider is Provider.BINANCE:
                await self._run_binance(connection, maximum_websocket_messages)
            else:
                await self._run_okx(connection, maximum_websocket_messages)
        finally:
            await connection.close()

    async def _run_binance(
        self,
        connection: WebSocketConnection,
        maximum_websocket_messages: int | None,
    ) -> None:
        synchronizer = BinanceBookSynchronizer(self.stream, self.mappings)
        first = await self._receive_frame(connection)
        self.state.audit("websocket_buffered", "snapshot_not_yet_requested", {})
        self.state.transition(CaptureState.SNAPSHOT_SYNC, "requesting_https_depth_snapshot")
        self._drain_segments()
        assert self.stream.rest_snapshot_url is not None
        response = await self.http.get(
            self.stream.rest_snapshot_url,
            timeout_seconds=self.receive_timeout_seconds,
        )
        snapshot_frame = self._https_frame(response.body, response.url, event_time=first.event_time)
        self.segment_writer.append(snapshot_frame)
        self._apply_outcome(synchronizer.admit_snapshot(snapshot_frame))
        self._apply_outcome(synchronizer.admit_update(first))
        while not synchronizer.live:
            self._apply_outcome(synchronizer.admit_update(await self._receive_frame(connection)))
        self.state.transition(CaptureState.LIVE, "rest_snapshot_bridge_complete")
        self._drain_segments()
        while (
            maximum_websocket_messages is None
            or self._websocket_messages < maximum_websocket_messages
        ):
            self._apply_outcome(synchronizer.admit_update(await self._receive_frame(connection)))
            self._drain_segments()

    async def _run_okx(
        self,
        connection: WebSocketConnection,
        maximum_websocket_messages: int | None,
    ) -> None:
        synchronizer = OKXBookSynchronizer(self.stream, self.mappings)
        subscription = canonical_json_bytes(
            {
                "op": "subscribe",
                "args": [{"channel": self.stream.channel, "instId": self.stream.native_symbol}],
            }
        )
        await connection.send(subscription)
        self.state.audit("subscription_sent", "public_books_channel", {})
        self.state.transition(CaptureState.SNAPSHOT_SYNC, "awaiting_websocket_snapshot")
        self._drain_segments()
        while not synchronizer.live:
            self._apply_outcome(synchronizer.admit_message(await self._receive_frame(connection)))
            self._drain_segments()
        self.state.transition(CaptureState.LIVE, "websocket_snapshot_admitted")
        self._drain_segments()
        while (
            maximum_websocket_messages is None
            or self._websocket_messages < maximum_websocket_messages
        ):
            self._apply_outcome(synchronizer.admit_message(await self._receive_frame(connection)))
            self._drain_segments()

    async def _receive_frame(self, connection: WebSocketConnection) -> RawFrame:
        if (
            self._probe_message_limit is not None
            and self._websocket_messages >= self._probe_message_limit
        ):
            raise ResyncRequired(
                "bounded probe websocket message budget exhausted before synchronization completed"
            )
        timeout = self.receive_timeout_seconds
        if self._probe_deadline is not None:
            remaining = self._probe_deadline - self.monotonic()
            if remaining <= 0:
                raise ResyncRequired("bounded probe synchronization timeout exhausted")
            timeout = min(timeout, remaining)
        payload = await connection.receive(timeout_seconds=timeout)
        observed, received = self._received_clock.now()
        metadata_error: ResyncRequired | None = None
        try:
            event_time, sequences = _frame_metadata(self.stream.provider, payload)
        except ResyncRequired as exc:
            event_time, sequences = None, {}
            metadata_error = exc
        frame = RawFrame(
            frame_kind="market_data",
            provider=self.stream.provider.value,
            stream_id=self.stream.stream_id,
            connection_id=self.connection_id,
            subscription=self.stream.channel,
            transport="wss",
            tls_url=self.stream.websocket_url,
            received_at=received,
            observed_at=observed,
            event_time=event_time,
            payload=payload,
            native_sequence=sequences,
            collector_commit=self.config.collector_commit,
        )
        self.segment_writer.append(frame)
        self._websocket_messages += 1
        self._first_received_at = self._first_received_at or received
        self._last_received_at = received
        if metadata_error is not None:
            raise metadata_error
        return frame

    def _https_frame(
        self,
        payload: bytes,
        url: str,
        *,
        event_time: datetime | None,
    ) -> RawFrame:
        observed, received = self._received_clock.now()
        try:
            value = json.loads(payload)
            sequences = {"lastUpdateId": int(value["lastUpdateId"])}
        except (UnicodeError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            sequences = {}
        return RawFrame(
            frame_kind="rest_snapshot",
            provider=self.stream.provider.value,
            stream_id=self.stream.stream_id,
            connection_id=self.connection_id,
            subscription=self.stream.channel,
            transport="https",
            tls_url=url,
            received_at=received,
            observed_at=observed,
            event_time=event_time,
            payload=payload,
            native_sequence=sequences,
            collector_commit=self.config.collector_commit,
        )

    def _apply_outcome(self, outcome: NormalizationOutcome) -> None:
        if self._journal is None:
            raise ValidationError("Normalized epoch journal was not initialized")
        self._journal.append(outcome.records)
        for observation in outcome.observations:
            count = self._observations.get(observation.event, 0) + 1
            self._observations[observation.event] = count
            if count == 1 or count % 1024 == 0:
                self.state.audit(
                    observation.event,
                    observation.reason,
                    {**dict(observation.details), "occurrence_count": count},
                )

    def _new_epoch(self) -> None:
        if self._journal is not None:
            raise ValidationError("previous Normalized epoch was not finalized")
        epoch_id = f"{utc_text(self.clock.now(), 'epoch start').replace(':', '').replace('-', '')}-{uuid.uuid4().hex}"
        self._journal = NormalizedEpochJournal(
            self.config.hot_root,
            epoch_id=epoch_id,
            stream_id=self.stream.stream_id,
            provider=self.stream.provider.value,
            venue=self.stream.venue,
            storage_guard=self.storage_guard,
            policy=self.policy,
            flush_records=self.config.durability.normalized_flush_records,
            flush_bytes=self.config.durability.normalized_flush_bytes,
            flush_seconds=self.config.durability.normalized_flush_seconds,
            monotonic=self.monotonic,
            normalization_executor=self.normalization_executor,
        )

    def _drain_segments(self) -> None:
        for segment in self.segment_writer.peek_completed():
            receipt = self.archive.archive_segment(segment)
            if not receipt.archive_restore_verified or not receipt.eligible_for_cleanup:
                raise CapturePausedError("CAPTURE_PAUSED: segment archive was not restore-verified")
            if self._journal is not None:
                self._journal.record_segment(segment)
            self._archive_receipts.append(receipt)
            self.segment_writer.acknowledge_completed(segment)

    def _finalize_epoch(self, abort_reason: str | None = None) -> None:
        if self._journal is None:
            return
        journal = self._journal
        if abort_reason is not None:
            path = journal.abort_visible(abort_reason)
            self._epoch_aborts.append(str(path))
            self._journal = None
            return
        receipt = journal.finalize(created_at=self.clock.now())
        self._epoch_receipts.append(receipt)
        self._journal = None

    async def _terminal_cleanup(self, reason: str, *, abort_reason: str | None = None) -> None:
        cleanup_errors: list[Exception] = []
        deferred_cancellation: asyncio.CancelledError | None = None
        try:
            self._pause(reason)
        except Exception as exc:  # noqa: BLE001 - later durability steps must still run
            cleanup_errors.append(exc)
        try:
            self.segment_writer.flush()
            self._drain_segments()
        except Exception as exc:  # noqa: BLE001 - retain Raw references for operator recovery
            cleanup_errors.append(exc)
        finalize_task = asyncio.create_task(asyncio.to_thread(self._finalize_epoch, abort_reason))
        while not finalize_task.done():
            try:
                await asyncio.shield(finalize_task)
            except asyncio.CancelledError as exc:
                deferred_cancellation = deferred_cancellation or exc
        try:
            finalize_task.result()
        except Exception as exc:  # noqa: BLE001 - retain journal on finalize failure
            cleanup_errors.append(exc)
        for exc in cleanup_errors:
            message = f"{type(exc).__name__}: {exc}"
            if message not in self._errors:
                self._errors.append(message)
            self._alert(
                f"TERMINAL_DURABILITY_FAILURE:{self.stream.stream_id}:{type(exc).__name__}:{exc}"
            )
        if cleanup_errors:
            self._outcome = "FAILED"
        if deferred_cancellation is not None:
            raise deferred_cancellation

    def _pause(self, reason: str, exc: Exception | None = None) -> None:
        if self.state.state is CaptureState.PAUSED:
            return
        details = {"exception": type(exc).__name__, "message": str(exc)} if exc is not None else {}
        # PAUSED is a legal terminal transition from every non-terminal capture state.
        # The state machine remains the single authority for rejecting illegal transitions.
        self.state.transition(CaptureState.PAUSED, reason, details)
        self._alert(f"CAPTURE_PAUSED:{self.stream.stream_id}:{reason}")

    def _alert(self, message: str) -> None:
        if message.startswith("AUDIT_PERSISTENCE_FAILED"):
            self._audit_failures += 1
        self.alert_sink(message)

    def _report(self) -> CaptureStreamReport:
        accepted = sum(item.accepted_rows for item in self._epoch_receipts)
        quarantined = sum(item.quarantined_rows for item in self._epoch_receipts)
        references = self.state.references
        audit_chain = references[-1].audit_sha256 if references else None
        return CaptureStreamReport(
            stream_id=self.stream.stream_id,
            provider=self.stream.provider.value,
            capability=self.stream.capability,
            outcome=self._outcome,
            final_state=self.state.state.value,
            websocket_messages=self._websocket_messages,
            raw_segments=self.segment_writer.segment_count,
            archived_segments=len(self._archive_receipts),
            normalized_epochs=len(self._epoch_receipts),
            accepted_rows=accepted,
            quarantined_rows=quarantined,
            resyncs=self._resyncs,
            duplicate_or_old_updates=self._observations.get("duplicate_or_old_update", 0),
            absent_level_deletes=self._observations.get("absent_level_delete", 0),
            heartbeats=self._observations.get("book_heartbeat", 0),
            first_received_at=(
                utc_text(self._first_received_at, "first_received_at")
                if self._first_received_at
                else None
            ),
            last_received_at=(
                utc_text(self._last_received_at, "last_received_at")
                if self._last_received_at
                else None
            ),
            audit_events=len(self.state.events),
            audit_failures=self._audit_failures,
            audit_chain_sha256=audit_chain,
            audit_references=tuple(item.audit_path for item in references),
            errors=tuple(self._errors),
            epoch_receipts=tuple(item.receipt_path for item in self._epoch_receipts),
            epoch_aborts=tuple(self._epoch_aborts),
            archive_receipts=tuple(item.receipt_path for item in self._archive_receipts),
            pending_raw_messages=self.segment_writer.pending_messages,
            pending_raw_segments=len(self.segment_writer.peek_completed()),
        )


class CryptoL2CaptureCoordinator:
    """Preflight all storage before any of the eight public market connections can start."""

    def __init__(
        self,
        config: CaptureConfig,
        *,
        storage_guard: CaptureStorageGuard | None = None,
        archive: LocalArchiveController | None = None,
        mappings: SymbolMappingResolver | None = None,
        http: HttpClient | None = None,
        websockets: WebSocketConnector | None = None,
        clock: Clock | None = None,
        jitter: Callable[[], float] = random_jitter,
        alert_sink: Callable[[str], None] | None = None,
        policy: StoragePolicy = _DEFAULT_POLICY,
        monotonic: Callable[[], float] = time.monotonic,
        normalization_executor: Executor | None = None,
    ) -> None:
        assert_m7_scope(config.streams)
        self.config = config
        self.clock = clock or SystemClock()
        self.alert_sink = alert_sink or (lambda _message: None)
        self.monotonic = monotonic
        self.storage_guard = storage_guard or CaptureStorageGuard(
            config.hot_root,
            config.archive_root,
            policy=policy,
            archive_reserve_bytes=config.archive_reserve_bytes,
            monotonic=monotonic,
            probe_messages=config.durability.capacity_probe_messages,
            probe_bytes=config.durability.capacity_probe_bytes,
            probe_seconds=config.durability.capacity_probe_seconds,
        )
        self.archive = archive or LocalArchiveController(
            config.hot_root,
            config.archive_root,
            config.restore_root,
            storage_guard=self.storage_guard,
            clock=self.clock.now,
        )
        self.mappings = mappings or SymbolMappingResolver(default_symbol_mappings(config.streams))
        self.http = http or UrllibHttpClient()
        self.websockets = websockets or WebsocketsConnector()
        self.jitter = jitter
        self.policy = policy
        self.normalization_executor = normalization_executor

    def preflight(self) -> ArchivePreflightReceipt:
        return self.archive.preflight()

    def preflight_only(self) -> CaptureRunReport:
        started = self.clock.now()
        run_id = f"capture-{uuid.uuid4().hex}"
        preflight: ArchivePreflightReceipt | None = None
        errors: tuple[str, ...] = ()
        try:
            preflight = self.preflight()
            status = "PREFLIGHT_PASSED_NETWORK_NOT_STARTED"
        except Exception as exc:  # noqa: BLE001 - all preflight failures become PAUSED
            status = "PAUSED_PREFLIGHT_FAILED"
            errors = (f"{type(exc).__name__}: {exc}",)
            self.alert_sink(f"CAPTURE_PAUSED:PREFLIGHT_FAILED:{type(exc).__name__}:{exc}")
        reports = tuple(
            self._empty_stream_report(stream, errors=errors) for stream in self.config.streams
        )
        return self._write_report(
            run_id,
            started,
            reports,
            preflight=preflight,
            status=status,
            long_running_capture_started=False,
        )

    async def run(self, *, maximum_websocket_messages: int | None) -> CaptureRunReport:
        started = self.clock.now()
        run_id = f"capture-{uuid.uuid4().hex}"
        try:
            preflight = self.preflight()
        except Exception as exc:  # noqa: BLE001 - preflight must convert every failure to PAUSED
            self.alert_sink(f"CAPTURE_PAUSED:PREFLIGHT_FAILED:{type(exc).__name__}:{exc}")
            reports = tuple(
                self._empty_stream_report(stream, errors=(f"{type(exc).__name__}: {exc}",))
                for stream in self.config.streams
            )
            return self._write_report(
                run_id,
                started,
                reports,
                preflight=None,
                status="PAUSED_PREFLIGHT_FAILED",
                long_running_capture_started=False,
            )
        try:
            reconciled = NormalizedEpochJournal.reconcile_pending(
                self.config.hot_root,
                storage_guard=self.storage_guard,
                policy=self.policy,
                normalization_executor=self.normalization_executor,
            )
        except Exception as exc:  # noqa: BLE001 - unresolved publication blocks every stream
            error = f"{type(exc).__name__}: {exc}"
            self.alert_sink(f"CAPTURE_PAUSED:EPOCH_RECOVERY_FAILED:{error}")
            reports = tuple(
                self._empty_stream_report(stream, errors=(error,)) for stream in self.config.streams
            )
            return self._write_report(
                run_id,
                started,
                reports,
                preflight=preflight,
                status="CAPTURE_FAILED",
                long_running_capture_started=False,
            )
        if reconciled:
            self.alert_sink(f"CAPTURE_EPOCH_RECOVERY_COMMITTED:{len(reconciled)}")
        runners: list[CaptureStreamRunner] = []
        try:
            for stream in self.config.streams:
                runners.append(
                    CaptureStreamRunner(
                        self.config,
                        stream,
                        storage_guard=self.storage_guard,
                        archive=self.archive,
                        mappings=self.mappings,
                        http=self.http,
                        websockets=self.websockets,
                        clock=self.clock,
                        jitter=self.jitter,
                        alert_sink=self.alert_sink,
                        policy=self.policy,
                        monotonic=self.monotonic,
                        normalization_executor=self.normalization_executor,
                    )
                )
        except Exception as exc:  # noqa: BLE001 - initialization failure blocks all streams
            reports = tuple(
                (
                    runners[index]._report()
                    if index < len(runners)
                    else self._empty_stream_report(
                        stream,
                        errors=(f"{type(exc).__name__}: {exc}",),
                    )
                )
                for index, stream in enumerate(self.config.streams)
            )
            return self._write_report(
                run_id,
                started,
                reports,
                preflight=preflight,
                status="CAPTURE_FAILED",
                long_running_capture_started=False,
            )
        tasks = {
            asyncio.create_task(
                runner.run(maximum_websocket_messages=maximum_websocket_messages)
            ): runner
            for runner in runners
        }
        first_cause: str | None = None
        try:
            pending = set(tasks)
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    runner = tasks[task]
                    try:
                        result = task.result()
                    except asyncio.CancelledError:
                        result = runner._last_report or runner._report()
                    except Exception as exc:  # noqa: BLE001 - coordinator retains first cause
                        first_cause = first_cause or f"{type(exc).__name__}: {exc}"
                        runner._errors.append(first_cause)
                        runner._outcome = "FAILED"
                        await runner._terminal_cleanup(
                            "coordinator_stream_exception", abort_reason=first_cause
                        )
                        result = runner._report()
                    if not self._stream_succeeded(result, maximum_websocket_messages):
                        first_cause = first_cause or (
                            result.errors[0] if result.errors else f"{result.stream_id}: failed"
                        )
                if first_cause is not None:
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                    break
        except asyncio.CancelledError:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            reports = tuple(runner._last_report or runner._report() for runner in runners)
            self._write_report(
                run_id,
                started,
                reports,
                preflight=preflight,
                status="CAPTURE_CANCELLED",
                long_running_capture_started=maximum_websocket_messages is None,
            )
            raise
        reports = tuple(runner._last_report or runner._report() for runner in runners)
        all_succeeded = all(
            self._stream_succeeded(item, maximum_websocket_messages) for item in reports
        )
        status = "BOUNDED_PROBE_COMPLETE" if all_succeeded else "CAPTURE_FAILED"
        if maximum_websocket_messages is None:
            status = "STOPPED_WITHOUT_CERTIFICATION" if all_succeeded else "CAPTURE_FAILED"
        return self._write_report(
            run_id,
            started,
            reports,
            preflight=preflight,
            status=status,
            long_running_capture_started=maximum_websocket_messages is None,
        )

    @staticmethod
    def _stream_succeeded(
        report: CaptureStreamReport, maximum_websocket_messages: int | None
    ) -> bool:
        return bool(
            maximum_websocket_messages is not None
            and report.outcome == "BOUNDED_COMPLETE"
            and report.final_state == CaptureState.PAUSED.value
            and not report.errors
            and report.audit_failures == 0
            and report.websocket_messages == maximum_websocket_messages
            and report.raw_segments == report.archived_segments
            and report.normalized_epochs >= 1
            and report.pending_raw_messages == 0
            and report.pending_raw_segments == 0
        )

    @staticmethod
    def _empty_stream_report(
        stream: StreamConfig, *, errors: tuple[str, ...]
    ) -> CaptureStreamReport:
        return CaptureStreamReport(
            stream_id=stream.stream_id,
            provider=stream.provider.value,
            capability=stream.capability,
            outcome="FAILED" if errors else "NOT_STARTED",
            final_state=CaptureState.PAUSED.value,
            websocket_messages=0,
            raw_segments=0,
            archived_segments=0,
            normalized_epochs=0,
            accepted_rows=0,
            quarantined_rows=0,
            resyncs=0,
            duplicate_or_old_updates=0,
            absent_level_deletes=0,
            heartbeats=0,
            first_received_at=None,
            last_received_at=None,
            audit_events=0,
            audit_failures=0,
            audit_chain_sha256=None,
            audit_references=(),
            errors=errors,
            epoch_receipts=(),
            epoch_aborts=(),
            archive_receipts=(),
            pending_raw_messages=0,
            pending_raw_segments=0,
        )

    def _write_report(
        self,
        run_id: str,
        started_at: datetime,
        streams: tuple[CaptureStreamReport, ...],
        *,
        preflight: ArchivePreflightReceipt | None,
        status: str,
        long_running_capture_started: bool,
    ) -> CaptureRunReport:
        ended = self.clock.now()
        audit_identity = [
            {
                "stream_id": item.stream_id,
                "audit_chain_sha256": item.audit_chain_sha256,
                "audit_references": list(item.audit_references),
            }
            for item in streams
        ]
        audit_reference_hash = hashlib.sha256(canonical_json_bytes(audit_identity)).hexdigest()
        identity = {
            "schema_version": "puresaber.crypto-l2-capture-run@1.1.0",
            "run_id": run_id,
            "started_at": utc_text(started_at, "started_at"),
            "ended_at": utc_text(ended, "ended_at"),
            "status": status,
            "collector_commit": self.config.collector_commit,
            "providers": list(self.config.providers),
            "capabilities": list(self.config.capabilities),
            "streams": [asdict(item) for item in streams],
            "archive_preflight_receipt": preflight.receipt_path if preflight else None,
            "audit_reference_sha256": audit_reference_hash,
            "continuous_days": 0,
            "market_data_certified": False,
            "long_running_capture_started": long_running_capture_started,
        }
        report_hash = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        payload = {**identity, "report_sha256": report_hash}
        report_path: Path | None = None
        try:
            report_path = self.config.hot_root / "capture" / "run-reports" / f"{run_id}.json"
            _atomic_immutable_write(
                report_path, canonical_json_bytes(payload), root=self.config.hot_root
            )
        except Exception as exc:  # noqa: BLE001 - report persistence is part of outcome
            self.alert_sink(f"CAPTURE_REPORT_PERSISTENCE_FAILED:{type(exc).__name__}:{exc}")
            report_path = None
            status = "CAPTURE_REPORT_PERSISTENCE_FAILED"
            identity["status"] = status
            report_hash = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        return CaptureRunReport(
            schema_version=identity["schema_version"],
            run_id=run_id,
            started_at=identity["started_at"],
            ended_at=identity["ended_at"],
            status=status,
            collector_commit=self.config.collector_commit,
            providers=self.config.providers,
            capabilities=self.config.capabilities,
            streams=streams,
            archive_preflight_receipt=identity["archive_preflight_receipt"],
            audit_reference_sha256=audit_reference_hash,
            continuous_days=0,
            market_data_certified=False,
            long_running_capture_started=long_running_capture_started,
            report_sha256=report_hash,
            report_path=str(report_path) if report_path else None,
        )
