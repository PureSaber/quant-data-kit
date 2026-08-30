"""Offline eight-stream M7 Raw→Normalized→archive/restore benchmark."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from collections import deque
from concurrent.futures import Executor, Future, ProcessPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from multiprocessing import get_context
from pathlib import Path
from threading import Lock, Timer

from quant_data_kit.capture_v2.collector import CryptoL2CaptureCoordinator
from quant_data_kit.capture_v2.epoch import _publish_epoch_group, _publish_epoch_parts
from quant_data_kit.capture_v2.models import (
    CaptureConfig,
    CaptureDurabilityPolicy,
    Clock,
    MarketKind,
    Provider,
    RetryPolicy,
    SegmentRotation,
    StreamConfig,
    canonical_json_bytes,
    default_crypto_l2_streams,
)
from quant_data_kit.capture_v2.storage import (
    CaptureStorageGuard,
    DiskCapacity,
    LocalArchiveController,
    VolumeIdentity,
)
from quant_data_kit.capture_v2.transport import HttpResponse
from quant_data_kit.data_lake import StoragePolicy
from quant_data_kit.exceptions import ProviderError

UTC = timezone.utc
START = datetime(2026, 8, 29, tzinfo=UTC)
START_MS = int(START.timestamp() * 1000)
MINIMUM_THROUGHPUT_MESSAGES_PER_SECOND = 240.0
MAXIMUM_EVENT_LOOP_P99_MILLISECONDS = 100.0
TARGET_LIVE_OFFER_RATE_MESSAGES_PER_SECOND = 80.0
MINIMUM_THROUGHPUT_HEADROOM_MULTIPLE = 3.0
NORMALIZATION_PROCESS_WORKERS = 4
STREAMS_PER_PROVIDER = 2


class ProviderBatchingExecutor(Executor):
    """Batch two same-provider durable journals to balance claims and parallelism."""

    def __init__(self, delegate: Executor, *, flush_seconds: float = 0.1) -> None:
        self.delegate = delegate
        self.flush_seconds = flush_seconds
        self._lock = Lock()
        self._pending: dict[str, list[tuple[tuple[object, ...], Future[object]]]] = {}
        self._timers: dict[str, Timer] = {}
        self.submit_times: list[float] = []
        self.group_start_times: list[float] = []
        self.group_end_times: list[float] = []
        self._delegated: list[Future[object]] = []

    def submit(self, fn, /, *args, **kwargs):
        if fn is not _publish_epoch_parts or kwargs:
            raise RuntimeError("provider batching executor only accepts sealed capture epochs")
        provider = str(args[3])
        future: Future[object] = Future()
        with self._lock:
            self.submit_times.append(time.perf_counter())
            entries = self._pending.setdefault(provider, [])
            entries.append((args, future))
            if len(entries) == 1:
                timer = Timer(self.flush_seconds, self._flush_provider, args=(provider,))
                timer.daemon = True
                self._timers[provider] = timer
                timer.start()
            if len(entries) == STREAMS_PER_PROVIDER:
                self._launch_locked(provider)
        return future

    def _flush_provider(self, provider: str) -> None:
        with self._lock:
            if self._pending.get(provider):
                self._launch_locked(provider)

    def _launch_locked(self, provider: str) -> None:
        entries = self._pending.pop(provider, [])
        timer = self._timers.pop(provider, None)
        if timer is not None:
            timer.cancel()
        if not entries:
            return
        group = tuple(entry[0] for entry in entries)
        self.group_start_times.append(time.perf_counter())
        delegated = self.delegate.submit(_publish_epoch_group, group)
        self._delegated.append(delegated)

        def complete(source: Future[object]) -> None:
            self.group_end_times.append(time.perf_counter())
            try:
                results = source.result()
                if not isinstance(results, tuple) or len(results) != len(entries):
                    raise RuntimeError("provider Normalized batch result count changed")
            except BaseException as exc:  # noqa: BLE001 - propagate exact worker failure
                for _arguments, target in entries:
                    target.set_exception(exc)
                return
            for result, (_arguments, target) in zip(results, entries, strict=True):
                target.set_result(result)

        delegated.add_done_callback(complete)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        with self._lock:
            providers = tuple(self._pending)
            for provider in providers:
                self._launch_locked(provider)
            delegated = tuple(self._delegated)
        if cancel_futures:
            for future in delegated:
                future.cancel()
        if wait:
            for future in delegated:
                future.result()

    def assert_quiescent(self) -> None:
        with self._lock:
            if self._pending or self._timers or any(not item.done() for item in self._delegated):
                raise RuntimeError("normalization executor is not quiescent")


@dataclass
class BenchmarkTiming:
    started_at: float
    workload_completed_at: float | None = None
    pool_closed_at: float | None = None

    def mark_workload_completed(self, observed_at: float) -> None:
        if self.workload_completed_at is not None or observed_at < self.started_at:
            raise RuntimeError("benchmark workload timing boundary is invalid")
        self.workload_completed_at = observed_at

    def mark_pool_closed(self, observed_at: float) -> None:
        if self.workload_completed_at is None or observed_at < self.workload_completed_at:
            raise RuntimeError("worker pool closed before workload completion")
        self.pool_closed_at = observed_at

    @property
    def workload_seconds(self) -> float:
        if self.workload_completed_at is None:
            raise RuntimeError("benchmark workload has not completed")
        return self.workload_completed_at - self.started_at

    @property
    def worker_teardown_seconds(self) -> float:
        if self.workload_completed_at is None or self.pool_closed_at is None:
            raise RuntimeError("benchmark worker teardown has not completed")
        return self.pool_closed_at - self.workload_completed_at

    @property
    def total_wall_seconds(self) -> float:
        if self.pool_closed_at is None:
            raise RuntimeError("benchmark worker pool has not closed")
        return self.pool_closed_at - self.started_at


@dataclass(frozen=True)
class BenchmarkScenario:
    name: str
    delta_levels_per_side: int
    binance_snapshot_levels_per_side: int
    okx_snapshot_levels_per_side: int
    description: str

    def __post_init__(self) -> None:
        if (
            min(
                self.delta_levels_per_side,
                self.binance_snapshot_levels_per_side,
                self.okx_snapshot_levels_per_side,
            )
            < 1
        ):
            raise ValueError("benchmark depth values must be positive")


SCENARIOS = {
    "sparse": BenchmarkScenario(
        name="sparse",
        delta_levels_per_side=1,
        binance_snapshot_levels_per_side=1,
        okx_snapshot_levels_per_side=1,
        description="One bid and one ask per snapshot/update; retained compatibility baseline.",
    ),
    "dense-burst": BenchmarkScenario(
        name="dense-burst",
        delta_levels_per_side=20,
        binance_snapshot_levels_per_side=1000,
        okx_snapshot_levels_per_side=400,
        description=(
            "Every incremental frame updates 20 bids and 20 asks. Binance REST snapshots cover "
            "the configured limit=1000 depth per side; OKX books snapshots cover the channel's "
            "representative 400-level depth per side."
        ),
    ),
}


class BenchmarkClock(Clock):
    def __init__(self) -> None:
        self.current = START

    def now(self) -> datetime:
        value = self.current
        self.current += timedelta(milliseconds=10)
        return value

    async def sleep(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)
        await asyncio.sleep(0)


class FixtureHttp:
    def __init__(self, streams: tuple[StreamConfig, ...], scenario: BenchmarkScenario) -> None:
        self._streams_by_url = {
            item.rest_snapshot_url: item for item in streams if item.rest_snapshot_url is not None
        }
        self._scenario = scenario

    async def get(self, url: str, *, timeout_seconds: float) -> HttpResponse:
        if timeout_seconds <= 0:
            raise ProviderError("benchmark HTTP timeout must be positive")
        try:
            stream = self._streams_by_url[url]
        except KeyError as exc:
            raise ProviderError(f"benchmark received an unexpected snapshot URL: {url}") from exc
        body = canonical_json_bytes(
            {
                "lastUpdateId": 100,
                "bids": _book_levels(
                    "bid", self._scenario.binance_snapshot_levels_per_side, quantity_seed=2_000_000
                ),
                "asks": _book_levels(
                    "ask", self._scenario.binance_snapshot_levels_per_side, quantity_seed=3_000_000
                ),
                "symbol": stream.native_symbol,
            }
        )
        return HttpResponse(url=url, status=200, body=body)


class FixtureConnection:
    def __init__(
        self,
        stream: StreamConfig,
        message_count: int,
        scenario: BenchmarkScenario,
    ) -> None:
        self.stream = stream
        self.messages = deque(_messages(stream, message_count, scenario))
        self.sent: list[bytes] = []
        self.closed = False

    async def send(self, payload: bytes) -> None:
        self.sent.append(payload)

    async def receive(self, *, timeout_seconds: float) -> bytes:
        if timeout_seconds <= 0:
            raise ProviderError("benchmark WebSocket timeout must be positive")
        await asyncio.sleep(0)
        if not self.messages:
            raise ProviderError("offline fixture was exhausted")
        return self.messages.popleft()

    async def close(self) -> None:
        self.closed = True


class FixtureConnector:
    def __init__(
        self,
        streams: tuple[StreamConfig, ...],
        message_count: int,
        scenario: BenchmarkScenario,
    ) -> None:
        self.unassigned = list(streams)
        self.message_count = message_count
        self.scenario = scenario
        self.connections: list[FixtureConnection] = []

    async def connect(self, url: str, *, timeout_seconds: float) -> FixtureConnection:
        if timeout_seconds <= 0:
            raise ProviderError("benchmark connect timeout must be positive")
        stream = next(item for item in self.unassigned if item.websocket_url == url)
        self.unassigned.remove(stream)
        connection = FixtureConnection(stream, self.message_count, self.scenario)
        self.connections.append(connection)
        return connection


def _book_levels(side: str, count: int, *, quantity_seed: int) -> list[list[str]]:
    if side not in {"bid", "ask"}:
        raise ValueError(f"unsupported book side: {side}")
    start = Decimal(100) if side == "bid" else Decimal(101)
    direction = Decimal("-0.01") if side == "bid" else Decimal("0.01")
    return [
        [format(start + direction * index, ".2f"), str(quantity_seed + index)]
        for index in range(count)
    ]


def _update_levels(
    side: str,
    count: int,
    *,
    message_index: int,
    quantity_seed: int,
) -> list[list[str]]:
    levels = _book_levels(side, count, quantity_seed=quantity_seed)
    for level_index, level in enumerate(levels):
        level[1] = str(quantity_seed + message_index * count + level_index)
    return levels


def _messages(
    stream: StreamConfig, count: int, scenario: BenchmarkScenario | None = None
) -> list[bytes]:
    selected = scenario or SCENARIOS["sparse"]
    if stream.provider is Provider.BINANCE:
        values = []
        for index in range(count):
            final = 101 + index
            first = 99 if index == 0 else final
            payload = {
                "e": "depthUpdate",
                "E": START_MS + index,
                "T": START_MS + index,
                "s": stream.native_symbol,
                "U": first,
                "u": final,
                "b": _update_levels(
                    "bid",
                    selected.delta_levels_per_side,
                    message_index=index,
                    quantity_seed=2_000_000,
                ),
                "a": _update_levels(
                    "ask",
                    selected.delta_levels_per_side,
                    message_index=index,
                    quantity_seed=3_000_000,
                ),
            }
            if stream.market is MarketKind.USDT_PERPETUAL:
                payload["pu"] = 100 if index == 0 else final - 1
            values.append(canonical_json_bytes(payload))
        return values
    values = [
        canonical_json_bytes(
            {
                "event": "subscribe",
                "arg": {"channel": "books", "instId": stream.native_symbol},
                "code": "0",
                "msg": "",
            }
        ),
        canonical_json_bytes(
            {
                "arg": {"channel": "books", "instId": stream.native_symbol},
                "action": "snapshot",
                "data": [
                    {
                        "ts": str(START_MS),
                        "seqId": 10,
                        "prevSeqId": -1,
                        "checksum": 0,
                        "bids": _book_levels(
                            "bid",
                            selected.okx_snapshot_levels_per_side,
                            quantity_seed=2_000_000,
                        ),
                        "asks": _book_levels(
                            "ask",
                            selected.okx_snapshot_levels_per_side,
                            quantity_seed=3_000_000,
                        ),
                    }
                ],
            }
        ),
    ]
    for index in range(2, count):
        sequence = 9 + index
        values.append(
            canonical_json_bytes(
                {
                    "arg": {"channel": "books", "instId": stream.native_symbol},
                    "action": "update",
                    "data": [
                        {
                            "ts": str(START_MS + index),
                            "seqId": sequence,
                            "prevSeqId": sequence - 1,
                            "checksum": 0,
                            "bids": _update_levels(
                                "bid",
                                selected.delta_levels_per_side,
                                message_index=index,
                                quantity_seed=2_000_000,
                            ),
                            "asks": _update_levels(
                                "ask",
                                selected.delta_levels_per_side,
                                message_index=index,
                                quantity_seed=3_000_000,
                            ),
                        }
                    ],
                }
            )
        )
    return values


def _expected_normalized_rows(messages_per_stream: int, scenario: BenchmarkScenario) -> int:
    records_per_update = scenario.delta_levels_per_side * 2
    binance_rows = 4 * (1 + messages_per_stream * records_per_update)
    okx_rows = 4 * (1 + (messages_per_stream - 2) * records_per_update)
    return binance_rows + okx_rows


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * quantile))
    return ordered[index]


async def _event_loop_sampler(stop: asyncio.Event, samples: list[float]) -> None:
    interval = 0.001
    expected = time.perf_counter() + interval
    while not stop.is_set():
        await asyncio.sleep(interval)
        observed = time.perf_counter()
        samples.append(max(0.0, observed - expected) * 1_000)
        expected = observed + interval


def _git_metadata(repository: Path) -> tuple[str, bool]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    return commit, dirty


async def _run_benchmark(
    repository: Path,
    messages_per_stream: int,
    scenario: BenchmarkScenario | str = "sparse",
) -> dict[str, object]:
    selected = SCENARIOS[scenario] if isinstance(scenario, str) else scenario
    streams = default_crypto_l2_streams()
    with tempfile.TemporaryDirectory(prefix="qdk-m7-benchmark-") as temporary_text:
        temporary = Path(temporary_text)
        hot, archive, restore = (
            temporary / "hot",
            temporary / "archive",
            temporary / "restore",
        )
        for root in (hot, archive, restore):
            root.mkdir()
        policy = StoragePolicy(
            hot_quota_bytes=16 * 1024**3,
            minimum_free_bytes=1,
            minimum_free_fraction=0.000001,
        )

        def identity(path: Path) -> VolumeIdentity:
            return VolumeIdentity(f"fixture-{path.name}", (f"fixture-device-{path.name}",))

        guard = CaptureStorageGuard(
            hot,
            archive,
            policy=policy,
            archive_reserve_bytes=1,
            volume_identity=identity,
            capacity_probe=lambda _path: DiskCapacity(64 * 1024**3, 48 * 1024**3),
        )
        clock = BenchmarkClock()
        connector = FixtureConnector(streams, messages_per_stream, selected)
        config = CaptureConfig(
            hot_root=hot,
            archive_root=archive,
            restore_root=restore,
            collector_commit=_git_metadata(repository)[0],
            streams=streams,
            rotation=SegmentRotation(
                max_messages=256,
                max_wire_bytes=8 * 1024**2,
                max_age_seconds=30,
            ),
            retry=RetryPolicy(max_attempts=1),
            durability=CaptureDurabilityPolicy(probe_timeout_seconds=120.0),
            archive_reserve_bytes=1,
        )
        samples: list[float] = []
        stop = asyncio.Event()
        sampler = asyncio.create_task(_event_loop_sampler(stop, samples))
        timing = BenchmarkTiming(started_at=time.perf_counter())
        with ProcessPoolExecutor(
            max_workers=NORMALIZATION_PROCESS_WORKERS,
            mp_context=get_context("spawn"),
        ) as process_executor:
            normalization_executor = ProviderBatchingExecutor(process_executor)
            coordinator = CryptoL2CaptureCoordinator(
                config,
                storage_guard=guard,
                archive=LocalArchiveController(
                    hot, archive, restore, storage_guard=guard, clock=clock.now
                ),
                http=FixtureHttp(streams, selected),
                websockets=connector,
                clock=clock,
                jitter=lambda: 0.5,
                policy=policy,
                normalization_executor=normalization_executor,
            )
            report = await coordinator.run(maximum_websocket_messages=messages_per_stream)
            normalization_executor.shutdown()
            normalization_executor.assert_quiescent()
            timing.mark_workload_completed(time.perf_counter())
            stop.set()
        timing.mark_pool_closed(time.perf_counter())
        await sampler
        elapsed = timing.workload_seconds
        websocket_messages = sum(item.websocket_messages for item in report.streams)
        raw_frames = websocket_messages + sum(
            1 for item in report.streams if item.provider == Provider.BINANCE.value
        )
        accepted_rows = sum(item.accepted_rows for item in report.streams)
        raw_segments = sum(item.raw_segments for item in report.streams)
        archive_segments = sum(item.archived_segments for item in report.streams)
        throughput = websocket_messages / elapsed
        normalized_throughput = accepted_rows / elapsed
        expected_rows = _expected_normalized_rows(messages_per_stream, selected)
        normalized_rows_per_message = accepted_rows / websocket_messages
        target_normalized_rows_per_second = (
            TARGET_LIVE_OFFER_RATE_MESSAGES_PER_SECOND * normalized_rows_per_message
        )
        minimum_normalized_rows_per_second = (
            target_normalized_rows_per_second * MINIMUM_THROUGHPUT_HEADROOM_MULTIPLE
        )
        p95 = _percentile(samples, 0.95)
        p99 = _percentile(samples, 0.99)
        last_epoch_submit_seconds = max(normalization_executor.submit_times) - timing.started_at
        normalization_group_window_seconds = max(normalization_executor.group_end_times) - min(
            normalization_executor.group_start_times
        )
        consistency = bool(
            report.status == "BOUNDED_PROBE_COMPLETE"
            and len(report.streams) == 8
            and websocket_messages == 8 * messages_per_stream
            and accepted_rows == expected_rows
            and raw_segments == archive_segments
            and all(item.normalized_epochs == 1 for item in report.streams)
            and all(not item.errors for item in report.streams)
            and all(item.pending_raw_messages == 0 for item in report.streams)
            and all(item.pending_raw_segments == 0 for item in report.streams)
        )
        gate_passed = bool(
            consistency
            and throughput >= MINIMUM_THROUGHPUT_MESSAGES_PER_SECOND
            and normalized_throughput >= minimum_normalized_rows_per_second
            and p99 <= MAXIMUM_EVENT_LOOP_P99_MILLISECONDS
        )
        commit, dirty = _git_metadata(repository)
        return {
            "schema_version": "puresaber.m7-offline-capture-benchmark@1.2.0",
            "scope": "OFFLINE_FIXTURE_NOT_NETWORK",
            "scenario": {
                **asdict(selected),
                "binance_configured_snapshot_depth_basis": "REST limit=1000 per side",
                "okx_configured_snapshot_depth_basis": "books channel representative depth=400 per side",
            },
            "measured_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
            "repository": str(repository),
            "collector_commit": commit,
            "working_tree_dirty": dirty,
            "machine": {
                "hostname": platform.node(),
                "platform": platform.platform(),
                "processor": platform.processor(),
                "logical_cpu_count": os.cpu_count(),
                "python": sys.version,
                "normalization_process_workers": NORMALIZATION_PROCESS_WORKERS,
                "normalization_strategy": "provider-batched durable epochs",
            },
            "fixture": {
                "providers": list(report.providers),
                "capabilities": list(report.capabilities),
                "streams": len(report.streams),
                "messages_per_stream": messages_per_stream,
                "websocket_messages": websocket_messages,
                "raw_frames_including_https_snapshots": raw_frames,
                "normalized_accepted_rows": accepted_rows,
                "expected_normalized_rows": expected_rows,
                "normalized_rows_per_websocket_message": normalized_rows_per_message,
                "raw_segments": raw_segments,
                "archive_restore_verified_segments": archive_segments,
                "stream_results": [
                    {
                        "stream_id": item.stream_id,
                        "outcome": item.outcome,
                        "websocket_messages": item.websocket_messages,
                        "accepted_rows": item.accepted_rows,
                        "raw_segments": item.raw_segments,
                        "archived_segments": item.archived_segments,
                        "errors": list(item.errors),
                    }
                    for item in report.streams
                ],
            },
            "result": {
                "run_status": report.status,
                "elapsed_seconds": elapsed,
                "workload_elapsed_seconds": elapsed,
                "worker_teardown_seconds": timing.worker_teardown_seconds,
                "total_wall_seconds": timing.total_wall_seconds,
                "throughput_websocket_messages_per_second": throughput,
                "throughput_normalized_rows_per_second": normalized_throughput,
                "event_loop_delay_samples": len(samples),
                "event_loop_delay_p95_milliseconds": p95,
                "event_loop_delay_p99_milliseconds": p99,
                "raw_normalized_archive_consistent": consistency,
                "last_epoch_submit_seconds": last_epoch_submit_seconds,
                "normalization_group_window_seconds": normalization_group_window_seconds,
            },
            "gate": {
                "minimum_throughput_messages_per_second": MINIMUM_THROUGHPUT_MESSAGES_PER_SECOND,
                "minimum_throughput_normalized_rows_per_second": (
                    minimum_normalized_rows_per_second
                ),
                "maximum_event_loop_p99_milliseconds": MAXIMUM_EVENT_LOOP_P99_MILLISECONDS,
                "target_live_offer_rate_messages_per_second": (
                    TARGET_LIVE_OFFER_RATE_MESSAGES_PER_SECOND
                ),
                "target_live_normalized_rows_per_second_at_scenario_density": (
                    target_normalized_rows_per_second
                ),
                "throughput_safety_multiple": (
                    throughput / TARGET_LIVE_OFFER_RATE_MESSAGES_PER_SECOND
                ),
                "normalized_rows_safety_multiple": (
                    normalized_throughput / target_normalized_rows_per_second
                ),
                "rationale": (
                    "The frozen eight streams at 100 ms imply 80 messages/s. Both message and "
                    "scenario-density Normalized-row throughput require at least 3x live offer-rate "
                    "headroom while including Raw segment publication, Normalized publication, "
                    "archive copy and restore hash. The capacity gate ends only after every stream "
                    "report is complete and the batching executor is quiescent; one-time worker "
                    "process teardown is reported separately and the pool is still fully closed."
                ),
                "passed": gate_passed,
            },
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages-per-stream", type=int, default=500)
    parser.add_argument("--scenario", choices=tuple(SCENARIOS), default="sparse")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.messages_per_stream < 3:
        parser.error("--messages-per-stream must be at least 3")
    repository = Path(__file__).resolve().parents[1]
    payload = asyncio.run(_run_benchmark(repository, args.messages_per_stream, args.scenario))
    identity_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    document = {**payload, "report_sha256": identity_hash}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / f"benchmark-{args.scenario}-sha256-{identity_hash}.json"
    with output.open("xb") as stream:
        stream.write(canonical_json_bytes(document))
        stream.flush()
        os.fsync(stream.fileno())
    print(output)
    print(json.dumps(document["result"], indent=2))
    print(json.dumps(document["gate"], indent=2))
    return 0 if bool(document["gate"]["passed"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
