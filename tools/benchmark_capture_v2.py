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
from datetime import datetime, timedelta, timezone
from pathlib import Path

from quant_data_kit.capture_v2.collector import CryptoL2CaptureCoordinator
from quant_data_kit.capture_v2.models import (
    CaptureConfig,
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
    async def get(self, url: str, *, timeout_seconds: float) -> HttpResponse:
        if timeout_seconds <= 0:
            raise ProviderError("benchmark HTTP timeout must be positive")
        body = canonical_json_bytes(
            {"lastUpdateId": 100, "bids": [["100", "2"]], "asks": [["101", "3"]]}
        )
        return HttpResponse(url=url, status=200, body=body)


class FixtureConnection:
    def __init__(self, stream: StreamConfig, message_count: int) -> None:
        self.stream = stream
        self.messages = _messages(stream, message_count)
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
        return self.messages.pop(0)

    async def close(self) -> None:
        self.closed = True


class FixtureConnector:
    def __init__(self, streams: tuple[StreamConfig, ...], message_count: int) -> None:
        self.unassigned = list(streams)
        self.message_count = message_count
        self.connections: list[FixtureConnection] = []

    async def connect(self, url: str, *, timeout_seconds: float) -> FixtureConnection:
        if timeout_seconds <= 0:
            raise ProviderError("benchmark connect timeout must be positive")
        stream = next(item for item in self.unassigned if item.websocket_url == url)
        self.unassigned.remove(stream)
        connection = FixtureConnection(stream, self.message_count)
        self.connections.append(connection)
        return connection


def _messages(stream: StreamConfig, count: int) -> list[bytes]:
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
                "b": [["100", str(2_000_000 + index)]],
                "a": [["101", str(3_000_000 + index)]],
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
                        "bids": [["100", "2000000"]],
                        "asks": [["101", "3000000"]],
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
                            "bids": [["100", str(2_000_000 + index)]],
                            "asks": [["101", str(3_000_000 + index)]],
                        }
                    ],
                }
            )
        )
    return values


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


async def _run_benchmark(repository: Path, messages_per_stream: int) -> dict[str, object]:
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
        connector = FixtureConnector(streams, messages_per_stream)
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
            archive_reserve_bytes=1,
        )
        coordinator = CryptoL2CaptureCoordinator(
            config,
            storage_guard=guard,
            archive=LocalArchiveController(
                hot, archive, restore, storage_guard=guard, clock=clock.now
            ),
            http=FixtureHttp(),
            websockets=connector,
            clock=clock,
            jitter=lambda: 0.5,
            policy=policy,
        )
        samples: list[float] = []
        stop = asyncio.Event()
        sampler = asyncio.create_task(_event_loop_sampler(stop, samples))
        started = time.perf_counter()
        report = await coordinator.run(maximum_websocket_messages=messages_per_stream)
        elapsed = time.perf_counter() - started
        stop.set()
        await sampler
        websocket_messages = sum(item.websocket_messages for item in report.streams)
        raw_frames = websocket_messages + sum(
            1 for item in report.streams if item.provider == Provider.BINANCE.value
        )
        accepted_rows = sum(item.accepted_rows for item in report.streams)
        raw_segments = sum(item.raw_segments for item in report.streams)
        archive_segments = sum(item.archived_segments for item in report.streams)
        throughput = websocket_messages / elapsed
        p95 = _percentile(samples, 0.95)
        p99 = _percentile(samples, 0.99)
        consistency = bool(
            report.status == "BOUNDED_PROBE_COMPLETE"
            and len(report.streams) == 8
            and websocket_messages == 8 * messages_per_stream
            and raw_segments == archive_segments
            and all(item.normalized_epochs == 1 for item in report.streams)
            and all(not item.errors for item in report.streams)
            and all(item.pending_raw_messages == 0 for item in report.streams)
            and all(item.pending_raw_segments == 0 for item in report.streams)
        )
        gate_passed = bool(
            consistency
            and throughput >= MINIMUM_THROUGHPUT_MESSAGES_PER_SECOND
            and p99 <= MAXIMUM_EVENT_LOOP_P99_MILLISECONDS
        )
        commit, dirty = _git_metadata(repository)
        return {
            "schema_version": "puresaber.m7-offline-capture-benchmark@1.0.0",
            "scope": "OFFLINE_FIXTURE_NOT_NETWORK",
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
            },
            "fixture": {
                "providers": list(report.providers),
                "capabilities": list(report.capabilities),
                "streams": len(report.streams),
                "messages_per_stream": messages_per_stream,
                "websocket_messages": websocket_messages,
                "raw_frames_including_https_snapshots": raw_frames,
                "normalized_accepted_rows": accepted_rows,
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
                "throughput_websocket_messages_per_second": throughput,
                "event_loop_delay_samples": len(samples),
                "event_loop_delay_p95_milliseconds": p95,
                "event_loop_delay_p99_milliseconds": p99,
                "raw_normalized_archive_consistent": consistency,
            },
            "gate": {
                "minimum_throughput_messages_per_second": MINIMUM_THROUGHPUT_MESSAGES_PER_SECOND,
                "maximum_event_loop_p99_milliseconds": MAXIMUM_EVENT_LOOP_P99_MILLISECONDS,
                "target_live_offer_rate_messages_per_second": 80,
                "throughput_safety_multiple": throughput / 80,
                "rationale": (
                    "The frozen eight streams at 100 ms imply 80 messages/s. The offline gate "
                    "requires at least 240 messages/s (3x offer-rate headroom) while including "
                    "Raw segment publication, Normalized publication, archive copy and restore hash."
                ),
                "passed": gate_passed,
            },
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages-per-stream", type=int, default=500)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.messages_per_stream < 3:
        parser.error("--messages-per-stream must be at least 3")
    repository = Path(__file__).resolve().parents[1]
    payload = asyncio.run(_run_benchmark(repository, args.messages_per_stream))
    identity_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    document = {**payload, "report_sha256": identity_hash}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / f"benchmark-sha256-{identity_hash}.json"
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
