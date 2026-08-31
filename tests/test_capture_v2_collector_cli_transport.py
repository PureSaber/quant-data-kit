from __future__ import annotations

import asyncio
import json
import urllib.error
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from quant_data_kit.capture_v2.cli import load_capture_config, main_capture
from quant_data_kit.capture_v2.collector import (
    CaptureStreamRunner,
    CryptoL2CaptureCoordinator,
    NormalizationOutcome,
    _frame_metadata,
)
from quant_data_kit.capture_v2.models import (
    CaptureConfig,
    CaptureState,
    MarketKind,
    Provider,
    RetryPolicy,
    SegmentRotation,
    SymbolMappingResolver,
    default_crypto_l2_streams,
    default_symbol_mappings,
)
from quant_data_kit.capture_v2.storage import (
    CapturePausedError,
    CaptureStorageGuard,
    DiskCapacity,
    LocalArchiveController,
    VolumeIdentity,
)
from quant_data_kit.capture_v2.synchronizers import ResyncRequired
from quant_data_kit.capture_v2.transport import (
    HttpResponse,
    UrllibHttpClient,
    WebsocketsConnector,
    _WebsocketsConnection,
)
from quant_data_kit.data_lake import StoragePolicy
from quant_data_kit.exceptions import ProviderError, ValidationError

UTC = timezone.utc
NOW = datetime(2026, 8, 29, 2, 0, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)
POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)


class FakeClock:
    def __init__(self) -> None:
        self.current = NOW
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        value = self.current
        self.current += timedelta(milliseconds=1)
        return value

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.current += timedelta(seconds=seconds)


class FakeConnection:
    def __init__(self, stream) -> None:
        self.stream = stream
        self.messages = messages_for(stream)
        self.sent: list[bytes] = []
        self.closed = False

    async def send(self, payload: bytes) -> None:
        self.sent.append(payload)

    async def receive(self, *, timeout_seconds: float) -> bytes:
        assert timeout_seconds > 0
        if not self.messages:
            raise ProviderError("injected disconnect")
        return self.messages.pop(0)

    async def close(self) -> None:
        self.closed = True


class FakeConnector:
    def __init__(self, streams) -> None:
        self.streams = tuple(streams)
        self.calls: list[str] = []
        self.connections: list[FakeConnection] = []
        self.unassigned = list(self.streams)

    async def connect(self, url: str, *, timeout_seconds: float) -> FakeConnection:
        assert timeout_seconds > 0
        self.calls.append(url)
        stream = next(item for item in self.unassigned if item.websocket_url == url)
        self.unassigned.remove(stream)
        connection = FakeConnection(stream)
        self.connections.append(connection)
        return connection


class FailedConnector:
    def __init__(self) -> None:
        self.calls = 0

    async def connect(self, url: str, *, timeout_seconds: float):
        self.calls += 1
        raise ProviderError(f"injected connect failure: {url}:{timeout_seconds}")


class FakeHttp:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get(self, url: str, *, timeout_seconds: float) -> HttpResponse:
        assert timeout_seconds > 0
        self.calls.append(url)
        body = json.dumps(
            {"lastUpdateId": 100, "bids": [["100", "2"]], "asks": [["101", "3"]]}
        ).encode()
        return HttpResponse(url=url, status=200, body=body)


def messages_for(stream) -> list[bytes]:
    if stream.provider is Provider.BINANCE:
        values = []
        for index, (first, final) in enumerate(((99, 101), (102, 102), (103, 103))):
            payload = {
                "e": "depthUpdate",
                "E": NOW_MS + index,
                "T": NOW_MS + index,
                "s": stream.native_symbol,
                "U": first,
                "u": final,
                "b": [["100", str(2.1 + index)]],
                "a": [],
            }
            if stream.market is MarketKind.USDT_PERPETUAL:
                payload["pu"] = 100 if index == 0 else final - 1
            values.append(json.dumps(payload).encode())
        return values
    return [
        json.dumps(
            {
                "event": "subscribe",
                "arg": {"channel": "books", "instId": stream.native_symbol},
                "code": "0",
                "msg": "",
            }
        ).encode(),
        json.dumps(
            {
                "arg": {"channel": "books", "instId": stream.native_symbol},
                "action": "snapshot",
                "data": [
                    {
                        "ts": str(NOW_MS),
                        "seqId": 10,
                        "prevSeqId": -1,
                        "checksum": 0,
                        "bids": [["100", "2"]],
                        "asks": [["101", "3"]],
                    }
                ],
            }
        ).encode(),
        json.dumps(
            {
                "arg": {"channel": "books", "instId": stream.native_symbol},
                "action": "update",
                "data": [
                    {
                        "ts": str(NOW_MS + 1),
                        "seqId": 11,
                        "prevSeqId": 10,
                        "checksum": 0,
                        "bids": [["100", "2.5"]],
                        "asks": [],
                    }
                ],
            }
        ).encode(),
    ]


def roots(tmp_path: Path):
    hot, archive, restore = tmp_path / "hot", tmp_path / "archive", tmp_path / "restore"
    hot.mkdir()
    archive.mkdir()
    restore.mkdir()
    return hot, archive, restore


def distinct_identity(path: Path) -> VolumeIdentity:
    value = path.name
    return VolumeIdentity(f"volume-{value}", (f"physical-{value}",))


def storage_guard(hot: Path, archive: Path, *, identity=distinct_identity):
    return CaptureStorageGuard(
        hot,
        archive,
        policy=POLICY,
        archive_reserve_bytes=1,
        volume_identity=identity,
        capacity_probe=lambda _path: DiskCapacity(10**9, 9 * 10**8),
        hot_size_probe=lambda _path: 0,
    )


def config(tmp_path: Path, *, attempts: int = 2):
    hot, archive, restore = roots(tmp_path)
    streams = default_crypto_l2_streams()
    return (
        CaptureConfig(
            hot_root=hot,
            archive_root=archive,
            restore_root=restore,
            collector_commit="abc123",
            streams=streams,
            rotation=SegmentRotation(max_messages=4, max_wire_bytes=100_000, max_age_seconds=60),
            retry=RetryPolicy(
                max_attempts=attempts,
                base_delay_seconds=0.01,
                maximum_delay_seconds=0.02,
                jitter_fraction=0,
            ),
            archive_reserve_bytes=1,
        ),
        streams,
    )


def test_full_eight_stream_bounded_probe_archives_and_normalizes(tmp_path: Path) -> None:
    capture_config, streams = config(tmp_path)
    guard = storage_guard(capture_config.hot_root, capture_config.archive_root)
    clock = FakeClock()
    connector = FakeConnector(streams)
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
        clock=clock.now,
    )
    coordinator = CryptoL2CaptureCoordinator(
        capture_config,
        storage_guard=guard,
        archive=archive,
        http=FakeHttp(),
        websockets=connector,
        clock=clock,
        monotonic=lambda: 0.0,
        jitter=lambda: 0.5,
        policy=POLICY,
    )
    report = asyncio.run(coordinator.run(maximum_websocket_messages=3))
    assert report.status == "BOUNDED_PROBE_COMPLETE", [
        (item.stream_id, item.errors) for item in report.streams if item.errors
    ]
    assert report.providers == ("binance", "okx")
    assert report.capabilities == (
        "btc-spot-l2",
        "btc-usdt-perpetual-l2",
        "eth-spot-l2",
        "eth-usdt-perpetual-l2",
    )
    assert report.continuous_days == 0
    assert report.market_data_certified is False
    assert report.long_running_capture_started is False
    assert len(connector.calls) == len(streams) == 8
    assert all(item.final_state == CaptureState.PAUSED.value for item in report.streams)
    assert all(item.websocket_messages == 3 for item in report.streams)
    assert all(item.raw_segments == item.archived_segments for item in report.streams)
    assert all(item.normalized_epochs == 1 for item in report.streams)
    assert all(item.quarantined_rows == 0 and item.accepted_rows >= 2 for item in report.streams)
    assert report.report_path and Path(report.report_path).is_file()
    assert all(connection.closed for connection in connector.connections)
    assert all(
        not connection.sent
        or json.loads(connection.sent[0])
        == {
            "args": [
                {
                    "channel": "books",
                    "instId": connection.stream.native_symbol,
                }
            ],
            "op": "subscribe",
        }
        for connection in connector.connections
    )


def test_preflight_same_physical_volume_pauses_without_network(tmp_path: Path) -> None:
    capture_config, streams = config(tmp_path)
    same = lambda _path: VolumeIdentity("same-volume", ("physical-0",))
    guard = storage_guard(capture_config.hot_root, capture_config.archive_root, identity=same)
    connector = FakeConnector(streams)
    coordinator = CryptoL2CaptureCoordinator(
        capture_config,
        storage_guard=guard,
        archive=LocalArchiveController(
            capture_config.hot_root,
            capture_config.archive_root,
            capture_config.restore_root,
            storage_guard=guard,
        ),
        http=FakeHttp(),
        websockets=connector,
        clock=FakeClock(),
        policy=POLICY,
    )
    report = coordinator.preflight_only()
    assert report.status == "PAUSED_PREFLIGHT_FAILED"
    assert not connector.calls
    assert all(item.websocket_messages == 0 for item in report.streams)
    assert all("share physical" in item.errors[0] for item in report.streams)


def test_connect_failures_use_bounded_retry_and_end_paused(tmp_path: Path) -> None:
    capture_config, streams = config(tmp_path, attempts=2)
    guard = storage_guard(capture_config.hot_root, capture_config.archive_root)
    clock = FakeClock()
    alerts: list[str] = []
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
        clock=clock.now,
    )
    archive.preflight()
    connector = FailedConnector()
    runner = CaptureStreamRunner(
        capture_config,
        streams[0],
        storage_guard=guard,
        archive=archive,
        mappings=SymbolMappingResolver(default_symbol_mappings(streams)),
        http=FakeHttp(),
        websockets=connector,
        clock=clock,
        jitter=lambda: 0.5,
        alert_sink=alerts.append,
        policy=POLICY,
    )
    report = asyncio.run(runner.run(maximum_websocket_messages=3))
    assert connector.calls == 2
    assert clock.sleeps == [0.01]
    assert report.final_state == CaptureState.PAUSED.value
    assert len(report.errors) == 2
    assert report.raw_segments == report.archived_segments
    assert any("retry_budget_exhausted" in item for item in alerts)


def test_archive_failure_pauses_stream_and_is_not_silently_counted(tmp_path: Path) -> None:
    capture_config, streams = config(tmp_path, attempts=1)
    guard = storage_guard(capture_config.hot_root, capture_config.archive_root)
    clock = FakeClock()

    class FailedArchive(LocalArchiveController):
        def archive_segment(self, segment):
            raise CapturePausedError(f"injected archive failure: {segment.stream_id}")

    archive = FailedArchive(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
        clock=clock.now,
    )
    archive.preflight()
    runner = CaptureStreamRunner(
        capture_config,
        streams[0],
        storage_guard=guard,
        archive=archive,
        mappings=SymbolMappingResolver(default_symbol_mappings(streams)),
        http=FakeHttp(),
        websockets=FakeConnector((streams[0],)),
        clock=clock,
        jitter=lambda: 0.5,
        alert_sink=lambda _message: None,
        policy=POLICY,
    )
    report = asyncio.run(runner.run(maximum_websocket_messages=3))
    assert report.final_state == CaptureState.PAUSED.value
    assert report.raw_segments > report.archived_segments
    assert any("archive failure" in item for item in report.errors)


def test_cli_config_rejects_credentials_unknowns_and_defaults_to_safe_preflight(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    hot, archive, restore = roots(tmp_path)
    path = tmp_path / "capture.json"
    payload = {
        "hot_root": str(hot),
        "archive_root": str(archive),
        "restore_root": str(restore),
        "collector_commit": "abc",
        "archive_reserve_bytes": 1,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    loaded = load_capture_config(path)
    assert len(loaded.streams) == 8
    assert main_capture([str(path)]) == 2
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "PAUSED_PREFLIGHT_FAILED"
    assert output["long_running_capture_started"] is False

    credential = tmp_path / "credential.json"
    credential.write_text(json.dumps({**payload, "api_key": "forbidden"}), encoding="utf-8")
    with pytest.raises(ValidationError, match="credential"):
        load_capture_config(credential)
    unknown = tmp_path / "unknown.json"
    unknown.write_text(json.dumps({**payload, "other": True}), encoding="utf-8")
    with pytest.raises(ValidationError, match="unsupported"):
        load_capture_config(unknown)
    malformed = tmp_path / "malformed.json"
    malformed.write_text("{", encoding="utf-8")
    with pytest.raises(ValidationError, match="malformed"):
        load_capture_config(malformed)
    missing = tmp_path / "missing.json"
    missing.write_text(json.dumps({"collector_commit": "x"}), encoding="utf-8")
    with pytest.raises(ValidationError, match="missing explicit"):
        load_capture_config(missing)


def test_cli_run_requires_confirmation_and_probe_bound(tmp_path: Path, capsys) -> None:
    hot, archive, restore = roots(tmp_path)
    path = tmp_path / "capture.json"
    path.write_text(
        json.dumps(
            {
                "hot_root": str(hot),
                "archive_root": str(archive),
                "restore_root": str(restore),
                "collector_commit": "abc",
            }
        ),
        encoding="utf-8",
    )
    assert main_capture([str(path), "--mode", "run"]) == 2
    assert "confirm-long-running" in capsys.readouterr().out
    assert main_capture([str(path), "--mode", "probe", "--max-messages", "1"]) == 2
    assert "max-messages" in capsys.readouterr().out


def test_cli_nested_guards_custom_streams_and_success_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import quant_data_kit.capture_v2.cli as cli_module

    hot, archive, restore = roots(tmp_path)
    base = {
        "hot_root": str(hot),
        "archive_root": str(archive),
        "restore_root": str(restore),
        "collector_commit": "abc",
        "archive_reserve_bytes": 1,
    }

    def write(name: str, value: Any) -> Path:
        path = tmp_path / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    with pytest.raises(ValidationError, match="credential"):
        load_capture_config(write("nested-credential.json", {**base, "streams": [{"headers": {}}]}))
    with pytest.raises(ValidationError, match="JSON object"):
        load_capture_config(write("not-object.json", []))
    with pytest.raises(ValidationError, match="rotation and retry"):
        load_capture_config(write("bad-rotation-shape.json", {**base, "rotation": []}))
    with pytest.raises(ValidationError, match="values are invalid"):
        load_capture_config(write("bad-rotation-key.json", {**base, "rotation": {"extra": 1}}))

    with pytest.raises(ValidationError, match="non-empty JSON array"):
        cli_module._streams([])
    with pytest.raises(ValidationError, match="must be an object"):
        cli_module._streams(["bad"])
    with pytest.raises(ValidationError, match="unsupported fields"):
        cli_module._streams([{"other": True}])
    with pytest.raises(ValidationError, match="is invalid"):
        cli_module._streams([{"provider": "unknown", "market": "spot"}])

    configured_streams = []
    for item in default_crypto_l2_streams():
        configured_streams.append(
            {
                "stream_id": item.stream_id,
                "provider": item.provider.value,
                "market": item.market.value,
                "native_symbol": item.native_symbol,
                "instrument_id": item.instrument_id,
                "venue": item.venue,
                "websocket_url": item.websocket_url,
                "channel": item.channel,
                "price_scale": item.price_scale,
                "quantity_scale": item.quantity_scale,
                "rest_snapshot_url": item.rest_snapshot_url,
            }
        )
    custom_path = write("custom-streams.json", {**base, "streams": configured_streams})
    assert len(load_capture_config(custom_path).streams) == 8

    @dataclass(frozen=True)
    class FakeReport:
        status: str
        mode: str

    calls: list[int | None | str] = []

    class FakeCoordinator:
        def __init__(self, _config: CaptureConfig) -> None:
            pass

        def preflight_only(self) -> FakeReport:
            calls.append("preflight")
            return FakeReport("PREFLIGHT_PASSED_NETWORK_NOT_STARTED", "preflight")

        async def run(self, *, maximum_websocket_messages: int | None) -> FakeReport:
            calls.append(maximum_websocket_messages)
            return FakeReport("BOUNDED_PROBE_COMPLETE", "run")

    monkeypatch.setattr(cli_module, "CryptoL2CaptureCoordinator", FakeCoordinator)
    assert main_capture([str(custom_path)]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "preflight"
    assert main_capture([str(custom_path), "--mode", "probe", "--max-messages", "2"]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "run"
    assert main_capture([str(custom_path), "--mode", "run", "--confirm-long-running"]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "run"
    assert calls == ["preflight", 2, None]


def test_runner_fail_closed_internal_guards_and_raw_metadata(tmp_path: Path) -> None:
    capture_config, streams = config(tmp_path, attempts=1)
    guard = storage_guard(capture_config.hot_root, capture_config.archive_root)
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
        clock=FakeClock().now,
    )
    archive.preflight()
    alerts: list[str] = []
    runner = CaptureStreamRunner(
        capture_config,
        streams[0],
        storage_guard=guard,
        archive=archive,
        mappings=SymbolMappingResolver(default_symbol_mappings(streams)),
        http=FakeHttp(),
        websockets=FailedConnector(),
        clock=FakeClock(),
        jitter=lambda: 0.5,
        alert_sink=alerts.append,
        policy=POLICY,
    )
    with pytest.raises(ValidationError, match="journal was not initialized"):
        runner._apply_outcome(NormalizationOutcome())
    runner._new_epoch()
    with pytest.raises(ValidationError, match="previous Normalized epoch"):
        runner._new_epoch()
    runner._finalize_epoch(abort_reason="injected empty epoch")
    runner._finalize_epoch()
    runner._alert("AUDIT_PERSISTENCE_FAILED:injected")
    assert runner._audit_failures == 1
    runner._pause("operator_pause", OSError("injected"))
    audit_count = len(runner.state.events)
    runner._pause("duplicate_pause")
    assert len(runner.state.events) == audit_count

    with pytest.raises(ResyncRequired, match="JSON object"):
        _frame_metadata(Provider.BINANCE, b"[]")
    event_time, sequences = _frame_metadata(
        Provider.BINANCE,
        json.dumps({"data": {"E": NOW_MS, "U": 1, "u": 2}}).encode(),
    )
    assert event_time == NOW and sequences == {"U": 1, "u": 2}


def test_unverified_archive_receipt_pauses_before_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture_config, streams = config(tmp_path, attempts=1)
    guard = storage_guard(capture_config.hot_root, capture_config.archive_root)
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
    )
    runner = CaptureStreamRunner(
        capture_config,
        streams[0],
        storage_guard=guard,
        archive=archive,
        mappings=SymbolMappingResolver(default_symbol_mappings(streams)),
        http=FakeHttp(),
        websockets=FailedConnector(),
        clock=FakeClock(),
        jitter=lambda: 0.5,
        alert_sink=lambda _message: None,
        policy=POLICY,
    )

    @dataclass(frozen=True)
    class UnverifiedReceipt:
        archive_restore_verified: bool = False
        eligible_for_cleanup: bool = False

    class UnverifiedArchive:
        def archive_segment(self, _segment) -> UnverifiedReceipt:
            return UnverifiedReceipt()

    monkeypatch.setattr(runner.segment_writer, "peek_completed", lambda: (object(),))
    runner.archive = UnverifiedArchive()  # type: ignore[assignment]
    with pytest.raises(CapturePausedError, match="not restore-verified"):
        runner._drain_segments()


def test_urllib_http_client_preserves_bytes_tls_and_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    client = UrllibHttpClient()
    with pytest.raises(ValidationError, match="https"):
        asyncio.run(client.get("http://example.invalid", timeout_seconds=1))

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return b"exact"

        def geturl(self):
            return "https://example.test/final"

    monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: Response())
    response = asyncio.run(client.get("https://example.test", timeout_seconds=1))
    assert response.body == b"exact" and response.status == 200

    def failed(*_args, **_kwargs):
        raise urllib.error.URLError("offline")

    monkeypatch.setattr("urllib.request.urlopen", failed)
    with pytest.raises(ProviderError, match="snapshot failed"):
        asyncio.run(client.get("https://example.test", timeout_seconds=1))

    class BadResponse(Response):
        status = 503

    monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: BadResponse())
    with pytest.raises(ProviderError, match="status=503"):
        asyncio.run(client.get("https://example.test", timeout_seconds=1))

    class DowngradedResponse(Response):
        def geturl(self):
            return "http://example.test/downgraded"

    monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: DowngradedResponse())
    with pytest.raises(ProviderError, match="non-TLS"):
        asyncio.run(client.get("https://example.test", timeout_seconds=1))


def test_websocket_transport_wrapper_bytes_text_timeout_and_tls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Underlying:
        def __init__(self, values: list[Any]) -> None:
            self.values = values
            self.sent: list[bytes] = []
            self.closed = False

        async def send(self, payload: bytes) -> None:
            self.sent.append(payload)

        async def recv(self):
            value = self.values.pop(0)
            if isinstance(value, Exception):
                raise value
            if value == "sleep":
                await asyncio.sleep(0.02)
                return "late"
            return value

        async def close(self):
            self.closed = True

    underlying = Underlying(["text", b"bytes", 123, "sleep"])
    wrapped = _WebsocketsConnection(underlying)
    assert asyncio.run(wrapped.receive(timeout_seconds=1)) == b"text"
    assert asyncio.run(wrapped.receive(timeout_seconds=1)) == b"bytes"
    with pytest.raises(ProviderError, match="unsupported"):
        asyncio.run(wrapped.receive(timeout_seconds=1))
    with pytest.raises(ProviderError, match="timed out"):
        asyncio.run(wrapped.receive(timeout_seconds=0.001))
    asyncio.run(wrapped.send(b"subscribe"))
    asyncio.run(wrapped.close())
    assert underlying.sent == [b"subscribe"] and underlying.closed

    connector = WebsocketsConnector()
    with pytest.raises(ValidationError, match="wss"):
        asyncio.run(connector.connect("ws://plain", timeout_seconds=1))

    async def connected(*_args, **_kwargs):
        return Underlying([])

    monkeypatch.setattr("websockets.asyncio.client.connect", connected)
    assert asyncio.run(connector.connect("wss://example.test", timeout_seconds=1))

    async def connection_failure(*_args, **_kwargs):
        raise OSError("TLS unavailable")

    monkeypatch.setattr("websockets.asyncio.client.connect", connection_failure)
    with pytest.raises(ProviderError, match="connection failed"):
        asyncio.run(connector.connect("wss://example.test", timeout_seconds=1))
