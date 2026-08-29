from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from quant_data_kit.capture_v2.models import (
    M7_CAPABILITIES,
    M7_PROVIDERS,
    AuditEvent,
    CaptureConfig,
    CaptureState,
    CaptureStateMachine,
    MonotonicReceivedClock,
    Provider,
    RawFrame,
    RetryPolicy,
    SegmentRotation,
    StreamConfig,
    SymbolMappingResolver,
    assert_m7_scope,
    default_crypto_l2_streams,
    default_symbol_mappings,
)
from quant_data_kit.capture_v2.storage import (
    CapturePausedError,
    CaptureStorageGuard,
    DiskCapacity,
    LocalArchiveController,
    RawSegmentWriter,
    VolumeIdentity,
    iter_segment_frames,
)
from quant_data_kit.data_lake import StoragePolicy
from quant_data_kit.domain_v2 import SymbolMapping
from quant_data_kit.exceptions import ValidationError

UTC = timezone.utc
NOW = datetime(2026, 8, 29, 0, 0, tzinfo=UTC)
TEST_POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)
AMPLE = DiskCapacity(total_bytes=10 * 1024**3, free_bytes=9 * 1024**3)


class FakeClock:
    def __init__(self, current: datetime = NOW) -> None:
        self.current = current
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        value = self.current
        self.current += timedelta(milliseconds=1)
        return value

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.current += timedelta(seconds=seconds)


def identities(path: Path) -> VolumeIdentity:
    name = "archive-disk" if "archive" in path.name else "hot-disk"
    return VolumeIdentity(identity=f"volume-{name}", physical_devices=(name,))


def guard(hot: Path, archive: Path, **kwargs: object) -> CaptureStorageGuard:
    return CaptureStorageGuard(
        hot,
        archive,
        policy=TEST_POLICY,
        archive_reserve_bytes=1024,
        volume_identity=kwargs.get("volume_identity", identities),  # type: ignore[arg-type]
        capacity_probe=kwargs.get("capacity_probe", lambda _path: AMPLE),  # type: ignore[arg-type]
        hot_size_probe=kwargs.get("hot_size_probe", lambda _path: 0),  # type: ignore[arg-type]
    )


def raw_frame(
    stream: StreamConfig,
    index: int,
    *,
    received_at: datetime | None = None,
    payload: bytes | None = None,
) -> RawFrame:
    received = received_at or NOW + timedelta(seconds=index)
    return RawFrame(
        frame_kind="market_data",
        provider=stream.provider.value,
        stream_id=stream.stream_id,
        connection_id="connection-1",
        subscription=stream.channel,
        transport="wss",
        tls_url=stream.websocket_url,
        received_at=received,
        observed_at=received,
        event_time=received,
        payload=payload or json.dumps({"index": index}).encode(),
        native_sequence={"u": index},
        collector_commit="deadbeef",
    )


def storage_roots(tmp_path: Path) -> tuple[Path, Path, Path]:
    hot = tmp_path / "hot"
    archive = tmp_path / "archive"
    restore = hot / "restore"
    hot.mkdir()
    archive.mkdir()
    restore.mkdir()
    return hot, archive, restore


def test_default_scope_is_exact_sorted_and_credential_free(tmp_path: Path) -> None:
    streams = default_crypto_l2_streams()
    assert len(streams) == 8
    assert tuple(item.stream_id for item in streams) == tuple(
        sorted(item.stream_id for item in streams)
    )
    assert {item.provider.value for item in streams} == set(M7_PROVIDERS)
    assert tuple(sorted({item.capability for item in streams})) == M7_CAPABILITIES
    assert all(item.websocket_url.startswith("wss://") for item in streams)
    assert all(
        item.rest_snapshot_url is None or item.rest_snapshot_url.startswith("https://")
        for item in streams
    )
    assert_m7_scope(streams)
    config = CaptureConfig(
        hot_root=(tmp_path / "hot").absolute(),
        archive_root=(tmp_path / "archive").absolute(),
        restore_root=(tmp_path / "restore").absolute(),
        collector_commit="abc123",
        streams=streams,
    )
    assert config.providers == M7_PROVIDERS
    assert config.capabilities == M7_CAPABILITIES


def test_scope_config_stream_and_retry_guards(tmp_path: Path) -> None:
    streams = default_crypto_l2_streams()
    with pytest.raises(ValidationError, match="exactly 8"):
        assert_m7_scope(streams[:-1])
    with pytest.raises(ValidationError, match="providers"):
        assert_m7_scope(
            tuple(replace(item, provider=Provider.OKX, rest_snapshot_url=None) for item in streams)
        )
    wrong = tuple(replace(item, native_symbol="SOL-USDT") for item in streams)
    with pytest.raises(ValidationError, match="capabilities"):
        assert_m7_scope(wrong)

    sample = streams[0]
    with pytest.raises(ValidationError, match="TLS"):
        replace(sample, websocket_url="ws://invalid")
    with pytest.raises(ValidationError, match="HTTPS"):
        replace(sample, rest_snapshot_url=None)
    okx = next(item for item in streams if item.provider is Provider.OKX)
    with pytest.raises(ValidationError, match="OKX"):
        replace(okx, rest_snapshot_url="https://example.invalid")
    with pytest.raises(ValidationError, match="fixed-point"):
        replace(sample, price_scale=-1)

    for kwargs in (
        {"max_attempts": 0},
        {"base_delay_seconds": 0},
        {"maximum_delay_seconds": 0.1},
        {"jitter_fraction": 2},
    ):
        with pytest.raises(ValidationError):
            RetryPolicy(**kwargs)
    retry = RetryPolicy(max_attempts=3, base_delay_seconds=2, maximum_delay_seconds=3)
    assert retry.delay(1, lambda: 0.5) == 2
    assert retry.delay(2, lambda: 0.5) == 3
    for attempt, jitter in ((0, lambda: 0.5), (3, lambda: 0.5), (1, lambda: 2.0)):
        with pytest.raises(ValidationError):
            retry.delay(attempt, jitter)

    with pytest.raises(ValidationError, match="at least two"):
        SegmentRotation(max_messages=1)
    with pytest.raises(ValidationError, match="positive"):
        SegmentRotation(max_age_seconds=0)
    with pytest.raises(ValidationError, match="absolute"):
        CaptureConfig(Path("hot"), tmp_path, tmp_path, "x", streams)
    with pytest.raises(ValidationError, match="explicit"):
        CaptureConfig(tmp_path, tmp_path, tmp_path, "", streams)
    with pytest.raises(ValidationError, match="include streams"):
        CaptureConfig(tmp_path, tmp_path, tmp_path, "x")
    with pytest.raises(ValidationError, match="unique"):
        CaptureConfig(tmp_path, tmp_path, tmp_path, "x", (sample, sample))
    with pytest.raises(ValidationError, match="archive_reserve"):
        CaptureConfig(tmp_path, tmp_path, tmp_path, "x", streams, archive_reserve_bytes=0)


def test_symbol_mapping_resolution_is_effective_and_point_in_time() -> None:
    streams = default_crypto_l2_streams()
    sample = streams[0]
    resolver = SymbolMappingResolver(default_symbol_mappings(streams))
    assert (
        resolver.resolve(
            sample.mapping_source,
            sample.native_symbol,
            event_time=NOW,
            known_at=NOW,
        )
        == sample.instrument_id
    )
    with pytest.raises(ValidationError, match="exactly one"):
        resolver.resolve("binance", "MISSING", event_time=NOW, known_at=NOW)
    future_mapping = SymbolMapping(
        source="binance",
        provider_symbol="BTCUSDT",
        instrument_id="future",
        effective_from=NOW,
        available_at=NOW + timedelta(days=1),
    )
    with pytest.raises(ValidationError, match="exactly one"):
        SymbolMappingResolver((future_mapping,)).resolve(
            "binance", "BTCUSDT", event_time=NOW, known_at=NOW
        )
    duplicate = replace(future_mapping, instrument_id="duplicate", available_at=NOW)
    with pytest.raises(ValidationError, match="matches=2"):
        SymbolMappingResolver((replace(future_mapping, available_at=NOW), duplicate)).resolve(
            "binance", "BTCUSDT", event_time=NOW, known_at=NOW
        )


def test_raw_frame_round_trip_and_monotonic_clock() -> None:
    stream = default_crypto_l2_streams()[0]
    frame = raw_frame(stream, 1, payload=b"\x00exact-wire\xff")
    record = frame.record()
    assert RawFrame.from_record(record) == frame
    changed = dict(record)
    changed["raw_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="integrity"):
        RawFrame.from_record(changed)
    with pytest.raises(ValidationError, match="schema"):
        RawFrame.from_record({**record, "schema_version": "wrong"})
    with pytest.raises(ValidationError, match="malformed"):
        RawFrame.from_record({**record, "payload_base64": "%%%"})
    with pytest.raises(ValidationError, match="transport"):
        replace(frame, transport="plain")
    with pytest.raises(ValidationError, match="URL"):
        replace(frame, tls_url="ws://plain")
    with pytest.raises(ValidationError, match="bytes"):
        replace(frame, payload="not-bytes")  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="collector_commit"):
        replace(frame, collector_commit="")

    clock = FakeClock()
    monotonic = MonotonicReceivedClock(clock)
    observed_one, received_one = monotonic.now()
    clock.current = observed_one
    observed_two, received_two = monotonic.now()
    assert observed_two == observed_one
    assert received_two > received_one


def test_state_machine_audits_legal_illegal_and_sink_failure() -> None:
    stream = default_crypto_l2_streams()[0]
    clock = FakeClock()
    frames: list[RawFrame] = []
    alerts: list[str] = []
    machine = CaptureStateMachine(
        stream,
        connection_id="one",
        collector_commit="abc",
        clock=clock,
        audit_sink=frames.append,
        alert_sink=alerts.append,
    )
    machine.transition(CaptureState.BUFFERING, "connected")
    machine.transition(CaptureState.SNAPSHOT_SYNC, "snapshot")
    machine.audit("heartbeat", "no-change", {"seq": 1})
    machine.transition(CaptureState.LIVE, "live")
    machine.transition(CaptureState.RESYNC, "gap")
    machine.transition(CaptureState.CONNECTING, "retry")
    machine.transition(CaptureState.PAUSED, "operator")
    assert len(frames) == len(machine.events) == 8
    assert isinstance(machine.events[0], AuditEvent)
    assert RawFrame.from_record(frames[0].record()).payload == frames[0].payload
    with pytest.raises(ValidationError, match="illegal"):
        machine.transition(CaptureState.LIVE, "invalid")
    assert alerts[-1].startswith("ILLEGAL_CAPTURE_TRANSITION")

    def failed_sink(_frame: RawFrame) -> None:
        raise OSError("disk full")

    failed = CaptureStateMachine(
        stream,
        connection_id="two",
        collector_commit="abc",
        clock=clock,
        audit_sink=failed_sink,
        alert_sink=alerts.append,
    )
    assert failed.events
    assert any(item.startswith("AUDIT_PERSISTENCE_FAILED") for item in alerts)


def test_storage_preflight_requires_distinct_physical_capacity_and_existing_paths(
    tmp_path: Path,
) -> None:
    hot, archive, _ = storage_roots(tmp_path)
    accepted = guard(hot, archive).require_preflight(projected_hot_bytes=10)
    assert accepted.allowed
    assert accepted.hot_identity != accepted.archive_identity

    shared = guard(
        hot,
        archive,
        volume_identity=lambda _path: VolumeIdentity("same", ("physical-0",)),
    ).preflight()
    assert not shared.allowed and "share physical" in (shared.alert or "")

    missing = guard(hot, tmp_path / "missing").preflight()
    assert not missing.allowed and "missing" in (missing.alert or "")

    def low_archive(path: Path) -> DiskCapacity:
        return DiskCapacity(10_000, 1) if path == archive else AMPLE

    low = guard(hot, archive, capacity_probe=low_archive).preflight()
    assert not low.allowed and "archive capacity" in (low.alert or "")

    def failed_identity(_path: Path) -> VolumeIdentity:
        raise OSError("identity unavailable")

    identity_failure = guard(hot, archive, volume_identity=failed_identity).preflight()
    assert not identity_failure.allowed and "identity unavailable" in (identity_failure.alert or "")

    def failed_capacity(_path: Path) -> DiskCapacity:
        raise OSError("probe unavailable")

    capacity_failure = guard(hot, archive, capacity_probe=failed_capacity).preflight()
    assert not capacity_failure.allowed and "capacity probe failed" in (
        capacity_failure.alert or ""
    )
    with pytest.raises(CapturePausedError, match="CAPTURE_PAUSED"):
        guard(hot, archive, capacity_probe=low_archive).require_preflight()


def test_archive_preflight_segment_rotation_restore_and_no_cleanup(tmp_path: Path) -> None:
    hot, archive, restore = storage_roots(tmp_path)
    storage_guard = guard(hot, archive)
    clock = FakeClock()
    controller = LocalArchiveController(
        hot,
        archive,
        restore,
        storage_guard=storage_guard,
        clock=clock.now,
    )
    preflight = controller.preflight()
    assert preflight.probe_sha256 == preflight.restored_sha256
    assert Path(preflight.receipt_path).is_file()

    stream = default_crypto_l2_streams()[0]
    writer = RawSegmentWriter(
        hot,
        stream.stream_id,
        stream.provider.value,
        collector_commit="deadbeef",
        rotation=SegmentRotation(max_messages=3, max_wire_bytes=10_000, max_age_seconds=30),
        storage_guard=storage_guard,
        policy=TEST_POLICY,
    )
    writer.append(raw_frame(stream, 1, payload=b"first"))
    writer.append(raw_frame(stream, 2, payload=b"second"))
    assert writer.pending_messages == 2 and writer.drain_completed() == ()
    writer.append(raw_frame(stream, 3, payload=b"third"))
    segments = writer.drain_completed()
    assert len(segments) == 1 and segments[0].message_count == 3
    assert segments[0].wire_byte_count == len(b"firstsecondthird")
    decoded = tuple(iter_segment_frames(hot, segments[0]))
    assert [item.payload for item in decoded] == [b"first", b"second", b"third"]
    receipt = controller.archive_segment(segments[0])
    assert receipt.archive_restore_verified is True
    assert receipt.eligible_for_cleanup is True
    assert receipt.cleanup_performed is False
    assert Path(receipt.archive_directory, "payload.bin").is_file()
    assert next(hot.rglob(f"object={segments[0].raw_manifest.object_id}/payload.bin")).is_file()
    assert writer.segment_count == 1


def test_segment_time_rotation_identity_order_and_capacity_fail_closed(tmp_path: Path) -> None:
    hot, archive, _ = storage_roots(tmp_path)
    stream = default_crypto_l2_streams()[0]
    storage_guard = guard(hot, archive)
    writer = RawSegmentWriter(
        hot,
        stream.stream_id,
        stream.provider.value,
        collector_commit="abc",
        rotation=SegmentRotation(max_messages=10, max_wire_bytes=10_000, max_age_seconds=1),
        storage_guard=storage_guard,
        policy=TEST_POLICY,
    )
    writer.append(raw_frame(stream, 1, received_at=NOW))
    writer.append(raw_frame(stream, 2, received_at=NOW + timedelta(seconds=2)))
    assert len(writer.drain_completed()) == 1
    with pytest.raises(ValidationError, match="identity"):
        writer.append(replace(raw_frame(stream, 3), stream_id="other"))
    with pytest.raises(ValidationError, match="regressed"):
        writer.append(raw_frame(stream, 4, received_at=NOW))

    low_guard = guard(
        hot,
        archive,
        capacity_probe=lambda _path: DiskCapacity(10_000, 0),
    )
    stopped = RawSegmentWriter(
        hot,
        stream.stream_id,
        stream.provider.value,
        collector_commit="abc",
        rotation=SegmentRotation(),
        storage_guard=low_guard,
        policy=TEST_POLICY,
    )
    with pytest.raises(CapturePausedError, match="COLLECTION_STOPPED"):
        stopped.append(raw_frame(stream, 5))


def test_archive_missing_restore_and_hash_failure_pause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hot, archive, restore = storage_roots(tmp_path)
    storage_guard = guard(hot, archive)
    missing_restore = LocalArchiveController(
        hot,
        archive,
        tmp_path / "missing-restore",
        storage_guard=storage_guard,
    )
    with pytest.raises(CapturePausedError, match="restore_root"):
        missing_restore.preflight()

    controller = LocalArchiveController(hot, archive, restore, storage_guard=storage_guard)
    stream = default_crypto_l2_streams()[0]
    writer = RawSegmentWriter(
        hot,
        stream.stream_id,
        stream.provider.value,
        collector_commit="abc",
        rotation=SegmentRotation(max_messages=2),
        storage_guard=storage_guard,
        policy=TEST_POLICY,
    )
    writer.append(raw_frame(stream, 1))
    writer.append(raw_frame(stream, 2))
    segment = writer.drain_completed()[0]

    import quant_data_kit.capture_v2.storage as storage_module

    real_hash = storage_module._sha256_file

    def wrong_restore_hash(path: Path) -> str:
        if "qdk-segment-restore" in str(path):
            return "0" * 64
        return real_hash(path)

    monkeypatch.setattr(storage_module, "_sha256_file", wrong_restore_hash)
    with pytest.raises(CapturePausedError, match="hash validation"):
        controller.archive_segment(segment)


def test_storage_negative_capacity_rotation_and_immutable_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import quant_data_kit.capture_v2.storage as storage_module

    with pytest.raises(ValidationError, match="physical devices"):
        VolumeIdentity("invalid", ())

    relative = CaptureStorageGuard(
        Path("relative-hot"),
        Path("relative-archive"),
        policy=TEST_POLICY,
        archive_reserve_bytes=1,
        volume_identity=identities,
        capacity_probe=lambda _path: AMPLE,
        hot_size_probe=lambda _path: 0,
    ).preflight()
    assert not relative.allowed
    assert sum("absolute path" in reason for reason in relative.reasons) == 2

    hot, archive, _ = storage_roots(tmp_path)
    low_guard = guard(
        hot,
        archive,
        capacity_probe=lambda _path: DiskCapacity(total_bytes=10_000, free_bytes=0),
    )
    with pytest.raises(CapturePausedError, match="archive free-space floor"):
        low_guard.require_archive_capacity(projected_write_bytes=1)

    stream = default_crypto_l2_streams()[0]
    writer = RawSegmentWriter(
        hot,
        stream.stream_id,
        stream.provider.value,
        collector_commit="abc",
        rotation=SegmentRotation(max_messages=10, max_wire_bytes=2, max_age_seconds=60),
        storage_guard=guard(hot, archive),
        policy=TEST_POLICY,
    )
    assert writer.flush() is None
    writer.append(
        replace(raw_frame(stream, 1, payload=b"abc"), event_time=None, native_sequence={})
    )
    segment = writer.drain_completed()[0]
    assert segment.first_event_time is None and segment.last_event_time is None
    assert dict(segment.native_sequence_range) == {}

    immutable = tmp_path / "immutable" / "value.bin"
    storage_module._atomic_immutable_write(immutable, b"same")
    storage_module._atomic_immutable_write(immutable, b"same")
    with pytest.raises(ValidationError, match="different bytes"):
        storage_module._atomic_immutable_write(immutable, b"different")

    failed_target = tmp_path / "immutable" / "replace-failure.bin"

    def replace_failed(_source: Path, _target: Path) -> None:
        raise OSError("injected atomic replace failure")

    monkeypatch.setattr(storage_module.os, "replace", replace_failed)
    with pytest.raises(OSError, match="atomic replace"):
        storage_module._atomic_immutable_write(failed_target, b"temporary")
    assert not tuple(failed_target.parent.glob(".replace-failure.bin.*.tmp"))


def test_archive_preflight_mismatch_and_raw_segment_line_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import quant_data_kit.capture_v2.storage as storage_module

    hot, archive, restore = storage_roots(tmp_path)
    storage_guard = guard(hot, archive)
    controller = LocalArchiveController(hot, archive, restore, storage_guard=storage_guard)
    real_hash = storage_module._sha256_file

    def wrong_preflight_hash(path: Path) -> str:
        if "qdk-archive-preflight" in str(path):
            return "0" * 64
        return real_hash(path)

    monkeypatch.setattr(storage_module, "_sha256_file", wrong_preflight_hash)
    with pytest.raises(CapturePausedError, match="preflight restore hash mismatch"):
        controller.preflight()

    monkeypatch.setattr(storage_module, "_sha256_file", real_hash)
    stream = default_crypto_l2_streams()[0]
    writer = RawSegmentWriter(
        hot,
        stream.stream_id,
        stream.provider.value,
        collector_commit="abc",
        rotation=SegmentRotation(max_messages=2),
        storage_guard=storage_guard,
        policy=TEST_POLICY,
    )
    first, second = raw_frame(stream, 1), raw_frame(stream, 2)
    writer.append(first)
    writer.append(second)
    segment = writer.drain_completed()[0]
    original_loader = storage_module.load_raw_object
    raw, _payload = original_loader(hot, segment.raw_manifest.reference())
    body = b"\n" + json.dumps(first.record()).encode("utf-8") + b"\n"
    monkeypatch.setattr(storage_module, "load_raw_object", lambda *_args: (raw, body))
    assert tuple(iter_segment_frames(hot, segment)) == (first,)
    monkeypatch.setattr(storage_module, "load_raw_object", lambda *_args: (raw, b"{\n"))
    with pytest.raises(ValidationError, match="line 1 is malformed"):
        tuple(iter_segment_frames(hot, segment))


def test_physical_volume_probe_failures_and_symlink_are_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ctypes
    from ctypes import wintypes

    import quant_data_kit.capture_v2.storage as storage_module

    class FakePath:
        anchor = "C:\\"

        def __init__(self, _value: object) -> None:
            pass

        def resolve(self, *, strict: bool) -> FakePath:
            assert strict is True
            return self

    monkeypatch.setattr(storage_module, "Path", FakePath)
    FakePath.anchor = "unsupported-anchor"
    with pytest.raises(CapturePausedError, match="cannot resolve Windows volume anchor"):
        storage_module._windows_volume_identity(tmp_path)
    FakePath.anchor = "C:\\"

    class CallableStub:
        def __init__(self, implementation) -> None:
            self.implementation = implementation

        def __call__(self, *args):
            return self.implementation(*args)

    def kernel(create_result: int, device_control) -> SimpleNamespace:
        return SimpleNamespace(
            CreateFileW=CallableStub(lambda *_args: create_result),
            DeviceIoControl=CallableStub(device_control),
            CloseHandle=CallableStub(lambda *_args: None),
        )

    invalid_handle = wintypes.HANDLE(-1).value
    monkeypatch.setattr(
        storage_module.ctypes,
        "windll",
        SimpleNamespace(kernel32=kernel(invalid_handle, lambda *_args: 0)),
        raising=False,
    )
    with pytest.raises(CapturePausedError, match="cannot open volume"):
        storage_module._windows_volume_identity(tmp_path)

    monkeypatch.setattr(
        storage_module.ctypes,
        "windll",
        SimpleNamespace(kernel32=kernel(1, lambda *_args: 0)),
        raising=False,
    )
    with pytest.raises(CapturePausedError, match="extents unavailable"):
        storage_module._windows_volume_identity(tmp_path)

    def device_response(count: int, returned_bytes: int):
        def respond(
            _handle,
            _control_code,
            _input,
            _input_length,
            output,
            _output_length,
            returned,
            _overlapped,
        ) -> int:
            ctypes.memmove(output, count.to_bytes(4, "little"), 4)
            returned._obj.value = returned_bytes
            return 1

        return respond

    monkeypatch.setattr(
        storage_module.ctypes,
        "windll",
        SimpleNamespace(kernel32=kernel(1, device_response(0, 16))),
        raising=False,
    )
    with pytest.raises(CapturePausedError, match="no physical disk extents"):
        storage_module._windows_volume_identity(tmp_path)

    monkeypatch.setattr(
        storage_module.ctypes,
        "windll",
        SimpleNamespace(kernel32=kernel(1, device_response(2, 16))),
        raising=False,
    )
    with pytest.raises(CapturePausedError, match="truncated physical disk extents"):
        storage_module._windows_volume_identity(tmp_path)

    monkeypatch.undo()
    symlink_case = tmp_path / "symlink-case"
    symlink_case.mkdir()
    hot, archive, _ = storage_roots(symlink_case)
    real_is_symlink = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda self: self == hot or real_is_symlink(self))
    decision = guard(hot, archive).preflight()
    assert not decision.allowed
    assert "symbolic link" in (decision.alert or "")
