from __future__ import annotations

import asyncio
import hashlib
import json
import multiprocessing
import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

import quant_data_kit.capture_v2.collector as collector_module
import quant_data_kit.capture_v2.epoch as epoch_module
import quant_data_kit.capture_v2.storage as storage_module
from quant_data_kit.capture_v2.collector import CaptureStreamRunner, CryptoL2CaptureCoordinator
from quant_data_kit.capture_v2.epoch import NormalizedEpochJournal
from quant_data_kit.capture_v2.models import (
    AuditEvent,
    AuditPersistenceError,
    CaptureState,
    CaptureStateMachine,
    Provider,
    SegmentRotation,
    SymbolMappingResolver,
    assert_m7_scope,
    default_crypto_l2_streams,
    default_symbol_mappings,
)
from quant_data_kit.capture_v2.storage import (
    CapturePausedError,
    CaptureStorageGuard,
    DiskCapacity,
    DurableAuditStore,
    LocalArchiveController,
    RawSegmentWriter,
    VolumeIdentity,
)
from quant_data_kit.exceptions import ProviderError, ValidationError
from tests import test_capture_v2_collector_cli_transport as collector_fixtures
from tests import test_capture_v2_models_storage as storage_fixtures
from tests import test_capture_v2_synchronizers_epoch as epoch_fixtures


def _process_publish(
    root_text: str,
    target_text: str,
    body: bytes,
    start: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
) -> None:
    start.wait(10)
    try:
        storage_module._atomic_immutable_write(Path(target_text), body, root=Path(root_text))
    except Exception as exc:  # noqa: BLE001 - child returns exact race result
        results.put(f"{type(exc).__name__}:{exc}")
    else:
        results.put("ok")


def _process_crash_after_epoch_snapshot(root_text: str, epoch_id: str) -> None:
    journal = _journal_with_lineage(Path(root_text), epoch_id)

    def terminate_after_snapshot(*_args: object, **_kwargs: object) -> None:
        (Path(root_text) / "receipt-boundary-reached.marker").write_bytes(b"snapshot-published")
        os._exit(87)

    journal._write_committed_receipt = (  # type: ignore[method-assign]
        terminate_after_snapshot
    )
    journal.finalize(created_at=epoch_fixtures.NOW)
    os._exit(88)


def test_durable_audit_commits_before_state_and_reloads(tmp_path: Path) -> None:
    hot, archive, _restore = collector_fixtures.roots(tmp_path)
    guard = collector_fixtures.storage_guard(hot, archive)
    guard.require_preflight()
    stream = default_crypto_l2_streams()[0]
    clock = collector_fixtures.FakeClock()
    store = DurableAuditStore(
        hot,
        stream_id=stream.stream_id,
        connection_id="durable-one",
        collector_commit="abc123",
        storage_guard=guard,
    )
    machine = CaptureStateMachine(
        stream,
        connection_id="durable-one",
        collector_commit="abc123",
        clock=clock,
        audit_sink=store.append,
    )
    machine.transition(CaptureState.BUFFERING, "connected")
    assert len(machine.events) == len(machine.references) == 2
    for reference in machine.references:
        payload = store.load(Path(reference.audit_path))
        assert payload["audit_sha256"] == reference.audit_sha256

    original = store.append

    def fail_after_initial(event):
        if event.ordinal == 3:
            raise OSError("injected durable audit failure")
        return original(event)

    machine.audit_sink = fail_after_initial
    with pytest.raises(AuditPersistenceError, match="before state commit"):
        machine.transition(CaptureState.SNAPSHOT_SYNC, "must-fail-closed")
    assert machine.state is CaptureState.BUFFERING
    assert len(machine.events) == len(machine.references) == 2


def test_illegal_transition_is_durable_and_failed_illegal_audit_is_not_counted(
    tmp_path: Path,
) -> None:
    hot, archive, _restore = collector_fixtures.roots(tmp_path)
    guard = collector_fixtures.storage_guard(hot, archive)
    guard.require_preflight()
    stream = default_crypto_l2_streams()[0]
    store = DurableAuditStore(
        hot,
        stream_id=stream.stream_id,
        connection_id="illegal-one",
        collector_commit="abc123",
        storage_guard=guard,
    )
    machine = CaptureStateMachine(
        stream,
        connection_id="illegal-one",
        collector_commit="abc123",
        clock=collector_fixtures.FakeClock(),
        audit_sink=store.append,
    )
    with pytest.raises(ValidationError, match="illegal"):
        machine.transition(CaptureState.LIVE, "skip-sync")
    assert machine.state is CaptureState.CONNECTING
    assert machine.events[-1].event == "illegal_transition"
    durable_count = len(machine.events)
    machine.audit_sink = lambda _event: (_ for _ in ()).throw(OSError("audit unavailable"))
    with pytest.raises(AuditPersistenceError):
        machine.transition(CaptureState.LIVE, "still-illegal")
    assert len(machine.events) == durable_count


def test_default_hot_path_amortizes_tree_capacity_and_fsync_probes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hot, archive, _restore = collector_fixtures.roots(tmp_path)
    counts = {"capacity": 0, "tree": 0, "fsync": 0}

    def capacity(_path: Path) -> DiskCapacity:
        counts["capacity"] += 1
        return DiskCapacity(10 * 1024**3, 9 * 1024**3)

    def tree(_path: Path) -> int:
        counts["tree"] += 1
        return 0

    guard = CaptureStorageGuard(
        hot,
        archive,
        policy=collector_fixtures.POLICY,
        archive_reserve_bytes=1,
        volume_identity=collector_fixtures.distinct_identity,
        capacity_probe=capacity,
        hot_size_probe=tree,
    )
    guard.require_preflight()
    stream = default_crypto_l2_streams()[0]
    writer = RawSegmentWriter(
        hot,
        stream.stream_id,
        stream.provider.value,
        collector_commit="abc123",
        rotation=SegmentRotation(
            max_messages=2_000, max_wire_bytes=64 * 1024**2, max_age_seconds=100_000
        ),
        storage_guard=guard,
        policy=collector_fixtures.POLICY,
    )
    original_fsync = storage_module.os.fsync

    def counted_fsync(descriptor: int) -> None:
        counts["fsync"] += 1
        original_fsync(descriptor)

    monkeypatch.setattr(storage_module.os, "fsync", counted_fsync)
    for index in range(1, 1_001):
        writer.append(storage_fixtures.raw_frame(stream, index))
    assert writer.pending_messages == 1_000
    assert counts["tree"] <= 5
    assert counts["capacity"] <= 6
    assert counts["fsync"] == 0
    statistics = guard.probe_statistics
    assert statistics["hot_reservations"] == 1_000
    assert statistics["maximum_unprobed_bytes"] == 4 * 1024 * 1024


def test_normalized_append_uses_bounded_fsync_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hot, _archive, guard = epoch_fixtures.storage(tmp_path)
    calls = 0
    original = epoch_module.os.fsync

    def counted(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        original(descriptor)

    monkeypatch.setattr(epoch_module.os, "fsync", counted)
    journal = NormalizedEpochJournal(
        hot,
        epoch_id="batched-fsync",
        stream_id="stream-one",
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=epoch_fixtures.POLICY,
    )
    before_append = calls
    journal.append({"value": index} for index in range(1_000))
    assert calls - before_append == 3
    journal.abort_visible("test-complete")
    assert calls > before_append + 3


def _journal_with_lineage(tmp_path: Path, epoch_id: str) -> NormalizedEpochJournal:
    tmp_path.mkdir(parents=True, exist_ok=True)
    hot, _archive, guard = epoch_fixtures.storage(tmp_path)
    config = epoch_fixtures.stream(Provider.BINANCE, epoch_fixtures.MarketKind.SPOT)
    writer = RawSegmentWriter(
        hot,
        config.stream_id,
        config.provider.value,
        collector_commit="abc",
        rotation=SegmentRotation(max_messages=2),
        storage_guard=guard,
        policy=epoch_fixtures.POLICY,
    )
    writer.append(epoch_fixtures.frame(config, {"raw": 1}, 1))
    writer.append(epoch_fixtures.frame(config, {"raw": 2}, 2))
    segment = writer.drain_completed()[0]
    sync = epoch_fixtures.BinanceBookSynchronizer(config, epoch_fixtures.resolver())
    records = list(sync.admit_snapshot(epoch_fixtures.binance_snapshot(config)).records)
    records.extend(sync.admit_update(epoch_fixtures.binance_update(config, 100, 101)).records)
    journal = NormalizedEpochJournal(
        hot,
        epoch_id=epoch_id,
        stream_id=config.stream_id,
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=epoch_fixtures.POLICY,
        max_part_rows=1,
    )
    journal.append(records)
    journal.record_segment(segment)
    return journal


def _identity_anchor(journal: NormalizedEpochJournal) -> dict[str, tuple[str, str]]:
    return {journal.stream_id: (journal.provider, journal.venue)}


def _existing_epoch_storage(
    root: Path,
) -> tuple[Path, CaptureStorageGuard, dict[str, tuple[str, str]]]:
    hot, archive = root / "hot", root / "archive"

    def identity(path: Path) -> VolumeIdentity:
        value = "archive" if path == archive else "hot"
        return VolumeIdentity(value, (value,))

    guard = CaptureStorageGuard(
        hot,
        archive,
        policy=epoch_fixtures.POLICY,
        archive_reserve_bytes=1,
        volume_identity=identity,
        capacity_probe=lambda _path: DiskCapacity(10**9, 9 * 10**8),
        hot_size_probe=lambda _path: 0,
    )
    stream = epoch_fixtures.stream(Provider.BINANCE, epoch_fixtures.MarketKind.SPOT)
    return hot, guard, {stream.stream_id: (stream.provider.value, stream.venue)}


def test_normalized_finalize_publish_failure_retains_journal_and_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal_with_lineage(tmp_path, "publish-retry")
    original = journal._publish_normalized
    calls = 0

    def fail_once():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("injected publish failure")
        return original()

    monkeypatch.setattr(journal, "_publish_normalized", fail_once)
    with pytest.raises(OSError, match="publish failure"):
        journal.finalize(created_at=epoch_fixtures.NOW)
    failures = tuple(journal.root.glob("finalize-failure-*.json"))
    assert failures
    failure = json.loads(failures[-1].read_text(encoding="utf-8"))
    assert failure["transaction_state"] == "ABORTED"
    assert failure["prepared_sha256"]
    receipt = journal.finalize(created_at=epoch_fixtures.NOW)
    assert receipt.normalized_snapshot_id and Path(receipt.receipt_path).is_file()
    assert receipt.transaction_state == "COMMITTED"


def test_normalized_crash_after_publish_reconciles_prepared_transaction(tmp_path: Path) -> None:
    crash_root = tmp_path / "crash-process"
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=_process_crash_after_epoch_snapshot,
        args=(str(crash_root), "crash-window"),
    )
    process.start()
    process.join(30)
    assert process.exitcode not in {None, 0, 88}
    assert (crash_root / "receipt-boundary-reached.marker").read_bytes() == b"snapshot-published"

    hot, guard, identities = _existing_epoch_storage(crash_root)
    stream_id = next(iter(identities))
    journal_root = (
        hot / "capture" / "normalized-epoch-journal" / f"stream={stream_id}" / "epoch=crash-window"
    )
    prepared = tuple(journal_root.glob("transaction-prepared-*.json"))
    assert len(prepared) == 1
    assert not tuple(journal_root.glob("receipt-sha256-*.json"))
    assert not tuple(journal_root.glob("finalize-failure-*.json"))
    snapshots = tuple((hot / "normalized" / "snapshots").glob("sha256-*"))
    assert len(snapshots) == 1

    receipts = NormalizedEpochJournal.reconcile_pending(
        hot,
        expected_streams=identities,
        storage_guard=guard,
        policy=epoch_fixtures.POLICY,
    )
    assert len(receipts) == 1
    assert receipts[0].transaction_state == "COMMITTED"
    assert receipts[0].prepared_sha256 in prepared[0].name
    assert receipts[0].normalized_snapshot_id == snapshots[0].name
    assert (
        NormalizedEpochJournal.reconcile_pending(
            hot,
            expected_streams=identities,
            storage_guard=guard,
            policy=epoch_fixtures.POLICY,
        )
        == ()
    )


def test_reconciliation_binds_aborts_to_their_prepared_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal_with_lineage(tmp_path, "retry-then-crash")
    original_publish = journal._publish_normalized
    monkeypatch.setattr(
        journal,
        "_publish_normalized",
        lambda: (_ for _ in ()).throw(OSError("first attempt")),
    )
    with pytest.raises(OSError, match="first attempt"):
        journal.finalize(created_at=epoch_fixtures.NOW)
    monkeypatch.setattr(journal, "_publish_normalized", original_publish)
    monkeypatch.setattr(
        journal,
        "_write_committed_receipt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SystemExit("second attempt crash")),
    )
    with pytest.raises(SystemExit, match="second attempt crash"):
        journal.finalize(created_at=epoch_fixtures.NOW)
    assert len(tuple(journal.root.glob("transaction-prepared-*.json"))) == 2
    assert len(tuple(journal.root.glob("finalize-failure-*.json"))) == 1
    journal._open_stream.close()
    receipts = NormalizedEpochJournal.reconcile_pending(
        journal.hot_root,
        expected_streams=_identity_anchor(journal),
        storage_guard=journal.storage_guard,
        policy=journal.policy,
    )
    assert len(receipts) == 1 and receipts[0].transaction_state == "COMMITTED"


def test_reconciliation_rejects_conflicting_terminal_states(tmp_path: Path) -> None:
    journal = _journal_with_lineage(tmp_path, "terminal-conflict")
    receipt = journal.finalize(created_at=epoch_fixtures.NOW)
    prepared = json.loads(
        next(journal.root.glob("transaction-prepared-*.json")).read_text(encoding="utf-8")
    )
    journal._record_finalize_failure(
        OSError("impossible post-commit failure"),
        prepared=prepared,
        result=epoch_module._NormalizationSummary(
            receipt.normalized_snapshot_id,
            receipt.accepted_rows,
            receipt.quarantined_rows,
        ),
        created_at=receipt.created_at,
    )
    with pytest.raises(ValidationError, match="conflicting terminal"):
        NormalizedEpochJournal.reconcile_pending(
            journal.hot_root,
            expected_streams=_identity_anchor(journal),
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )


def test_pending_transaction_rejects_mutation_and_malformed_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal_with_lineage(tmp_path, "pending-mutation")
    monkeypatch.setattr(
        journal,
        "_write_committed_receipt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SystemExit("pending")),
    )
    with pytest.raises(SystemExit):
        journal.finalize(created_at=epoch_fixtures.NOW)
    journal._records += 1
    with pytest.raises(ValidationError, match="changed after PREPARED"):
        journal._prepare_transaction(epoch_fixtures.NOW.isoformat())
    journal._records -= 1
    journal._open_stream.close()
    prepared = next(journal.root.glob("transaction-prepared-*.json"))
    _rewrite_content_addressed(prepared, "prepared_sha256", created_at="not-a-time")
    with pytest.raises(ValidationError, match="created_at is malformed"):
        NormalizedEpochJournal.reconcile_pending(
            journal.hot_root,
            expected_streams=_identity_anchor(journal),
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )


def test_epoch_recovery_rejects_reparse_journal_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _failed_epoch_for_recovery(tmp_path, "reparse-recovery")
    real_reparse = storage_module._is_reparse_point
    blocked = journal.root.parent
    monkeypatch.setattr(
        storage_module,
        "_is_reparse_point",
        lambda path: Path(path) == blocked or real_reparse(path),
    )
    with pytest.raises(ValidationError, match="reparse"):
        NormalizedEpochJournal.recover(
            journal.hot_root,
            epoch_id=journal.epoch_id,
            stream_id=journal.stream_id,
            provider=journal.provider,
            venue=journal.venue,
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )


@pytest.mark.skipif(os.name != "nt", reason="requires a real Windows directory junction")
def test_epoch_creation_rejects_real_windows_junction(tmp_path: Path) -> None:
    hot, _archive, guard = epoch_fixtures.storage(tmp_path)
    outside = tmp_path / "outside-journal"
    outside.mkdir()
    capture = hot / "capture"
    capture.mkdir()
    junction = capture / "normalized-epoch-journal"
    created = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(outside)],
        check=False,
        capture_output=True,
        text=True,
    )
    if created.returncode != 0:
        pytest.skip(f"Windows junction creation unavailable: {created.stderr.strip()}")
    try:
        with pytest.raises(ValidationError, match="reparse"):
            NormalizedEpochJournal(
                hot,
                epoch_id="junction",
                stream_id="stream",
                provider="binance",
                venue="BINANCE",
                storage_guard=guard,
                policy=epoch_fixtures.POLICY,
            )
        assert not tuple(outside.iterdir())
    finally:
        junction.rmdir()


@pytest.mark.skipif(os.name != "nt", reason="requires a real Windows directory junction")
def test_bootstrap_lock_rejects_real_windows_junction_before_lock_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hot, _archive, _guard = epoch_fixtures.storage(tmp_path)
    outside = tmp_path / "outside-bootstrap"
    outside.mkdir()
    sentinel = outside / "sentinel.bin"
    sentinel.write_bytes(b"outside-must-not-be-opened")
    lock = hot / ".capture-path-bootstrap.lock"
    created = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(lock), str(outside)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert created.returncode == 0, created.stderr
    lock_opened = False

    def must_not_open_lock(*_args: object, **_kwargs: object):
        nonlocal lock_opened
        lock_opened = True
        raise AssertionError("unsafe bootstrap lock open")

    monkeypatch.setattr(storage_module, "process_file_lock", must_not_open_lock)
    try:
        with pytest.raises(ValidationError, match="reparse"):
            storage_module._safe_mkdir(hot, hot / "capture")
        assert not lock_opened
        assert sentinel.read_bytes() == b"outside-must-not-be-opened"
        assert not (hot / "capture").exists()
    finally:
        lock.rmdir()


def test_epoch_journal_helpers_and_reconcile_shapes_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hot, _archive, guard = epoch_fixtures.storage(tmp_path)
    plain = hot / "plain.bin"
    plain.write_bytes(b"plain")
    assert epoch_module._sha256_file(plain) == hashlib.sha256(b"plain").hexdigest()

    malformed = hot / "malformed-sha256-deadbeef.json"
    malformed.write_text("{", encoding="utf-8")
    with pytest.raises(ValidationError, match="unreadable"):
        epoch_module._load_hashed_json(hot, malformed, hash_field="digest", description="malformed")
    scalar = hot / "scalar-sha256-deadbeef.json"
    scalar.write_text("[]", encoding="utf-8")
    with pytest.raises(ValidationError, match="JSON object"):
        epoch_module._load_hashed_json(hot, scalar, hash_field="digest", description="scalar")

    exclusive = hot / "exclusive.bin"
    original_validate = epoch_module._validate_safe_path

    def fail_after_open(root: Path, path: Path, *, allow_missing: bool) -> Path:
        if Path(path) == exclusive and not allow_missing:
            raise ValidationError("injected post-open path replacement")
        return original_validate(root, path, allow_missing=allow_missing)

    monkeypatch.setattr(epoch_module, "_validate_safe_path", fail_after_open)
    with pytest.raises(ValidationError, match="post-open"):
        epoch_module._open_journal_file(hot, exclusive, "xb")
    monkeypatch.setattr(epoch_module, "_validate_safe_path", original_validate)

    base = hot / "capture" / "normalized-epoch-journal"
    base.mkdir(parents=True)
    (base / "stream=not-a-directory").write_text("x", encoding="utf-8")
    with pytest.raises(ValidationError, match="stream journal is not a directory"):
        NormalizedEpochJournal.reconcile_pending(
            hot,
            expected_streams={},
            storage_guard=guard,
            policy=epoch_fixtures.POLICY,
        )

    other_root = tmp_path / "epoch-shape"
    other_root.mkdir()
    other_hot, _other_archive, other_guard = epoch_fixtures.storage(other_root)
    stream_root = other_hot / "capture" / "normalized-epoch-journal" / "stream=stream"
    stream_root.mkdir(parents=True)
    (stream_root / "epoch=not-a-directory").write_text("x", encoding="utf-8")
    with pytest.raises(ValidationError, match="epoch journal is not a directory"):
        NormalizedEpochJournal.reconcile_pending(
            other_hot,
            expected_streams={"stream": ("binance", "BINANCE")},
            storage_guard=other_guard,
            policy=epoch_fixtures.POLICY,
        )


def test_normalized_receipt_failure_abort_failure_and_process_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal_with_lineage(tmp_path / "recoverable", "receipt-recovery")
    original_publish = epoch_module._atomic_immutable_write
    failed = False

    def receipt_fails_once(path: Path, body: bytes, *, root: Path | None = None) -> None:
        nonlocal failed
        if path.name.startswith("receipt-") and not failed:
            failed = True
            raise OSError("injected receipt publish failure")
        original_publish(path, body, root=root)

    monkeypatch.setattr(epoch_module, "_atomic_immutable_write", receipt_fails_once)
    with pytest.raises(OSError, match="receipt publish"):
        journal.finalize(created_at=epoch_fixtures.NOW)
    journal._open_stream.close()
    recovered = NormalizedEpochJournal.recover(
        journal.hot_root,
        epoch_id=journal.epoch_id,
        stream_id=journal.stream_id,
        provider=journal.provider,
        venue=journal.venue,
        storage_guard=journal.storage_guard,
        policy=journal.policy,
    )
    receipt = recovered.finalize(created_at=epoch_fixtures.NOW)
    assert Path(receipt.receipt_path).is_file()

    broken = _journal_with_lineage(tmp_path / "double", "double-failure")
    monkeypatch.setattr(
        broken, "_publish_normalized", lambda: (_ for _ in ()).throw(OSError("publish"))
    )

    def failure_record_fails(path: Path, body: bytes, *, root: Path | None = None) -> None:
        if path.name.startswith("finalize-failure-"):
            raise OSError("abort audit")
        original_publish(path, body, root=root)

    monkeypatch.setattr(epoch_module, "_atomic_immutable_write", failure_record_fails)
    with pytest.raises(
        ValidationError, match="primary=OSError: publish.*audit=OSError: abort audit"
    ):
        broken.finalize(created_at=epoch_fixtures.NOW)
    assert broken._state == "PREPARED"
    assert tuple(broken.root.glob("transaction-prepared-*.json"))


def test_frozen_scope_rejects_fake_duplicate_crosswire_and_wrong_domains() -> None:
    streams = default_crypto_l2_streams()
    sample = streams[0]
    cases = (
        tuple(replace(item, native_symbol="BTCFAKE") for item in streams),
        (*streams[:-1], streams[-2]),
        tuple(
            replace(item, websocket_url="wss://stream.binance.com.evil.invalid/ws/x")
            if item == sample
            else item
            for item in streams
        ),
        tuple(
            replace(item, market=epoch_fixtures.MarketKind.USDT_PERPETUAL)
            if item == sample
            else item
            for item in streams
        ),
    )
    for candidate in cases:
        with pytest.raises(ValidationError):
            assert_m7_scope(candidate)


def test_atomic_immutable_publish_is_thread_and_process_race_safe(tmp_path: Path) -> None:
    root = tmp_path / "immutable-root"
    root.mkdir()
    same = root / "same.bin"
    barrier = threading.Barrier(8)

    def publish_same() -> None:
        barrier.wait()
        storage_module._atomic_immutable_write(same, b"same", root=root)

    with ThreadPoolExecutor(max_workers=8) as pool:
        tuple(pool.map(lambda _index: publish_same(), range(8)))
    assert same.read_bytes() == b"same"

    context = multiprocessing.get_context("spawn")
    start = context.Event()
    results = context.Queue()
    target = root / "process-race.bin"
    processes = [
        context.Process(
            target=_process_publish,
            args=(str(root), str(target), body, start, results),
        )
        for body in (b"first", b"second")
    ]
    for process in processes:
        process.start()
    start.set()
    for process in processes:
        process.join(20)
        assert process.exitcode == 0
    outcomes = sorted(results.get(timeout=5) for _ in processes)
    assert outcomes.count("ok") == 1
    assert sum(item.startswith("ValidationError:") for item in outcomes) == 1
    assert target.read_bytes() in {b"first", b"second"}


def test_archive_rejects_identity_change_and_reparse_final_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture_config, streams = collector_fixtures.config(tmp_path)
    changed = False

    def identity(path: Path) -> VolumeIdentity:
        base = collector_fixtures.distinct_identity(path)
        if changed and path == capture_config.archive_root:
            return VolumeIdentity("changed", ("changed-device",))
        return base

    guard = collector_fixtures.storage_guard(
        capture_config.hot_root, capture_config.archive_root, identity=identity
    )
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
    )
    archive.preflight()
    writer = RawSegmentWriter(
        capture_config.hot_root,
        streams[0].stream_id,
        streams[0].provider.value,
        collector_commit="abc",
        rotation=SegmentRotation(max_messages=2),
        storage_guard=guard,
        policy=collector_fixtures.POLICY,
    )
    writer.append(storage_fixtures.raw_frame(streams[0], 1))
    writer.append(storage_fixtures.raw_frame(streams[0], 2))
    segment = writer.drain_completed()[0]
    changed = True
    with pytest.raises(CapturePausedError, match="identity changed"):
        archive.archive_segment(segment)

    changed = False
    real_reparse = storage_module._is_reparse_point
    monkeypatch.setattr(
        storage_module,
        "_is_reparse_point",
        lambda path: path.name == "raw-segments" or real_reparse(path),
    )
    with pytest.raises(ValidationError, match="reparse"):
        archive.archive_segment(segment)


def test_probe_budget_covers_okx_control_frames_without_snapshot(tmp_path: Path) -> None:
    capture_config, streams = collector_fixtures.config(tmp_path, attempts=1)
    stream = next(item for item in streams if item.provider is Provider.OKX)
    guard = collector_fixtures.storage_guard(capture_config.hot_root, capture_config.archive_root)
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
    )
    archive.preflight()

    class ControlOnlyConnection(collector_fixtures.FakeConnection):
        def __init__(self, selected) -> None:
            super().__init__(selected)
            self.messages = [
                json.dumps({"event": "subscribe", "code": "0", "msg": ""}).encode()
                for _ in range(3)
            ]

    class Connector:
        async def connect(self, _url: str, *, timeout_seconds: float):
            assert timeout_seconds > 0
            return ControlOnlyConnection(stream)

    runner = CaptureStreamRunner(
        capture_config,
        stream,
        storage_guard=guard,
        archive=archive,
        mappings=SymbolMappingResolver(default_symbol_mappings(streams)),
        http=collector_fixtures.FakeHttp(),
        websockets=Connector(),
        clock=collector_fixtures.FakeClock(),
        jitter=lambda: 0.5,
        alert_sink=lambda _message: None,
        policy=collector_fixtures.POLICY,
    )
    report = asyncio.run(runner.run(maximum_websocket_messages=3))
    assert report.outcome == "FAILED"
    assert report.websocket_messages == 3
    assert any("budget exhausted" in item for item in report.errors)
    assert report.audit_events == len(report.audit_references) > 0


def test_cancel_flushes_archives_aborts_and_persists_paused(tmp_path: Path) -> None:
    capture_config, streams = collector_fixtures.config(tmp_path, attempts=1)
    stream = next(
        item
        for item in streams
        if item.provider is Provider.BINANCE and item.market is epoch_fixtures.MarketKind.SPOT
    )
    guard = collector_fixtures.storage_guard(capture_config.hot_root, capture_config.archive_root)
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
    )
    archive.preflight()

    async def exercise() -> CaptureStreamRunner:
        blocked = asyncio.Event()

        class BlockingConnection(collector_fixtures.FakeConnection):
            async def receive(self, *, timeout_seconds: float) -> bytes:
                if self.messages:
                    return self.messages.pop(0)
                blocked.set()
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        connection = BlockingConnection(stream)

        class Connector:
            async def connect(self, _url: str, *, timeout_seconds: float):
                return connection

        runner = CaptureStreamRunner(
            capture_config,
            stream,
            storage_guard=guard,
            archive=archive,
            mappings=SymbolMappingResolver(default_symbol_mappings(streams)),
            http=collector_fixtures.FakeHttp(),
            websockets=Connector(),
            clock=collector_fixtures.FakeClock(),
            jitter=lambda: 0.5,
            alert_sink=lambda _message: None,
            policy=collector_fixtures.POLICY,
        )
        task = asyncio.create_task(runner.run(maximum_websocket_messages=None))
        await asyncio.wait_for(blocked.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return runner

    runner = asyncio.run(exercise())
    report = runner._last_report
    assert report is not None and report.outcome == "CANCELLED"
    assert report.final_state == CaptureState.PAUSED.value
    assert report.pending_raw_messages == report.pending_raw_segments == 0
    assert report.raw_segments == report.archived_segments > 0
    assert report.epoch_aborts
    assert Path(report.epoch_aborts[0]).is_file()
    paused = json.loads(Path(report.audit_references[-1]).read_text(encoding="utf-8"))
    assert paused["event"]["to_state"] == CaptureState.PAUSED.value


def test_coordinator_cancels_peer_streams_and_still_persists_eight_failures(
    tmp_path: Path,
) -> None:
    capture_config, streams = collector_fixtures.config(tmp_path, attempts=1)
    guard = collector_fixtures.storage_guard(capture_config.hot_root, capture_config.archive_root)
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
    )

    class MixedConnector:
        async def connect(self, url: str, *, timeout_seconds: float):
            if url == streams[0].websocket_url:
                raise ProviderError("injected first-stream failure")

            class Blocked:
                async def send(self, _payload: bytes) -> None:
                    return None

                async def receive(self, *, timeout_seconds: float) -> bytes:
                    await asyncio.Event().wait()
                    raise AssertionError("unreachable")

                async def close(self) -> None:
                    return None

            return Blocked()

    coordinator = CryptoL2CaptureCoordinator(
        capture_config,
        storage_guard=guard,
        archive=archive,
        http=collector_fixtures.FakeHttp(),
        websockets=MixedConnector(),
        clock=collector_fixtures.FakeClock(),
        jitter=lambda: 0.5,
        policy=collector_fixtures.POLICY,
    )
    report = asyncio.run(coordinator.run(maximum_websocket_messages=3))
    assert report.status == "CAPTURE_FAILED"
    assert len(report.streams) == 8
    assert all(item.outcome in {"FAILED", "CANCELLED"} for item in report.streams)
    assert report.report_path and Path(report.report_path).is_file()
    payload = json.loads(Path(report.report_path).read_text(encoding="utf-8"))
    assert payload["collector_commit"] == capture_config.collector_commit
    assert payload["audit_reference_sha256"] == report.audit_reference_sha256


def test_preflight_report_uses_honest_zero_audit_and_report_publish_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture_config, _streams = collector_fixtures.config(tmp_path)
    same = lambda _path: VolumeIdentity("same", ("same-device",))
    guard = collector_fixtures.storage_guard(
        capture_config.hot_root, capture_config.archive_root, identity=same
    )
    coordinator = CryptoL2CaptureCoordinator(
        capture_config,
        storage_guard=guard,
        archive=LocalArchiveController(
            capture_config.hot_root,
            capture_config.archive_root,
            capture_config.restore_root,
            storage_guard=guard,
        ),
        clock=collector_fixtures.FakeClock(),
        policy=collector_fixtures.POLICY,
    )
    report = coordinator.preflight_only()
    assert all(item.audit_events == 0 and not item.audit_references for item in report.streams)
    assert report.collector_commit == capture_config.collector_commit

    monkeypatch.setattr(
        collector_module,
        "_atomic_immutable_write",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("report disk failure")),
    )
    failed = coordinator.preflight_only()
    assert failed.status == "CAPTURE_REPORT_PERSISTENCE_FAILED"
    assert failed.report_path is None


def test_storage_guard_path_audit_and_incremental_negative_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hot, archive, _restore = collector_fixtures.roots(tmp_path)
    with pytest.raises(ValidationError, match="probe intervals"):
        CaptureStorageGuard(hot, archive, probe_messages=0)
    guard = collector_fixtures.storage_guard(hot, archive)
    guard.require_preflight()
    with pytest.raises(ValidationError, match="cannot be negative"):
        guard.require_hot_capacity(projected_write_bytes=-1)
    with pytest.raises(ValidationError, match="cannot be negative"):
        guard.require_archive_capacity(projected_write_bytes=-1)
    with pytest.raises(CapturePausedError):
        guard.require_hot_capacity(projected_write_bytes=2 * 1024**3)
    with pytest.raises(CapturePausedError, match="archive free-space floor"):
        guard.require_archive_capacity(projected_write_bytes=2 * 1024**3)
    guard.force_hot_probe()

    with pytest.raises(ValidationError, match="root is missing"):
        storage_module._validate_safe_path(
            tmp_path / "missing-root", tmp_path / "missing-root" / "x", allow_missing=True
        )
    with pytest.raises(ValidationError, match="escapes trusted root"):
        storage_module._validate_safe_path(hot, archive / "escape", allow_missing=True)
    with pytest.raises(ValidationError, match="path is missing"):
        storage_module._validate_safe_path(hot, hot / "missing", allow_missing=False)
    assert storage_module._is_reparse_point(hot / "does-not-exist") is False

    target = hot / "hash-mismatch.bin"
    real_hash = storage_module._sha256_file

    def wrong_staging_hash(path: Path) -> str:
        if path.name.endswith(".tmp"):
            return "0" * 64
        return real_hash(path)

    monkeypatch.setattr(storage_module, "_sha256_file", wrong_staging_hash)
    with pytest.raises(ValidationError, match="staging hash mismatch"):
        storage_module._atomic_immutable_write(target, b"body", root=hot)
    monkeypatch.setattr(storage_module, "_sha256_file", real_hash)

    stream = default_crypto_l2_streams()[0]
    store = DurableAuditStore(
        hot,
        stream_id=stream.stream_id,
        connection_id="negative-audit",
        collector_commit="abc",
        storage_guard=guard,
    )
    wrong_event = AuditEvent(
        ordinal=1,
        stream_id="wrong-stream",
        occurred_at=collector_fixtures.NOW,
        event="state_transition",
        from_state=None,
        to_state=CaptureState.CONNECTING,
        reason="test",
    )
    with pytest.raises(ValidationError, match="stream identity"):
        store.append(wrong_event)
    malformed = store.root / "malformed.json"
    malformed.write_text("{", encoding="utf-8")
    with pytest.raises(ValidationError, match="unreadable"):
        store.load(malformed)
    scalar = store.root / "scalar.json"
    scalar.write_text("[]", encoding="utf-8")
    with pytest.raises(ValidationError, match="JSON object"):
        store.load(scalar)
    changed = store.root / "changed.json"
    changed.write_text(json.dumps({"audit_sha256": "0" * 64}), encoding="utf-8")
    with pytest.raises(ValidationError, match="hash verification"):
        store.load(changed)


def test_storage_ack_reload_identity_and_restore_parent_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture_config, streams = collector_fixtures.config(tmp_path)
    guard = collector_fixtures.storage_guard(capture_config.hot_root, capture_config.archive_root)
    writer = RawSegmentWriter(
        capture_config.hot_root,
        streams[0].stream_id,
        streams[0].provider.value,
        collector_commit="abc",
        rotation=SegmentRotation(max_messages=2),
        storage_guard=guard,
        policy=collector_fixtures.POLICY,
    )
    with pytest.raises(ValidationError, match="out of order"):
        writer.acknowledge_completed(object())  # type: ignore[arg-type]

    guard.require_preflight()
    store = DurableAuditStore(
        capture_config.hot_root,
        stream_id=streams[0].stream_id,
        connection_id="reload-mismatch",
        collector_commit="abc",
        storage_guard=guard,
    )
    event = AuditEvent(
        ordinal=1,
        stream_id=streams[0].stream_id,
        occurred_at=collector_fixtures.NOW,
        event="state_transition",
        from_state=None,
        to_state=CaptureState.CONNECTING,
        reason="test",
    )
    monkeypatch.setattr(
        store,
        "load",
        lambda _path: {"audit_sha256": "wrong", "ordinal": event.ordinal},
    )
    with pytest.raises(ValidationError, match="reload verification"):
        store.append(event)

    controller = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
    )
    controller.preflight()
    monkeypatch.setattr(
        guard,
        "volume_identity",
        lambda _path: (_ for _ in ()).throw(OSError("identity probe failed")),
    )
    with pytest.raises(CapturePausedError, match="identity recheck failed"):
        controller._assert_identity(
            capture_config.archive_root,
            collector_fixtures.distinct_identity(capture_config.archive_root),
            "archive_root",
        )
    nested = capture_config.restore_root / "nested"
    nested.mkdir()
    restore_temp = nested / "temp"
    restore_temp.mkdir()
    with pytest.raises(CapturePausedError, match="escaped configured restore_root"):
        controller._remove_restore_temp(restore_temp)
    assert restore_temp.is_dir()


def test_epoch_recovery_and_abort_negative_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty_root = tmp_path / "empty"
    empty_root.mkdir()
    hot, _archive, guard = epoch_fixtures.storage(empty_root)
    empty = NormalizedEpochJournal(
        hot,
        epoch_id="empty-flush",
        stream_id="stream",
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=epoch_fixtures.POLICY,
    )
    empty.flush()
    empty.abort_visible("first")
    with pytest.raises(ValidationError, match="already aborted"):
        empty.abort_visible("second")
    with pytest.raises(ValidationError, match="not a retryable"):
        NormalizedEpochJournal.recover(
            hot,
            epoch_id="missing",
            stream_id="stream",
            provider="binance",
            venue="BINANCE",
            storage_guard=guard,
            policy=epoch_fixtures.POLICY,
        )

    finalized = _journal_with_lineage(tmp_path / "finalized", "done")
    finalized.finalize(created_at=epoch_fixtures.NOW)
    with pytest.raises(ValidationError, match="not a retryable"):
        NormalizedEpochJournal.recover(
            finalized.hot_root,
            epoch_id=finalized.epoch_id,
            stream_id=finalized.stream_id,
            provider=finalized.provider,
            venue=finalized.venue,
            storage_guard=finalized.storage_guard,
            policy=finalized.policy,
        )

    mismatch = _journal_with_lineage(tmp_path / "mismatch", "mismatch")
    monkeypatch.setattr(
        mismatch,
        "_publish_normalized",
        lambda: (_ for _ in ()).throw(OSError("publish")),
    )
    with pytest.raises(OSError):
        mismatch.finalize(created_at=epoch_fixtures.NOW)
    failure_path = next(mismatch.root.glob("finalize-failure-*.json"))
    payload = json.loads(failure_path.read_text(encoding="utf-8"))
    payload["failure_sha256"] = "0" * 64
    failure_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="failure record hash changed"):
        NormalizedEpochJournal.recover(
            mismatch.hot_root,
            epoch_id=mismatch.epoch_id,
            stream_id=mismatch.stream_id,
            provider=mismatch.provider,
            venue=mismatch.venue,
            storage_guard=mismatch.storage_guard,
            policy=mismatch.policy,
        )


def _runner_for_negative_test(tmp_path: Path) -> CaptureStreamRunner:
    tmp_path.mkdir(parents=True, exist_ok=True)
    capture_config, streams = collector_fixtures.config(tmp_path, attempts=1)
    guard = collector_fixtures.storage_guard(capture_config.hot_root, capture_config.archive_root)
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
    )
    archive.preflight()
    return CaptureStreamRunner(
        capture_config,
        streams[0],
        storage_guard=guard,
        archive=archive,
        mappings=SymbolMappingResolver(default_symbol_mappings(streams)),
        http=collector_fixtures.FakeHttp(),
        websockets=collector_fixtures.FakeConnector((streams[0],)),
        clock=collector_fixtures.FakeClock(),
        jitter=lambda: 0.5,
        alert_sink=lambda _message: None,
        policy=collector_fixtures.POLICY,
    )


def test_coordinator_blocks_network_when_epoch_reconciliation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture_config, _streams = collector_fixtures.config(tmp_path, attempts=1)
    guard = collector_fixtures.storage_guard(capture_config.hot_root, capture_config.archive_root)
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
    )

    def fail_reconciliation(_cls, *_args: object, **_kwargs: object):
        raise ValidationError("injected unresolved PREPARED transaction")

    monkeypatch.setattr(
        NormalizedEpochJournal,
        "reconcile_pending",
        classmethod(fail_reconciliation),
    )

    class NetworkMustNotStart:
        async def connect(self, *_args: object, **_kwargs: object):
            raise AssertionError("network opened before epoch reconciliation")

    coordinator = CryptoL2CaptureCoordinator(
        capture_config,
        storage_guard=guard,
        archive=archive,
        websockets=NetworkMustNotStart(),
        clock=collector_fixtures.FakeClock(),
        policy=collector_fixtures.POLICY,
    )
    report = asyncio.run(coordinator.run(maximum_websocket_messages=3))
    assert report.status == "CAPTURE_FAILED"
    assert report.long_running_capture_started is False
    assert all(item.websocket_messages == 0 for item in report.streams)
    assert all("unresolved PREPARED" in item.errors[0] for item in report.streams)


def test_coordinator_reports_reconciled_epoch_before_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture_config, streams = collector_fixtures.config(tmp_path, attempts=1)
    guard = collector_fixtures.storage_guard(capture_config.hot_root, capture_config.archive_root)
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
    )
    monkeypatch.setattr(
        NormalizedEpochJournal,
        "reconcile_pending",
        classmethod(lambda _cls, *_args, **_kwargs: (object(),)),
    )
    alerts: list[str] = []
    coordinator = CryptoL2CaptureCoordinator(
        capture_config,
        storage_guard=guard,
        archive=archive,
        http=collector_fixtures.FakeHttp(),
        websockets=collector_fixtures.FakeConnector(streams),
        clock=collector_fixtures.FakeClock(),
        alert_sink=alerts.append,
        policy=collector_fixtures.POLICY,
    )
    report = asyncio.run(coordinator.run(maximum_websocket_messages=3))
    assert report.status == "BOUNDED_PROBE_COMPLETE"
    assert "CAPTURE_EPOCH_RECOVERY_COMMITTED:1" in alerts


def test_collector_deadline_malformed_snapshot_and_internal_guards(tmp_path: Path) -> None:
    runner = _runner_for_negative_test(tmp_path)
    runner._probe_message_limit = 3
    runner._probe_deadline = 0
    runner.monotonic = lambda: 1

    class NeverReceive:
        async def receive(self, *, timeout_seconds: float) -> bytes:
            raise AssertionError("deadline must fail before receive")

    with pytest.raises(ValidationError, match="timeout exhausted"):
        asyncio.run(runner._receive_frame(NeverReceive()))  # type: ignore[arg-type]
    malformed_snapshot = runner._https_frame(b"{}", "https://api.binance.com", event_time=None)
    assert dict(malformed_snapshot.native_sequence) == {}
    with pytest.raises(ValidationError, match="not initialized"):
        runner._apply_outcome(collector_module.NormalizationOutcome())
    runner._new_epoch()
    with pytest.raises(ValidationError, match="previous Normalized epoch"):
        runner._new_epoch()
    runner._finalize_epoch(abort_reason="test")
    runner._pause("first", ValueError("injected"))
    event_count = len(runner.state.events)
    runner._pause("already-paused")
    assert len(runner.state.events) == event_count


def test_runner_audit_failure_and_coordinator_run_preflight_failure(tmp_path: Path) -> None:
    runner = _runner_for_negative_test(tmp_path / "runner")
    runner.state.audit_sink = lambda _event: (_ for _ in ()).throw(OSError("audit disk full"))
    report = asyncio.run(runner.run(maximum_websocket_messages=3))
    assert report.outcome == "FAILED"
    assert report.audit_failures > 0
    assert report.final_state == CaptureState.CONNECTING.value

    coordinator_root = tmp_path / "coordinator"
    coordinator_root.mkdir()
    capture_config, _streams = collector_fixtures.config(coordinator_root)
    same = lambda _path: VolumeIdentity("same", ("same-device",))
    guard = collector_fixtures.storage_guard(
        capture_config.hot_root, capture_config.archive_root, identity=same
    )
    coordinator = CryptoL2CaptureCoordinator(
        capture_config,
        storage_guard=guard,
        archive=LocalArchiveController(
            capture_config.hot_root,
            capture_config.archive_root,
            capture_config.restore_root,
            storage_guard=guard,
        ),
        clock=collector_fixtures.FakeClock(),
        policy=collector_fixtures.POLICY,
    )
    failed = asyncio.run(coordinator.run(maximum_websocket_messages=3))
    assert failed.status == "PAUSED_PREFLIGHT_FAILED"
    assert all(item.audit_events == 0 for item in failed.streams)


def test_coordinator_unhandled_stream_exception_and_outer_cancel_still_report_eight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    exception_root = tmp_path / "exception"
    exception_root.mkdir()
    capture_config, _streams = collector_fixtures.config(exception_root, attempts=1)
    guard = collector_fixtures.storage_guard(capture_config.hot_root, capture_config.archive_root)
    archive = LocalArchiveController(
        capture_config.hot_root,
        capture_config.archive_root,
        capture_config.restore_root,
        storage_guard=guard,
    )

    async def unhandled(self, *, maximum_websocket_messages):
        if self.stream == capture_config.streams[0]:
            raise RuntimeError("unhandled finalize failure")
        await asyncio.Event().wait()

    monkeypatch.setattr(CaptureStreamRunner, "run", unhandled)
    coordinator = CryptoL2CaptureCoordinator(
        capture_config,
        storage_guard=guard,
        archive=archive,
        clock=collector_fixtures.FakeClock(),
        policy=collector_fixtures.POLICY,
    )
    report = asyncio.run(coordinator.run(maximum_websocket_messages=3))
    assert report.status == "CAPTURE_FAILED" and len(report.streams) == 8
    assert any("unhandled finalize" in "".join(item.errors) for item in report.streams)

    monkeypatch.undo()
    cancel_root = tmp_path / "cancel"
    cancel_root.mkdir()
    cancel_config, _ = collector_fixtures.config(cancel_root, attempts=1)
    cancel_guard = collector_fixtures.storage_guard(
        cancel_config.hot_root, cancel_config.archive_root
    )
    cancel_archive = LocalArchiveController(
        cancel_config.hot_root,
        cancel_config.archive_root,
        cancel_config.restore_root,
        storage_guard=cancel_guard,
    )
    connected = asyncio.Event()

    class BlockingConnector:
        async def connect(self, _url: str, *, timeout_seconds: float):
            connected.set()
            await asyncio.Event().wait()

    cancel_coordinator = CryptoL2CaptureCoordinator(
        cancel_config,
        storage_guard=cancel_guard,
        archive=cancel_archive,
        websockets=BlockingConnector(),
        clock=collector_fixtures.FakeClock(),
        policy=collector_fixtures.POLICY,
    )

    async def cancel_outer() -> None:
        task = asyncio.create_task(cancel_coordinator.run(maximum_websocket_messages=3))
        await asyncio.wait_for(connected.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_outer())
    reports = tuple((cancel_config.hot_root / "capture" / "run-reports").glob("*.json"))
    assert reports
    payload = json.loads(reports[-1].read_text(encoding="utf-8"))
    assert payload["status"] == "CAPTURE_CANCELLED"
    assert len(payload["streams"]) == 8


def _failed_epoch_for_recovery(tmp_path: Path, epoch_id: str) -> NormalizedEpochJournal:
    journal = _journal_with_lineage(tmp_path, epoch_id)
    journal._publish_normalized = lambda: (_ for _ in ()).throw(OSError("publish"))  # type: ignore[method-assign]
    with pytest.raises(OSError):
        journal.finalize(created_at=epoch_fixtures.NOW)
    journal._open_stream.close()
    return journal


def _rewrite_content_addressed(path: Path, hash_field: str, **changes: object) -> Path:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.pop(hash_field)
    payload.update(changes)
    digest = hashlib.sha256(epoch_module.canonical_json_bytes(payload)).hexdigest()
    payload[hash_field] = digest
    prefix = path.name.split("-sha256-", maxsplit=1)[0]
    rewritten = path.with_name(f"{prefix}-sha256-{digest}{path.suffix}")
    path.rename(rewritten)
    rewritten.write_text(json.dumps(payload), encoding="utf-8")
    return rewritten


def _rewrite_failure(journal: NormalizedEpochJournal, **changes: object) -> None:
    path = next(journal.root.glob("finalize-failure-*.json"))
    _rewrite_content_addressed(path, "failure_sha256", **changes)


@pytest.mark.parametrize(
    ("value", "message"),
    (
        ({}, "Raw lineage is malformed"),
        ([{"source": "binance"}], "Raw lineage is malformed"),
    ),
)
def test_epoch_raw_lineage_rejects_non_list_and_malformed_entries(
    tmp_path: Path, value: object, message: str
) -> None:
    journal = _failed_epoch_for_recovery(tmp_path, "malformed-raw-lineage")
    with pytest.raises(ValidationError, match=message):
        epoch_module._validate_raw_references(
            journal.hot_root,
            value,
            description="test terminal",
        )


def test_epoch_raw_lineage_rejects_duplicates_and_untrusted_objects(tmp_path: Path) -> None:
    journal = _failed_epoch_for_recovery(tmp_path, "duplicate-raw-lineage")
    prepared = json.loads(
        next(journal.root.glob("transaction-prepared-*.json")).read_text(encoding="utf-8")
    )
    reference = prepared["raw_references"][0]
    with pytest.raises(ValidationError, match="contains duplicates"):
        epoch_module._validate_raw_references(
            journal.hot_root,
            [reference, reference],
            description="test terminal",
        )

    unavailable = {**reference, "object_id": "sha256-" + "0" * 64}
    with pytest.raises(ValidationError):
        epoch_module._validate_raw_references(
            journal.hot_root,
            [unavailable],
            description="test terminal",
        )


def test_epoch_binding_rejects_noncanonical_time_and_missing_raw_lineage(
    tmp_path: Path,
) -> None:
    journal = _failed_epoch_for_recovery(tmp_path, "binding-negative-paths")
    prepared = json.loads(
        next(journal.root.glob("transaction-prepared-*.json")).read_text(encoding="utf-8")
    )
    failure = json.loads(
        next(journal.root.glob("finalize-failure-*.json")).read_text(encoding="utf-8")
    )
    parts = epoch_module._load_epoch_parts(journal.hot_root, journal.root)

    with pytest.raises(ValidationError, match="not canonical UTC"):
        epoch_module._parse_created_at("2026-08-29T01:00:00+00:00", "test transaction")
    with pytest.raises(ValidationError, match="require Raw segment lineage"):
        epoch_module._validate_prepared_binding(
            journal.hot_root,
            {**prepared, "raw_references": []},
            epoch_id=journal.epoch_id,
            stream_id=journal.stream_id,
            provider=journal.provider,
            venue=journal.venue,
            policy=journal.policy,
            actual_parts=parts,
        )
    with pytest.raises(ValidationError, match="require Raw segment lineage"):
        epoch_module._validate_terminal_binding(
            journal.hot_root,
            {**failure, "raw_references": []},
            prepared=None,
            epoch_id=journal.epoch_id,
            stream_id=journal.stream_id,
            provider=journal.provider,
            venue=journal.venue,
            policy=journal.policy,
            actual_parts=parts,
            schema_version="puresaber.normalized-epoch-finalize-failure@1.2.0",
            description="test terminal",
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"records": "bad"}, "row counts are malformed"),
        ({"accepted_rows": -1}, "row counts changed"),
        ({"normalized_snapshot_id": None}, "snapshot identity is missing"),
        ({"normalized_snapshot_id": 123}, "snapshot identity is malformed"),
        ({"provider": "okx"}, "snapshot identity changed"),
    ),
)
def test_epoch_snapshot_binding_rejects_malformed_and_mismatched_state(
    tmp_path: Path, changes: dict[str, object], message: str
) -> None:
    journal = _journal_with_lineage(tmp_path, "snapshot-binding")
    journal.finalize(created_at=epoch_fixtures.NOW)
    receipt = json.loads(
        next(journal.root.glob("receipt-sha256-*.json")).read_text(encoding="utf-8")
    )
    references = epoch_module._validate_raw_references(
        journal.hot_root,
        receipt["raw_references"],
        description="test receipt",
    )
    with pytest.raises(ValidationError, match=message):
        epoch_module._validate_snapshot_binding(
            journal.hot_root,
            {**receipt, **changes},
            references=references,
            description="test receipt",
        )


def test_epoch_snapshot_binding_allows_zero_accepted_rows_without_snapshot(
    tmp_path: Path,
) -> None:
    journal = _journal_with_lineage(tmp_path, "zero-accepted")
    journal.finalize(created_at=epoch_fixtures.NOW)
    receipt = json.loads(
        next(journal.root.glob("receipt-sha256-*.json")).read_text(encoding="utf-8")
    )
    references = epoch_module._validate_raw_references(
        journal.hot_root,
        receipt["raw_references"],
        description="test receipt",
    )
    epoch_module._validate_snapshot_binding(
        journal.hot_root,
        {
            **receipt,
            "records": 0,
            "accepted_rows": 0,
            "quarantined_rows": 0,
            "normalized_snapshot_id": None,
        },
        references=references,
        description="test receipt",
    )


def test_epoch_recovery_rejects_prepared_filename_attempt_mismatch(tmp_path: Path) -> None:
    journal = _failed_epoch_for_recovery(tmp_path, "duplicate-prepared")
    prepared = next(journal.root.glob("transaction-prepared-*.json"))
    duplicate = prepared.with_name(prepared.name.replace("prepared-0001-", "prepared-9999-"))
    duplicate.write_bytes(prepared.read_bytes())

    with pytest.raises(ValidationError, match="filename hash changed"):
        NormalizedEpochJournal.recover(
            journal.hot_root,
            epoch_id=journal.epoch_id,
            stream_id=journal.stream_id,
            provider=journal.provider,
            venue=journal.venue,
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )
    with pytest.raises(ValidationError, match="filename hash changed"):
        NormalizedEpochJournal.reconcile_pending(
            journal.hot_root,
            expected_streams=_identity_anchor(journal),
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )


def test_epoch_receipt_cannot_rebind_a_different_valid_snapshot(tmp_path: Path) -> None:
    journal = _journal_with_lineage(tmp_path, "snapshot-rebind")
    receipt = journal.finalize(created_at=epoch_fixtures.NOW)
    assert receipt.normalized_snapshot_id is not None
    original = epoch_module.normalized_v3.load_normalized_snapshot_v3(
        journal.hot_root,
        receipt.normalized_snapshot_id,
    )
    batches = []
    changed = False
    source_batches = epoch_module._iter_epoch_record_batches(
        journal.root,
        receipt.journal_parts,
        trusted_root=journal.hot_root,
    )
    for batch in source_batches:
        rows = batch.to_pylist()
        if not changed:
            rows[0]["event_id"] = f"{rows[0]['event_id']}-forged"
            changed = True
        batches.append(epoch_module.pa.RecordBatch.from_pylist(rows, schema=batch.schema))
    forged = epoch_module.write_normalized_batches(
        journal.hot_root,
        batches,
        provider=journal.provider,
        venue=journal.venue,
        upstream_raw_references=receipt.raw_references,
        policy=journal.policy,
    )
    assert forged.snapshot is not None
    assert forged.snapshot.snapshot_id != original.snapshot_id
    receipt_path = next(journal.root.glob("receipt-sha256-*.json"))
    _rewrite_content_addressed(
        receipt_path,
        "receipt_sha256",
        normalized_snapshot_id=forged.snapshot.snapshot_id,
    )

    with pytest.raises(ValidationError, match="does not match its journal content"):
        NormalizedEpochJournal.reconcile_pending(
            journal.hot_root,
            expected_streams=_identity_anchor(journal),
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )


@pytest.mark.parametrize(
    ("artifact", "changes"),
    (
        ("prepared", {"unexpected_field": True}),
        ("receipt", {"unexpected_field": True}),
        ("abort", {"unexpected_field": True}),
        ("failure", {"unexpected_field": True}),
        ("failure", {"exception": []}),
        ("failure", {"message": {}}),
        ("failure", {"restart_recovery": 0}),
    ),
)
def test_epoch_terminal_json_contracts_are_closed_and_strict(
    tmp_path: Path,
    artifact: str,
    changes: dict[str, object],
) -> None:
    root = tmp_path / f"{artifact}-{next(iter(changes))}"
    if artifact == "receipt":
        journal = _journal_with_lineage(root, "closed-receipt")
        journal.finalize(created_at=epoch_fixtures.NOW)
        path = next(journal.root.glob("receipt-sha256-*.json"))
        _rewrite_content_addressed(path, "receipt_sha256", **changes)
    elif artifact == "abort":
        root.mkdir(parents=True)
        hot, _archive, guard = epoch_fixtures.storage(root)
        journal = NormalizedEpochJournal(
            hot,
            epoch_id="closed-abort",
            stream_id="stream",
            provider="binance",
            venue="BINANCE",
            storage_guard=guard,
            policy=epoch_fixtures.POLICY,
        )
        path = journal.abort_visible("operator")
        _rewrite_content_addressed(path, "abort_sha256", **changes)
    else:
        journal = _failed_epoch_for_recovery(root, f"closed-{artifact}")
        if artifact == "prepared":
            path = next(journal.root.glob("transaction-prepared-*.json"))
            _rewrite_content_addressed(path, "prepared_sha256", **changes)
        else:
            _rewrite_failure(journal, **changes)

    with pytest.raises(ValidationError, match="fields changed|not recoverable"):
        NormalizedEpochJournal.reconcile_pending(
            journal.hot_root,
            expected_streams=_identity_anchor(journal),
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )


def test_epoch_binding_helper_negative_branches_are_explicit(tmp_path: Path) -> None:
    generic_identity = {"value": "bound"}
    generic_hash = hashlib.sha256(epoch_module.canonical_json_bytes(generic_identity)).hexdigest()
    generic = tmp_path / "generic.json"
    generic.write_text(
        json.dumps({**generic_identity, "digest": generic_hash}),
        encoding="utf-8",
    )
    assert (
        epoch_module._load_hashed_json(
            tmp_path,
            generic,
            hash_field="digest",
            description="generic",
        )["digest"]
        == generic_hash
    )

    digest = epoch_module._CanonicalRowsDigest()
    digest.update([])
    digest.update([{"value": 1}])
    digest.update([{"value": 2}])
    assert len(digest.hexdigest()) == 64

    journal = _failed_epoch_for_recovery(tmp_path / "failure", "helper-failure")
    prepared = json.loads(
        next(journal.root.glob("transaction-prepared-*.json")).read_text(encoding="utf-8")
    )
    failure = json.loads(
        next(journal.root.glob("finalize-failure-*.json")).read_text(encoding="utf-8")
    )
    parts = epoch_module._load_epoch_parts(journal.hot_root, journal.root)
    shared = {
        "prepared": prepared,
        "epoch_id": journal.epoch_id,
        "stream_id": journal.stream_id,
        "provider": journal.provider,
        "venue": journal.venue,
        "policy": journal.policy,
        "actual_parts": parts,
        "description": "test terminal",
    }
    with pytest.raises(ValidationError, match="unsupported"):
        epoch_module._validate_terminal_binding(
            journal.hot_root,
            failure,
            schema_version="unsupported",
            **shared,
        )
    with pytest.raises(ValidationError, match="PREPARED hash is malformed"):
        epoch_module._validate_terminal_binding(
            journal.hot_root,
            {**failure, "prepared_sha256": "BAD"},
            schema_version="puresaber.normalized-epoch-finalize-failure@1.2.0",
            **shared,
        )
    with pytest.raises(ValidationError, match="snapshot identity is malformed"):
        epoch_module._validate_terminal_binding(
            journal.hot_root,
            {**failure, "published_snapshot_id": "bad"},
            schema_version="puresaber.normalized-epoch-finalize-failure@1.2.0",
            **shared,
        )
    with pytest.raises(ValidationError, match="snapshot state changed"):
        epoch_module._validate_terminal_binding(
            journal.hot_root,
            {**failure, "published_snapshot_id": "sha256-" + "0" * 64},
            schema_version="puresaber.normalized-epoch-finalize-failure@1.2.0",
            **shared,
        )


def test_epoch_terminal_specific_types_and_required_journal_binding(tmp_path: Path) -> None:
    committed = _journal_with_lineage(tmp_path / "receipt", "helper-receipt")
    committed_receipt = committed.finalize(created_at=epoch_fixtures.NOW)
    receipt = json.loads(Path(committed_receipt.receipt_path).read_text(encoding="utf-8"))
    prepared = json.loads(
        next(committed.root.glob("transaction-prepared-*.json")).read_text(encoding="utf-8")
    )
    parts = epoch_module._load_epoch_parts(committed.hot_root, committed.root)
    with pytest.raises(ValidationError, match="identity changed"):
        epoch_module._validate_terminal_binding(
            committed.hot_root,
            {**receipt, "transaction_state": "BROKEN"},
            prepared=prepared,
            epoch_id=committed.epoch_id,
            stream_id=committed.stream_id,
            provider=committed.provider,
            venue=committed.venue,
            policy=committed.policy,
            actual_parts=parts,
            schema_version="puresaber.normalized-epoch-receipt@1.3.0",
            description="test receipt",
        )
    references = epoch_module._validate_raw_references(
        committed.hot_root,
        receipt["raw_references"],
        description="test receipt",
    )
    with pytest.raises(ValidationError, match="lacks its journal content binding"):
        epoch_module._validate_snapshot_binding(
            committed.hot_root,
            receipt,
            references=references,
            description="test receipt",
        )

    abort_root = tmp_path / "abort"
    abort_root.mkdir(parents=True)
    hot, _archive, guard = epoch_fixtures.storage(abort_root)
    aborted = NormalizedEpochJournal(
        hot,
        epoch_id="helper-abort",
        stream_id="stream",
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=epoch_fixtures.POLICY,
    )
    abort = json.loads(aborted.abort_visible("operator").read_text(encoding="utf-8"))
    abort_parts = epoch_module._load_epoch_parts(aborted.hot_root, aborted.root)
    with pytest.raises(ValidationError, match="identity changed"):
        epoch_module._validate_terminal_binding(
            aborted.hot_root,
            {**abort, "reason": ""},
            prepared=None,
            epoch_id=aborted.epoch_id,
            stream_id=aborted.stream_id,
            provider=aborted.provider,
            venue=aborted.venue,
            policy=aborted.policy,
            actual_parts=abort_parts,
            schema_version="puresaber.normalized-epoch-abort@1.3.0",
            description="test abort",
        )


def test_epoch_reconcile_rejects_missing_anchor_open_part_and_orphan_journal(
    tmp_path: Path,
) -> None:
    missing_anchor = _failed_epoch_for_recovery(tmp_path / "anchor", "missing-anchor")
    with pytest.raises(ValidationError, match="independent identity anchor"):
        NormalizedEpochJournal.reconcile_pending(
            missing_anchor.hot_root,
            expected_streams={},
            storage_guard=missing_anchor.storage_guard,
            policy=missing_anchor.policy,
        )

    opened = _failed_epoch_for_recovery(tmp_path / "open", "open-part")
    opened._open_path.write_bytes(b'{"partial":true}\n')
    with pytest.raises(ValidationError, match="unsealed open part"):
        NormalizedEpochJournal.reconcile_pending(
            opened.hot_root,
            expected_streams=_identity_anchor(opened),
            storage_guard=opened.storage_guard,
            policy=opened.policy,
        )

    root = tmp_path / "orphan"
    root.mkdir()
    hot, _archive, guard = epoch_fixtures.storage(root)
    orphan = NormalizedEpochJournal(
        hot,
        epoch_id="orphan",
        stream_id="stream",
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=epoch_fixtures.POLICY,
    )
    orphan._open_stream.close()
    with pytest.raises(ValidationError, match="no durable transaction state"):
        NormalizedEpochJournal.reconcile_pending(
            hot,
            expected_streams=_identity_anchor(orphan),
            storage_guard=guard,
            policy=orphan.policy,
        )


@pytest.mark.parametrize(
    ("case", "changes"),
    (
        ("schema", {"schema_version": "puresaber.normalized-epoch-transaction@9.9.9"}),
        ("state", {"transaction_state": "BROKEN"}),
        ("epoch", {"epoch_id": "different"}),
        ("stream", {"stream_id": "different"}),
        ("provider", {"provider": "okx"}),
        ("venue", {"venue": "OKX"}),
        ("policy", {"policy": {}}),
        ("attempt", {"attempt": 0}),
        ("attempt-type", {"attempt": "1"}),
    ),
)
def test_epoch_recovery_rejects_each_prepared_identity_field(
    tmp_path: Path, case: str, changes: dict[str, object]
) -> None:
    journal = _failed_epoch_for_recovery(tmp_path / case, case)
    prepared = next(journal.root.glob("transaction-prepared-*.json"))
    _rewrite_content_addressed(prepared, "prepared_sha256", **changes)
    with pytest.raises(ValidationError, match="transaction identity"):
        NormalizedEpochJournal.recover(
            journal.hot_root,
            epoch_id=journal.epoch_id,
            stream_id=journal.stream_id,
            provider=journal.provider,
            venue=journal.venue,
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )
    with pytest.raises(ValidationError, match="transaction identity"):
        NormalizedEpochJournal.reconcile_pending(
            journal.hot_root,
            expected_streams=_identity_anchor(journal),
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )


@pytest.mark.parametrize(
    ("case", "changes", "message"),
    (
        (
            "schema",
            {"schema_version": "puresaber.normalized-epoch-receipt@9.9.9"},
            "receipt identity",
        ),
        ("state", {"transaction_state": "BROKEN"}, "receipt identity"),
        ("epoch", {"epoch_id": "different"}, "receipt identity"),
        ("stream", {"stream_id": "different"}, "receipt identity"),
        ("prepared", {"prepared_sha256": "0" * 64}, "receipt identity"),
        ("provider", {"provider": "okx"}, "receipt identity"),
        ("venue", {"venue": "OKX"}, "receipt identity"),
        ("policy", {"policy": {}}, "receipt identity"),
        ("records", {"records": 999}, "record count changed"),
        ("raw-lineage", {"raw_references": []}, "Raw lineage changed"),
        ("journal-parts", {"journal_parts": []}, "part manifest changed"),
        ("raw-count", {"raw_segments": 0}, "Raw count changed"),
        ("raw-count-type", {"raw_segments": True}, "Raw count changed"),
        ("row-counts", {"accepted_rows": 0}, "row counts changed"),
        ("snapshot", {"normalized_snapshot_id": None}, "snapshot identity is missing"),
    ),
)
def test_epoch_recovery_rejects_each_committed_identity_field(
    tmp_path: Path, case: str, changes: dict[str, object], message: str
) -> None:
    journal = _journal_with_lineage(tmp_path / case, f"receipt-{case}")
    journal.finalize(created_at=epoch_fixtures.NOW)
    receipt = next(journal.root.glob("receipt-sha256-*.json"))
    _rewrite_content_addressed(receipt, "receipt_sha256", **changes)
    with pytest.raises(ValidationError, match=message):
        NormalizedEpochJournal.recover(
            journal.hot_root,
            epoch_id=journal.epoch_id,
            stream_id=journal.stream_id,
            provider=journal.provider,
            venue=journal.venue,
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )
    with pytest.raises(ValidationError, match=message):
        NormalizedEpochJournal.reconcile_pending(
            journal.hot_root,
            expected_streams=_identity_anchor(journal),
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )


@pytest.mark.parametrize(
    ("case", "changes", "message"),
    (
        (
            "schema",
            {"schema_version": "puresaber.normalized-epoch-abort@9.9.9"},
            "ABORTED transaction identity",
        ),
        ("state", {"transaction_state": "BROKEN"}, "ABORTED identity"),
        ("epoch", {"epoch_id": "different"}, "ABORTED identity"),
        ("stream", {"stream_id": "different"}, "ABORTED identity"),
        ("prepared", {"prepared_sha256": "0" * 64}, "ABORTED identity"),
        ("provider", {"provider": "okx"}, "ABORTED transaction identity"),
        ("venue", {"venue": "OKX"}, "ABORTED transaction identity"),
        ("policy", {"policy": {}}, "ABORTED transaction identity"),
        ("records", {"records": 1}, "record count changed"),
        ("journal-parts", {"journal_parts": [{}]}, "part manifest changed"),
        ("created-at", {"created_at": "not-a-time"}, "created_at is malformed"),
        ("retryable", {"retryable": True}, "ABORTED identity"),
    ),
)
def test_epoch_recovery_rejects_each_explicit_abort_identity_field(
    tmp_path: Path, case: str, changes: dict[str, object], message: str
) -> None:
    root = tmp_path / case
    root.mkdir()
    hot, _archive, guard = epoch_fixtures.storage(root)
    journal = NormalizedEpochJournal(
        hot,
        epoch_id=f"abort-{case}",
        stream_id="stream",
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=epoch_fixtures.POLICY,
    )
    abort = journal.abort_visible("operator")
    _rewrite_content_addressed(abort, "abort_sha256", **changes)
    with pytest.raises(ValidationError, match=message):
        NormalizedEpochJournal.recover(
            hot,
            epoch_id=journal.epoch_id,
            stream_id=journal.stream_id,
            provider=journal.provider,
            venue=journal.venue,
            storage_guard=guard,
            policy=journal.policy,
        )
    with pytest.raises(ValidationError, match=message):
        NormalizedEpochJournal.reconcile_pending(
            hot,
            expected_streams=_identity_anchor(journal),
            storage_guard=guard,
            policy=journal.policy,
        )


@pytest.mark.parametrize(
    ("case", "changes", "message"),
    (
        (
            "schema",
            {"schema_version": "puresaber.normalized-epoch-finalize-failure@9.9.9"},
            "failure record identity changed",
        ),
        ("attempt", {"attempt": 0}, "not recoverable"),
        ("attempt-type", {"attempt": "1"}, "not recoverable"),
        ("retryable-type", {"retryable_in_process": 1}, "not recoverable"),
        ("record-type", {"records": True}, "record count changed"),
        ("snapshot-state", {"accepted_rows": 0}, "snapshot state changed"),
    ),
)
def test_epoch_recovery_rejects_each_failure_schema_and_type_field(
    tmp_path: Path, case: str, changes: dict[str, object], message: str
) -> None:
    journal = _failed_epoch_for_recovery(tmp_path / case, f"failure-{case}")
    _rewrite_failure(journal, **changes)
    with pytest.raises(ValidationError, match=message):
        NormalizedEpochJournal.recover(
            journal.hot_root,
            epoch_id=journal.epoch_id,
            stream_id=journal.stream_id,
            provider=journal.provider,
            venue=journal.venue,
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )
    with pytest.raises(ValidationError, match=message):
        NormalizedEpochJournal.reconcile_pending(
            journal.hot_root,
            expected_streams=_identity_anchor(journal),
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )


def test_epoch_recovery_rejects_failure_lineage_reference_and_filename(
    tmp_path: Path,
) -> None:
    retryable = _failed_epoch_for_recovery(tmp_path / "retryable", "retryable")
    _rewrite_failure(retryable, retryable_in_process=False)
    with pytest.raises(ValidationError, match="not recoverable"):
        NormalizedEpochJournal.recover(
            retryable.hot_root,
            epoch_id=retryable.epoch_id,
            stream_id=retryable.stream_id,
            provider=retryable.provider,
            venue=retryable.venue,
            storage_guard=retryable.storage_guard,
            policy=retryable.policy,
        )

    lineage = _failed_epoch_for_recovery(tmp_path / "lineage", "lineage")
    _rewrite_failure(lineage, raw_references=[])
    with pytest.raises(ValidationError, match="Raw lineage changed"):
        NormalizedEpochJournal.recover(
            lineage.hot_root,
            epoch_id=lineage.epoch_id,
            stream_id=lineage.stream_id,
            provider=lineage.provider,
            venue=lineage.venue,
            storage_guard=lineage.storage_guard,
            policy=lineage.policy,
        )

    unknown = _failed_epoch_for_recovery(tmp_path / "unknown", "unknown")
    _rewrite_failure(unknown, prepared_sha256="0" * 64)
    with pytest.raises(ValidationError, match="transaction identity changed"):
        NormalizedEpochJournal.recover(
            unknown.hot_root,
            epoch_id=unknown.epoch_id,
            stream_id=unknown.stream_id,
            provider=unknown.provider,
            venue=unknown.venue,
            storage_guard=unknown.storage_guard,
            policy=unknown.policy,
        )

    filename = _failed_epoch_for_recovery(tmp_path / "filename", "filename")
    failure = next(filename.root.glob("finalize-failure-*.json"))
    payload = json.loads(failure.read_text(encoding="utf-8"))
    payload.pop("failure_sha256")
    payload["message"] = "changed"
    payload["failure_sha256"] = hashlib.sha256(
        epoch_module.canonical_json_bytes(
            {key: value for key, value in payload.items() if key != "failure_sha256"}
        )
    ).hexdigest()
    failure.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="filename hash changed"):
        NormalizedEpochJournal.recover(
            filename.hot_root,
            epoch_id=filename.epoch_id,
            stream_id=filename.stream_id,
            provider=filename.provider,
            venue=filename.venue,
            storage_guard=filename.storage_guard,
            policy=filename.policy,
        )


def test_epoch_legacy_failure_and_pending_manifest_negative_paths(tmp_path: Path) -> None:
    legacy = _failed_epoch_for_recovery(tmp_path / "legacy", "legacy")
    _rewrite_failure(legacy, prepared_sha256=None)
    for path in legacy.root.glob("transaction-prepared-*.json"):
        path.unlink()
    with pytest.raises(ValidationError, match="no durable PREPARED"):
        NormalizedEpochJournal.recover(
            legacy.hot_root,
            epoch_id=legacy.epoch_id,
            stream_id=legacy.stream_id,
            provider=legacy.provider,
            venue=legacy.venue,
            storage_guard=legacy.storage_guard,
            policy=legacy.policy,
        )
    with pytest.raises(ValidationError, match="no durable PREPARED"):
        NormalizedEpochJournal.reconcile_pending(
            legacy.hot_root,
            expected_streams=_identity_anchor(legacy),
            storage_guard=legacy.storage_guard,
            policy=legacy.policy,
        )

    records = _journal_with_lineage(tmp_path / "records", "records")
    monkey = records._write_committed_receipt
    records._write_committed_receipt = (  # type: ignore[method-assign]
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SystemExit("pending"))
    )
    with pytest.raises(SystemExit):
        records.finalize(created_at=epoch_fixtures.NOW)
    records._write_committed_receipt = monkey  # type: ignore[method-assign]
    records._open_stream.close()
    prepared = next(records.root.glob("transaction-prepared-*.json"))
    _rewrite_content_addressed(prepared, "prepared_sha256", records=records.records + 1)
    with pytest.raises(ValidationError, match="record count changed"):
        NormalizedEpochJournal.recover(
            records.hot_root,
            epoch_id=records.epoch_id,
            stream_id=records.stream_id,
            provider=records.provider,
            venue=records.venue,
            storage_guard=records.storage_guard,
            policy=records.policy,
        )

    multiple = _journal_with_lineage(tmp_path / "multiple", "multiple")
    original_publish = multiple._publish_normalized
    multiple._publish_normalized = (  # type: ignore[method-assign]
        lambda: (_ for _ in ()).throw(OSError("first"))
    )
    with pytest.raises(OSError):
        multiple.finalize(created_at=epoch_fixtures.NOW)
    multiple._publish_normalized = original_publish  # type: ignore[method-assign]
    multiple._write_committed_receipt = (  # type: ignore[method-assign]
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SystemExit("second"))
    )
    with pytest.raises(SystemExit):
        multiple.finalize(created_at=epoch_fixtures.NOW)
    multiple._open_stream.close()
    _rewrite_failure(multiple, prepared_sha256=None)
    with pytest.raises(ValidationError, match="multiple pending"):
        NormalizedEpochJournal.recover(
            multiple.hot_root,
            epoch_id=multiple.epoch_id,
            stream_id=multiple.stream_id,
            provider=multiple.provider,
            venue=multiple.venue,
            storage_guard=multiple.storage_guard,
            policy=multiple.policy,
        )


def test_epoch_recovery_rejects_identity_part_count_open_part_and_bad_flush(
    tmp_path: Path,
) -> None:
    invalid_root = tmp_path / "invalid"
    invalid_root.mkdir()
    hot, _archive, guard = epoch_fixtures.storage(invalid_root)
    with pytest.raises(ValidationError, match="flush limits"):
        NormalizedEpochJournal(
            hot,
            epoch_id="bad-flush",
            stream_id="stream",
            provider="binance",
            venue="BINANCE",
            storage_guard=guard,
            flush_records=0,
        )

    identity = _failed_epoch_for_recovery(tmp_path / "identity", "identity")
    _rewrite_failure(identity, stream_id="different")
    with pytest.raises(ValidationError, match="not recoverable"):
        NormalizedEpochJournal.recover(
            identity.hot_root,
            epoch_id=identity.epoch_id,
            stream_id=identity.stream_id,
            provider=identity.provider,
            venue=identity.venue,
            storage_guard=identity.storage_guard,
            policy=identity.policy,
        )

    part = _failed_epoch_for_recovery(tmp_path / "part", "part")
    part_path = next(part.root.glob("part-*.ndjson"))
    part_path.write_bytes(part_path.read_bytes() + b" ")
    with pytest.raises(ValidationError, match="part hash changed"):
        NormalizedEpochJournal.recover(
            part.hot_root,
            epoch_id=part.epoch_id,
            stream_id=part.stream_id,
            provider=part.provider,
            venue=part.venue,
            storage_guard=part.storage_guard,
            policy=part.policy,
        )

    count = _failed_epoch_for_recovery(tmp_path / "count", "count")
    _rewrite_failure(count, records=count.records + 1)
    with pytest.raises(ValidationError, match="record count changed"):
        NormalizedEpochJournal.recover(
            count.hot_root,
            epoch_id=count.epoch_id,
            stream_id=count.stream_id,
            provider=count.provider,
            venue=count.venue,
            storage_guard=count.storage_guard,
            policy=count.policy,
        )

    opened = _failed_epoch_for_recovery(tmp_path / "open", "open")
    opened._open_path.write_bytes(b'{"partial":true}\n')
    with pytest.raises(ValidationError, match="unsealed open part"):
        NormalizedEpochJournal.recover(
            opened.hot_root,
            epoch_id=opened.epoch_id,
            stream_id=opened.stream_id,
            provider=opened.provider,
            venue=opened.venue,
            storage_guard=opened.storage_guard,
            policy=opened.policy,
        )


def test_epoch_receipt_reload_mismatch_is_audited_and_retryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal_with_lineage(tmp_path, "reload-mismatch")
    original = epoch_module._atomic_immutable_write

    def corrupt_receipt(path: Path, body: bytes, *, root: Path | None = None) -> None:
        original(path, body, root=root)
        if path.name.startswith("receipt-"):
            path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(epoch_module, "_atomic_immutable_write", corrupt_receipt)
    with pytest.raises(ValidationError, match="receipt hash changed"):
        journal.finalize(created_at=epoch_fixtures.NOW)
    assert tuple(journal.root.glob("finalize-failure-*.json"))
    assert journal._state == "OPEN"
    with pytest.raises(ValidationError, match="receipt hash changed"):
        NormalizedEpochJournal.reconcile_pending(
            journal.hot_root,
            expected_streams=_identity_anchor(journal),
            storage_guard=journal.storage_guard,
            policy=journal.policy,
        )


def test_epoch_abort_publish_failure_keeps_retryable_open_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "abort"
    root.mkdir()
    hot, _archive, guard = epoch_fixtures.storage(root)
    journal = NormalizedEpochJournal(
        hot,
        epoch_id="abort-publish",
        stream_id="stream",
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=epoch_fixtures.POLICY,
    )
    original = epoch_module._atomic_immutable_write
    monkeypatch.setattr(
        epoch_module,
        "_atomic_immutable_write",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("abort publish")),
    )
    with pytest.raises(OSError, match="abort publish"):
        journal.abort_visible("cancelled")
    assert journal._state == "OPEN"
    monkeypatch.setattr(epoch_module, "_atomic_immutable_write", original)
    assert journal.abort_visible("cancelled").is_file()
    assert journal._state == "ABORTED"


def test_runner_prepaused_retry_success_and_binance_multiframe_bridge(tmp_path: Path) -> None:
    prepaused = _runner_for_negative_test(tmp_path / "prepaused")
    prepaused._pause("operator")
    report = asyncio.run(prepaused.run(maximum_websocket_messages=3))
    assert report.final_state == CaptureState.PAUSED.value
    assert report.websocket_messages == 0

    retry = _runner_for_negative_test(tmp_path / "retry")
    successful_connection = collector_fixtures.FakeConnection(retry.stream)

    class FailThenConnect:
        def __init__(self) -> None:
            self.calls = 0

        async def connect(self, _url: str, *, timeout_seconds: float):
            self.calls += 1
            if self.calls == 1:
                raise ProviderError("first connect failed")
            return successful_connection

    retry.config = replace(
        retry.config,
        retry=collector_fixtures.RetryPolicy(
            max_attempts=2,
            base_delay_seconds=0.01,
            maximum_delay_seconds=0.01,
            jitter_fraction=0,
        ),
    )
    retry.websockets = FailThenConnect()
    retried = asyncio.run(retry.run(maximum_websocket_messages=3))
    assert retried.outcome == "BOUNDED_COMPLETE"
    assert retried.websocket_messages == 3
    assert any("first connect failed" in item for item in retried.errors)

    resync = _runner_for_negative_test(tmp_path / "resync")
    resync.config = replace(
        resync.config,
        retry=collector_fixtures.RetryPolicy(
            max_attempts=2,
            base_delay_seconds=0.01,
            maximum_delay_seconds=0.01,
            jitter_fraction=0,
        ),
    )
    malformed = collector_fixtures.FakeConnection(resync.stream)
    malformed.messages = [b"{"]
    recovered_connection = collector_fixtures.FakeConnection(resync.stream)

    class MalformedThenRecover:
        def __init__(self) -> None:
            self.connections = [malformed, recovered_connection]

        async def connect(self, _url: str, *, timeout_seconds: float):
            return self.connections.pop(0)

    resync.websockets = MalformedThenRecover()
    resynced = asyncio.run(resync.run(maximum_websocket_messages=3))
    assert resynced.outcome == "BOUNDED_COMPLETE"
    assert resynced.raw_segments == resynced.archived_segments >= 2
    assert any("valid JSON" in item for item in resynced.errors)

    bridge = _runner_for_negative_test(tmp_path / "bridge")
    messages = collector_fixtures.messages_for(bridge.stream)
    first = json.loads(messages[0])
    first["U"], first["u"] = 90, 100
    second = json.loads(messages[1])
    second["U"], second["u"] = 99, 101
    third = json.loads(messages[2])
    third["U"], third["u"] = 102, 102

    class BridgeConnection(collector_fixtures.FakeConnection):
        def __init__(self) -> None:
            super().__init__(bridge.stream)
            self.messages = [json.dumps(item).encode() for item in (first, second, third)]

    connection = BridgeConnection()

    class BridgeConnector:
        async def connect(self, _url: str, *, timeout_seconds: float):
            return connection

    bridge.websockets = BridgeConnector()
    bridged = asyncio.run(bridge.run(maximum_websocket_messages=3))
    assert bridged.outcome == "BOUNDED_COMPLETE"
    assert bridged.websocket_messages == 3


def test_cancellation_during_retry_backoff_still_persists_terminal_report(
    tmp_path: Path,
) -> None:
    runner = _runner_for_negative_test(tmp_path)
    runner.config = replace(
        runner.config,
        retry=collector_fixtures.RetryPolicy(
            max_attempts=2,
            base_delay_seconds=1,
            maximum_delay_seconds=1,
            jitter_fraction=0,
        ),
    )
    runner.websockets = collector_fixtures.FailedConnector()

    async def exercise() -> None:
        sleeping = asyncio.Event()

        class BackoffClock:
            def now(self):
                return runner.state.clock.now()

            async def sleep(self, _seconds: float) -> None:
                sleeping.set()
                await asyncio.Event().wait()

        runner.clock = BackoffClock()
        task = asyncio.create_task(runner.run(maximum_websocket_messages=3))
        await asyncio.wait_for(sleeping.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(exercise())
    report = runner._last_report
    assert report is not None and report.outcome == "CANCELLED"
    assert report.final_state == CaptureState.PAUSED.value
    assert report.pending_raw_messages == report.pending_raw_segments == 0
