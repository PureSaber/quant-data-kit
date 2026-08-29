from __future__ import annotations

import multiprocessing
import os
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Empty
from typing import Any

import pytest

import quant_data_kit.adapters_v2.base as adapter_base
import quant_data_kit.curated as curated_module
import quant_data_kit.data_lake as lake_module
from quant_data_kit.adapters_v2 import adapt_fixture_messages
from quant_data_kit.curated import curate_trade_bars_from_snapshot
from quant_data_kit.data_lake import (
    ArchiveReceipt,
    QuarantineEntry,
    RawObjectReference,
    StoragePolicy,
    cleanup_archived_raw_object,
    load_normalized_snapshot,
    load_raw_object,
    validate_raw_reference,
    write_normalized_events,
    write_raw_bytes,
)
from quant_data_kit.exceptions import ValidationError
from tests.test_adapters_v2 import binance_adapter, cn_adapter, load_messages, okx_adapter
from tests.test_m2_audit_regressions import trade

TEST_POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)
WORKER_ERRORS = (ValidationError, OSError, ValueError, TypeError, TimeoutError)


def _raw_process(
    root: str,
    payload: bytes,
    start: Any,
    results: Any,
    *,
    crash_before_publish: bool = False,
) -> None:
    try:
        if crash_before_publish:
            real_replace = lake_module.os.replace

            def crash(source: Path, destination: Path) -> None:
                if Path(source).parent.name == ".staging" and Path(destination).name.startswith(
                    "object="
                ):
                    os._exit(73)
                real_replace(source, destination)

            lake_module.os.replace = crash
        start.wait(20)
        manifest = write_raw_bytes(
            Path(root),
            source="binance",
            request={"fixture": "concurrent"},
            collected_at="2026-01-02T00:00:00Z",
            payload=payload,
            idempotency_key="shared-key",
            policy=TEST_POLICY,
        )
        results.put(("ok", manifest.object_id))
    except WORKER_ERRORS as exc:
        results.put(("error", f"{type(exc).__name__}: {exc}"))


def _cleanup_process(
    root: str,
    reference: dict[str, str],
    receipt: dict[str, str],
    start: Any,
    results: Any,
) -> None:
    try:
        start.wait(20)
        tombstone = cleanup_archived_raw_object(
            Path(root),
            RawObjectReference(**reference),
            ArchiveReceipt(**receipt),
            confirm=True,
            now="2026-02-02T00:00:01Z",
        )
        results.put(("ok", str(tombstone)))
    except WORKER_ERRORS as exc:
        results.put(("error", f"{type(exc).__name__}: {exc}"))


def _quarantine_process(root: str, start: Any, results: Any) -> None:
    try:
        start.wait(20)
        path = lake_module._write_quarantine(
            Path(root),
            "binance",
            "BINANCE",
            [QuarantineEntry(0, "injected", {"event_id": "bad"})],
            policy=TEST_POLICY,
        )
        results.put(("ok", str(path)))
    except WORKER_ERRORS as exc:
        results.put(("error", f"{type(exc).__name__}: {exc}"))


def _normalized_process(
    root: str,
    raw_reference: dict[str, str],
    record: dict[str, Any],
    start: Any,
    results: Any,
) -> None:
    try:
        start.wait(20)
        result = write_normalized_events(
            Path(root),
            [record],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[RawObjectReference(**raw_reference)],
            policy=TEST_POLICY,
        )
        results.put(("ok", result.snapshot.snapshot_id if result.snapshot else "none"))
    except WORKER_ERRORS as exc:
        results.put(("error", f"{type(exc).__name__}: {exc}"))


def _atomic_hard_exit_process(root: str, target_name: str, body: bytes) -> None:
    lake_root = lake_module._resolved_lake_root(Path(root), create=True)
    target = lake_root / target_name
    real_replace = lake_module.os.replace

    def crash_before_replace(source: Path, destination: Path) -> None:
        if Path(destination) == target:
            os._exit(71)
        real_replace(source, destination)

    lake_module.os.replace = crash_before_replace
    with lake_module._lake_lock(lake_root, "atomic-test", {"target": target_name}):
        lake_module._atomic_write_bytes(lake_root, target, body)


def _normalized_hard_exit_process(
    root: str,
    raw_reference: dict[str, str],
    record: dict[str, Any],
) -> None:
    real_replace = lake_module.os.replace

    def crash_before_publish(source: Path, destination: Path) -> None:
        if Path(destination).parent.name == "snapshots":
            os._exit(72)
        real_replace(source, destination)

    lake_module.os.replace = crash_before_publish
    write_normalized_events(
        Path(root),
        [record],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[RawObjectReference(**raw_reference)],
        policy=TEST_POLICY,
    )


def _curated_process(
    root: str,
    normalized_snapshot_id: str,
    start: Any,
    results: Any,
) -> None:
    try:
        start.wait(20)
        snapshot = curate_trade_bars_from_snapshot(
            Path(root),
            normalized_snapshot_id=normalized_snapshot_id,
            dataset="concurrent-bars",
            revision_id="revision-1",
            recipe_version="session-bars-v1",
            interval=timedelta(minutes=1),
            session_starts={
                "binance-24x7-BTC-USDT-SPOT": datetime(2026, 1, 2, tzinfo=timezone.utc)
            },
            policy=TEST_POLICY,
        )
        results.put(("ok", snapshot.snapshot_id))
    except WORKER_ERRORS as exc:
        results.put(("error", f"{type(exc).__name__}: {exc}"))


def _curated_hard_exit_process(root: str, normalized_snapshot_id: str) -> None:
    real_replace = curated_module.os.replace

    def crash_before_publish(source: Path, destination: Path) -> None:
        if Path(destination).parent.name == "snapshots":
            os._exit(73)
        real_replace(source, destination)

    curated_module.os.replace = crash_before_publish
    curate_trade_bars_from_snapshot(
        Path(root),
        normalized_snapshot_id=normalized_snapshot_id,
        dataset="crash-bars",
        revision_id="revision-1",
        recipe_version="session-bars-v1",
        interval=timedelta(minutes=1),
        session_starts={"binance-24x7-BTC-USDT-SPOT": datetime(2026, 1, 2, tzinfo=timezone.utc)},
        policy=TEST_POLICY,
    )


def _hold_staging_process(
    root: str,
    staging_relative: str,
    namespace: str,
    identity: dict[str, str],
    ready: Any,
    release: Any,
    result: Any,
) -> None:
    lake_root = lake_module._resolved_lake_root(Path(root), create=True)
    with lake_module._stable_staging_directory(
        lake_root,
        lake_root / staging_relative,
        namespace=namespace,
        identity=identity,
    ) as stage:
        (stage / "active.marker").write_text("active", encoding="utf-8")
        result.put(str(stage))
        ready.set()
        release.wait(20)


def _run_pair(target: Any, args: list[tuple[Any, ...]]) -> tuple[list[tuple[str, str]], list[int]]:
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    manager = context.Manager()
    results = manager.Queue()
    processes = [context.Process(target=target, args=(*item, start, results)) for item in args]
    try:
        for process in processes:
            process.start()
        start.set()
        for process in processes:
            process.join(30)
            if process.is_alive():
                process.terminate()
                process.join(5)
                raise AssertionError("child process did not terminate")
        received: list[tuple[str, str]] = []
        for _ in processes:
            try:
                received.append(results.get(timeout=5))
            except Empty:
                break
        return received, [int(process.exitcode or 0) for process in processes]
    finally:
        manager.shutdown()


def _raw(root: Path, *, key: str, payload: bytes):
    return write_raw_bytes(
        root,
        source="binance",
        request={"fixture": key},
        collected_at="2026-01-02T00:00:00Z",
        payload=payload,
        idempotency_key=key,
        policy=TEST_POLICY,
    )


def _normalized(root: Path, *, key: str, record: dict[str, Any]):
    admitted = _raw(root, key=key, payload=key.encode())
    result = write_normalized_events(
        root,
        [record],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    return result.snapshot


@pytest.mark.parametrize("same_payload", [True, False])
def test_raw_key_lock_is_process_safe(tmp_path: Path, same_payload: bool) -> None:
    root = tmp_path / "lake"
    second = b"same" if same_payload else b"different"
    results, exit_codes = _run_pair(
        _raw_process,
        [(str(root), b"same"), (str(root), second)],
    )
    assert exit_codes == [0, 0]
    successes = [value for status, value in results if status == "ok"]
    errors = [value for status, value in results if status == "error"]
    if same_payload:
        assert len(successes) == 2 and len(set(successes)) == 1, results
        assert errors == []
    else:
        assert len(successes) == 1
        assert len(errors) == 1 and "Conflicting immutable Raw idempotency key" in errors[0]
    object_dirs = [path for path in root.rglob("object=*") if path.is_dir()]
    assert len(object_dirs) == 1


def test_raw_key_claim_survives_corruption_and_hard_exit_staging(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    first = _raw(root, key="bound", payload=b"original")
    object_dir = next(root.rglob(f"object={first.object_id}"))
    (object_dir / "payload.bin").write_bytes(b"corrupt!")
    with pytest.raises(ValidationError, match="Conflicting immutable Raw idempotency key"):
        _raw(root, key="bound", payload=b"different")
    recovered = _raw(root, key="bound", payload=b"original")
    assert recovered == first

    context = multiprocessing.get_context("spawn")
    start = context.Event()
    results = context.Queue()
    process = context.Process(
        target=_raw_process,
        args=(str(root), b"crash", start, results),
        kwargs={"crash_before_publish": True},
    )
    process.start()
    start.set()
    process.join(30)
    assert process.exitcode == 73
    assert list((root / "raw" / ".staging").glob("raw-*-*"))
    resumed = write_raw_bytes(
        root,
        source="binance",
        request={"fixture": "concurrent"},
        collected_at="2026-01-02T00:00:00Z",
        payload=b"crash",
        idempotency_key="shared-key",
        policy=TEST_POLICY,
    )
    assert load_raw_object(root, resumed.reference())[1] == b"crash"
    assert not list((root / "raw" / ".staging").glob("raw-*-*"))


def test_raw_key_claim_is_source_global_across_collection_dates(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    first = write_raw_bytes(
        root,
        source="binance",
        request={"fixture": "global-key"},
        collected_at="2026-01-02T00:00:00Z",
        payload=b"original",
        idempotency_key="global-key",
        policy=TEST_POLICY,
    )
    with pytest.raises(ValidationError, match="Conflicting immutable Raw idempotency key"):
        write_raw_bytes(
            root,
            source="binance",
            request={"fixture": "global-key"},
            collected_at="2026-01-03T00:00:00Z",
            payload=b"different",
            idempotency_key="global-key",
            policy=TEST_POLICY,
        )
    assert load_raw_object(root, first.reference())[1] == b"original"


@pytest.mark.parametrize("failed_name", ["payload.bin", "manifest.json"])
def test_cleanup_failure_is_visible_and_retry_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_name: str,
) -> None:
    root = tmp_path / "lake"
    manifest = _raw(root, key=f"cleanup-{failed_name}", payload=b"archive-me")
    archive = (tmp_path / f"{failed_name}.archive").resolve()
    archive.write_bytes(b"archive-me")
    receipt = ArchiveReceipt(
        object_id=manifest.object_id,
        archive_uri=str(archive),
        source_sha256=manifest.content_sha256,
        archive_sha256=manifest.content_sha256,
        restored_sha256=manifest.content_sha256,
        verified_at="2026-02-02T00:00:00Z",
    )
    real_unlink = Path.unlink
    injected = False

    def fail_once(path: Path, missing_ok: bool = False) -> None:
        nonlocal injected
        if path.name == failed_name and path.parent.name.startswith("deleting=") and not injected:
            injected = True
            raise OSError(f"injected {failed_name} unlink failure")
        real_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_once)
    with pytest.raises(OSError, match="injected"):
        cleanup_archived_raw_object(
            root,
            manifest.reference(),
            receipt,
            confirm=True,
            now="2026-02-02T00:00:01Z",
        )
    assert injected
    assert validate_raw_reference(root, manifest.reference(), allow_archived=True) == manifest
    if failed_name == "payload.bin":
        deleting_dir = next(root.rglob(f"deleting={manifest.object_id}"))
        real_unlink(deleting_dir / "manifest.json")
        legacy_live_dir = deleting_dir.with_name(f"object={manifest.object_id}")
        deleting_dir.rename(legacy_live_dir)
    monkeypatch.setattr(Path, "unlink", real_unlink)
    tombstone = cleanup_archived_raw_object(
        root,
        manifest.reference(),
        receipt,
        confirm=True,
        now="2026-02-02T00:00:01Z",
    )
    assert tombstone.is_file()
    assert not list(root.rglob("deleting=*"))


def test_cleanup_same_key_is_process_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    manifest = _raw(root, key="cleanup-process", payload=b"archive-me")
    archive = (tmp_path / "process.archive").resolve()
    archive.write_bytes(b"archive-me")
    receipt = ArchiveReceipt(
        object_id=manifest.object_id,
        archive_uri=str(archive),
        source_sha256=manifest.content_sha256,
        archive_sha256=manifest.content_sha256,
        restored_sha256=manifest.content_sha256,
        verified_at="2026-02-02T00:00:00Z",
    )
    results, exit_codes = _run_pair(
        _cleanup_process,
        [
            (str(root), asdict(manifest.reference()), asdict(receipt)),
            (str(root), asdict(manifest.reference()), asdict(receipt)),
        ],
    )
    assert exit_codes == [0, 0]
    assert [status for status, _ in results] == ["ok", "ok"], results
    assert len({value for _, value in results}) == 1


def test_quarantine_failure_and_process_concurrency_are_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    entries = [QuarantineEntry(0, "injected", {"event_id": "bad"})]
    real_open = Path.open
    injected = False

    def fail_manifest(path: Path, *args: Any, **kwargs: Any):
        nonlocal injected
        if path.name == "manifest.json" and path.parent.parent.name == ".staging" and not injected:
            injected = True
            raise OSError("injected quarantine manifest failure")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_manifest)
    with pytest.raises(OSError, match="quarantine manifest"):
        lake_module._write_quarantine(
            root,
            "binance",
            "BINANCE",
            entries,
            policy=TEST_POLICY,
        )
    monkeypatch.setattr(Path, "open", real_open)
    recovered = lake_module._write_quarantine(
        root,
        "binance",
        "BINANCE",
        entries,
        policy=TEST_POLICY,
    )
    assert recovered.is_file()

    concurrent_root = tmp_path / "concurrent-quarantine"
    results, exit_codes = _run_pair(
        _quarantine_process,
        [(str(concurrent_root),), (str(concurrent_root),)],
    )
    assert exit_codes == [0, 0]
    assert [status for status, _ in results] == ["ok", "ok"], results
    assert len({value for _, value in results}) == 1


@pytest.mark.parametrize("provider", ["binance", "okx", "cn_neutral"])
def test_adapter_batch_transaction_rolls_back_final_validation_failure(
    provider: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factories = {
        "binance": binance_adapter,
        "okx": okx_adapter,
        "cn_neutral": cn_adapter,
    }
    adapter = factories[provider]()
    messages = load_messages(provider)
    broken = deepcopy(messages)
    if provider in {"binance", "okx"}:
        broken.append(deepcopy(messages[0]))
    else:
        real_validate = adapter_base.validate_event_stream

        def fail_final_validation(records: Any) -> None:
            list(records)
            raise ValidationError("injected final stream validation failure")

        monkeypatch.setattr(adapter_base, "validate_event_stream", fail_final_validation)
    with pytest.raises(ValidationError):
        adapt_fixture_messages(adapter, broken)
    if provider == "cn_neutral":
        monkeypatch.setattr(adapter_base, "validate_event_stream", real_validate)
    assert adapt_fixture_messages(adapter, messages)


def test_event_claim_reuse_conflict_tamper_and_publish_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "lake"
    record = trade("lake-global-id")
    first = _normalized(root, key="claim-1", record=record)
    second = _normalized(root, key="claim-2", record=deepcopy(record))
    assert first.snapshot_id != second.snapshot_id
    claim_files = list((root / "normalized" / "event-claim-index-v3").rglob("*.parquet"))
    assert len(claim_files) == 2
    assert not list((root / "normalized" / "event-claims").rglob("*.json"))

    changed = deepcopy(record)
    changed["price"]["units"] += 1
    admitted = _raw(root, key="claim-3", payload=b"claim-3")
    with pytest.raises(ValidationError, match="Conflicting lake event_id claim"):
        write_normalized_events(
            root,
            [changed],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[admitted.reference()],
            policy=TEST_POLICY,
        )

    first_claim = next(
        (root / "normalized" / "event-claim-index-v3" / f"snapshot={first.snapshot_id}").rglob(
            "*.parquet"
        )
    )
    first_claim.write_bytes(first_claim.read_bytes() + b"tampered")
    with pytest.raises(ValidationError, match="physical content changed"):
        load_normalized_snapshot(root, first.snapshot_id)

    retry_root = tmp_path / "claim-retry"
    retry_raw = _raw(retry_root, key="retry", payload=b"retry")
    real_replace = lake_module.os.replace
    injected = False

    def fail_snapshot_publish(source: Path, destination: Path) -> None:
        nonlocal injected
        if destination.parent.name == "snapshots" and not injected:
            injected = True
            raise OSError("injected normalized publish failure")
        real_replace(source, destination)

    monkeypatch.setattr(lake_module.os, "replace", fail_snapshot_publish)
    with pytest.raises(OSError, match="normalized publish"):
        write_normalized_events(
            retry_root,
            [trade("retry-claim")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[retry_raw.reference()],
            policy=TEST_POLICY,
        )
    monkeypatch.setattr(lake_module.os, "replace", real_replace)
    assert not list((retry_root / "normalized" / "event-claim-index-v3").rglob("*.parquet"))
    retried = write_normalized_events(
        retry_root,
        [trade("retry-claim")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[retry_raw.reference()],
        policy=TEST_POLICY,
    )
    assert retried.snapshot is not None


def test_event_claim_and_curated_revision_are_process_safe(tmp_path: Path) -> None:
    event_root = tmp_path / "event-lake"
    first_raw = _raw(event_root, key="event-1", payload=b"event-1")
    second_raw = _raw(event_root, key="event-2", payload=b"event-2")
    first_record = trade("process-global-id")
    second_record = deepcopy(first_record)
    second_record["price"]["units"] += 1
    results, exit_codes = _run_pair(
        _normalized_process,
        [
            (str(event_root), asdict(first_raw.reference()), first_record),
            (str(event_root), asdict(second_raw.reference()), second_record),
        ],
    )
    assert exit_codes == [0, 0]
    assert len([1 for status, _ in results if status == "ok"]) == 1
    conflicts = [value for status, value in results if status == "error"]
    assert len(conflicts) == 1 and "Conflicting lake event_id claim" in conflicts[0]

    same_event_root = tmp_path / "same-event-lake"
    same_event_raw = _raw(same_event_root, key="same-event", payload=b"same-event")
    same_record = trade("same-process-global-id")
    results, exit_codes = _run_pair(
        _normalized_process,
        [
            (str(same_event_root), asdict(same_event_raw.reference()), same_record),
            (str(same_event_root), asdict(same_event_raw.reference()), same_record),
        ],
    )
    assert exit_codes == [0, 0]
    assert [status for status, _ in results] == ["ok", "ok"], results
    assert len({value for _, value in results}) == 1


def test_deleted_event_claim_recovers_from_snapshot_and_preserves_binding(tmp_path: Path) -> None:
    root = tmp_path / "event-claim-recovery"
    record = trade("recover-global-id")
    original = _normalized(root, key="original", record=record)
    index_root = root / "normalized" / "event-claim-index-v3" / f"snapshot={original.snapshot_id}"
    claim_path = next(index_root.rglob("*.parquet"))
    claim_path.unlink()

    repeated = _normalized(root, key="repeated", record=deepcopy(record))
    assert repeated.snapshot_id != original.snapshot_id
    assert load_normalized_snapshot(root, original.snapshot_id) == original
    assert list(index_root.rglob("*.parquet"))

    next(index_root.rglob("*.parquet")).unlink()
    changed = deepcopy(record)
    changed["price"]["units"] += 1
    with pytest.raises(ValidationError, match="Conflicting lake event_id claim"):
        _normalized(root, key="changed", record=changed)
    assert list(index_root.rglob("*.parquet"))


def test_deleted_event_claim_recovery_is_process_safe(tmp_path: Path) -> None:
    root = tmp_path / "event-claim-process-recovery"
    record = trade("recover-process-id")
    original = _normalized(root, key="original", record=record)
    index_root = root / "normalized" / "event-claim-index-v3" / f"snapshot={original.snapshot_id}"
    next(index_root.rglob("*.parquet")).unlink()
    admitted = _raw(root, key="same", payload=b"same")
    results, exit_codes = _run_pair(
        _normalized_process,
        [
            (str(root), asdict(admitted.reference()), deepcopy(record)),
            (str(root), asdict(admitted.reference()), deepcopy(record)),
        ],
    )
    assert exit_codes == [0, 0]
    assert [status for status, _ in results] == ["ok", "ok"], results
    assert list(index_root.rglob("*.parquet"))
    assert load_normalized_snapshot(root, original.snapshot_id) == original

    next(index_root.rglob("*.parquet")).unlink()
    changed_raw = _raw(root, key="changed", payload=b"changed")
    changed = deepcopy(record)
    changed["price"]["units"] += 1
    results, exit_codes = _run_pair(
        _normalized_process,
        [
            (str(root), asdict(admitted.reference()), deepcopy(record)),
            (str(root), asdict(changed_raw.reference()), changed),
        ],
    )
    assert exit_codes == [0, 0]
    assert len([1 for status, _ in results if status == "ok"]) == 1
    errors = [value for status, value in results if status == "error"]
    assert len(errors) == 1 and "Conflicting lake event_id claim" in errors[0]


def test_deleted_curated_revision_recovers_and_preserves_binding(tmp_path: Path) -> None:
    root = tmp_path / "curated-revision-recovery"
    original_normalized = _normalized(root, key="original", record=trade("curated-original"))
    original = curated_module.curate_trade_bars_from_snapshot(
        root,
        normalized_snapshot_id=original_normalized.snapshot_id,
        dataset="recovery-bars",
        revision_id="revision-1",
        recipe_version="session-bars-v1",
        interval=timedelta(minutes=1),
        session_starts={"binance-24x7-BTC-USDT-SPOT": datetime(2026, 1, 2, tzinfo=timezone.utc)},
        policy=TEST_POLICY,
    )
    registry = root / "curated" / "recovery-bars" / "revisions" / "revision-1.json"
    registry.unlink()
    repeated = curated_module.curate_trade_bars_from_snapshot(
        root,
        normalized_snapshot_id=original_normalized.snapshot_id,
        dataset="recovery-bars",
        revision_id="revision-1",
        recipe_version="session-bars-v1",
        interval=timedelta(minutes=1),
        session_starts={"binance-24x7-BTC-USDT-SPOT": datetime(2026, 1, 2, tzinfo=timezone.utc)},
        policy=TEST_POLICY,
    )
    assert repeated == original
    assert (
        curated_module.load_curated_snapshot(root, "recovery-bars", original.snapshot_id)
        == original
    )

    registry.unlink()
    changed_normalized = _normalized(root, key="changed", record=trade("curated-changed"))
    with pytest.raises(ValidationError, match="maps to different content"):
        curated_module.curate_trade_bars_from_snapshot(
            root,
            normalized_snapshot_id=changed_normalized.snapshot_id,
            dataset="recovery-bars",
            revision_id="revision-1",
            recipe_version="session-bars-v1",
            interval=timedelta(minutes=1),
            session_starts={
                "binance-24x7-BTC-USDT-SPOT": datetime(2026, 1, 2, tzinfo=timezone.utc)
            },
            policy=TEST_POLICY,
        )


def test_deleted_curated_revision_recovery_is_process_safe(tmp_path: Path) -> None:
    root = tmp_path / "curated-revision-process-recovery"
    original_normalized = _normalized(root, key="original", record=trade("curated-original"))
    changed_normalized = _normalized(root, key="changed", record=trade("curated-changed"))
    original = _curated_process_once(root, original_normalized.snapshot_id)
    registry = root / "curated" / "concurrent-bars" / "revisions" / "revision-1.json"
    registry.unlink()
    results, exit_codes = _run_pair(
        _curated_process,
        [
            (str(root), original_normalized.snapshot_id),
            (str(root), changed_normalized.snapshot_id),
        ],
    )
    assert exit_codes == [0, 0]
    assert len([1 for status, _ in results if status == "ok"]) == 1
    errors = [value for status, value in results if status == "error"]
    assert len(errors) == 1 and "maps to different content" in errors[0]
    assert (
        curated_module.load_curated_snapshot(root, "concurrent-bars", original.snapshot_id)
        == original
    )


def _curated_process_once(root: Path, normalized_snapshot_id: str):
    return curate_trade_bars_from_snapshot(
        root,
        normalized_snapshot_id=normalized_snapshot_id,
        dataset="concurrent-bars",
        revision_id="revision-1",
        recipe_version="session-bars-v1",
        interval=timedelta(minutes=1),
        session_starts={"binance-24x7-BTC-USDT-SPOT": datetime(2026, 1, 2, tzinfo=timezone.utc)},
        policy=TEST_POLICY,
    )


def test_atomic_normalized_and_curated_staging_recover_after_hard_exit(tmp_path: Path) -> None:
    root = tmp_path / "staging-hard-exit"
    root.mkdir()
    context = multiprocessing.get_context("spawn")
    for target_name in ("first.json", "second.json"):
        process = context.Process(
            target=_atomic_hard_exit_process,
            args=(str(root), target_name, target_name.encode()),
        )
        process.start()
        process.join(30)
        assert process.exitcode == 71
    assert len(list(root.glob(".atomic-*.tmp"))) == 2
    with lake_module._lake_lock(root, "atomic-test", {"target": "first.json"}):
        lake_module._atomic_write_bytes(root, root / "first.json", b"first.json")
    assert (root / "first.json").read_bytes() == b"first.json"
    assert len(list(root.glob(".atomic-*.tmp"))) == 1
    with lake_module._lake_lock(root, "atomic-test", {"target": "second.json"}):
        lake_module._atomic_write_bytes(root, root / "second.json", b"second.json")
    assert not list(root.glob(".atomic-*.tmp"))

    raw = _raw(root, key="normalized-crash", payload=b"normalized-crash")
    record = trade("normalized-crash")
    process = context.Process(
        target=_normalized_hard_exit_process,
        args=(str(root), asdict(raw.reference()), record),
    )
    process.start()
    process.join(30)
    assert process.exitcode == 72
    normalized_staging = root / "normalized" / "staging"
    crashed_stage = list(normalized_staging.glob("normalized-batch-*-*"))
    assert len(crashed_stage) == 1
    active_ready = context.Event()
    active_release = context.Event()
    active_result = context.Queue()
    active_process = context.Process(
        target=_hold_staging_process,
        args=(
            str(root),
            "normalized/staging",
            "normalized-batch",
            {"batch": "different-active-batch"},
            active_ready,
            active_release,
            active_result,
        ),
    )
    active_process.start()
    assert active_ready.wait(20)
    unrelated_stage = Path(active_result.get(timeout=5))
    recovered = write_normalized_events(
        root,
        [record],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )
    assert recovered.snapshot is not None
    assert unrelated_stage.is_dir()
    assert not crashed_stage[0].exists()
    active_release.set()
    active_process.join(30)
    assert active_process.exitcode == 0
    assert not unrelated_stage.exists()

    process = context.Process(
        target=_curated_hard_exit_process,
        args=(str(root), recovered.snapshot.snapshot_id),
    )
    process.start()
    process.join(30)
    assert process.exitcode == 73
    curated_staging = root / "curated" / "crash-bars" / "staging"
    crashed_stage = list(curated_staging.glob("curated-revision-*-*"))
    assert len(crashed_stage) == 1
    active_ready = context.Event()
    active_release = context.Event()
    active_result = context.Queue()
    active_process = context.Process(
        target=_hold_staging_process,
        args=(
            str(root),
            "curated/crash-bars/staging",
            "curated-revision",
            {"dataset": "crash-bars", "revision_id": "revision-2"},
            active_ready,
            active_release,
            active_result,
        ),
    )
    active_process.start()
    assert active_ready.wait(20)
    unrelated_stage = Path(active_result.get(timeout=5))
    recovered_curated = curated_module.curate_trade_bars_from_snapshot(
        root,
        normalized_snapshot_id=recovered.snapshot.snapshot_id,
        dataset="crash-bars",
        revision_id="revision-1",
        recipe_version="session-bars-v1",
        interval=timedelta(minutes=1),
        session_starts={"binance-24x7-BTC-USDT-SPOT": datetime(2026, 1, 2, tzinfo=timezone.utc)},
        policy=TEST_POLICY,
    )
    assert recovered_curated.snapshot_id.startswith("sha256-")
    assert unrelated_stage.is_dir()
    assert not crashed_stage[0].exists()
    active_release.set()
    active_process.join(30)
    assert active_process.exitcode == 0
    assert not unrelated_stage.exists()

    curated_root = tmp_path / "curated-lake"
    first_snapshot = _normalized(
        curated_root,
        key="curated-1",
        record=trade("curated-1", timestamp="2026-01-02T00:00:01Z"),
    )
    second_snapshot = _normalized(
        curated_root,
        key="curated-2",
        record=trade("curated-2", timestamp="2026-01-02T00:00:02Z"),
    )
    results, exit_codes = _run_pair(
        _curated_process,
        [
            (str(curated_root), first_snapshot.snapshot_id),
            (str(curated_root), second_snapshot.snapshot_id),
        ],
    )
    assert exit_codes == [0, 0]
    assert len([1 for status, _ in results if status == "ok"]) == 1
    conflicts = [value for status, value in results if status == "error"]
    assert len(conflicts) == 1 and "maps to different content" in conflicts[0]

    same_root = tmp_path / "curated-same"
    same_snapshot = _normalized(same_root, key="same", record=trade("same-curated"))
    results, exit_codes = _run_pair(
        _curated_process,
        [
            (str(same_root), same_snapshot.snapshot_id),
            (str(same_root), same_snapshot.snapshot_id),
        ],
    )
    assert exit_codes == [0, 0]
    assert [status for status, _ in results] == ["ok", "ok"], results
    assert len({value for _, value in results}) == 1
