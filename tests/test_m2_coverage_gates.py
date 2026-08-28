from __future__ import annotations

import json
import shutil
import time
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import quant_data_kit.curated as curated_module
import quant_data_kit.data_lake as lake_module
import quant_data_kit.process_lock as lock_module
from quant_data_kit.curated import build_session_bars, curate_trade_bars_from_snapshot
from quant_data_kit.data_lake import (
    ArchiveReceipt,
    StoragePolicy,
    cleanup_archived_raw_object,
    evaluate_capacity,
    load_normalized_snapshot,
    validate_raw_reference,
    write_normalized_events,
    write_raw_bytes,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.schemas_v2 import (
    BAR_EVENT_SCHEMA_ID,
    BOOK_DELTA_EVENT_SCHEMA_ID,
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    CORPORATE_ACTION_EVENT_SCHEMA_ID,
    FUNDING_RATE_EVENT_SCHEMA_ID,
    INSTRUMENT_SPEC_SCHEMA_ID,
    QUOTE_EVENT_SCHEMA_ID,
    STATUS_EVENT_SCHEMA_ID,
    SYMBOL_MAPPING_SCHEMA_ID,
    TRADING_SESSION_SCHEMA_ID,
    get_arrow_schema,
    get_json_schema,
    validate_arrow_table,
    validate_event_stream,
    validate_json_record,
)
from tests.test_m2_audit_regressions import TEST_POLICY, normalized, raw, trade

UTC = timezone.utc
GOLDEN = json.loads(
    (Path(__file__).parent / "golden" / "v2" / "records.json").read_text(encoding="utf-8")
)


def _replace(payload: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    changed = deepcopy(payload)
    target: Any = changed
    parts = path.split(".")
    for part in parts[:-1]:
        target = target[int(part)] if part.isdigit() else target[part]
    target[parts[-1]] = value
    return changed


@pytest.mark.parametrize(
    ("schema_id", "path", "value"),
    [
        (QUOTE_EVENT_SCHEMA_ID, "bid_price.scale", 3),
        (QUOTE_EVENT_SCHEMA_ID, "bid_price.units", 10002),
        (QUOTE_EVENT_SCHEMA_ID, "bid_quantity.units", -1),
        (BAR_EVENT_SCHEMA_ID, "bar_end", "2026-01-02T01:00:00Z"),
        (BAR_EVENT_SCHEMA_ID, "event_time", "2026-01-02T01:00:59Z"),
        (BAR_EVENT_SCHEMA_ID, "close_price.scale", 3),
        (BAR_EVENT_SCHEMA_ID, "high_price.units", 9995),
        (BAR_EVENT_SCHEMA_ID, "low_price.units", 10006),
        (BAR_EVENT_SCHEMA_ID, "volume.units", -1),
        (BOOK_SNAPSHOT_EVENT_SCHEMA_ID, "asks.0.price.scale", 3),
        (BOOK_SNAPSHOT_EVENT_SCHEMA_ID, "bids.0.quantity.units", -1),
        (BOOK_SNAPSHOT_EVENT_SCHEMA_ID, "bids.0.price.units", 10001),
        (BOOK_DELTA_EVENT_SCHEMA_ID, "previous_sequence", 5),
        (BOOK_DELTA_EVENT_SCHEMA_ID, "quantity.units", 0),
        (FUNDING_RATE_EVENT_SCHEMA_ID, "rate", float("nan")),
        (FUNDING_RATE_EVENT_SCHEMA_ID, "interval_end", "2026-01-02T00:00:00Z"),
        (CORPORATE_ACTION_EVENT_SCHEMA_ID, "cash_amount", None),
        (CORPORATE_ACTION_EVENT_SCHEMA_ID, "cash_amount.units", -1),
        (CORPORATE_ACTION_EVENT_SCHEMA_ID, "currency", ""),
        (INSTRUMENT_SPEC_SCHEMA_ID, "price_tick.units", 0),
        (INSTRUMENT_SPEC_SCHEMA_ID, "effective_to", "2025-12-31T00:00:00Z"),
        (INSTRUMENT_SPEC_SCHEMA_ID, "superseded_at", "2025-11-30T00:00:00Z"),
        (SYMBOL_MAPPING_SCHEMA_ID, "effective_to", "2025-12-31T00:00:00Z"),
        (TRADING_SESSION_SCHEMA_ID, "closes_at", "2026-01-02T00:00:00Z"),
    ],
)
def test_schema_negative_branch_matrix(schema_id: str, path: str, value: Any) -> None:
    with pytest.raises(ValidationError):
        validate_json_record(schema_id, _replace(GOLDEN[schema_id], path, value))


def test_schema_delete_delta_corporate_ratio_and_registry_fail_closed() -> None:
    delete = _replace(GOLDEN[BOOK_DELTA_EVENT_SCHEMA_ID], "action", "delete")
    with pytest.raises(ValidationError, match="delete delta"):
        validate_json_record(BOOK_DELTA_EVENT_SCHEMA_ID, delete)

    ratio_only = deepcopy(GOLDEN[CORPORATE_ACTION_EVENT_SCHEMA_ID])
    ratio_only["ratio"] = {"units": 1, "scale": 0}
    ratio_only["cash_amount"] = None
    ratio_only["currency"] = "CNY"
    with pytest.raises(ValidationError, match="currency requires"):
        validate_json_record(CORPORATE_ACTION_EVENT_SCHEMA_ID, ratio_only)
    ratio_only["currency"] = None
    ratio_only["ratio"]["units"] = -1
    with pytest.raises(ValidationError, match="ratio"):
        validate_json_record(CORPORATE_ACTION_EVENT_SCHEMA_ID, ratio_only)

    for getter in (get_arrow_schema, get_json_schema):
        with pytest.raises(ValidationError, match="Unsupported schema version"):
            getter(INSTRUMENT_SPEC_SCHEMA_ID, "1.0.0")
        with pytest.raises(ValidationError, match="Unknown schema ID"):
            getter("puresaber.unknown")
    with pytest.raises(ValidationError, match="Unsupported schema version"):
        validate_json_record(INSTRUMENT_SPEC_SCHEMA_ID, {}, version="1.0.0")
    with pytest.raises(ValidationError, match="Unknown market event_type"):
        validate_event_stream([{"event_type": "unknown"}])
    with pytest.raises(ValidationError, match="JSON schema validation failed"):
        validate_event_stream([_replace(GOLDEN["puresaber.trade-event"], "sequence", None)])
    with pytest.raises(ValidationError, match="Arrow schema mismatch"):
        validate_arrow_table(INSTRUMENT_SPEC_SCHEMA_ID, pa.table({"wrong": [1]}))


def test_data_lake_guard_branches_are_explicit(tmp_path: Path) -> None:
    invalid_policies = (
        {"hot_retention_days": 29},
        {"hot_quota_bytes": 0},
        {"minimum_free_fraction": 1.0},
    )
    for changes in invalid_policies:
        with pytest.raises(ValidationError):
            StoragePolicy(**changes)
    for kwargs in (
        {"projected_write_bytes": -1},
        {"current_hot_bytes": -1},
        {"disk_total_bytes": 0, "disk_free_bytes": 0},
    ):
        with pytest.raises(ValidationError):
            evaluate_capacity(tmp_path, **kwargs)

    with pytest.raises(ValidationError, match="Raw payload must be bytes"):
        write_raw_bytes(
            tmp_path,
            source="binance",
            request={},
            collected_at="2026-01-02T00:00:00Z",
            payload="not-bytes",  # type: ignore[arg-type]
            policy=TEST_POLICY,
        )
    with pytest.raises(ValidationError, match="UTC-aware"):
        write_raw_bytes(
            tmp_path,
            source="binance",
            request={},
            collected_at="2026-01-02T00:00:00",
            payload=b"raw",
            policy=TEST_POLICY,
        )
    for value in ("latest", "CON", "bad:value"):
        with pytest.raises(ValidationError):
            write_raw_bytes(
                tmp_path,
                source="binance",
                request={},
                collected_at="2026-01-02T00:00:00Z",
                payload=b"raw",
                idempotency_key=value,
                policy=TEST_POLICY,
            )

    assert lake_module._json_evidence(b"secret")["sha256"]
    assert lake_module._json_evidence(object())["invalid_type"] == "object"
    assert lake_module._json_evidence(date(2026, 1, 2)) == "2026-01-02"
    with pytest.raises(ValidationError, match="partition value"):
        lake_module._partition_segment("", "partition")
    with pytest.raises(ValidationError, match="safe partition"):
        lake_module._partition_segment("latest", "partition")
    assert lake_module._tree_size(tmp_path / "missing") == 0
    assert lake_module._disk_probe_path(tmp_path / "missing" / "nested").exists()

    lake = tmp_path / "paths"
    lake.mkdir()
    with pytest.raises(ValidationError, match="escapes"):
        lake_module._validate_lake_path(lake, tmp_path / "outside", allow_missing=True)
    with pytest.raises(ValidationError, match="missing"):
        lake_module._validate_lake_path(lake, lake / "missing", allow_missing=False)
    file_root = tmp_path / "not-a-directory"
    file_root.write_text("file", encoding="utf-8")
    with pytest.raises(ValidationError, match="not a directory"):
        lake_module._resolved_lake_root(file_root, create=False)


def test_atomic_metadata_and_raw_staging_recovery_are_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "atomic"
    lake.mkdir()
    real_replace = lake_module.os.replace

    def fail_replace(source: Path, destination: Path) -> None:
        raise OSError(f"injected atomic failure: {source} -> {destination}")

    monkeypatch.setattr(lake_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="atomic failure"):
        lake_module._atomic_write_bytes(lake, lake / "metadata.json", b"body")
    assert not list(lake.glob(".metadata.json.*.tmp"))
    monkeypatch.setattr(lake_module.os, "replace", real_replace)

    admitted = raw(tmp_path / "staging", key="recover-stage")
    root = tmp_path / "staging"
    object_dir = next(root.rglob(f"object={admitted.object_id}"))
    staging_root = root / "raw" / ".staging"
    duplicate_stage = staging_root / f"{lake_module._raw_stage_prefix(admitted.reference())}copy"
    shutil.copytree(object_dir, duplicate_stage)
    assert raw(root, key="recover-stage") == admitted
    assert not duplicate_stage.exists()

    invalid_stage = staging_root / f"{lake_module._raw_stage_prefix(admitted.reference())}invalid"
    invalid_stage.mkdir()
    (invalid_stage / "manifest.json").write_text("{}", encoding="utf-8")
    assert raw(root, key="recover-stage") == admitted
    assert not invalid_stage.exists()
    assert list((root / "quarantine" / "raw-unpublished").iterdir())

    with pytest.raises(ValidationError, match="non-staging"):
        lake_module._remove_staging_directory(root, object_dir)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", "1.0.0", "schema/layer"),
        ("data_path", "other.bin", "data_path"),
        ("request", {"changed": True}, "request hash"),
        ("hot_retention_days", 31, "retention metadata"),
    ],
)
def test_raw_manifest_fields_are_independently_anchored(
    tmp_path: Path,
    field: str,
    value: Any,
    message: str,
) -> None:
    admitted = raw(tmp_path, key=f"manifest-{field}")
    manifest_path = next(tmp_path.rglob(f"object={admitted.object_id}/manifest.json"))
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload[field] = value
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match=message):
        validate_raw_reference(tmp_path, admitted.reference(), allow_archived=False)


def test_raw_directory_claim_and_cleanup_state_guards(tmp_path: Path) -> None:
    claim_root = tmp_path / "claim"
    admitted = raw(claim_root, key="claim-anchor")
    claim_path = lake_module._raw_claim_path(claim_root, admitted.reference())
    claim = json.loads(claim_path.read_text(encoding="utf-8"))
    claim["source"] = "okx"
    claim_path.write_text(json.dumps(claim), encoding="utf-8")
    with pytest.raises(ValidationError, match="claim integrity"):
        validate_raw_reference(claim_root, admitted.reference(), allow_archived=False)

    deleting_root = tmp_path / "deleting"
    deleting = raw(deleting_root, key="deleting")
    object_dir = next(deleting_root.rglob(f"object={deleting.object_id}"))
    object_dir.rename(object_dir.with_name(f"deleting={deleting.object_id}"))
    with pytest.raises(ValidationError, match="cleanup is in progress"):
        raw(deleting_root, key="deleting")

    missing_root = tmp_path / "missing-cleanup"
    missing = raw(missing_root, key="missing")
    object_dir = next(missing_root.rglob(f"object={missing.object_id}"))
    shutil.rmtree(object_dir)
    archive = (tmp_path / "missing.archive").resolve()
    archive.write_bytes(b"payload-missing")
    receipt = ArchiveReceipt(
        object_id=missing.object_id,
        archive_uri=str(archive),
        source_sha256=missing.content_sha256,
        archive_sha256=missing.content_sha256,
        restored_sha256=missing.content_sha256,
        verified_at="2026-02-02T00:00:00Z",
    )
    with pytest.raises(ValidationError, match="unavailable"):
        cleanup_archived_raw_object(
            missing_root,
            missing.reference(),
            receipt,
            confirm=True,
            now="2026-02-02T00:00:01Z",
        )


@pytest.mark.parametrize("mutation", ["records", "manifest-json", "manifest", "extra"])
def test_quarantine_published_batch_is_fully_verified(tmp_path: Path, mutation: str) -> None:
    root = tmp_path / mutation
    entries = [lake_module.QuarantineEntry(0, "invalid", {"value": object()})]
    manifest_path = lake_module._write_quarantine(
        root,
        "binance",
        "BINANCE",
        entries,
        policy=TEST_POLICY,
    )
    assert manifest_path is not None
    batch_dir = manifest_path.parent
    if mutation == "records":
        (batch_dir / "records.jsonl").write_text("changed", encoding="utf-8")
    elif mutation == "manifest-json":
        manifest_path.write_text("{", encoding="utf-8")
    elif mutation == "manifest":
        stored = json.loads(manifest_path.read_text(encoding="utf-8"))
        stored["venue"] = "CHANGED"
        manifest_path.write_text(json.dumps(stored), encoding="utf-8")
    else:
        (batch_dir / "unexpected").write_text("blocked", encoding="utf-8")
    with pytest.raises(ValidationError, match="Quarantine"):
        lake_module._write_quarantine(
            root,
            "binance",
            "BINANCE",
            entries,
            policy=TEST_POLICY,
        )


def test_empty_quarantine_and_unknown_event_helpers_are_closed(tmp_path: Path) -> None:
    assert (
        lake_module._write_quarantine(
            tmp_path,
            "binance",
            "BINANCE",
            [],
            policy=TEST_POLICY,
        )
        is None
    )
    with pytest.raises(ValidationError, match="Unknown event_type"):
        lake_module._event_schema_id({"event_type": "unknown"})
    assert lake_module._stream_key({}, 7) == ("invalid-7", "invalid-7", "invalid-7")


def test_process_lock_platform_dispatch_and_timeout_are_explicit(tmp_path: Path) -> None:
    with (
        pytest.raises(ValidationError, match="positive"),
        lock_module.process_file_lock(tmp_path / "invalid.lock", timeout_seconds=0),
    ):
        pass

    windows_calls: list[int] = []

    def windows_lock(_fd: int, mode: int, _length: int) -> None:
        windows_calls.append(mode)
        if len(windows_calls) == 1:
            raise OSError("contended")

    lock_module._acquire_windows_lock(
        11,
        tmp_path / "windows.lock",
        time.monotonic() + 1,
        locking=windows_lock,
        nonblocking_mode=1,
    )
    assert windows_calls == [1, 1]
    lock_module._release_windows_lock(11, locking=windows_lock, unlock_mode=2)
    assert windows_calls[-1] == 2

    def always_contended(_fd: int, _mode: int, _length: int) -> None:
        raise OSError("still contended")

    with pytest.raises(TimeoutError, match="Timed out"):
        lock_module._acquire_windows_lock(
            11,
            tmp_path / "windows-timeout.lock",
            0.0,
            locking=always_contended,
            nonblocking_mode=1,
        )

    posix_calls: list[int] = []

    def posix_lock(_fd: int, operation: int) -> None:
        posix_calls.append(operation)
        if len(posix_calls) == 1:
            raise BlockingIOError("contended")

    lock_module._acquire_posix_lock(
        12,
        tmp_path / "posix.lock",
        time.monotonic() + 1,
        flock=posix_lock,
        exclusive_nonblocking_operation=3,
    )
    assert posix_calls == [3, 3]
    lock_module._release_posix_lock(12, flock=posix_lock, unlock_operation=4)
    assert posix_calls[-1] == 4

    def posix_always_contended(_fd: int, _operation: int) -> None:
        raise BlockingIOError("still contended")

    with pytest.raises(TimeoutError, match="Timed out"):
        lock_module._acquire_posix_lock(
            12,
            tmp_path / "posix-timeout.lock",
            0.0,
            flock=posix_always_contended,
            exclusive_nonblocking_operation=3,
        )

    assert lock_module._platform_lock_backend("nt") is lock_module._windows_file_lock
    assert lock_module._platform_lock_backend("posix") is lock_module._posix_file_lock
    assert lock_module._platform_lock_backend("other") is lock_module._posix_file_lock
    real_lock_path = tmp_path / "real-platform.lock"
    with lock_module.process_file_lock(real_lock_path, timeout_seconds=1):
        assert real_lock_path.is_file()
    assert real_lock_path.read_bytes() == b"\0"


def test_raw_claim_cleanup_and_deleting_state_tampering_fail_closed(tmp_path: Path) -> None:
    admitted = raw(tmp_path, key="claim-tamper")
    claim_path = lake_module._raw_claim_path(tmp_path, admitted.reference())
    original_claim = claim_path.read_text(encoding="utf-8")
    claim_path.write_text("{}", encoding="utf-8")
    with pytest.raises(ValidationError, match="malformed|integrity"):
        validate_raw_reference(tmp_path, admitted.reference(), allow_archived=False)
    claim_path.write_text(original_claim, encoding="utf-8")

    archive = (tmp_path.parent / f"{tmp_path.name}-claim.archive").resolve()
    archive.write_bytes(b"payload-claim-tamper")
    common = {
        "object_id": admitted.object_id,
        "archive_uri": str(archive),
        "source_sha256": admitted.content_sha256,
        "archive_sha256": admitted.content_sha256,
        "restored_sha256": admitted.content_sha256,
        "verified_at": "2026-02-02T00:00:00Z",
    }
    for changed in (
        {"verified_at": "2025-12-01T00:00:00Z"},
        {"object_id": "sha256-" + "0" * 64},
    ):
        with pytest.raises(ValidationError):
            cleanup_archived_raw_object(
                tmp_path,
                admitted.reference(),
                ArchiveReceipt(**{**common, **changed}),
                confirm=True,
                now="2026-02-02T00:00:01Z",
            )

    object_dir = next(tmp_path.rglob(f"object={admitted.object_id}"))
    deleting_dir = object_dir.with_name(f"deleting={admitted.object_id}")
    object_dir.rename(deleting_dir)
    (deleting_dir / "unexpected").write_text("blocked", encoding="utf-8")
    with pytest.raises(ValidationError, match="unexpected"):
        cleanup_archived_raw_object(
            tmp_path,
            admitted.reference(),
            ArchiveReceipt(**common),
            confirm=True,
            now="2026-02-02T00:00:01Z",
        )


def test_normalized_admission_and_snapshot_metadata_guards(tmp_path: Path) -> None:
    admitted = raw(tmp_path, key="admission")
    with pytest.raises(ValidationError, match="at least one"):
        write_normalized_events(
            tmp_path,
            [trade("none")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[],
            policy=TEST_POLICY,
        )
    with pytest.raises(ValidationError, match="duplicate Raw"):
        write_normalized_events(
            tmp_path,
            [trade("duplicate-raw")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[admitted.reference(), admitted.reference()],
            policy=TEST_POLICY,
        )
    with pytest.raises(ValidationError, match="provider does not match"):
        write_normalized_events(
            tmp_path,
            [trade("wrong-provider")],
            provider="okx",
            venue="OKX",
            upstream_raw_references=[admitted.reference()],
            policy=TEST_POLICY,
        )

    snapshot = normalized(tmp_path / "snapshot", key="identity")
    snapshot_dir = tmp_path / "snapshot" / "normalized" / "snapshots" / snapshot.snapshot_id
    manifest_path = snapshot_dir / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["layer"] = "wrong"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="identity mismatch"):
        load_normalized_snapshot(tmp_path / "snapshot", snapshot.snapshot_id)


def test_curated_writer_and_reader_negative_guards(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="positive"):
        build_session_bars([], interval=timedelta(0), session_starts={})
    with pytest.raises(ValidationError, match="reserved"):
        curated_module._dataset_segment("CON")
    with pytest.raises(ValidationError, match="revision_id"):
        curated_module._revision_segment("bad:value")
    with pytest.raises(ValidationError, match="UTC-aware"):
        curated_module._utc(datetime(2026, 1, 2, tzinfo=UTC).replace(tzinfo=None), "session")

    source_root = tmp_path / "source"
    source_snapshot = normalized(source_root, key="curated-guards")
    with pytest.raises(ValidationError, match="recipe_version"):
        curated_module._write_curated_bars(
            source_root,
            [],
            dataset="bars",
            revision_id="r1",
            recipe_version=" ",
            normalized_snapshot_id=source_snapshot.snapshot_id,
            policy=TEST_POLICY,
        )
    with pytest.raises(ValidationError, match="empty Curated"):
        curated_module._write_curated_bars(
            source_root,
            [],
            dataset="bars",
            revision_id="r1",
            recipe_version="v1",
            normalized_snapshot_id=source_snapshot.snapshot_id,
            policy=TEST_POLICY,
        )

    no_trade_root = tmp_path / "no-trades"
    no_trade_raw = raw(no_trade_root, key="status")
    status = deepcopy(GOLDEN[STATUS_EVENT_SCHEMA_ID])
    status["source"] = "binance"
    result = write_normalized_events(
        no_trade_root,
        [status],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[no_trade_raw.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    with pytest.raises(ValidationError, match="no trades"):
        curate_trade_bars_from_snapshot(
            no_trade_root,
            normalized_snapshot_id=result.snapshot.snapshot_id,
            dataset="no-trade-bars",
            revision_id="r1",
            recipe_version="v1",
            interval=timedelta(minutes=1),
            session_starts={},
            policy=TEST_POLICY,
        )


def test_curated_manifest_and_revision_tampering_are_rejected(tmp_path: Path) -> None:
    source = normalized(tmp_path, key="curated-metadata")
    curated = curate_trade_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="metadata-bars",
        revision_id="r1",
        recipe_version="v1",
        interval=timedelta(minutes=1),
        session_starts={"binance-24x7-BTC-USDT-SPOT": datetime(2026, 1, 2, tzinfo=UTC)},
        policy=TEST_POLICY,
    )
    revision_path = tmp_path / "curated" / "metadata-bars" / "revisions" / "r1.json"
    revision = json.loads(revision_path.read_text(encoding="utf-8"))
    revision["snapshot_id"] = "sha256-" + "0" * 64
    revision_path.write_text(json.dumps(revision), encoding="utf-8")
    with pytest.raises(ValidationError, match="registry integrity"):
        curated_module.load_curated_snapshot(tmp_path, "metadata-bars", curated.snapshot_id)
