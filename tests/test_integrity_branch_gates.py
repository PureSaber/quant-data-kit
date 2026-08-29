from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

import quant_data_kit.curated as curated_module
import quant_data_kit.data_lake as lake_module
from quant_data_kit import normalized_v3 as normalized_v3_module
from quant_data_kit.curated import CuratedPartition, curate_trade_bars_from_snapshot
from quant_data_kit.data_lake import (
    DuckDBSnapshot,
    EventClaimReference,
    NormalizedSnapshot,
    PartitionManifest,
    RawObjectReference,
    StoragePolicy,
    load_normalized_snapshot,
    write_normalized_events,
    write_raw_bytes,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.l2_replay import L2BookReconstructor, L2ReplayError, ReconstructedLevel
from quant_data_kit.schemas_v2 import TRADE_EVENT_SCHEMA_ID
from tests.test_m2_audit_regressions import trade

TEST_POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)


def normalized(root: Path) -> NormalizedSnapshot:
    raw = write_raw_bytes(
        root,
        source="binance",
        request={"fixture": "branch-gates"},
        collected_at="2026-01-02T00:00:00Z",
        payload=b"branch-gates",
        idempotency_key="branch-gates",
        policy=TEST_POLICY,
    )
    result = write_normalized_events(
        root,
        [trade("branch-gates")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    return result.snapshot


def legacy_normalized(root: Path, *, key: str = "legacy-branch-gates") -> NormalizedSnapshot:
    raw = write_raw_bytes(
        root,
        source="binance",
        request={"fixture": key},
        collected_at="2026-01-02T00:00:00Z",
        payload=key.encode(),
        idempotency_key=key,
        policy=TEST_POLICY,
    )
    result = lake_module._write_normalized_events_legacy(
        root,
        [trade("legacy-branch-gates")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    return result.snapshot


def test_legacy_mapping_writer_rejects_untrusted_duplicate_and_cross_provider_raw(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="at least one trusted Raw reference"):
        lake_module._write_normalized_events_legacy(
            tmp_path,
            [trade("no-raw")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[],
            policy=TEST_POLICY,
        )

    raw = write_raw_bytes(
        tmp_path,
        source="binance",
        request={"fixture": "legacy-reference-guards"},
        collected_at="2026-01-02T00:00:00Z",
        payload=b"legacy-reference-guards",
        idempotency_key="legacy-reference-guards",
        policy=TEST_POLICY,
    )
    reference = raw.reference()
    with pytest.raises(ValidationError, match="duplicate Raw references"):
        lake_module._write_normalized_events_legacy(
            tmp_path,
            [trade("duplicate-raw")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[reference, reference],
            policy=TEST_POLICY,
        )
    with pytest.raises(ValidationError, match="does not match its Raw source"):
        lake_module._write_normalized_events_legacy(
            tmp_path,
            [trade("cross-provider-raw")],
            provider="okx",
            venue="OKX",
            upstream_raw_references=[reference],
            policy=TEST_POLICY,
        )


def readdress_normalized(
    root: Path,
    snapshot: NormalizedSnapshot,
    mutate: Callable[[dict[str, Any]], None],
    *,
    readdress: bool = True,
) -> str:
    snapshot_dir = root / "normalized" / "snapshots" / snapshot.snapshot_id
    manifest_path = snapshot_dir / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutate(payload)
    if readdress:
        raw_references = tuple(
            RawObjectReference(**item) for item in payload["upstream_raw_references"]
        )
        partitions = tuple(PartitionManifest(**item) for item in payload["partitions"])
        if payload.get("layout_version") == normalized_v3_module.LAYOUT_VERSION:
            index_payload = dict(payload["event_claim_index"])
            index_payload["shards"] = tuple(
                lake_module.EventClaimShardManifest(**item) for item in index_payload["shards"]
            )
            identity = normalized_v3_module._snapshot_payload_v3(
                provider=payload["provider"],
                venue=payload["venue"],
                created_at=payload["created_at"],
                upstream_raw_references=raw_references,
                partitions=partitions,
                event_claim_index=lake_module.EventClaimIndexManifest(**index_payload),
                l2_checkpoints=tuple(
                    lake_module.L2CheckpointManifest(**item)
                    for item in payload.get("l2_checkpoints", [])
                ),
            )
        else:
            identity = lake_module._normalized_snapshot_payload(
                provider=payload["provider"],
                venue=payload["venue"],
                created_at=payload["created_at"],
                upstream_raw_references=raw_references,
                event_claims=tuple(EventClaimReference(**item) for item in payload["event_claims"]),
                partitions=partitions,
            )
        logical_sha256 = lake_module._sha256_bytes(lake_module._canonical_json_bytes(identity))
        payload["logical_sha256"] = logical_sha256
        payload["snapshot_id"] = f"sha256-{logical_sha256}"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    if payload["snapshot_id"] != snapshot.snapshot_id:
        destination = snapshot_dir.with_name(payload["snapshot_id"])
        snapshot_dir.rename(destination)
    return str(payload["snapshot_id"])


@pytest.mark.parametrize(
    ("mutate", "readdress", "message"),
    [
        (lambda payload: payload.__setitem__("layer", "raw"), False, "identity mismatch"),
        (lambda payload: payload.__setitem__("provider", "okx"), True, "Raw source"),
        (
            lambda payload: payload["partitions"].append(deepcopy(payload["partitions"][0])),
            True,
            "duplicate partition",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__("provider", "okx"),
            True,
            "provider/venue",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__(
                "schema_id", "puresaber.quote-event"
            ),
            True,
            "event/schema",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__(
                "relative_path", "provider=binance/wrong/data.parquet"
            ),
            True,
            "path metadata",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__(
                "rows", payload["partitions"][0]["rows"] + 1
            ),
            True,
            "row count",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__("logical_sha256", "0" * 64),
            True,
            "logical content",
        ),
        (lambda payload: payload.__setitem__("rows", payload["rows"] + 1), False, "total row"),
        (
            lambda payload: payload["event_claim_index"].__setitem__(
                "rows", payload["event_claim_index"]["rows"] + 1
            ),
            True,
            "event claims",
        ),
    ],
)
def test_normalized_manifest_defense_branches(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None],
    readdress: bool,
    message: str,
) -> None:
    snapshot = normalized(tmp_path)
    snapshot_id = readdress_normalized(tmp_path, snapshot, mutate, readdress=readdress)
    with pytest.raises(ValidationError, match=message):
        load_normalized_snapshot(tmp_path, snapshot_id)


def test_normalized_file_shape_and_private_guard_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = normalized(tmp_path)
    snapshot_dir = tmp_path / "normalized" / "snapshots" / snapshot.snapshot_id
    (snapshot_dir / "unexpected.bin").write_bytes(b"unexpected")
    with pytest.raises(ValidationError, match="unexpected or missing"):
        load_normalized_snapshot(tmp_path, snapshot.snapshot_id)
    (snapshot_dir / "unexpected.bin").unlink()

    with pytest.raises(ValidationError, match="Unsafe partition"):
        lake_module._safe_snapshot_partition(tmp_path, snapshot_dir, "../escape.parquet")
    directory_partition = snapshot_dir / "directory.parquet"
    directory_partition.mkdir()
    with pytest.raises(ValidationError, match="partition missing"):
        lake_module._safe_snapshot_partition(tmp_path, snapshot_dir, "directory.parquet")

    with pytest.raises(ValidationError, match="differs from the verified manifest"):
        DuckDBSnapshot(tmp_path, replace(snapshot, rows=snapshot.rows + 1))

    real_load = lake_module.load_normalized_snapshot

    def mismatched_load(root: Path, snapshot_id: str) -> NormalizedSnapshot:
        return replace(real_load(root, snapshot_id), logical_sha256="0" * 64)

    monkeypatch.setattr(lake_module, "load_normalized_snapshot", mismatched_load)
    with pytest.raises(ValidationError, match="snapshot collision"):
        write_normalized_events(
            tmp_path,
            [trade("branch-gates")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=list(snapshot.upstream_raw_references),
            policy=TEST_POLICY,
        )


def test_atomic_staging_and_missing_claim_slow_path_guards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = lake_module._resolved_lake_root(tmp_path / "atomic", create=True)
    target = root / "metadata.json"
    identity = target.relative_to(root).as_posix()
    prefix = f".atomic-{lake_module._sha256_bytes(identity.encode('utf-8'))}-"
    (root / f"{prefix}invalid.tmp").mkdir()
    with (
        lake_module._lake_lock(root, "atomic-test", {"target": target.name}),
        pytest.raises(ValidationError, match="not a file"),
    ):
        lake_module._atomic_write_bytes(root, target, b"body")
    (root / f"{prefix}invalid.tmp").rmdir()

    real_hash = lake_module._sha256_file

    def wrong_staging_hash(path: Path) -> str:
        if path.name.startswith(prefix):
            return "0" * 64
        return real_hash(path)

    monkeypatch.setattr(lake_module, "_sha256_file", wrong_staging_hash)
    with (
        lake_module._lake_lock(root, "atomic-test", {"target": target.name}),
        pytest.raises(ValidationError, match="verification failed"),
    ):
        lake_module._atomic_write_bytes(root, target, b"body")
    monkeypatch.setattr(lake_module, "_sha256_file", real_hash)

    staging_root = root / "stable"
    operation = {"batch": "one"}
    operation_hash = lake_module._sha256_bytes(lake_module._canonical_json_bytes(operation))
    invalid_stage = staging_root / f"normalized-batch-{operation_hash}-invalid"
    lake_module._mkdir_in_lake(root, staging_root)
    invalid_stage.write_bytes(b"not-a-directory")
    with (
        pytest.raises(ValidationError, match="not a directory"),
        lake_module._stable_staging_directory(
            root,
            staging_root,
            namespace="normalized-batch",
            identity=operation,
        ),
    ):
        pass

    claim = lake_module._event_claim_reference(TRADE_EVENT_SCHEMA_ID, trade("missing-claim"))
    empty_root = lake_module._resolved_lake_root(tmp_path / "empty", create=True)
    assert lake_module._recover_missing_event_claim(empty_root, claim) is False
    snapshots_root = empty_root / "normalized" / "snapshots"
    snapshots_root.mkdir(parents=True)
    (snapshots_root / ("sha256-" + "0" * 64)).write_bytes(b"not-a-directory")
    with pytest.raises(ValidationError, match="not a directory"):
        lake_module._recover_missing_event_claim(empty_root, claim)


def test_normalized_claim_schema_and_manifest_file_shape(tmp_path: Path) -> None:
    snapshot = normalized(tmp_path)
    claim_path = next((tmp_path / "normalized" / "event-claim-index-v3").rglob("*.parquet"))
    claim_path.write_bytes(claim_path.read_bytes() + b"tampered")
    with pytest.raises(ValidationError, match="physical content changed"):
        load_normalized_snapshot(tmp_path, snapshot.snapshot_id)

    other_root = tmp_path / "manifest-directory"
    other_snapshot = normalized(other_root)
    manifest_path = (
        other_root / "normalized" / "snapshots" / other_snapshot.snapshot_id / "manifest.json"
    )
    manifest_path.unlink()
    manifest_path.mkdir()
    with pytest.raises(ValidationError, match="manifest missing"):
        load_normalized_snapshot(other_root, other_snapshot.snapshot_id)


@pytest.mark.parametrize(
    ("mutate", "readdress", "message"),
    [
        (lambda payload: payload.__setitem__("layer", "raw"), False, "identity mismatch"),
        (lambda payload: payload.__setitem__("provider", "okx"), True, "Raw source"),
        (
            lambda payload: payload["partitions"].append(deepcopy(payload["partitions"][0])),
            True,
            "duplicate partition",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__("provider", "okx"),
            True,
            "provider/venue",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__(
                "schema_id", "puresaber.quote-event"
            ),
            True,
            "event/schema",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__(
                "relative_path", "provider=binance/wrong/data.parquet"
            ),
            True,
            "path metadata",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__(
                "rows", payload["partitions"][0]["rows"] + 1
            ),
            True,
            "row count",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__("logical_sha256", "0" * 64),
            True,
            "logical content",
        ),
        (lambda payload: payload.__setitem__("rows", payload["rows"] + 1), False, "total row"),
        (lambda payload: payload.__setitem__("event_claims", []), True, "event claims"),
    ],
)
def test_legacy_normalized_manifest_remains_strictly_verifiable(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None],
    readdress: bool,
    message: str,
) -> None:
    snapshot = legacy_normalized(tmp_path)
    snapshot_id = readdress_normalized(tmp_path, snapshot, mutate, readdress=readdress)
    with pytest.raises(ValidationError, match=message):
        load_normalized_snapshot(tmp_path, snapshot_id)


def test_legacy_claim_recovery_conflict_and_file_shape_are_fail_closed(tmp_path: Path) -> None:
    snapshot = legacy_normalized(tmp_path)
    claim = snapshot.event_claims[0]
    claim_path = lake_module._event_claim_path(tmp_path, claim)
    claim_path.unlink()

    second_raw = write_raw_bytes(
        tmp_path,
        source="binance",
        request={"fixture": "legacy-second"},
        collected_at="2026-01-02T00:00:00Z",
        payload=b"legacy-second",
        idempotency_key="legacy-second",
        policy=TEST_POLICY,
    )
    recovered = lake_module._write_normalized_events_legacy(
        tmp_path,
        [trade("legacy-branch-gates")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[second_raw.reference()],
        policy=TEST_POLICY,
    )
    assert recovered.snapshot is not None
    assert claim_path.is_file()

    changed = trade("legacy-branch-gates")
    changed["price"]["units"] += 1
    third_raw = write_raw_bytes(
        tmp_path,
        source="binance",
        request={"fixture": "legacy-third"},
        collected_at="2026-01-02T00:00:00Z",
        payload=b"legacy-third",
        idempotency_key="legacy-third",
        policy=TEST_POLICY,
    )
    with pytest.raises(ValidationError, match="Conflicting lake event_id claim"):
        lake_module._write_normalized_events_legacy(
            tmp_path,
            [changed],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[third_raw.reference()],
            policy=TEST_POLICY,
        )

    payload = json.loads(claim_path.read_text(encoding="utf-8"))
    payload["layer"] = "wrong"
    claim_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="claim schema changed"):
        load_normalized_snapshot(tmp_path, snapshot.snapshot_id)


def test_legacy_snapshot_rejects_unexpected_file(tmp_path: Path) -> None:
    snapshot = legacy_normalized(tmp_path)
    snapshot_dir = tmp_path / "normalized" / "snapshots" / snapshot.snapshot_id
    (snapshot_dir / "unexpected.bin").write_bytes(b"unexpected")
    with pytest.raises(ValidationError, match="unexpected or missing"):
        lake_module._load_normalized_snapshot(
            tmp_path,
            snapshot.snapshot_id,
            verify_event_claim_files=False,
        )


def test_raw_path_staging_and_archive_negative_guards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = write_raw_bytes(
        tmp_path,
        source="binance",
        request={"fixture": "raw-guards"},
        collected_at="2026-01-02T00:00:00Z",
        payload=b"raw-guards",
        idempotency_key="raw-guards",
        policy=TEST_POLICY,
    )
    reference = manifest.reference()
    object_dir = lake_module._raw_object_dir(tmp_path, reference)
    copied = tmp_path / "raw" / "copied-object"
    shutil.copytree(object_dir, copied)
    with pytest.raises(ValidationError, match="directory identity"):
        lake_module._load_raw_from_dir(tmp_path, copied)
    with pytest.raises(ValidationError, match="trusted reference"):
        lake_module._load_raw_from_dir(
            tmp_path,
            object_dir,
            expected=replace(reference, idempotency_key="other-key"),
        )

    staging_root = lake_module._mkdir_in_lake(tmp_path, tmp_path / "raw" / ".staging")
    non_directory = staging_root / f"{lake_module._raw_stage_prefix(reference)}file"
    non_directory.write_bytes(b"not-a-directory")
    assert lake_module._recover_raw_staging(tmp_path, reference, manifest) is None

    assert lake_module._is_reparse_point(tmp_path / "missing") is False
    monkeypatch.setattr(lake_module, "_is_reparse_point", lambda _: True)
    with pytest.raises(ValidationError, match="cannot be a reparse point"):
        lake_module._resolved_lake_root(tmp_path, create=False)
    monkeypatch.undo()

    archive = tmp_path.parent / f"{tmp_path.name}-archive.bin"
    archive.write_bytes(b"archive")
    assert lake_module._local_archive_path(archive.as_uri()) == archive.resolve()
    with pytest.raises(ValidationError, match="non-empty"):
        lake_module._local_archive_path(" ")
    with pytest.raises(ValidationError, match="Remote archive cleanup"):
        lake_module._local_archive_path("https://example.invalid/archive.bin")
    with pytest.raises(ValidationError, match="missing, unreadable"):
        lake_module._local_archive_path("relative-missing.bin")


def test_cleanup_audit_and_legacy_claim_malformed_evidence_fail_closed(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw-audit"
    manifest = write_raw_bytes(
        raw_root,
        source="binance",
        request={"fixture": "audit"},
        collected_at="2026-01-02T00:00:00Z",
        payload=b"audit",
        idempotency_key="audit",
        policy=TEST_POLICY,
    )
    reference = manifest.reference()
    tombstone = lake_module._raw_tombstone_path(raw_root, reference)
    tombstone.parent.mkdir(parents=True, exist_ok=True)
    tombstone.write_bytes(b"not-json")
    with pytest.raises(ValidationError, match="audit is unreadable"):
        lake_module._read_cleanup_audit(raw_root, reference)
    tombstone.write_text(json.dumps({"audit_sha256": "0" * 64}), encoding="utf-8")
    with pytest.raises(ValidationError, match="audit integrity changed"):
        lake_module._read_cleanup_audit(raw_root, reference)

    unreadable_root = tmp_path / "claim-unreadable"
    unreadable = legacy_normalized(unreadable_root)
    unreadable_claim = unreadable.event_claims[0]
    unreadable_path = lake_module._event_claim_path(unreadable_root, unreadable_claim)
    unreadable_path.write_bytes(b"not-json")
    with pytest.raises(ValidationError, match="claim is unreadable"):
        lake_module._validate_event_claim(unreadable_root, unreadable_claim)

    malformed_root = tmp_path / "claim-malformed"
    malformed = legacy_normalized(malformed_root)
    malformed_claim = malformed.event_claims[0]
    malformed_path = lake_module._event_claim_path(malformed_root, malformed_claim)
    payload = json.loads(malformed_path.read_text(encoding="utf-8"))
    payload.pop("event_id")
    malformed_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="claim is malformed"):
        lake_module._validate_event_claim(malformed_root, malformed_claim)

    absent = lake_module._event_claim_reference(
        TRADE_EVENT_SCHEMA_ID,
        trade("not-present-in-any-snapshot"),
    )
    assert lake_module._recover_missing_event_claim(malformed_root, absent) is False


def readdress_curated(
    root: Path,
    snapshot_id: str,
    mutate: Callable[[dict[str, Any]], None],
    *,
    readdress: bool = True,
) -> str:
    snapshot_dir = root / "curated" / "branch-bars" / "snapshots" / snapshot_id
    manifest_path = snapshot_dir / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutate(payload)
    if readdress:
        identity = curated_module._snapshot_identity(
            dataset=payload["dataset"],
            revision_id=payload["revision_id"],
            recipe_version=payload["recipe_version"],
            created_at=payload["created_at"],
            lineage=payload["lineage"],
            partitions=tuple(CuratedPartition(**item) for item in payload["partitions"]),
        )
        logical_sha256 = curated_module._hash_bytes(curated_module._canonical(identity))
        payload["logical_sha256"] = logical_sha256
        payload["snapshot_id"] = f"sha256-{logical_sha256}"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    if payload["snapshot_id"] != snapshot_id:
        snapshot_dir.rename(snapshot_dir.with_name(payload["snapshot_id"]))
    return str(payload["snapshot_id"])


def curated(root: Path):
    source = normalized(root)
    return curate_trade_bars_from_snapshot(
        root,
        normalized_snapshot_id=source.snapshot_id,
        dataset="branch-bars",
        revision_id="revision-1",
        recipe_version="session-bars-v1",
        interval=timedelta(minutes=1),
        session_starts={"binance-24x7-BTC-USDT-SPOT": datetime(2026, 1, 2, tzinfo=timezone.utc)},
        policy=TEST_POLICY,
    )


@pytest.mark.parametrize(
    ("mutate", "readdress", "message"),
    [
        (lambda payload: payload.__setitem__("layer", "raw"), False, "identity mismatch"),
        (lambda payload: payload.__setitem__("lineage", {"bad": "value"}), True, "lineage"),
        (
            lambda payload: payload["lineage"].__setitem__("normalized_logical_sha256", "0" * 64),
            True,
            "lineage hash",
        ),
        (
            lambda payload: payload["partitions"].append(deepcopy(payload["partitions"][0])),
            True,
            "duplicate partition",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__(
                "schema_id", "puresaber.trade-event"
            ),
            True,
            "frozen Bar schema",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__(
                "relative_path", "date=wrong/data.parquet"
            ),
            True,
            "path metadata",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__(
                "rows", payload["partitions"][0]["rows"] + 1
            ),
            True,
            "row count",
        ),
        (
            lambda payload: payload["partitions"][0].__setitem__("logical_sha256", "0" * 64),
            True,
            "logical content",
        ),
        (lambda payload: payload.__setitem__("rows", payload["rows"] + 1), False, "row count"),
    ],
)
def test_curated_manifest_defense_branches(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None],
    readdress: bool,
    message: str,
) -> None:
    snapshot = curated(tmp_path)
    snapshot_id = readdress_curated(tmp_path, snapshot.snapshot_id, mutate, readdress=readdress)
    with pytest.raises(ValidationError, match=message):
        curated_module._load_curated_snapshot(
            tmp_path,
            "branch-bars",
            snapshot_id,
            verify_revision_registry=False,
        )


def test_curated_manifest_file_and_extra_file_guards(tmp_path: Path) -> None:
    snapshot = curated(tmp_path)
    snapshot_dir = tmp_path / "curated" / "branch-bars" / "snapshots" / snapshot.snapshot_id
    (snapshot_dir / "extra.bin").write_bytes(b"extra")
    with pytest.raises(ValidationError, match="unexpected or missing"):
        curated_module._load_curated_snapshot(
            tmp_path,
            "branch-bars",
            snapshot.snapshot_id,
            verify_revision_registry=False,
        )

    other_root = tmp_path / "manifest-directory"
    other = curated(other_root)
    manifest_path = other_root / "curated" / "branch-bars" / "snapshots" / other.snapshot_id
    manifest_path = manifest_path / "manifest.json"
    manifest_path.unlink()
    manifest_path.mkdir()
    with pytest.raises(ValidationError, match="manifest missing"):
        curated_module._load_curated_snapshot(
            other_root,
            "branch-bars",
            other.snapshot_id,
            verify_revision_registry=False,
        )


def test_l2_reconstructed_empty_level_guard() -> None:
    empty = ReconstructedLevel(100, 2, 0, 2, None)
    with pytest.raises(L2ReplayError, match="empty price level"):
        L2BookReconstructor._assert_book_valid({100: empty}, {})
