from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

import quant_data_kit.curated as curated_module
import quant_data_kit.data_lake as lake_module
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
        identity = lake_module._normalized_snapshot_payload(
            provider=payload["provider"],
            venue=payload["venue"],
            created_at=payload["created_at"],
            upstream_raw_references=tuple(
                RawObjectReference(**item) for item in payload["upstream_raw_references"]
            ),
            event_claims=tuple(EventClaimReference(**item) for item in payload["event_claims"]),
            partitions=tuple(PartitionManifest(**item) for item in payload["partitions"]),
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
        (lambda payload: payload.__setitem__("event_claims", []), True, "event claims"),
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
    claim_path = next((tmp_path / "normalized" / "event-claims").rglob("*.json"))
    claim = snapshot.event_claims[0]
    payload = json.loads(claim_path.read_text(encoding="utf-8"))
    payload["layer"] = "wrong"
    claim_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="claim schema"):
        lake_module._validate_event_claim(tmp_path, claim)

    other_root = tmp_path / "manifest-directory"
    other_snapshot = normalized(other_root)
    manifest_path = (
        other_root / "normalized" / "snapshots" / other_snapshot.snapshot_id / "manifest.json"
    )
    manifest_path.unlink()
    manifest_path.mkdir()
    with pytest.raises(ValidationError, match="manifest missing"):
        load_normalized_snapshot(other_root, other_snapshot.snapshot_id)


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
