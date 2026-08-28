from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

import quant_data_kit.data_lake as lake_module
from quant_data_kit.curated import curate_trade_bars_from_snapshot, load_curated_snapshot
from quant_data_kit.data_lake import (
    ArchiveReceipt,
    CollectionStoppedError,
    DuckDBCatalog,
    RawObjectReference,
    StoragePolicy,
    cleanup_archived_raw_object,
    load_normalized_snapshot,
    load_raw_object,
    write_normalized_events,
    write_raw_bytes,
)
from quant_data_kit.exceptions import ValidationError

TEST_POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)
STOP_POLICY = StoragePolicy(
    hot_quota_bytes=1,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)


def trade(
    event_id: str,
    *,
    source: str = "binance",
    instrument_id: str = "BTC-USDT-SPOT",
    timestamp: str = "2026-01-02T00:00:01.001Z",
) -> dict:
    return {
        "event_type": "trade",
        "event_id": event_id,
        "instrument_id": instrument_id,
        "event_time": timestamp,
        "received_at": timestamp,
        "available_at": timestamp,
        "source": source,
        "trading_day": "2026-01-02",
        "session_id": f"{source}-24x7-{instrument_id}",
        "sequence": 1,
        "price": {"units": 100_000, "scale": 2},
        "quantity": {"units": 10, "scale": 3},
        "aggressor_side": "buy",
    }


def raw(root: Path, *, key: str = "raw-1", source: str = "binance"):
    return write_raw_bytes(
        root,
        source=source,
        request={"fixture": key},
        collected_at="2026-01-02T00:00:00Z",
        payload=f"payload-{key}".encode(),
        idempotency_key=key,
        policy=TEST_POLICY,
    )


def normalized(root: Path, *, key: str = "raw-1", record: dict | None = None):
    admitted = raw(root, key=key)
    result = write_normalized_events(
        root,
        [record or trade(f"trade-{key}")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    return result.snapshot


def test_windows_illegal_colon_is_rejected_and_atomic_publish_is_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValidationError, match="safe path segment"):
        write_raw_bytes(
            tmp_path,
            source="C:",
            request={},
            collected_at="2026-01-02T00:00:00Z",
            payload=b"raw",
            policy=TEST_POLICY,
        )

    real_replace = lake_module.os.replace

    def interrupted_replace(source: Path, destination: Path) -> None:
        if Path(source).parent.name == ".staging" and Path(destination).name.startswith("object="):
            raise OSError("simulated publish interruption")
        real_replace(source, destination)

    monkeypatch.setattr(lake_module.os, "replace", interrupted_replace)
    with pytest.raises(OSError, match="interruption"):
        raw(tmp_path, key="interrupted")
    assert not list(tmp_path.rglob("key=interrupted/object=*"))
    monkeypatch.setattr(lake_module.os, "replace", real_replace)
    recovered = raw(tmp_path, key="interrupted")
    assert load_raw_object(tmp_path, recovered.reference())[1] == b"payload-interrupted"


def test_preexisting_half_object_is_quarantined_without_blocking_retry(tmp_path: Path) -> None:
    template_root = tmp_path / "template"
    target_root = tmp_path / "target"
    expected = raw(template_root, key="half")
    partial = (
        target_root
        / "raw"
        / "source=binance"
        / "date=2026-01-02"
        / "key=half"
        / f"object={expected.object_id}"
    )
    partial.mkdir(parents=True)
    (partial / "payload.bin").write_bytes(b"partial")
    (partial / "manifest.json").write_text("{}", encoding="utf-8")
    recovered = raw(target_root, key="half")
    assert recovered == expected
    quarantined = list(target_root.glob("quarantine/raw-unpublished/*"))
    assert len(quarantined) == 1
    assert (quarantined[0] / "quarantine.json").is_file()


def test_manifest_anchor_detects_coordinated_retention_and_collection_mutation(
    tmp_path: Path,
) -> None:
    admitted = raw(tmp_path, key="anchored")
    manifest_path = next(tmp_path.rglob(f"object={admitted.object_id}/manifest.json"))
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["collected_at"] = "2026-01-01T00:00:00Z"
    payload["collection_date"] = "2026-01-01"
    payload["hot_until"] = "2026-01-31T00:00:00Z"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="integrity anchor|directory identity"):
        load_raw_object(tmp_path, admitted.reference())


@pytest.mark.skipif(os.name != "nt", reason="Windows junction regression")
def test_raw_write_read_and_cleanup_reject_junction_escape(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    outside = tmp_path / "outside"
    lake.mkdir()
    outside.mkdir()
    junction = lake / "raw"
    result = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(outside)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"junction unavailable: {result.stderr}")
    with pytest.raises(ValidationError, match="reparse point|escapes"):
        raw(lake, key="junction-write")
    assert not list(outside.rglob("payload.bin"))

    safe_lake = tmp_path / "safe-lake"
    admitted = raw(safe_lake, key="junction-read")
    raw_dir = safe_lake / "raw"
    relocated = tmp_path / "relocated-raw"
    raw_dir.rename(relocated)
    result = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(raw_dir), str(relocated)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"object junction unavailable: {result.stderr}")
    with pytest.raises(ValidationError, match="reparse point|escapes"):
        load_raw_object(safe_lake, admitted.reference())
    fake_receipt = ArchiveReceipt(
        object_id=admitted.object_id,
        archive_uri=str((tmp_path / "missing-archive.bin").resolve()),
        source_sha256=admitted.content_sha256,
        archive_sha256=admitted.content_sha256,
        restored_sha256=admitted.content_sha256,
        verified_at="2026-02-02T00:00:00Z",
    )
    with pytest.raises(ValidationError, match="reparse point|escapes"):
        cleanup_archived_raw_object(
            safe_lake,
            admitted.reference(),
            fake_receipt,
            confirm=True,
            now="2026-02-02T00:00:01Z",
        )


@pytest.mark.skipif(os.name == "nt", reason="POSIX symbolic-link regression")
def test_raw_write_rejects_symbolic_link_escape(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    outside = tmp_path / "outside"
    lake.mkdir()
    outside.mkdir()
    (lake / "raw").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValidationError, match="reparse point|escapes"):
        raw(lake, key="symlink-write")
    assert not list(outside.rglob("payload.bin"))


def test_cleanup_rejects_fabricated_missing_and_remote_archives(tmp_path: Path) -> None:
    admitted = raw(tmp_path, key="archive-check")
    common = {
        "object_id": admitted.object_id,
        "source_sha256": admitted.content_sha256,
        "archive_sha256": admitted.content_sha256,
        "restored_sha256": admitted.content_sha256,
        "verified_at": "2026-02-02T00:00:00Z",
    }
    for archive_uri in (str((tmp_path / "missing.bin").resolve()), "s3://bucket/object"):
        with pytest.raises(ValidationError, match="missing|unsupported"):
            cleanup_archived_raw_object(
                tmp_path,
                admitted.reference(),
                ArchiveReceipt(archive_uri=archive_uri, **common),
                confirm=True,
                now="2026-02-02T00:00:01Z",
            )
    raw_payload = next(tmp_path.rglob(f"object={admitted.object_id}/payload.bin"))
    with pytest.raises(ValidationError, match="outside the hot data lake"):
        cleanup_archived_raw_object(
            tmp_path,
            admitted.reference(),
            ArchiveReceipt(archive_uri=str(raw_payload.resolve()), **common),
            confirm=True,
            now="2026-02-02T00:00:01Z",
        )


def test_normalized_requires_existing_unmodified_raw_reference(tmp_path: Path) -> None:
    admitted = raw(tmp_path, key="trusted")
    missing = RawObjectReference(
        **{**admitted.reference().__dict__, "object_id": "sha256-" + "0" * 64}
    )
    with pytest.raises(ValidationError, match="unavailable|Conflicting immutable"):
        write_normalized_events(
            tmp_path,
            [trade("missing-raw")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[missing],
            policy=TEST_POLICY,
        )
    object_dir = next(tmp_path.rglob(f"object={admitted.object_id}"))
    (object_dir / "payload.bin").write_bytes(b"corrupt")
    with pytest.raises(ValidationError, match="length changed|hash changed"):
        write_normalized_events(
            tmp_path,
            [trade("corrupt-raw")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[admitted.reference()],
            policy=TEST_POLICY,
        )


def test_global_event_id_duplicates_across_streams_are_all_quarantined(tmp_path: Path) -> None:
    admitted = raw(tmp_path, key="duplicates")
    first = trade("same-id", instrument_id="BTC-USDT-SPOT")
    second = trade("same-id", instrument_id="ETH-USDT-SPOT")
    result = write_normalized_events(
        tmp_path,
        [first, second],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is None
    assert result.quarantined_rows == 2
    assert "global_duplicate_event_id" in result.quarantine_manifest.with_name(
        "records.jsonl"
    ).read_text(encoding="utf-8")


def test_arrow_canonical_timestamp_and_physical_snapshot_binding(tmp_path: Path) -> None:
    snapshot = normalized(tmp_path, record=trade("fractional"))
    repeated_raw = raw(tmp_path, key="raw-1")
    repeated = write_normalized_events(
        tmp_path,
        [trade("fractional")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[repeated_raw.reference()],
        policy=TEST_POLICY,
    )
    assert repeated.snapshot == snapshot
    assert load_normalized_snapshot(tmp_path, snapshot.snapshot_id) == snapshot

    snapshot_dir = tmp_path / "normalized" / "snapshots" / snapshot.snapshot_id
    partition_path = snapshot_dir / snapshot.partitions[0].relative_path
    table = pq.ParquetFile(partition_path).read()
    pq.write_table(table, partition_path, compression="gzip", use_dictionary=False)
    manifest_path = snapshot_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["partitions"][0]["content_sha256"] = hashlib.sha256(
        partition_path.read_bytes()
    ).hexdigest()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValidationError, match="logical hash changed"):
        load_normalized_snapshot(tmp_path, snapshot.snapshot_id)


def test_duckdb_blocks_direct_cte_and_parameterized_external_reads(tmp_path: Path) -> None:
    snapshot = normalized(tmp_path, key="duckdb")
    with DuckDBCatalog(tmp_path).open_snapshot(snapshot.snapshot_id) as catalog:
        assert catalog.query("SELECT count(*) AS rows FROM event_trade").to_pylist() == [
            {"rows": 1}
        ]
        forbidden = [
            ("SELECT * FROM read_text('secret.txt')", None),
            ("WITH stolen AS (SELECT * FROM read_csv('secret.csv')) SELECT * FROM stolen", None),
            ("SELECT * FROM read_parquet(?)", [str(tmp_path / "secret.parquet")]),
            ("SELECT * FROM glob('*.parquet')", None),
            ("SELECT * FROM pragma_database_list()", None),
        ]
        for sql, parameters in forbidden:
            with pytest.raises(ValidationError, match="external files"):
                catalog.query(sql, parameters)


def test_curated_rejects_fake_lineage_and_binds_physical_content(tmp_path: Path) -> None:
    snapshot = normalized(tmp_path, key="curated-lineage")
    curated = curate_trade_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=snapshot.snapshot_id,
        dataset="audit-bars",
        revision_id="revision-1",
        recipe_version="session-bars-v1",
        interval=timedelta(minutes=1),
        session_starts={
            "binance-24x7-BTC-USDT-SPOT": lake_module._utc_datetime(
                "2026-01-02T00:00:00Z", "session_start"
            )
        },
        policy=TEST_POLICY,
    )
    curated_dir = tmp_path / "curated" / "audit-bars" / "snapshots" / curated.snapshot_id
    manifest_path = curated_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["lineage"]["normalized_snapshot_id"] = "latest"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValidationError, match="reserved|unavailable"):
        load_curated_snapshot(tmp_path, "audit-bars", curated.snapshot_id)

    manifest = json.loads(json.dumps(curated, default=lambda value: value.__dict__))
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    partition_path = curated_dir / curated.partitions[0].relative_path
    table = pq.ParquetFile(partition_path).read()
    pq.write_table(table, partition_path, compression="gzip", use_dictionary=False)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["partitions"][0]["content_sha256"] = hashlib.sha256(
        partition_path.read_bytes()
    ).hexdigest()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValidationError, match="logical hash changed"):
        load_curated_snapshot(tmp_path, "audit-bars", curated.snapshot_id)


def test_normalized_and_curated_writers_apply_capacity_gate(tmp_path: Path) -> None:
    admitted = raw(tmp_path, key="capacity")
    with pytest.raises(CollectionStoppedError, match="COLLECTION_STOPPED"):
        write_normalized_events(
            tmp_path,
            [trade("capacity")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[admitted.reference()],
            policy=STOP_POLICY,
        )

    snapshot = normalized(tmp_path, key="curated-capacity")
    with pytest.raises(CollectionStoppedError, match="COLLECTION_STOPPED"):
        curate_trade_bars_from_snapshot(
            tmp_path,
            normalized_snapshot_id=snapshot.snapshot_id,
            dataset="capacity-bars",
            revision_id="revision-1",
            recipe_version="session-bars-v1",
            interval=timedelta(minutes=1),
            session_starts={
                "binance-24x7-BTC-USDT-SPOT": lake_module._utc_datetime(
                    "2026-01-02T00:00:00Z", "session_start"
                )
            },
            policy=STOP_POLICY,
        )
