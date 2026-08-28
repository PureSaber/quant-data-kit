from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import pytest

from quant_data_kit.data_lake import (
    ArchiveReceipt,
    CollectionStoppedError,
    DuckDBCatalog,
    StoragePolicy,
    cleanup_archived_raw_object,
    evaluate_capacity,
    load_normalized_snapshot,
    load_raw_object,
    require_collection_capacity,
    write_normalized_events,
    write_raw_bytes,
)
from quant_data_kit.exceptions import ValidationError

UTC = timezone.utc
TEST_POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)


def trade_record(
    *,
    event_id: str,
    instrument_id: str = "BTC-USDT-SPOT",
    event_time: str = "2026-01-02T00:00:01Z",
    sequence: int | None = None,
) -> dict:
    return {
        "event_type": "trade",
        "event_id": event_id,
        "instrument_id": instrument_id,
        "event_time": event_time,
        "received_at": event_time,
        "available_at": event_time,
        "source": "binance",
        "trading_day": "2026-01-02",
        "session_id": f"binance-24x7-{instrument_id}",
        "sequence": sequence,
        "price": {"units": 100_000, "scale": 2},
        "quantity": {"units": 10, "scale": 3},
        "aggressor_side": "buy",
    }


def admitted_raw(
    root: Path,
    *,
    source: str = "binance",
    key: str = "fixture-raw",
    payload: bytes = b"trusted-raw",
):
    return write_raw_bytes(
        root,
        source=source,
        request={"fixture": key},
        collected_at="2026-01-02T00:00:00Z",
        payload=payload,
        idempotency_key=key,
        policy=TEST_POLICY,
    )


def test_raw_bytes_are_immutable_idempotent_and_conflict_on_key_reuse(tmp_path: Path) -> None:
    collected_at = datetime(2026, 1, 2, tzinfo=UTC)
    first = write_raw_bytes(
        tmp_path,
        source="binance",
        request={"stream": "btcusdt@trade"},
        collected_at=collected_at,
        payload=b"exact-wire-bytes",
        idempotency_key="capture-1",
        policy=TEST_POLICY,
    )
    second = write_raw_bytes(
        tmp_path,
        source="binance",
        request={"stream": "btcusdt@trade"},
        collected_at=collected_at,
        payload=b"exact-wire-bytes",
        idempotency_key="capture-1",
        policy=TEST_POLICY,
    )
    assert first == second
    assert first.hot_retention_days == 30
    assert first.hot_until == "2026-02-01T00:00:00Z"
    loaded, payload = load_raw_object(tmp_path, first.reference())
    assert loaded.content_sha256 == first.content_sha256
    assert payload == b"exact-wire-bytes"

    with pytest.raises(ValidationError, match="Conflicting immutable Raw idempotency key"):
        write_raw_bytes(
            tmp_path,
            source="binance",
            request={"stream": "btcusdt@trade"},
            collected_at=collected_at,
            payload=b"changed-wire-bytes",
            idempotency_key="capture-1",
            policy=TEST_POLICY,
        )


def test_raw_mutation_is_detected(tmp_path: Path) -> None:
    raw = write_raw_bytes(
        tmp_path,
        source="okx",
        request={"channel": "trades"},
        collected_at="2026-01-02T00:00:00Z",
        payload=b"original",
        idempotency_key="capture-2",
        policy=TEST_POLICY,
    )
    object_dir = next(tmp_path.rglob(f"object={raw.object_id}"))
    (object_dir / "payload.bin").write_bytes(b"mutation")
    with pytest.raises(ValidationError, match="hash changed"):
        load_raw_object(tmp_path, raw.reference())


def test_raw_request_metadata_must_be_canonical_json(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="canonical JSON"):
        write_raw_bytes(
            tmp_path,
            source="binance",
            request={"invalid": {1, 2}},
            collected_at="2026-01-02T00:00:00Z",
            payload=b"raw",
            policy=TEST_POLICY,
        )


def test_capacity_policy_stops_with_explicit_alert() -> None:
    gib = 1024**3
    decision = evaluate_capacity(
        Path("."),
        projected_write_bytes=2 * gib,
        current_hot_bytes=149 * gib,
        disk_total_bytes=500 * gib,
        disk_free_bytes=101 * gib,
    )
    assert decision.allowed is False
    assert decision.alert is not None and decision.alert.startswith("COLLECTION_STOPPED")
    assert len(decision.reasons) == 2
    with pytest.raises(CollectionStoppedError, match="COLLECTION_STOPPED"):
        require_collection_capacity(
            Path("."),
            projected_write_bytes=2 * gib,
            current_hot_bytes=149 * gib,
            disk_total_bytes=500 * gib,
            disk_free_bytes=101 * gib,
        )


def test_cleanup_requires_verified_archive_restore_and_explicit_confirmation(
    tmp_path: Path,
) -> None:
    manifest = write_raw_bytes(
        tmp_path,
        source="cn-fixture",
        request={"channel": "l2"},
        collected_at="2026-01-02T01:00:00Z",
        payload=b"desensitized-l2",
        idempotency_key="capture-archive",
        policy=TEST_POLICY,
    )
    archive_path = (tmp_path.parent / f"{tmp_path.name}-archive.bin").resolve()
    archive_path.write_bytes(b"desensitized-l2")
    receipt = ArchiveReceipt(
        object_id=manifest.object_id,
        archive_uri=str(archive_path),
        source_sha256=manifest.content_sha256,
        archive_sha256=manifest.content_sha256,
        restored_sha256=manifest.content_sha256,
        verified_at="2026-01-03T00:00:00Z",
    )
    with pytest.raises(ValidationError, match="confirm=True"):
        cleanup_archived_raw_object(tmp_path, manifest.reference(), receipt)
    bad = ArchiveReceipt(**{**receipt.__dict__, "restored_sha256": "0" * 64})
    with pytest.raises(ValidationError, match="hash validation"):
        cleanup_archived_raw_object(
            tmp_path,
            manifest.reference(),
            bad,
            confirm=True,
            now="2026-02-02T00:00:00Z",
        )
    with pytest.raises(ValidationError, match="hot-retention window"):
        cleanup_archived_raw_object(
            tmp_path,
            manifest.reference(),
            receipt,
            confirm=True,
            now="2026-01-31T00:00:00Z",
        )
    audit = cleanup_archived_raw_object(
        tmp_path,
        manifest.reference(),
        receipt,
        confirm=True,
        now="2026-02-02T00:00:00Z",
    )
    assert audit.is_file()
    with pytest.raises(ValidationError, match="unavailable"):
        load_raw_object(tmp_path, manifest.reference())
    with pytest.raises(ValidationError, match="already archived and cleaned"):
        write_raw_bytes(
            tmp_path,
            source="cn-fixture",
            request={"channel": "l2"},
            collected_at="2026-01-02T01:00:00Z",
            payload=b"desensitized-l2",
            idempotency_key="capture-archive",
            policy=TEST_POLICY,
        )


def test_normalized_partitions_quarantine_bad_stream_and_pin_duckdb_snapshot(
    tmp_path: Path,
) -> None:
    raw = admitted_raw(tmp_path)
    accepted = trade_record(event_id="trade-ok")
    duplicate_one = trade_record(event_id="duplicate", instrument_id="ETH-USDT-SPOT")
    duplicate_two = deepcopy(duplicate_one)
    duplicate_two["event_time"] = "2026-01-02T00:00:02Z"
    duplicate_two["received_at"] = duplicate_two["event_time"]
    duplicate_two["available_at"] = duplicate_two["event_time"]
    result = write_normalized_events(
        tmp_path,
        [accepted, duplicate_one, duplicate_two],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    assert result.accepted_rows == 1
    assert result.quarantined_rows == 2
    assert result.quarantine_manifest is not None and result.quarantine_manifest.is_file()
    snapshot = load_normalized_snapshot(tmp_path, result.snapshot.snapshot_id)
    assert snapshot.rows == 1
    assert "provider=binance/venue=BINANCE/event_type=trade" in snapshot.partitions[0].relative_path

    repeated = write_normalized_events(
        tmp_path,
        [accepted],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )
    assert repeated.snapshot is not None
    assert repeated.snapshot.snapshot_id == snapshot.snapshot_id
    with DuckDBCatalog(tmp_path).open_snapshot(snapshot.snapshot_id) as catalog:
        table = catalog.query("SELECT instrument_id, count(*) AS rows FROM event_trade GROUP BY 1")
        assert table.to_pylist() == [{"instrument_id": "BTC-USDT-SPOT", "rows": 1}]
        with pytest.raises(ValidationError, match="read-only"):
            catalog.query("CREATE TABLE forbidden AS SELECT 1")
        with pytest.raises(ValidationError, match="non-read-only"):
            catalog.query("SELECT 1; COPY (SELECT 1) TO 'forbidden.parquet'")
    with pytest.raises(ValidationError, match="reserved"):
        DuckDBCatalog(tmp_path).open_snapshot("latest")


def test_normalized_partition_mutation_fails_hash_validation(tmp_path: Path) -> None:
    raw = admitted_raw(tmp_path)
    result = write_normalized_events(
        tmp_path,
        [trade_record(event_id="trade-hash")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    snapshot_dir = tmp_path / "normalized" / "snapshots" / result.snapshot.snapshot_id
    partition = snapshot_dir / result.snapshot.partitions[0].relative_path
    partition.write_bytes(partition.read_bytes() + b"mutation")
    with pytest.raises(ValidationError, match="partition hash changed"):
        load_normalized_snapshot(tmp_path, result.snapshot.snapshot_id)


def test_normalized_l2_cross_is_quarantined_before_research_layer(tmp_path: Path) -> None:
    raw = admitted_raw(tmp_path)
    book_snapshot = {
        "event_type": "book_snapshot",
        "event_id": "snapshot-1",
        "instrument_id": "BTC-USDT-SPOT",
        "event_time": "2026-01-02T00:00:00Z",
        "received_at": "2026-01-02T00:00:00Z",
        "available_at": "2026-01-02T00:00:00Z",
        "source": "binance",
        "trading_day": "2026-01-02",
        "session_id": "binance-24x7-BTC-USDT-SPOT",
        "sequence": 10,
        "bids": [
            {
                "price": {"units": 100_000, "scale": 2},
                "quantity": {"units": 1, "scale": 0},
                "order_count": None,
            }
        ],
        "asks": [
            {
                "price": {"units": 100_100, "scale": 2},
                "quantity": {"units": 1, "scale": 0},
                "order_count": None,
            }
        ],
    }
    crossed_delta = {
        "event_type": "book_delta",
        "event_id": "delta-11",
        "instrument_id": "BTC-USDT-SPOT",
        "event_time": "2026-01-02T00:00:01Z",
        "received_at": "2026-01-02T00:00:01Z",
        "available_at": "2026-01-02T00:00:01Z",
        "source": "binance",
        "trading_day": "2026-01-02",
        "session_id": "binance-24x7-BTC-USDT-SPOT",
        "sequence": 11,
        "side": "bid",
        "action": "upsert",
        "price": {"units": 100_100, "scale": 2},
        "quantity": {"units": 1, "scale": 0},
        "previous_sequence": 10,
    }
    result = write_normalized_events(
        tmp_path,
        [book_snapshot, crossed_delta],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is None
    assert result.accepted_rows == 0
    assert result.quarantined_rows == 2
    assert result.quarantine_manifest is not None


def test_normalized_l2_checkpoint_mismatch_is_quarantined(tmp_path: Path) -> None:
    raw = admitted_raw(tmp_path)
    book_snapshot = {
        "event_type": "book_snapshot",
        "event_id": "snapshot-checksum",
        "instrument_id": "BTC-USDT-SPOT",
        "event_time": "2026-01-02T00:00:00Z",
        "received_at": "2026-01-02T00:00:00Z",
        "available_at": "2026-01-02T00:00:00Z",
        "source": "binance",
        "trading_day": "2026-01-02",
        "session_id": "binance-24x7-BTC-USDT-SPOT",
        "sequence": 10,
        "bids": [
            {
                "price": {"units": 100_000, "scale": 2},
                "quantity": {"units": 1, "scale": 0},
                "order_count": None,
            }
        ],
        "asks": [
            {
                "price": {"units": 100_100, "scale": 2},
                "quantity": {"units": 1, "scale": 0},
                "order_count": None,
            }
        ],
    }
    stream_key = ("binance", "BTC-USDT-SPOT", "binance-24x7-BTC-USDT-SPOT")
    result = write_normalized_events(
        tmp_path,
        [book_snapshot],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        expected_l2_checkpoint_hashes={stream_key: {10: "0" * 64}},
        policy=TEST_POLICY,
    )
    assert result.snapshot is None
    assert result.quarantined_rows == 1
    assert result.quarantine_manifest is not None
    assert "checkpoint hash mismatch" in result.quarantine_manifest.with_name(
        "records.jsonl"
    ).read_text(encoding="utf-8")


def test_provider_mismatch_and_non_finite_payload_are_quarantine_evidence(
    tmp_path: Path,
) -> None:
    raw = admitted_raw(tmp_path)
    wrong_source = trade_record(event_id="wrong-source")
    wrong_source["source"] = "okx"
    non_finite = trade_record(event_id="non-finite", instrument_id="ETH-USDT-SPOT")
    non_finite["price"]["units"] = float("nan")
    result = write_normalized_events(
        tmp_path,
        [wrong_source, non_finite],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is None
    assert result.quarantined_rows == 2
    assert result.quarantine_manifest is not None
    records_path = result.quarantine_manifest.with_name("records.jsonl")
    evidence = records_path.read_text(encoding="utf-8")
    assert "invalid_float" in evidence
    assert "stream source does not match provider" in evidence
