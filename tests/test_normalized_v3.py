from __future__ import annotations

import json
import shutil
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

import quant_data_kit.data_lake as lake_module
from quant_data_kit import normalized_v3
from quant_data_kit.adapters_v2.base import BOOK_SEQUENCE_FACTOR
from quant_data_kit.data_lake import (
    CollectionStoppedError,
    StoragePolicy,
    load_normalized_snapshot,
    read_normalized_events,
    write_normalized_batches,
    write_normalized_events,
    write_raw_bytes,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.schemas_v2 import validate_json_record
from tests.test_adapters_v2 import binance_adapter, load_messages
from tests.test_l2_replay import delta, snapshot
from tests.test_m2_audit_regressions import trade

TEST_POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)


def _record_batch(records: list[dict]) -> pa.RecordBatch:
    schema_id = lake_module._event_schema_id(records[0])
    schema = normalized_v3.get_arrow_schema(schema_id)
    prepared = normalized_v3._arrow_ready_rows(schema, [deepcopy(record) for record in records])
    return pa.RecordBatch.from_pylist(prepared, schema=schema)


def _strict_batches(
    root: Path,
    batches,
    *,
    key: str,
    policy: StoragePolicy = TEST_POLICY,
    expected_l2_checkpoint_hashes=None,
):
    admitted = _raw(root, key)
    return write_normalized_batches(
        root,
        batches,
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        expected_l2_checkpoint_hashes=expected_l2_checkpoint_hashes,
        policy=policy,
    )


def _assert_no_snapshot(root: Path) -> None:
    snapshots = root / "normalized" / "snapshots"
    assert not snapshots.exists() or not list(snapshots.glob("sha256-*"))


def _raw(root: Path, key: str = "normalized-v3"):
    return write_raw_bytes(
        root,
        source="binance",
        request={"fixture": key},
        collected_at="2026-01-02T00:00:00Z",
        payload=key.encode(),
        idempotency_key=key,
        policy=TEST_POLICY,
    )


def test_small_golden_legacy_and_v3_have_identical_logical_events_and_claims(
    tmp_path: Path,
) -> None:
    from quant_data_kit.adapters_v2 import adapt_fixture_messages

    records = adapt_fixture_messages(binance_adapter(), load_messages("binance"))
    legacy_root = tmp_path / "legacy"
    current_root = tmp_path / "current"
    legacy_raw = _raw(legacy_root)
    current_raw = _raw(current_root)
    legacy = lake_module._write_normalized_events_legacy(
        legacy_root,
        records,
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[legacy_raw.reference()],
        policy=TEST_POLICY,
    )
    current = write_normalized_events(
        current_root,
        iter(records),
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[current_raw.reference()],
        policy=TEST_POLICY,
    )
    assert legacy.snapshot is not None and current.snapshot is not None
    assert legacy.accepted_rows == current.accepted_rows == len(records)
    assert legacy.quarantined_rows == current.quarantined_rows == 0
    legacy_partitions = {
        (item.event_type, item.trading_date, item.instrument_id): item
        for item in legacy.snapshot.partitions
    }
    current_partitions = {
        (item.event_type, item.trading_date, item.instrument_id): item
        for item in current.snapshot.partitions
    }
    assert legacy_partitions.keys() == current_partitions.keys()
    for key, legacy_partition in legacy_partitions.items():
        current_partition = current_partitions[key]
        assert current_partition.rows == legacy_partition.rows
        assert current_partition.logical_sha256 == legacy_partition.logical_sha256
    assert tuple(current.snapshot.event_claims) == tuple(legacy.snapshot.event_claims)
    for event_type in {record["event_type"] for record in records}:
        assert read_normalized_events(
            current_root,
            current.snapshot.snapshot_id,
            event_type=event_type,
        ) == read_normalized_events(
            legacy_root,
            legacy.snapshot.snapshot_id,
            event_type=event_type,
        )
    assert current.snapshot.layout_version == normalized_v3.LAYOUT_VERSION
    assert current.snapshot.l2_checkpoints


def test_native_writer_validator_matches_frozen_validator_for_l2_edges() -> None:
    valid = [snapshot(), delta(101, 100)]
    invalid = []
    wrong_previous = delta(101, 100)
    wrong_previous["previous_sequence"] = 101
    invalid.append(wrong_previous)
    crossed = snapshot()
    crossed["bids"][0]["price"] = deepcopy(crossed["asks"][0]["price"])
    invalid.append(crossed)
    negative_quantity = delta(101, 100)
    negative_quantity["quantity"]["units"] = -1
    invalid.append(negative_quantity)
    for record in valid:
        schema_id = lake_module._event_schema_id(record)
        validate_json_record(schema_id, deepcopy(record))
        normalized_v3._validate_event_record(schema_id, deepcopy(record))
    for record in invalid:
        schema_id = lake_module._event_schema_id(record)
        with pytest.raises(ValidationError):
            validate_json_record(schema_id, deepcopy(record))
        with pytest.raises(ValidationError):
            normalized_v3._validate_event_record(schema_id, deepcopy(record))


def test_v3_manifest_is_compact_and_does_not_create_per_event_json(tmp_path: Path) -> None:
    admitted = _raw(tmp_path)
    records = [snapshot()]
    for sequence in range(101, 1101):
        item = delta(101, 100)
        item["event_id"] = f"delta-{sequence}"
        item["sequence"] = sequence
        item["previous_sequence"] = sequence - 1
        records.append(item)
    result = write_normalized_events(
        tmp_path,
        records,
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    manifest_path = (
        tmp_path / "normalized" / "snapshots" / result.snapshot.snapshot_id / "manifest.json"
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "event_claims" not in manifest
    assert manifest["event_claim_index"]["rows"] == 1001
    assert manifest_path.stat().st_size < 100_000
    assert not (tmp_path / "normalized" / "event-claims").exists()
    claim_files = list(
        (
            tmp_path
            / "normalized"
            / "event-claim-index-v3"
            / f"snapshot={result.snapshot.snapshot_id}"
        ).rglob("*.parquet")
    )
    assert 1 <= len(claim_files) <= 256


def test_missing_claim_index_recovers_but_tampering_fails_closed(tmp_path: Path) -> None:
    admitted = _raw(tmp_path)
    result = write_normalized_events(
        tmp_path,
        [trade("claim-recovery")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    index_root = (
        tmp_path / "normalized" / "event-claim-index-v3" / f"snapshot={result.snapshot.snapshot_id}"
    )
    next(index_root.rglob("*.parquet")).unlink()
    recovered = load_normalized_snapshot(tmp_path, result.snapshot.snapshot_id)
    assert recovered.snapshot_id == result.snapshot.snapshot_id
    assert list(index_root.rglob("*.parquet"))
    assert list((tmp_path / "normalized" / "event-claim-index-v3" / "recovery-evidence").iterdir())

    claim_path = next(index_root.rglob("*.parquet"))
    claim_path.write_bytes(claim_path.read_bytes() + b"tampered")
    with pytest.raises(ValidationError, match="physical content changed"):
        load_normalized_snapshot(tmp_path, result.snapshot.snapshot_id)

    shutil.rmtree(index_root)
    recovered_again = load_normalized_snapshot(tmp_path, result.snapshot.snapshot_id)
    assert recovered_again.snapshot_id == result.snapshot.snapshot_id


def test_streaming_writer_applies_capacity_gate_before_partition_write(tmp_path: Path) -> None:
    admitted = _raw(tmp_path)
    stopped = StoragePolicy(
        hot_quota_bytes=1,
        minimum_free_bytes=1,
        minimum_free_fraction=0.000001,
    )
    with pytest.raises(CollectionStoppedError, match="COLLECTION_STOPPED"):
        write_normalized_events(
            tmp_path,
            [trade("capacity-stop")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[admitted.reference()],
            policy=stopped,
        )
    assert not list((tmp_path / "normalized" / "snapshots").glob("sha256-*"))


def test_arrow_batches_preserve_events_claims_and_l2_state_across_entrypoints(
    tmp_path: Path,
) -> None:
    from tools.benchmark_normalized_l2 import synthetic_l2_batches, synthetic_l2_events

    mapping_root = tmp_path / "mapping"
    arrow_root = tmp_path / "arrow"
    mapping_raw = _raw(mapping_root, "same-input")
    mapping = write_normalized_events(
        mapping_root,
        synthetic_l2_events(32),
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[mapping_raw.reference()],
        policy=TEST_POLICY,
    )
    arrow = _strict_batches(
        arrow_root,
        synthetic_l2_batches(32, 8),
        key="same-input",
    )
    assert mapping.snapshot is not None and arrow.snapshot is not None
    assert arrow.accepted_rows == mapping.accepted_rows == 32
    assert tuple(arrow.snapshot.event_claims) == tuple(mapping.snapshot.event_claims)
    assert arrow.snapshot.l2_checkpoints == mapping.snapshot.l2_checkpoints
    assert arrow.snapshot.partition_logical_hash_version == normalized_v3.ARROW_IPC_PARTITION_HASH
    assert mapping.snapshot.partition_logical_hash_version == (
        normalized_v3.CANONICAL_JSON_PARTITION_HASH
    )
    for event_type in ("book_snapshot", "book_delta"):
        assert read_normalized_events(
            arrow_root,
            arrow.snapshot.snapshot_id,
            event_type=event_type,
        ) == read_normalized_events(
            mapping_root,
            mapping.snapshot.snapshot_id,
            event_type=event_type,
        )
    assert load_normalized_snapshot(arrow_root, arrow.snapshot.snapshot_id) == arrow.snapshot


def test_record_batch_reader_is_a_supported_strict_input(tmp_path: Path) -> None:
    batch = _record_batch([snapshot()])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    result = _strict_batches(tmp_path, reader, key="reader")
    assert result.snapshot is not None
    assert result.accepted_rows == 1
    assert result.quarantined_rows == 0


def test_encoded_dense_group_is_atomic_across_input_batch_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = snapshot()
    initial["sequence"] = 100 * BOOK_SEQUENCE_FACTOR
    first = delta(101, 100, side="bid", price_units=100_150, quantity_units=25)
    second = delta(
        102,
        101,
        side="ask",
        action="delete",
        price_units=100_100,
        quantity_units=0,
    )
    first["sequence"] = 101 * BOOK_SEQUENCE_FACTOR + 1
    first["previous_sequence"] = initial["sequence"]
    second["sequence"] = 101 * BOOK_SEQUENCE_FACTOR + 2
    second["previous_sequence"] = first["sequence"]
    for index, event in enumerate((first, second), start=1):
        event["event_id"] = f"encoded-delta-101-{index}"
        for field in ("event_time", "received_at", "available_at"):
            event[field] = "2026-01-02T00:00:01Z"

    applied_group_sizes: list[int] = []
    real_apply = normalized_v3.L2BookReconstructor._apply_validated_atomic_delta_group

    def observed_apply(self, **kwargs) -> None:
        applied_group_sizes.append(len(kwargs["sequences"]))
        real_apply(self, **kwargs)

    monkeypatch.setattr(
        normalized_v3.L2BookReconstructor,
        "_apply_validated_atomic_delta_group",
        observed_apply,
    )
    result = _strict_batches(
        tmp_path,
        [
            _record_batch([initial]),
            _record_batch([first]),
            _record_batch([second]),
        ],
        key="encoded-atomic-boundary",
    )
    assert result.snapshot is not None
    assert result.accepted_rows == 3
    assert applied_group_sizes == [2]
    checkpoint = result.snapshot.l2_checkpoints[0]
    assert checkpoint.sequence == second["sequence"]


def test_arrow_snapshot_identity_is_independent_of_input_batch_boundaries(
    tmp_path: Path,
) -> None:
    from tools.benchmark_normalized_l2 import synthetic_l2_batches

    narrow = _strict_batches(
        tmp_path / "narrow",
        synthetic_l2_batches(131_089, 7_919),
        key="same-layout-input",
    )
    wide = _strict_batches(
        tmp_path / "wide",
        synthetic_l2_batches(131_089, 100_003),
        key="same-layout-input",
    )
    assert narrow.snapshot is not None and wide.snapshot is not None
    assert narrow.snapshot.snapshot_id == wide.snapshot.snapshot_id
    assert narrow.snapshot.partitions == wide.snapshot.partitions
    assert narrow.snapshot.event_claim_index == wide.snapshot.event_claim_index


def test_arrow_schema_pit_sequence_and_duplicate_fail_closed(tmp_path: Path) -> None:
    from tools.benchmark_normalized_l2 import synthetic_l2_batches

    cases: list[tuple[str, list[pa.RecordBatch], str]] = []
    valid = list(synthetic_l2_batches(4, 8))
    delta_batch = valid[1]
    sequence_index = delta_batch.schema.get_field_index("sequence")
    wrong_schema = delta_batch.set_column(
        sequence_index,
        "sequence",
        pa.array([2, 3, 4], type=pa.int32()),
    )
    cases.append(("schema", [valid[0], wrong_schema], "Arrow schema mismatch"))

    received_index = delta_batch.schema.get_field_index("received_at")
    earlier = pa.repeat(
        pa.scalar(
            datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            type=pa.timestamp("ns", tz="UTC"),
        ),
        delta_batch.num_rows,
    )
    pit = delta_batch.set_column(
        received_index,
        delta_batch.schema.field(received_index),
        earlier,
    )
    cases.append(("pit", [valid[0], pit], "received_at must not be earlier"))

    repeated_sequence = delta_batch.set_column(
        sequence_index,
        delta_batch.schema.field(sequence_index),
        pa.array([2, 2, 4], type=pa.int64()),
    )
    cases.append(("sequence", [valid[0], repeated_sequence], "previous_sequence"))

    event_id_index = delta_batch.schema.get_field_index("event_id")
    duplicate_ids = delta_batch.set_column(
        event_id_index,
        delta_batch.schema.field(event_id_index),
        pa.array(["duplicate", "duplicate", "unique"]),
    )
    cases.append(("duplicate", [valid[0], duplicate_ids], "duplicate claims"))

    for name, batches, message in cases:
        root = tmp_path / name
        with pytest.raises((ValidationError, pa.ArrowInvalid), match=message):
            _strict_batches(root, batches, key=name)
        _assert_no_snapshot(root)


def test_arrow_l2_cross_delete_heartbeat_and_reset_are_explicitly_rejected(
    tmp_path: Path,
) -> None:
    crossed = snapshot()
    crossed["bids"][0]["price"] = deepcopy(crossed["asks"][0]["price"])
    absent_delete = delta(
        101,
        100,
        action="delete",
        price_units=98_000,
        quantity_units=0,
    )
    heartbeat = delta(101, 101)
    reset = delta(90, 89)
    for name in ("event_time", "received_at", "available_at"):
        reset[name] = "2026-01-02T00:00:02Z"
    cases = [
        ("crossed", [_record_batch([crossed])], "locked or crossed"),
        (
            "absent-delete",
            [_record_batch([snapshot()]), _record_batch([absent_delete])],
            "absent price level",
        ),
        (
            "equal-sequence-heartbeat",
            [_record_batch([snapshot()]), _record_batch([heartbeat])],
            "previous_sequence must precede sequence",
        ),
        (
            "maintenance-reset",
            [_record_batch([snapshot()]), _record_batch([reset])],
            "expected previous_sequence=100",
        ),
    ]
    for name, batches, message in cases:
        root = tmp_path / name
        with pytest.raises(ValidationError, match=message):
            _strict_batches(root, batches, key=name)
        _assert_no_snapshot(root)


def test_arrow_claim_conflict_capacity_and_nonmonotonic_order_fail_closed(
    tmp_path: Path,
) -> None:
    conflict_root = tmp_path / "claim-conflict"
    first = _strict_batches(conflict_root, [_record_batch([snapshot()])], key="first")
    assert first.snapshot is not None
    changed = snapshot()
    changed["bids"][0]["quantity"]["units"] += 1
    with pytest.raises(ValidationError, match="Conflicting lake event_id claim"):
        _strict_batches(conflict_root, [_record_batch([changed])], key="changed")
    assert len(list((conflict_root / "normalized" / "snapshots").glob("sha256-*"))) == 1

    capacity_root = tmp_path / "capacity"
    stopped = StoragePolicy(
        hot_quota_bytes=1,
        minimum_free_bytes=1,
        minimum_free_fraction=0.000001,
    )
    with pytest.raises(CollectionStoppedError, match="COLLECTION_STOPPED"):
        _strict_batches(
            capacity_root,
            [_record_batch([snapshot()])],
            key="capacity",
            policy=stopped,
        )
    _assert_no_snapshot(capacity_root)

    first_trade = trade("trade-1", timestamp="2026-01-02T00:00:02Z")
    second_trade = trade("trade-2", timestamp="2026-01-02T00:00:01Z")
    second_trade["sequence"] = 2
    order_root = tmp_path / "nonmonotonic"
    with pytest.raises(ValidationError, match="sort order moved backwards"):
        _strict_batches(
            order_root,
            [_record_batch([first_trade, second_trade])],
            key="nonmonotonic",
        )
    _assert_no_snapshot(order_root)


def test_arrow_float_schema_preserves_frozen_claims_and_logical_rows(tmp_path: Path) -> None:
    from quant_data_kit.adapters_v2 import adapt_fixture_messages

    funding = next(
        record
        for record in adapt_fixture_messages(binance_adapter(), load_messages("binance"))
        if record["event_type"] == "funding_rate"
    )
    mapping_root = tmp_path / "float-mapping"
    arrow_root = tmp_path / "float-arrow"
    admitted = _raw(mapping_root, "float")
    mapping = write_normalized_events(
        mapping_root,
        [funding],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    arrow = _strict_batches(arrow_root, [_record_batch([funding])], key="float")
    assert mapping.snapshot is not None and arrow.snapshot is not None
    assert tuple(arrow.snapshot.event_claims) == tuple(mapping.snapshot.event_claims)
    assert read_normalized_events(
        arrow_root,
        arrow.snapshot.snapshot_id,
        event_type="funding_rate",
    ) == read_normalized_events(
        mapping_root,
        mapping.snapshot.snapshot_id,
        event_type="funding_rate",
    )


def test_mapping_and_arrow_sort_chronologically_across_fractional_second_boundary(
    tmp_path: Path,
) -> None:
    first = trade("whole-second", timestamp="2026-01-02T00:00:01Z")
    second = trade("fractional-second", timestamp="2026-01-02T00:00:01.001Z")
    second["sequence"] = 2
    mapping_root = tmp_path / "mapping"
    arrow_root = tmp_path / "arrow"
    mapping_raw = _raw(mapping_root, "fractional-order")
    mapping = write_normalized_events(
        mapping_root,
        [first, second],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[mapping_raw.reference()],
        policy=TEST_POLICY,
    )
    arrow = _strict_batches(
        arrow_root,
        [_record_batch([first, second])],
        key="fractional-order",
    )
    assert mapping.snapshot is not None and arrow.snapshot is not None
    assert mapping.accepted_rows == arrow.accepted_rows == 2
    assert read_normalized_events(
        mapping_root,
        mapping.snapshot.snapshot_id,
        event_type="trade",
    ) == read_normalized_events(
        arrow_root,
        arrow.snapshot.snapshot_id,
        event_type="trade",
    )


def test_event_claim_sequence_supports_sequence_indexing_and_reverse_slices(
    tmp_path: Path,
) -> None:
    records = [trade(f"claim-{index}") for index in range(4)]
    for index, record in enumerate(records, start=1):
        record["sequence"] = index
    admitted = _raw(tmp_path, "claim-sequence")
    result = write_normalized_events(
        tmp_path,
        records,
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    claims = result.snapshot.event_claims
    materialized = tuple(claims)
    assert len(claims) == 4
    assert claims[0] == materialized[0]
    assert claims[-1] == materialized[-1]
    assert claims[::2] == materialized[::2]
    assert claims[:2] == materialized[:2]
    assert claims[::-1] == materialized[::-1]
    assert claims.__eq__(claims) is True
    assert claims == materialized
    assert claims != materialized[:-1]
    assert "rows=4" in repr(claims)
    with pytest.raises(IndexError):
        _ = claims[4]
    with pytest.raises(IndexError):
        _ = claims[-5]


def test_arrow_snapshot_publish_failure_rolls_back_claim_index_and_retries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_replace = normalized_v3.os.replace
    injected = False

    def fail_snapshot_publish(source: Path, destination: Path) -> None:
        nonlocal injected
        if destination.parent.name == "snapshots" and not injected:
            injected = True
            raise OSError("injected strict snapshot publish failure")
        real_replace(source, destination)

    monkeypatch.setattr(normalized_v3.os, "replace", fail_snapshot_publish)
    with pytest.raises(OSError, match="strict snapshot publish"):
        _strict_batches(tmp_path, [_record_batch([snapshot()])], key="atomic-arrow")
    _assert_no_snapshot(tmp_path)
    assert not list((tmp_path / "normalized" / "event-claim-index-v3").rglob("*.parquet"))

    monkeypatch.setattr(normalized_v3.os, "replace", real_replace)
    retried = _strict_batches(tmp_path, [_record_batch([snapshot()])], key="atomic-arrow")
    assert retried.snapshot is not None
    assert load_normalized_snapshot(tmp_path, retried.snapshot.snapshot_id) == retried.snapshot
