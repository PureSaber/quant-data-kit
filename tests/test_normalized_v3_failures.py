from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import quant_data_kit.data_lake as lake_module
from quant_data_kit import normalized_v3
from quant_data_kit.adapters_v2.base import BOOK_SEQUENCE_FACTOR
from quant_data_kit.data_lake import (
    load_normalized_snapshot,
    write_normalized_batches,
    write_normalized_events,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.l2_replay import L2ReplayError, replay_l2
from quant_data_kit.schemas_v2 import BOOK_DELTA_EVENT_SCHEMA_ID
from tests.test_l2_replay import delta, snapshot
from tests.test_m2_audit_regressions import trade
from tests.test_normalized_v3 import (
    TEST_POLICY,
    _assert_no_snapshot,
    _raw,
    _record_batch,
    _strict_batches,
)


def _encoded_delta_group(
    provider_sequence: int,
    previous_sequence: int,
    *,
    levels: int = 2,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    previous = previous_sequence
    timestamp = f"2026-01-02T00:00:{provider_sequence - 100:02d}Z"
    for index in range(1, levels + 1):
        sequence = provider_sequence * BOOK_SEQUENCE_FACTOR + index
        record = delta(101, 100, price_units=100_000 - index * 10)
        record["event_id"] = f"encoded-{provider_sequence}-{index}"
        record["sequence"] = sequence
        record["previous_sequence"] = previous
        for field in ("event_time", "received_at", "available_at"):
            record[field] = timestamp
        records.append(record)
        previous = sequence
    return records


def _set_path(payload: dict[str, Any], path: tuple[str | int, ...], value: Any) -> None:
    target: Any = payload
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value


@pytest.mark.parametrize(
    ("factory", "path", "value", "message"),
    [
        (snapshot, ("received_at",), "2026-01-01T23:59:59Z", "received_at"),
        (snapshot, ("available_at",), "2026-01-01T23:59:59Z", "available_at"),
        (snapshot, ("asks", 0, "price", "scale"), 3, "one scale"),
        (snapshot, ("bids", 0, "price", "units"), 0, "price must be positive"),
        (snapshot, ("bids", 0, "quantity", "units"), -1, "quantity must be non-negative"),
        (snapshot, ("bids", 1, "price", "units"), 100_050, "bids must be strictly"),
        (snapshot, ("asks", 1, "price", "units"), 100_050, "asks must be strictly"),
        (
            lambda: delta(101, 100),
            ("price", "units"),
            0,
            "price must be positive",
        ),
        (
            lambda: delta(101, 100, action="delete"),
            ("quantity", "units"),
            1,
            "delete delta quantity",
        ),
        (
            lambda: delta(101, 100),
            ("quantity", "units"),
            0,
            "upsert delta quantity",
        ),
    ],
)
def test_native_l2_validator_exercises_failure_semantics(
    factory: Callable[[], dict[str, Any]],
    path: tuple[str | int, ...],
    value: Any,
    message: str,
) -> None:
    record = factory()
    _set_path(record, path, value)
    schema_id = lake_module._event_schema_id(record)
    with pytest.raises(ValidationError, match=message):
        normalized_v3._validate_event_record(schema_id, record)


def test_input_spool_fallback_and_malformed_records_are_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(normalized_v3, "_BATCH_ROWS", 2)
    spool_path = tmp_path / "spool.json-seq"
    with normalized_v3._InputSpool(spool_path) as spool:
        spool.append({"value": 1})
        spool.append({"value": object()})
        spool.flush()
    rows = list(normalized_v3._iter_spooled_records(spool_path))
    assert rows[0] == {"value": 1}
    assert rows[1]["value"]["invalid_type"] == "object"

    non_list = tmp_path / "non-list.json-seq"
    non_list.write_bytes(b"\n{}\n")
    with pytest.raises(ValidationError, match="batch is malformed"):
        list(normalized_v3._iter_spooled_records(non_list))
    non_record = tmp_path / "non-record.json-seq"
    non_record.write_bytes(b"[1]\n")
    with pytest.raises(ValidationError, match="record is malformed"):
        list(normalized_v3._iter_spooled_records(non_record))

    assert normalized_v3._has_non_finite(1.0) is False
    assert normalized_v3._has_non_finite(float("nan")) is True
    assert normalized_v3._has_non_finite({"nested": float("inf")}) is True
    assert normalized_v3._has_non_finite([0, (float("-inf"),)]) is True
    assert normalized_v3._has_non_finite("finite") is False


def test_digest_and_staging_corruption_guards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    digest = normalized_v3._JsonArrayDigest(preserve_stdlib_float_format=True)
    digest.update([])
    empty_hash = digest.hexdigest()
    assert len(empty_hash) == 64

    monkeypatch.setattr(normalized_v3, "_canonical_json_bytes", lambda _: b"{}")
    with pytest.raises(ValidationError, match="serialization changed"):
        digest.update([{"value": 1}])

    root = lake_module._resolved_lake_root(tmp_path / "lake", create=True)
    staging_root = root / "normalized" / "staging"
    stale = staging_root / "normalized-batch-stream-stale"
    stale.mkdir(parents=True)
    with normalized_v3._streaming_stage(root) as stage:
        assert stage.is_dir()
        assert not stale.exists()

    invalid = staging_root / "normalized-batch-stream-file"
    invalid.write_bytes(b"not-a-directory")
    with (
        pytest.raises(ValidationError, match="not a directory"),
        normalized_v3._streaming_stage(root),
    ):
        pass


def test_streaming_stage_serializes_owner_file_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = lake_module._resolved_lake_root(tmp_path / "lake", create=True)
    real_lock = normalized_v3.process_file_lock
    real_unlink = Path.unlink
    held_locks: set[Path] = set()

    @contextmanager
    def tracked_lock(path: Path, *, timeout_seconds: float = 60.0):
        checked = Path(path)
        with real_lock(checked, timeout_seconds=timeout_seconds):
            held_locks.add(checked)
            try:
                yield
            finally:
                held_locks.remove(checked)

    def guarded_unlink(path: Path, *args: object, **kwargs: object) -> None:
        if path.parent.name == ".stage-owners" and path.name.startswith("normalized-batch-stream-"):
            assert any(item.name == ".gc.lock" for item in held_locks)
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(normalized_v3, "process_file_lock", tracked_lock)
    monkeypatch.setattr(Path, "unlink", guarded_unlink)
    with normalized_v3._streaming_stage(root) as stage:
        assert stage.is_dir()
    owners_root = root / "normalized" / ".stage-owners"
    assert sorted(item.name for item in owners_root.iterdir()) == [".gc.lock"]


def _rewrite_index_manifest(
    manifest_path: Path,
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload.pop("manifest_sha256")
    mutate(payload)
    payload["manifest_sha256"] = lake_module._sha256_bytes(
        lake_module._canonical_json_bytes(payload)
    )
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")


@pytest.mark.parametrize("case", ["anchor", "identity", "unsafe", "duplicate", "unreadable"])
def test_claim_index_manifest_tampering_fails_closed(tmp_path: Path, case: str) -> None:
    root = tmp_path / case
    result = _strict_batches(root, [_record_batch([snapshot()])], key=case)
    assert result.snapshot is not None
    index_root = (
        root / "normalized" / "event-claim-index-v3" / f"snapshot={result.snapshot.snapshot_id}"
    )
    manifest_path = index_root / "manifest.json"
    if case == "anchor":
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        payload["manifest_sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(payload), encoding="utf-8")
        message = "manifest integrity changed"
    elif case == "identity":
        _rewrite_index_manifest(
            manifest_path,
            lambda payload: payload.__setitem__("snapshot_id", "sha256-" + "0" * 64),
        )
        message = "index identity changed"
    elif case == "unsafe":
        _rewrite_index_manifest(
            manifest_path,
            lambda payload: payload["files"][0].__setitem__("relative_path", "../escape.parquet"),
        )
        message = "path is unsafe"
    elif case == "duplicate":
        _rewrite_index_manifest(
            manifest_path,
            lambda payload: payload["files"].append(deepcopy(payload["files"][0])),
        )
        message = "duplicated"
    else:
        manifest_path.write_bytes(b"not-json")
        message = "manifest is unreadable"
    with pytest.raises(ValidationError, match=message):
        load_normalized_snapshot(root, result.snapshot.snapshot_id)


def test_claim_index_schema_logical_shape_and_missing_manifest_paths(
    tmp_path: Path,
) -> None:
    unexpected_root = tmp_path / "unexpected"
    unexpected = _strict_batches(
        unexpected_root,
        [_record_batch([snapshot()])],
        key="unexpected",
    )
    assert unexpected.snapshot is not None
    unexpected_index = (
        unexpected_root
        / "normalized"
        / "event-claim-index-v3"
        / f"snapshot={unexpected.snapshot.snapshot_id}"
    )
    (unexpected_index / "unexpected.bin").write_bytes(b"unexpected")
    with pytest.raises(ValidationError, match="unexpected or missing"):
        load_normalized_snapshot(unexpected_root, unexpected.snapshot.snapshot_id)

    recovery_root = tmp_path / "missing-manifest"
    recovery = _strict_batches(
        recovery_root,
        [_record_batch([snapshot()])],
        key="missing-manifest",
    )
    assert recovery.snapshot is not None
    recovery_index = (
        recovery_root
        / "normalized"
        / "event-claim-index-v3"
        / f"snapshot={recovery.snapshot.snapshot_id}"
    )
    (recovery_index / "manifest.json").unlink()
    with pytest.raises(ValidationError, match="explicit StoragePolicy"):
        load_normalized_snapshot(recovery_root, recovery.snapshot.snapshot_id)
    assert (
        load_normalized_snapshot(
            recovery_root,
            recovery.snapshot.snapshot_id,
            recovery_policy=TEST_POLICY,
        )
        == recovery.snapshot
    )
    assert (recovery_index / "manifest.json").is_file()

    schema_root = tmp_path / "schema"
    schema_result = _strict_batches(schema_root, [_record_batch([snapshot()])], key="schema")
    assert schema_result.snapshot is not None
    schema_index = (
        schema_root
        / "normalized"
        / "event-claim-index-v3"
        / f"snapshot={schema_result.snapshot.snapshot_id}"
    )
    schema_claim = next(schema_index.rglob("*.parquet"))
    original = pq.ParquetFile(schema_claim).read()
    changed_schema = pa.schema(
        [pa.field("event_id_hash", pa.large_string()), *list(original.schema)[1:]]
    )
    changed = pa.Table.from_arrays(
        [original.column(0).cast(pa.large_string()), *original.columns[1:]],
        schema=changed_schema,
    )
    pq.write_table(changed, schema_claim)
    schema_manifest = schema_index / "manifest.json"
    _rewrite_index_manifest(
        schema_manifest,
        lambda payload: payload["files"][0].__setitem__(
            "content_sha256", lake_module._sha256_file(schema_claim)
        ),
    )
    with pytest.raises(ValidationError, match="Arrow schema changed"):
        load_normalized_snapshot(schema_root, schema_result.snapshot.snapshot_id)

    logical_root = tmp_path / "logical"
    logical = _strict_batches(logical_root, [_record_batch([snapshot()])], key="logical")
    assert logical.snapshot is not None
    logical_index = (
        logical_root
        / "normalized"
        / "event-claim-index-v3"
        / f"snapshot={logical.snapshot.snapshot_id}"
    )
    logical_claim = next(logical_index.rglob("*.parquet"))
    table = pq.ParquetFile(logical_claim).read()
    changed_hash = pa.array(["f" * 64], type=pa.string())
    table = table.set_column(0, table.schema.field(0), changed_hash)
    pq.write_table(table, logical_claim)
    logical_manifest = logical_index / "manifest.json"
    _rewrite_index_manifest(
        logical_manifest,
        lambda payload: payload["files"][0].__setitem__(
            "content_sha256", lake_module._sha256_file(logical_claim)
        ),
    )
    with pytest.raises(ValidationError, match="logical content changed"):
        load_normalized_snapshot(logical_root, logical.snapshot.snapshot_id)


def test_v3_claim_conflicts_include_historical_legacy_snapshots(tmp_path: Path) -> None:
    legacy_raw = _raw(tmp_path, "legacy-source")
    legacy = lake_module._write_normalized_events_legacy(
        tmp_path,
        [trade("legacy-event")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[legacy_raw.reference()],
        policy=TEST_POLICY,
    )
    assert legacy.snapshot is not None

    distinct_raw = _raw(tmp_path, "v3-distinct")
    distinct = write_normalized_events(
        tmp_path,
        [trade("v3-event")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[distinct_raw.reference()],
        policy=TEST_POLICY,
    )
    assert distinct.snapshot is not None
    legacy_index = (
        tmp_path / "normalized" / "event-claim-index-v3" / f"snapshot={legacy.snapshot.snapshot_id}"
    )
    assert list(legacy_index.rglob("*.parquet"))

    conflict = trade("legacy-event")
    conflict["price"]["units"] += 1
    conflict_raw = _raw(tmp_path, "v3-conflict")
    with pytest.raises(ValidationError, match="Conflicting lake event_id claim"):
        write_normalized_events(
            tmp_path,
            [conflict],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[conflict_raw.reference()],
            policy=TEST_POLICY,
        )


def _replace_column(batch: pa.RecordBatch, name: str, values: pa.Array) -> pa.RecordBatch:
    index = batch.schema.get_field_index(name)
    return batch.set_column(index, batch.schema.field(index), values)


@pytest.mark.parametrize(
    ("case", "column", "values", "message"),
    [
        ("side", "side", pa.array(["invalid"]), "side is invalid"),
        ("action", "action", pa.array(["invalid"]), "action is invalid"),
        (
            "price-zero",
            "price",
            pa.StructArray.from_arrays(
                [pa.array([0], type=pa.int64()), pa.array([2], type=pa.int16())],
                fields=list(
                    normalized_v3.get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID).field("price").type
                ),
            ),
            "price must be positive",
        ),
        (
            "quantity-negative",
            "quantity",
            pa.StructArray.from_arrays(
                [pa.array([-1], type=pa.int64()), pa.array([3], type=pa.int16())],
                fields=list(
                    normalized_v3.get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID)
                    .field("quantity")
                    .type
                ),
            ),
            "quantity must be non-negative",
        ),
        (
            "price-scale-negative",
            "price",
            pa.StructArray.from_arrays(
                [pa.array([100_000], type=pa.int64()), pa.array([-1], type=pa.int16())],
                fields=list(
                    normalized_v3.get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID).field("price").type
                ),
            ),
            "scale is negative",
        ),
        (
            "quantity-scale-large",
            "quantity",
            pa.StructArray.from_arrays(
                [pa.array([25], type=pa.int64()), pa.array([19], type=pa.int16())],
                fields=list(
                    normalized_v3.get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID)
                    .field("quantity")
                    .type
                ),
            ),
            "scale exceeds 18",
        ),
        (
            "previous-negative",
            "previous_sequence",
            pa.array([-1], type=pa.int64()),
            "previous_sequence must be non-negative",
        ),
        (
            "previous-equal",
            "previous_sequence",
            pa.array([101], type=pa.int64()),
            "previous_sequence must precede sequence",
        ),
    ],
)
def test_arrow_vector_delta_constraints_fail_closed(
    tmp_path: Path,
    case: str,
    column: str,
    values: pa.Array,
    message: str,
) -> None:
    changed = _replace_column(_record_batch([delta(101, 100)]), column, values)
    with pytest.raises(ValidationError, match=message):
        _strict_batches(
            tmp_path / case,
            [_record_batch([snapshot()]), changed],
            key=case,
        )
    _assert_no_snapshot(tmp_path / case)


@pytest.mark.parametrize(
    ("case", "action", "quantity", "message"),
    [
        ("delete-positive", "delete", 1, "action and quantity disagree"),
        ("upsert-zero", "upsert", 0, "action and quantity disagree"),
    ],
)
def test_arrow_vector_action_quantity_contract(
    tmp_path: Path,
    case: str,
    action: str,
    quantity: int,
    message: str,
) -> None:
    changed = delta(101, 100, action=action, quantity_units=quantity)
    with pytest.raises(ValidationError, match=message):
        _strict_batches(
            tmp_path / case,
            [_record_batch([snapshot()]), _record_batch([changed])],
            key=case,
        )


def test_arrow_common_null_empty_precision_source_and_sequence_guards(tmp_path: Path) -> None:
    base = _record_batch([delta(101, 100)])
    cases = [
        ("null", _replace_column(base, "event_id", pa.array([None], type=pa.string())), "null"),
        ("empty", _replace_column(base, "event_id", pa.array([""])), "non-empty"),
        ("source", _replace_column(base, "source", pa.array(["okx"])), "provider"),
        (
            "sequence",
            _replace_column(base, "sequence", pa.array([-1], type=pa.int64())),
            "sequence must be non-negative",
        ),
    ]
    nanosecond = pa.array(
        [1767312000000000001],
        type=pa.timestamp("ns", tz="UTC"),
    )
    precision = base
    for name in ("event_time", "received_at", "available_at"):
        precision = _replace_column(precision, name, nanosecond)
    cases.append(("precision", precision, "nanoseconds"))
    for name, changed, message in cases:
        root = tmp_path / name
        with pytest.raises(ValidationError, match=message):
            _strict_batches(root, [_record_batch([snapshot()]), changed], key=name)
        _assert_no_snapshot(root)


def test_arrow_input_reference_and_empty_batch_contracts(tmp_path: Path) -> None:
    root = tmp_path / "references"
    admitted = _raw(root, "references")
    batch = _record_batch([snapshot()])
    with pytest.raises(ValidationError, match="at least one trusted Raw"):
        write_normalized_batches(
            root,
            [batch],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[],
            policy=TEST_POLICY,
        )
    with pytest.raises(ValidationError, match="duplicate Raw references"):
        write_normalized_batches(
            root,
            [batch],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[admitted.reference(), admitted.reference()],
            policy=TEST_POLICY,
        )
    with pytest.raises(ValidationError, match="Raw source"):
        write_normalized_batches(
            root,
            [batch],
            provider="okx",
            venue="OKX",
            upstream_raw_references=[admitted.reference()],
            policy=TEST_POLICY,
        )
    with pytest.raises(ValidationError, match="RecordBatch objects"):
        write_normalized_batches(
            root,
            [object()],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[admitted.reference()],
            policy=TEST_POLICY,
        )
    empty = write_normalized_batches(
        root,
        [batch.slice(0, 0)],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    assert empty.snapshot is None
    assert empty.accepted_rows == empty.quarantined_rows == 0


def test_encoded_group_batch_coalescer_preserves_order_and_atomic_boundaries() -> None:
    first = _encoded_delta_group(101, 100 * BOOK_SEQUENCE_FACTOR)
    second = _encoded_delta_group(102, first[-1]["sequence"])
    third = _encoded_delta_group(103, second[-1]["sequence"])
    expected_sequences = [event["sequence"] for event in (*first, *second, *third)]

    cut_batches = [
        _record_batch(first[:1]),
        _record_batch([*first[1:], *second, *third[:1]]),
        _record_batch(third[1:]),
    ]
    coalesced = list(normalized_v3._iter_atomic_l2_record_batches(cut_batches))
    assert [batch.num_rows for batch in coalesced] == [4, 2]
    assert [
        value
        for batch in coalesced
        for value in batch.column(batch.schema.get_field_index("sequence")).to_pylist()
    ] == expected_sequences

    whole = list(
        normalized_v3._iter_atomic_l2_record_batches([_record_batch([*first, *second, *third])])
    )
    assert [batch.num_rows for batch in whole] == [4, 2]

    mismatched = list(
        normalized_v3._iter_atomic_l2_record_batches(
            [_record_batch(first[:1]), _record_batch(second)]
        )
    )
    assert [batch.num_rows for batch in mismatched] == [1, 2]

    unencoded = list(
        normalized_v3._iter_atomic_l2_record_batches(
            [_record_batch(first[:1]), _record_batch([delta(101, 100)])]
        )
    )
    assert [batch.num_rows for batch in unencoded] == [1, 1]


def test_encoded_group_layout_and_concat_helpers_fail_closed() -> None:
    first = _encoded_delta_group(101, 100 * BOOK_SEQUENCE_FACTOR)
    valid = _record_batch(first)
    sequence_index = valid.schema.get_field_index("sequence")
    noncontiguous = valid.set_column(
        sequence_index,
        valid.schema.field(sequence_index),
        pa.array(
            [101 * BOOK_SEQUENCE_FACTOR + 1, 101 * BOOK_SEQUENCE_FACTOR + 3],
            type=pa.int64(),
        ),
    )
    wrong_boundary = valid.set_column(
        sequence_index,
        valid.schema.field(sequence_index),
        pa.array(
            [101 * BOOK_SEQUENCE_FACTOR + 1, 102 * BOOK_SEQUENCE_FACTOR + 2],
            type=pa.int64(),
        ),
    )
    nonpositive_provider = valid.set_column(
        sequence_index,
        valid.schema.field(sequence_index),
        pa.array([101 * BOOK_SEQUENCE_FACTOR + 1, 1], type=pa.int64()),
    )
    group_zero = valid.set_column(
        sequence_index,
        valid.schema.field(sequence_index),
        pa.array([1, 2], type=pa.int64()),
    )
    assert normalized_v3._encoded_l2_group_ranges(valid) == ((0, 2),)
    assert normalized_v3._encoded_l2_group_ranges(noncontiguous) is None
    assert normalized_v3._encoded_l2_group_ranges(wrong_boundary) is None
    assert normalized_v3._encoded_l2_group_ranges(nonpositive_provider) is None
    assert normalized_v3._encoded_l2_group_ranges(group_zero) is None
    assert normalized_v3._encoded_l2_group_ranges(valid.slice(0, 0)) is None
    assert normalized_v3._encoded_l2_group_ranges(_record_batch([trade("trade")])) is None

    assert normalized_v3._concat_record_batches((valid,)) is valid
    with pytest.raises(ValidationError, match="empty"):
        normalized_v3._concat_record_batches(())
    with pytest.raises(ValidationError, match="different schemas"):
        normalized_v3._concat_record_batches((valid, _record_batch([trade("trade")])))


def test_encoded_group_checkpoint_and_time_fallbacks_remain_strict(tmp_path: Path) -> None:
    initial = snapshot()
    initial["sequence"] = 100 * BOOK_SEQUENCE_FACTOR
    group = _encoded_delta_group(101, initial["sequence"])
    replayed_first = replay_l2([initial, group[0]])
    stream = ("binance", "BTC-USDT-SPOT", "binance-24x7-BTC-USDT-SPOT")
    expected = {stream: {group[0]["sequence"]: replayed_first.final_checkpoint.state_sha256}}
    checkpointed = _strict_batches(
        tmp_path / "intermediate-checkpoint",
        [_record_batch([initial]), _record_batch(group)],
        key="intermediate-checkpoint",
        expected_l2_checkpoint_hashes=expected,
    )
    assert checkpointed.snapshot is not None
    assert checkpointed.snapshot.l2_checkpoints[0].sequence == group[-1]["sequence"]

    different_time = deepcopy(group)
    for field in ("event_time", "received_at", "available_at"):
        different_time[1][field] = "2026-01-02T00:00:02Z"
    timed = _strict_batches(
        tmp_path / "different-time",
        [_record_batch([initial]), _record_batch(different_time)],
        key="different-time",
    )
    assert timed.snapshot is not None

    single = _strict_batches(
        tmp_path / "single-level",
        [_record_batch([initial]), _record_batch(group[:1])],
        key="single-level",
    )
    assert single.snapshot is not None

    with pytest.raises(L2ReplayError, match="checkpoint hash mismatch"):
        _strict_batches(
            tmp_path / "final-mismatch",
            [_record_batch([initial]), _record_batch(group)],
            key="final-mismatch",
            expected_l2_checkpoint_hashes={stream: {group[-1]["sequence"]: "0" * 64}},
        )
    _assert_no_snapshot(tmp_path / "final-mismatch")


@pytest.mark.parametrize(
    ("case", "mutate", "message"),
    [
        (
            "previous-gap",
            lambda records: records[1].update(previous_sequence=records[0]["previous_sequence"]),
            "previous_sequence",
        ),
        (
            "negative-quantity",
            lambda records: records[1]["quantity"].update(units=-1),
            "non-negative",
        ),
        (
            "price-scale",
            lambda records: records[1]["price"].update(scale=3),
            "price scale changed",
        ),
        (
            "pit",
            lambda records: records[1].update(received_at="2026-01-02T00:00:00Z"),
            "received_at must not be earlier",
        ),
        (
            "final-cross",
            lambda records: records[1]["price"].update(units=100_100),
            "locked or crossed",
        ),
    ],
)
def test_encoded_atomic_group_negative_gates_publish_nothing(
    tmp_path: Path,
    case: str,
    mutate: Callable[[list[dict[str, Any]]], Any],
    message: str,
) -> None:
    initial = snapshot()
    initial["sequence"] = 100 * BOOK_SEQUENCE_FACTOR
    group = _encoded_delta_group(101, initial["sequence"])
    mutate(group)
    root = tmp_path / case
    with pytest.raises(ValidationError, match=message):
        _strict_batches(
            root,
            [_record_batch([initial]), _record_batch(group)],
            key=case,
        )
    _assert_no_snapshot(root)


def test_encoded_atomic_group_duplicate_claim_is_not_bypassed(tmp_path: Path) -> None:
    initial = snapshot()
    initial["sequence"] = 100 * BOOK_SEQUENCE_FACTOR
    group = _encoded_delta_group(101, initial["sequence"])
    group[1]["event_id"] = group[0]["event_id"]
    with pytest.raises(ValidationError, match="duplicate claims"):
        _strict_batches(
            tmp_path,
            [_record_batch([initial]), _record_batch(group)],
            key="encoded-duplicate-claim",
        )
    _assert_no_snapshot(tmp_path)


def test_mixed_l2_batch_uses_serial_semantics_and_checkpoint_contracts(tmp_path: Path) -> None:
    events = [
        snapshot(),
        delta(101, 100, side="bid", price_units=100_000, quantity_units=26),
        delta(102, 101, side="ask", price_units=100_100, quantity_units=27),
    ]
    replayed = replay_l2(events)
    expected_hash = replayed.checkpoints[1].state_sha256
    stream = ("binance", "BTC-USDT-SPOT", "binance-24x7-BTC-USDT-SPOT")
    result = _strict_batches(
        tmp_path / "mixed",
        [_record_batch([events[0]]), _record_batch(events[1:])],
        key="mixed",
        expected_l2_checkpoint_hashes={stream: {101: expected_hash}},
    )
    assert result.snapshot is not None
    assert result.snapshot.l2_checkpoints[0].state_sha256 == replayed.final_checkpoint.state_sha256

    failures = [
        ("mismatch", {stream: {102: "0" * 64}}, "checkpoint hash mismatch"),
        ("missing", {stream: {999: "0" * 64}}, "not reached"),
        (
            "absent-stream",
            {("binance", "ETH-USDT-SPOT", "missing-session"): {1: "0" * 64}},
            "was not observed",
        ),
    ]
    for name, expected, message in failures:
        root = tmp_path / name
        with pytest.raises(L2ReplayError, match=message):
            _strict_batches(
                root,
                [_record_batch([events[0]]), _record_batch(events[1:])],
                key=name,
                expected_l2_checkpoint_hashes=expected,
            )
        _assert_no_snapshot(root)


def test_mapping_missing_expected_l2_stream_fails_instead_of_silently_passing(
    tmp_path: Path,
) -> None:
    admitted = _raw(tmp_path, "mapping-missing-stream")
    expected = {("binance", "ETH-USDT-SPOT", "missing-session"): {1: "0" * 64}}
    with pytest.raises(L2ReplayError, match="was not observed"):
        write_normalized_events(
            tmp_path,
            [trade("mapping-only-trade")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[admitted.reference()],
            expected_l2_checkpoint_hashes=expected,
            policy=TEST_POLICY,
        )
    _assert_no_snapshot(tmp_path)


def test_arrow_batches_reject_cross_batch_partition_time_regression(tmp_path: Path) -> None:
    later = trade("later", timestamp="2026-01-02T00:00:02Z")
    earlier = trade("earlier", timestamp="2026-01-02T00:00:01Z")
    earlier["session_id"] = "second-session"
    with pytest.raises(ValidationError, match="globally monotonic"):
        _strict_batches(
            tmp_path,
            [_record_batch([later]), _record_batch([earlier])],
            key="cross-batch-order",
        )
    _assert_no_snapshot(tmp_path)


def test_mapping_unsorted_partition_is_sorted_without_changing_events(tmp_path: Path) -> None:
    later = trade("later", timestamp="2026-01-02T00:00:02Z")
    earlier = trade("earlier", timestamp="2026-01-02T00:00:01Z")
    earlier["session_id"] = "second-session"
    admitted = _raw(tmp_path, "mapping-sort")
    result = write_normalized_events(
        tmp_path,
        [later, earlier],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    rows = lake_module.read_normalized_events(
        tmp_path,
        result.snapshot.snapshot_id,
        event_type="trade",
    )
    assert [row["event_id"] for row in rows] == ["earlier", "later"]


def test_arrow_datetime_input_and_capacity_recheck_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[int] = []
    real_capacity = normalized_v3.require_collection_capacity

    def observed_capacity(root: Path, *, projected_write_bytes: int, policy: Any) -> None:
        calls.append(projected_write_bytes)
        real_capacity(root, projected_write_bytes=projected_write_bytes, policy=policy)

    monkeypatch.setattr(normalized_v3, "_CAPACITY_CHECK_ROWS", 1)
    monkeypatch.setattr(normalized_v3, "require_collection_capacity", observed_capacity)
    result = _strict_batches(
        tmp_path,
        [_record_batch([snapshot()])],
        key="capacity-recheck",
    )
    assert result.snapshot is not None
    assert len(calls) >= 3
    assert all(value >= 0 for value in calls)


def test_arrow_common_datetime_is_utc_and_partition_digest_is_strict(tmp_path: Path) -> None:
    result = _strict_batches(tmp_path, [_record_batch([snapshot()])], key="scan")
    assert result.snapshot is not None
    partition = result.snapshot.partitions[0]
    path = (
        tmp_path
        / "normalized"
        / "snapshots"
        / result.snapshot.snapshot_id
        / partition.relative_path
    )
    with pytest.raises(ValidationError, match="Arrow schema mismatch"):
        normalized_v3._scan_partition_arrow_ipc(path, "puresaber.trade-event")
    unknown_root = tmp_path / "unknown-hash"
    unknown_root.mkdir()
    partitions = normalized_v3._PartitionSet(unknown_root, "binance", "BINANCE")
    partitions.append(snapshot())
    partitions.close()
    with pytest.raises(ValidationError, match="Unknown normalized partition logical hash"):
        normalized_v3._partition_manifests(
            partitions,
            provider="binance",
            venue="BINANCE",
            logical_hash_version="unknown",
        )


def test_arrow_conversion_digest_and_type_helpers_cover_nested_contracts() -> None:
    schema = normalized_v3.get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID)
    record = delta(101, 100)
    timestamp = datetime(2026, 1, 2, tzinfo=timezone.utc)
    record["event_time"] = timestamp
    record["received_at"] = timestamp
    record["available_at"] = timestamp
    record["trading_day"] = date(2026, 1, 2)
    prepared = normalized_v3._arrow_ready_rows(schema, [record])
    assert prepared[0]["event_time"] is timestamp
    assert prepared[0]["trading_day"] == date(2026, 1, 2)

    digest = normalized_v3._JsonArrayDigest(preserve_stdlib_float_format=False)
    digest.update([{"value": 1}])
    first = digest.hexdigest()
    digest.update([{"value": 2}])
    assert digest.hexdigest() != first

    assert normalized_v3._contains_floating_type(pa.float64()) is True
    assert normalized_v3._contains_floating_type(pa.struct([pa.field("x", pa.float64())]))
    assert normalized_v3._contains_floating_type(pa.list_(pa.float64()))
    assert normalized_v3._contains_floating_type(pa.int64()) is False


def test_partition_buffers_reject_internal_schema_and_sort_corruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = normalized_v3.get_arrow_schema("puresaber.trade-event")
    batch = _record_batch([trade("partition")])
    timestamp = datetime(2026, 1, 2, 0, 0, 1, 1000, tzinfo=timezone.utc)
    key = (timestamp, 1, "partition")
    partition = normalized_v3._OpenPartition(
        key=("trade", "2026-01-02", "BTC-USDT-SPOT"),
        schema_id="puresaber.trade-event",
        relative_path=Path("data.parquet"),
        path=tmp_path / "buffer" / "data.parquet",
        buffer=[],
    )
    assert partition.flush() == 0
    partition._flush_arrow_buffer()
    partition.append(trade("partition"))
    partition.append_record_batch(batch, first_sort_key=key, last_sort_key=key)
    partition.close()
    assert partition.rows == 2

    reversed_partition = normalized_v3._OpenPartition(
        key=("trade", "2026-01-02", "BTC-USDT-SPOT"),
        schema_id="puresaber.trade-event",
        relative_path=Path("data.parquet"),
        path=tmp_path / "reversed" / "data.parquet",
        buffer=[],
    )
    with pytest.raises(ValidationError, match="sort order moved backwards"):
        reversed_partition.append_record_batch(
            batch,
            first_sort_key=(timestamp + timedelta(seconds=1), 2, "later"),
            last_sort_key=key,
        )
    wrong_schema = _record_batch([snapshot()])
    with pytest.raises(ValidationError, match="Arrow schema mismatch"):
        reversed_partition.append_record_batch(
            wrong_schema,
            first_sort_key=key,
            last_sort_key=key,
        )

    mapping_set = normalized_v3._PartitionSet(tmp_path / "mapping-set", "binance", "BINANCE")
    mapping_set.append(trade("mixed-schema"))
    only_partition = next(iter(mapping_set.partitions.values()))
    only_partition.schema_id = "puresaber.quote-event"
    with pytest.raises(ValidationError, match="Mixed schemas"):
        mapping_set.append(trade("mixed-schema-2"))
    only_partition.schema_id = "puresaber.trade-event"
    mapping_set.close()

    arrow_set = normalized_v3._PartitionSet(tmp_path / "arrow-set", "binance", "BINANCE")
    arrow_set.append_record_batch(
        batch,
        schema_id="puresaber.trade-event",
        event_type="trade",
        trading_date="2026-01-02",
        instrument_id="BTC-USDT-SPOT",
        first_sort_key=key,
        last_sort_key=key,
    )
    with pytest.raises(ValidationError, match="Mixed schemas"):
        arrow_set.append_record_batch(
            batch,
            schema_id="puresaber.quote-event",
            event_type="trade",
            trading_date="2026-01-02",
            instrument_id="BTC-USDT-SPOT",
            first_sort_key=key,
            last_sort_key=key,
        )
    arrow_set.close()

    monkeypatch.setattr(normalized_v3, "_BATCH_ROWS", 2)
    bounded = normalized_v3._PartitionSet(tmp_path / "bounded", "binance", "BINANCE")
    for index in range(4):
        record = trade(f"bounded-{index}", instrument_id=f"INSTRUMENT-{index}")
        bounded.append(record)
    assert bounded.buffered_rows == 0
    bounded.close()
    assert len(bounded.paths()) == 4
    assert schema == batch.schema


def test_partition_scans_and_empty_claim_helpers_are_defensive(tmp_path: Path) -> None:
    records = [trade("scan-1"), trade("scan-2")]
    records[1]["sequence"] = 2
    root = tmp_path / "scan"
    admitted = _raw(root, "scan-rows")
    result = write_normalized_events(
        root,
        records,
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    partition = result.snapshot.partitions[0]
    path = root / "normalized" / "snapshots" / result.snapshot.snapshot_id / partition.relative_path
    rows, logical = normalized_v3._scan_partition(path, partition.schema_id)
    assert rows == 2
    assert logical == partition.logical_sha256
    with pytest.raises(ValidationError, match="Arrow schema mismatch"):
        normalized_v3._scan_partition(path, "puresaber.quote-event")
    assert normalized_v3._claim_shard_manifests_from_paths([]) == ()
    assert (
        normalized_v3._build_claim_index_files(
            tmp_path,
            (),
            tmp_path / "empty-index",
            tmp_path / "empty-temp",
        )
        == ()
    )
    assert (
        normalized_v3._publish_quarantine_file(
            root,
            tmp_path / "does-not-need-to-exist",
            provider="binance",
            venue="BINANCE",
            rows=0,
            policy=TEST_POLICY,
        )
        is None
    )


def test_arrow_uniform_final_checkpoint_and_idempotent_snapshot_publish(tmp_path: Path) -> None:
    events = [snapshot(), delta(101, 100)]
    replayed = replay_l2(events)
    stream = ("binance", "BTC-USDT-SPOT", "binance-24x7-BTC-USDT-SPOT")
    expected = {stream: {101: replayed.final_checkpoint.state_sha256}}
    first = _strict_batches(
        tmp_path,
        [_record_batch([events[0]]), _record_batch([events[1]])],
        key="idempotent",
        expected_l2_checkpoint_hashes=expected,
    )
    second = _strict_batches(
        tmp_path,
        [_record_batch([events[0]]), _record_batch([events[1]])],
        key="idempotent",
        expected_l2_checkpoint_hashes=expected,
    )
    assert first.snapshot is not None and second.snapshot is not None
    assert first.snapshot.snapshot_id == second.snapshot.snapshot_id

    empty_expectation = _strict_batches(
        tmp_path / "empty-expectation",
        [_record_batch([events[0]])],
        key="empty-expectation",
        expected_l2_checkpoint_hashes={("binance", "ETH-USDT-SPOT", "not-observed"): {}},
    )
    assert empty_expectation.snapshot is not None


def test_arrow_existing_snapshot_collision_is_visible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _strict_batches(tmp_path, [_record_batch([snapshot()])], key="collision")
    assert first.snapshot is not None
    real_load = lake_module.load_normalized_snapshot

    def mismatched_load(root: Path, snapshot_id: str):
        return replace(real_load(root, snapshot_id), logical_sha256="0" * 64)

    monkeypatch.setattr(lake_module, "load_normalized_snapshot", mismatched_load)
    with pytest.raises(ValidationError, match="snapshot collision"):
        _strict_batches(tmp_path, [_record_batch([snapshot()])], key="collision")


def test_v3_snapshot_manifest_malformed_and_unknown_hash_fail_closed(tmp_path: Path) -> None:
    malformed_root = tmp_path / "malformed"
    malformed = _strict_batches(
        malformed_root,
        [_record_batch([snapshot()])],
        key="malformed",
    )
    assert malformed.snapshot is not None
    manifest_path = (
        malformed_root
        / "normalized"
        / "snapshots"
        / malformed.snapshot.snapshot_id
        / "manifest.json"
    )
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload.pop("event_claim_index")
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="manifest is malformed"):
        load_normalized_snapshot(malformed_root, malformed.snapshot.snapshot_id)

    hash_root = tmp_path / "unknown-hash"
    hashed = _strict_batches(hash_root, [_record_batch([snapshot()])], key="unknown-hash")
    assert hashed.snapshot is not None
    hash_manifest = (
        hash_root / "normalized" / "snapshots" / hashed.snapshot.snapshot_id / "manifest.json"
    )
    hash_payload = json.loads(hash_manifest.read_text(encoding="utf-8"))
    hash_payload["partition_logical_hash_version"] = "unknown"
    identity = normalized_v3._snapshot_payload_v3(
        provider=hash_payload["provider"],
        venue=hash_payload["venue"],
        created_at=hash_payload["created_at"],
        upstream_raw_references=hashed.snapshot.upstream_raw_references,
        partitions=hashed.snapshot.partitions,
        event_claim_index=hashed.snapshot.event_claim_index,
        l2_checkpoints=hashed.snapshot.l2_checkpoints,
        partition_logical_hash_version="unknown",
    )
    logical = lake_module._sha256_bytes(lake_module._canonical_json_bytes(identity))
    hash_payload["logical_sha256"] = logical
    hash_payload["snapshot_id"] = f"sha256-{logical}"
    hash_manifest.write_text(json.dumps(hash_payload), encoding="utf-8")
    destination = hash_manifest.parent.with_name(hash_payload["snapshot_id"])
    hash_manifest.parent.rename(destination)
    with pytest.raises(ValidationError, match="Unknown normalized partition logical hash"):
        load_normalized_snapshot(hash_root, hash_payload["snapshot_id"])


def test_mapping_invalid_stream_capacity_and_claim_count_faults(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invalid_root = tmp_path / "invalid-stream"
    admitted = _raw(invalid_root, "invalid-stream")
    events = [snapshot(), delta(101, 99), delta(102, 101)]
    invalid = write_normalized_events(
        invalid_root,
        events,
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[admitted.reference()],
        policy=TEST_POLICY,
    )
    assert invalid.snapshot is None
    assert invalid.quarantined_rows == 3

    calls: list[int] = []
    real_capacity = normalized_v3.require_collection_capacity

    def observed_capacity(root: Path, *, projected_write_bytes: int, policy: Any) -> None:
        calls.append(projected_write_bytes)
        real_capacity(root, projected_write_bytes=projected_write_bytes, policy=policy)

    monkeypatch.setattr(normalized_v3, "_CAPACITY_CHECK_ROWS", 1)
    monkeypatch.setattr(normalized_v3, "require_collection_capacity", observed_capacity)
    capacity_root = tmp_path / "mapping-capacity"
    capacity_raw = _raw(capacity_root, "mapping-capacity")
    capacity = write_normalized_events(
        capacity_root,
        [trade("mapping-capacity")],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[capacity_raw.reference()],
        policy=TEST_POLICY,
    )
    assert capacity.snapshot is not None
    assert len(calls) >= 3

    monkeypatch.setattr(normalized_v3, "_claim_shard_manifests", lambda _: ())
    mismatch_root = tmp_path / "mapping-claim-count"
    mismatch_raw = _raw(mismatch_root, "mapping-claim-count")
    with pytest.raises(ValidationError, match="claim row count changed"):
        write_normalized_events(
            mismatch_root,
            [trade("mapping-claim-count")],
            provider="binance",
            venue="BINANCE",
            upstream_raw_references=[mismatch_raw.reference()],
            policy=TEST_POLICY,
        )
    _assert_no_snapshot(mismatch_root)


def test_strict_claim_count_fault_does_not_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(normalized_v3, "_build_claim_index_files", lambda *_: ())
    with pytest.raises(ValidationError, match="claim row count changed"):
        _strict_batches(tmp_path, [_record_batch([snapshot()])], key="claim-count")
    _assert_no_snapshot(tmp_path)


def test_arrow_uniformity_fixed_point_null_and_missing_event_type_guards(
    tmp_path: Path,
) -> None:
    two = [trade("uniform-1"), trade("uniform-2")]
    two[1]["sequence"] = 2
    mixed_instrument = _record_batch(two)
    mixed_instrument = _replace_column(
        mixed_instrument,
        "instrument_id",
        pa.array(["BTC-USDT-SPOT", "ETH-USDT-SPOT"]),
    )
    with pytest.raises(ValidationError, match="one instrument_id"):
        _strict_batches(tmp_path / "instrument", [mixed_instrument], key="instrument")

    delta_batch = _record_batch([delta(101, 100)])
    quantity_type = delta_batch.schema.field("quantity").type
    null_quantity = pa.StructArray.from_arrays(
        [pa.array([None], type=pa.int64()), pa.array([3], type=pa.int16())],
        fields=list(quantity_type),
    )
    null_batch = _replace_column(delta_batch, "quantity", null_quantity)
    with pytest.raises(ValidationError, match="null fixed-point"):
        _strict_batches(
            tmp_path / "null-fixed",
            [_record_batch([snapshot()]), null_batch],
            key="null-fixed",
        )

    missing_event_type = pa.RecordBatch.from_arrays(
        [pa.array([1], type=pa.int64())],
        names=["value"],
    )
    with pytest.raises(ValidationError, match="at least one event_type"):
        normalized_v3._validate_common_arrow_batch(missing_event_type, provider="binance")

    null_event_type = _replace_column(
        _record_batch([trade("null-event-type")]),
        "event_type",
        pa.array([None], type=pa.string()),
    )
    with pytest.raises(ValidationError, match="event_type must be non-null"):
        _strict_batches(
            tmp_path / "null-event-type",
            [null_event_type],
            key="null-event-type",
        )


def test_duplicate_spool_ignores_non_string_ids_and_finds_repeats(tmp_path: Path) -> None:
    path = tmp_path / "duplicates.json-seq"
    path.write_text(
        json.dumps(
            [
                {"event_id": 1},
                {"event_id": "duplicate"},
                {"event_id": "duplicate"},
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    assert normalized_v3._all_duplicate_ids_from_spool(path) == {"duplicate"}


def test_mapping_duplicate_exclusion_cannot_publish_a_gapped_l2_stream(tmp_path: Path) -> None:
    raw = _raw(tmp_path, "mapping-duplicate-l2-gap")
    duplicate_one = delta(101, 100)
    duplicate_two = delta(102, 101)
    duplicate_one["event_id"] = "duplicate-l2"
    duplicate_two["event_id"] = "duplicate-l2"
    survivor = trade("unrelated-survivor")
    survivor["instrument_id"] = "ETH-USDT-SPOT"
    survivor["session_id"] = "binance-24x7-ETH-USDT-SPOT"

    result = write_normalized_events(
        tmp_path,
        [snapshot(), duplicate_one, duplicate_two, delta(103, 102), survivor],
        provider="binance",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )

    assert result.snapshot is not None
    assert result.accepted_rows == 1
    assert result.quarantined_rows == 4
    assert result.snapshot.rows == 1
    assert result.snapshot.l2_checkpoints == ()


def test_duplicate_exclusion_revalidation_rechecks_expected_l2_streams(tmp_path: Path) -> None:
    spool_path = tmp_path / "duplicate-revalidation.json-seq"
    with normalized_v3._InputSpool(spool_path) as spool:
        spool.append(snapshot())
    observed = ("binance", "BTC-USDT-SPOT", "binance-24x7-BTC-USDT-SPOT")
    absent_required = ("binance", "ETH-USDT-SPOT", "absent-required")
    absent_empty = ("binance", "SOL-USDT-SPOT", "absent-empty")
    already_invalid = ("binance", "XRP-USDT-SPOT", "already-invalid")

    invalid = normalized_v3._revalidate_streams_after_duplicate_exclusions(
        spool_path,
        provider="binance",
        invalid_streams={already_invalid: "original failure"},
        duplicate_event_ids={"unrelated-duplicate"},
        expected_l2={
            observed: {999: "not-reached"},
            absent_required: {1: "not-observed"},
            absent_empty: {},
            already_invalid: {1: "ignored-after-original-failure"},
        },
    )

    assert "not reached" in invalid[observed]
    assert "removed by duplicate" in invalid[absent_required]
    assert absent_empty not in invalid
    assert invalid[already_invalid] == "original failure"

    assert (
        normalized_v3._revalidate_streams_after_duplicate_exclusions(
            spool_path,
            provider="binance",
            invalid_streams={},
            duplicate_event_ids={"unrelated-duplicate"},
            expected_l2={observed: {}},
        )
        == {}
    )
