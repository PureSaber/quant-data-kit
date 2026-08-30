"""Streaming normalized-v3 storage with recoverable sharded event-claim indexes."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from functools import cache, lru_cache
from itertools import pairwise
from pathlib import Path
from typing import Any

import duckdb
import jsonschema_rs
import orjson
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from typing_extensions import Self

from quant_data_kit.adapters_v2.base import BOOK_SEQUENCE_FACTOR
from quant_data_kit.data_lake import (
    EventClaimIndexManifest,
    EventClaimReference,
    EventClaimSequence,
    EventClaimShardManifest,
    L2CheckpointManifest,
    NormalizationResult,
    NormalizedSnapshot,
    PartitionManifest,
    QuarantineEntry,
    RawObjectReference,
    StoragePolicy,
    _canonical_json_bytes,
    _event_claim_reference,
    _event_schema_id,
    _json_evidence,
    _lake_lock,
    _mkdir_in_lake,
    _partition_segment,
    _publish_tree_entry,
    _replace_tree_entry,
    _resolved_lake_root,
    _safe_snapshot_partition,
    _segment,
    _sha256_bytes,
    _sha256_file,
    _stream_key,
    _utc_datetime,
    _utc_text,
    _validate_lake_path,
    _validate_quarantine_batch,
    require_collection_capacity,
    validate_raw_reference,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.l2_replay import L2BookReconstructor, L2ReplayError
from quant_data_kit.process_lock import process_file_lock
from quant_data_kit.schemas_v2 import (
    BOOK_DELTA_EVENT_SCHEMA_ID,
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    SCHEMA_VERSION_V2,
    _validate_record_semantics,
    get_arrow_schema,
    get_json_schema,
    validate_arrow_table,
)

LAYOUT_VERSION = "3.0.0"
CLAIM_FORMAT = "streaming-parquet-v2"
CLAIM_VERSION = "3.1.0"
CANONICAL_JSON_PARTITION_HASH = "canonical-json-array-v1"
ARROW_IPC_PARTITION_HASH = "arrow-ipc-record-batch-v1"
_BATCH_ROWS = 65_536
_CAPACITY_CHECK_ROWS = 1_000_000
_CLAIM_SHARD_PREFIX_LENGTH = 1
_CLAIM_FILE_SCHEMA = pa.schema(
    [
        pa.field("event_id_hash", pa.string()),
        pa.field("event_id", pa.string()),
        pa.field("schema_id", pa.string()),
        pa.field("event_sha256", pa.string()),
        pa.field("claim_sha256", pa.string()),
    ]
)

_SortKey = tuple[datetime, int, str]


class _ClaimIndexMissingError(ValidationError):
    """Raised only when a recoverable claim-index acceleration file is absent."""


@cache
def _native_validator(schema_id: str) -> jsonschema_rs.Draft202012Validator:
    return jsonschema_rs.Draft202012Validator(
        get_json_schema(schema_id),
        validate_formats=True,
    )


def _validate_l2_semantics(schema_id: str, payload: Mapping[str, Any]) -> None:
    event_time = str(payload["event_time"])
    received_at = str(payload["received_at"])
    available_at = str(payload["available_at"])
    if not (event_time == received_at == available_at):
        parsed_event = _utc_datetime(event_time, "event_time")
        parsed_received = _utc_datetime(received_at, "received_at")
        parsed_available = _utc_datetime(available_at, "available_at")
        if parsed_event > parsed_received:
            raise ValidationError("received_at must not be earlier than event_time")
        if parsed_received > parsed_available:
            raise ValidationError("available_at must not be earlier than received_at")

    if schema_id == BOOK_SNAPSHOT_EVENT_SCHEMA_ID:
        levels = [*payload["bids"], *payload["asks"]]
        if len({int(level["price"]["scale"]) for level in levels}) != 1:
            raise ValidationError("book prices must use one scale")
        if any(int(level["price"]["units"]) <= 0 for level in levels):
            raise ValidationError("price must be positive")
        if any(int(level["quantity"]["units"]) < 0 for level in levels):
            raise ValidationError("quantity must be non-negative")
        bids = [int(level["price"]["units"]) for level in payload["bids"]]
        asks = [int(level["price"]["units"]) for level in payload["asks"]]
        if bids != sorted(bids, reverse=True) or len(bids) != len(set(bids)):
            raise ValidationError("book bids must be strictly ordered without duplicates")
        if asks != sorted(asks) or len(asks) != len(set(asks)):
            raise ValidationError("book asks must be strictly ordered without duplicates")
        if bids[0] >= asks[0]:
            raise ValidationError("book snapshot is locked or crossed")
        return

    price_units = int(payload["price"]["units"])
    quantity_units = int(payload["quantity"]["units"])
    if price_units <= 0:
        raise ValidationError("price must be positive")
    if quantity_units < 0:
        raise ValidationError("quantity must be non-negative")
    if int(payload["previous_sequence"]) >= int(payload["sequence"]):
        raise ValidationError("previous_sequence must precede sequence")
    if payload["action"] == "delete" and quantity_units != 0:
        raise ValidationError("delete delta quantity must be zero")
    if payload["action"] == "upsert" and quantity_units == 0:
        raise ValidationError("upsert delta quantity must be positive")


def _validate_event_record(schema_id: str, payload: dict[str, Any]) -> None:
    try:
        _native_validator(schema_id).validate(payload)
    except jsonschema_rs.ValidationError as exc:
        raise ValidationError(f"JSON schema validation failed for {schema_id}: {exc}") from exc
    if schema_id in {BOOK_SNAPSHOT_EVENT_SCHEMA_ID, BOOK_DELTA_EVENT_SCHEMA_ID}:
        _validate_l2_semantics(schema_id, payload)
    else:
        _validate_record_semantics(schema_id, payload)


@lru_cache(maxsize=4096)
def _cached_timestamp(value: str) -> datetime:
    return _utc_datetime(value, "event_timestamp")


@lru_cache(maxsize=1024)
def _cached_date(value: str) -> date:
    return date.fromisoformat(value)


@lru_cache(maxsize=4096)
def _cached_utc_text(value: datetime, field_name: str) -> str:
    return _utc_text(value, field_name)


@lru_cache(maxsize=1024)
def _cached_date_text(value: date) -> str:
    return value.isoformat()


def _arrow_ready_rows(schema: pa.Schema, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    timestamp_names = {field.name for field in schema if pa.types.is_timestamp(field.type)}
    date_names = {field.name for field in schema if pa.types.is_date32(field.type)}
    for row in rows:
        item = dict(row)
        for name in timestamp_names:
            value = item[name]
            if value is not None and not isinstance(value, datetime):
                item[name] = _cached_timestamp(str(value))
        for name in date_names:
            value = item[name]
            if value is not None and not isinstance(value, date):
                item[name] = _cached_date(str(value))
        prepared.append(item)
    return prepared


class _InputSpool:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._stream = path.open("xb")
        self._buffer: list[dict[str, Any]] = []
        self._requires_evidence_encoding = False

    def append(self, record: dict[str, Any], *, requires_evidence_encoding: bool = False) -> None:
        self._buffer.append(record)
        self._requires_evidence_encoding |= requires_evidence_encoding
        if len(self._buffer) >= _BATCH_ROWS:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        try:
            body = (
                orjson.dumps(
                    [_json_evidence(record) for record in self._buffer],
                    option=orjson.OPT_SORT_KEYS,
                )
                if self._requires_evidence_encoding
                else orjson.dumps(self._buffer, option=orjson.OPT_SORT_KEYS)
            )
        except (TypeError, orjson.JSONEncodeError):
            body = orjson.dumps(
                [_json_evidence(record) for record in self._buffer],
                option=orjson.OPT_SORT_KEYS,
            )
        self._stream.write(body)
        self._stream.write(b"\n")
        self._buffer.clear()
        self._requires_evidence_encoding = False

    def close(self) -> None:
        self.flush()
        self._stream.flush()
        os.fsync(self._stream.fileno())
        self._stream.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _iter_spooled_records(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("rb") as stream:
        for line in stream:
            if not line.strip():
                continue
            batch = orjson.loads(line)
            if not isinstance(batch, list):
                raise ValidationError("Normalized input spool batch is malformed")
            for record in batch:
                if not isinstance(record, dict):
                    raise ValidationError("Normalized input spool record is malformed")
                yield record


def _has_non_finite(value: Any) -> bool:
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Mapping):
        return any(_has_non_finite(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_has_non_finite(item) for item in value)
    return False


@dataclass
class _OpenPartition:
    key: tuple[str, str, str]
    schema_id: str
    relative_path: Path
    path: Path
    buffer: list[dict[str, Any]]
    writer: pq.ParquetWriter | None = None
    rows: int = 0
    last_sort_key: _SortKey | None = None
    monotonic: bool = True
    logical_digest: _JsonArrayDigest | None = None
    arrow_buffer: list[pa.RecordBatch] = field(default_factory=list)
    arrow_buffered_rows: int = 0

    def append(self, record: dict[str, Any]) -> None:
        sort_key = (
            _cached_timestamp(str(record["event_time"])),
            int(record["sequence"]),
            str(record["event_id"]),
        )
        if self.last_sort_key is not None and sort_key < self.last_sort_key:
            self.monotonic = False
        self.last_sort_key = sort_key
        self.buffer.append(record)

    def flush(self) -> int:
        if not self.buffer:
            return 0
        schema = get_arrow_schema(self.schema_id)
        prepared = _arrow_ready_rows(schema, self.buffer)
        table = pa.Table.from_pylist(prepared, schema=schema)
        validate_arrow_table(self.schema_id, table)
        if self.logical_digest is None:
            self.logical_digest = _JsonArrayDigest(
                preserve_stdlib_float_format=any(
                    _contains_floating_type(field.type) for field in schema
                )
            )
        self.logical_digest.update(_logical_rows(schema, prepared))
        if self.writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.writer = pq.ParquetWriter(
                self.path,
                schema,
                compression="zstd",
                use_dictionary=False,
            )
        self.writer.write_table(table, row_group_size=_BATCH_ROWS)
        flushed = table.num_rows
        self.rows += flushed
        self.buffer.clear()
        return flushed

    def append_record_batch(
        self,
        batch: pa.RecordBatch,
        *,
        first_sort_key: _SortKey,
        last_sort_key: _SortKey,
    ) -> None:
        if self.buffer:
            self.flush()
        if self.last_sort_key is not None and first_sort_key < self.last_sort_key:
            raise ValidationError("Arrow event batches must be globally monotonic per partition")
        if last_sort_key < first_sort_key:
            raise ValidationError("Arrow event batch sort order moved backwards")
        self.last_sort_key = last_sort_key
        schema = get_arrow_schema(self.schema_id)
        if batch.schema != schema:
            raise ValidationError(
                f"Arrow schema mismatch for {self.schema_id}: "
                f"expected={schema}, actual={batch.schema}"
            )
        offset = 0
        while offset < batch.num_rows:
            take = min(_BATCH_ROWS - self.arrow_buffered_rows, batch.num_rows - offset)
            self.arrow_buffer.append(batch.slice(offset, take))
            self.arrow_buffered_rows += take
            offset += take
            if self.arrow_buffered_rows == _BATCH_ROWS:
                self._flush_arrow_buffer()

    def _flush_arrow_buffer(self) -> None:
        if not self.arrow_buffer:
            return
        schema = get_arrow_schema(self.schema_id)
        table = pa.Table.from_batches(self.arrow_buffer, schema=schema).combine_chunks()
        batches = table.to_batches(max_chunksize=_BATCH_ROWS)
        if len(batches) != 1 or batches[0].num_rows != self.arrow_buffered_rows:
            raise ValidationError("Arrow partition buffer did not form one bounded batch")
        if self.writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.writer = pq.ParquetWriter(
                self.path,
                schema,
                compression="zstd",
                use_dictionary=False,
            )
        self.writer.write_batch(batches[0], row_group_size=_BATCH_ROWS)
        self.rows += batches[0].num_rows
        self.arrow_buffer.clear()
        self.arrow_buffered_rows = 0

    def close(self) -> None:
        self.flush()
        self._flush_arrow_buffer()
        if self.writer is not None:
            self.writer.close()
            self.writer = None


class _PartitionSet:
    def __init__(self, root: Path, provider: str, venue: str) -> None:
        self.root = root
        self.provider = provider
        self.venue = venue
        self.partitions: dict[tuple[str, str, str], _OpenPartition] = {}
        self.buffered_rows = 0

    def append(self, record: dict[str, Any]) -> None:
        event_type, trading_date, instrument_id, instrument_segment = _partition_identity(
            str(record["event_type"]),
            str(record["trading_day"]),
            str(record["instrument_id"]),
        )
        key = (event_type, trading_date, instrument_id)
        partition = self.partitions.get(key)
        if partition is None:
            relative = Path(
                f"provider={self.provider}/venue={self.venue}/event_type={event_type}/"
                f"date={trading_date}/instrument={instrument_segment}/data.parquet"
            )
            partition = _OpenPartition(
                key=key,
                schema_id=_event_schema_id(record),
                relative_path=relative,
                path=self.root / relative,
                buffer=[],
            )
            self.partitions[key] = partition
        elif _event_schema_id(record) != partition.schema_id:
            raise ValidationError("Mixed schemas entered one normalized partition")
        partition.append(record)
        self.buffered_rows += 1
        if len(partition.buffer) >= _BATCH_ROWS:
            self.buffered_rows -= partition.flush()
        elif self.buffered_rows >= _BATCH_ROWS * 2:
            self.flush_all()

    def flush_all(self) -> None:
        for partition in self.partitions.values():
            self.buffered_rows -= partition.flush()
        if self.buffered_rows != 0:
            raise ValidationError("Normalized partition buffer accounting changed")

    def append_record_batch(
        self,
        batch: pa.RecordBatch,
        *,
        schema_id: str,
        event_type: str,
        trading_date: str,
        instrument_id: str,
        first_sort_key: _SortKey,
        last_sort_key: _SortKey,
    ) -> None:
        event_type, trading_date, instrument_id, instrument_segment = _partition_identity(
            event_type,
            trading_date,
            instrument_id,
        )
        key = (event_type, trading_date, instrument_id)
        partition = self.partitions.get(key)
        if partition is None:
            relative = Path(
                f"provider={self.provider}/venue={self.venue}/event_type={event_type}/"
                f"date={trading_date}/instrument={instrument_segment}/data.parquet"
            )
            partition = _OpenPartition(
                key=key,
                schema_id=schema_id,
                relative_path=relative,
                path=self.root / relative,
                buffer=[],
            )
            self.partitions[key] = partition
        elif schema_id != partition.schema_id:
            raise ValidationError("Mixed schemas entered one normalized partition")
        partition.append_record_batch(
            batch,
            first_sort_key=first_sort_key,
            last_sort_key=last_sort_key,
        )

    def close(self) -> None:
        self.flush_all()
        for partition in self.partitions.values():
            partition.close()

    def paths(self) -> tuple[Path, ...]:
        return tuple(
            self.partitions[key].path
            for key in sorted(self.partitions)
            if self.partitions[key].rows
        )


@lru_cache(maxsize=4096)
def _partition_identity(
    event_type: str,
    trading_date: str,
    instrument_id: str,
) -> tuple[str, str, str, str]:
    return (
        _segment(event_type, "event_type"),
        _segment(trading_date, "trading_day"),
        instrument_id,
        _partition_segment(instrument_id, "instrument_id"),
    )


@contextmanager
def _streaming_stage(root: Path) -> Iterator[Path]:
    staging_root = _mkdir_in_lake(root, root / "normalized" / "staging")
    owners_root = _mkdir_in_lake(root, root / "normalized" / ".stage-owners")
    for stale in sorted(staging_root.glob("normalized-batch-stream-*")):
        checked = _validate_lake_path(root, stale, allow_missing=False)
        if not checked.is_dir():
            raise ValidationError(f"Normalized staging entry is not a directory: {checked}")
        owner = owners_root / f"{checked.name}.lock"
        try:
            with process_file_lock(owner, timeout_seconds=0.01):
                if checked.exists():
                    shutil.rmtree(checked)
        except TimeoutError:
            continue
    operation = uuid.uuid4().hex
    stage = staging_root / f"normalized-batch-stream-{operation}"
    owner = owners_root / f"{stage.name}.lock"
    with process_file_lock(owner):
        stage.mkdir(exist_ok=False)
        try:
            yield stage
        finally:
            if stage.exists():
                shutil.rmtree(stage)
    if owner.exists():
        owner.unlink()


def _sql_text(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sql_path_list(paths: Iterable[Path]) -> str:
    return "[" + ",".join(_sql_text(path) for path in paths) + "]"


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _canonical_sql_value(data_type: pa.DataType, expression: str) -> str:
    if pa.types.is_timestamp(data_type):
        return (
            "CASE WHEN microsecond("
            + expression
            + ") % 1000000 = 0 THEN strftime("
            + expression
            + ", '%Y-%m-%dT%H:%M:%SZ') ELSE strftime("
            + expression
            + ", '%Y-%m-%dT%H:%M:%S.%fZ') END"
        )
    if pa.types.is_date32(data_type):
        return f"strftime({expression}, '%Y-%m-%d')"
    if pa.types.is_struct(data_type):
        entries: list[str] = []
        for field in sorted(data_type, key=lambda item: item.name):
            child = f"({expression}).{_sql_identifier(field.name)}"
            entries.extend(
                [
                    _sql_text(field.name),
                    _canonical_sql_value(field.type, child),
                ]
            )
        return "json_object(" + ",".join(entries) + ")"
    if pa.types.is_list(data_type):
        child = _canonical_sql_value(data_type.value_type, "claim_item")
        return f"list_transform({expression}, claim_item -> {child})"
    return expression


def _canonical_row_sql(schema_id: str) -> str:
    entries: list[str] = []
    for schema_field in sorted(get_arrow_schema(schema_id), key=lambda item: item.name):
        entries.extend(
            [
                _sql_text(schema_field.name),
                _canonical_sql_value(schema_field.type, _sql_identifier(schema_field.name)),
            ]
        )
    return "json_object(" + ",".join(entries) + ")"


def _canonical_event_sql(schema_id: str) -> str:
    return (
        "json_object('record',"
        + _canonical_row_sql(schema_id)
        + ",'schema_id',"
        + _sql_text(schema_id)
        + ")"
    )


def _sort_partition(partition: _OpenPartition) -> None:
    if partition.monotonic or partition.rows == 0:
        return
    sorted_path = partition.path.with_name("data.sorted.parquet")
    schema = get_arrow_schema(partition.schema_id)
    connection = duckdb.connect(database=":memory:")
    writer: pq.ParquetWriter | None = None
    try:
        connection.execute("SET TimeZone = 'UTC'")
        reader = connection.sql(
            "SELECT * FROM read_parquet("
            + _sql_text(partition.path)
            + ", hive_partitioning=false) "
            "ORDER BY event_time, sequence, event_id"
        ).to_arrow_reader(_BATCH_ROWS)
        writer = pq.ParquetWriter(
            sorted_path,
            schema,
            compression="zstd",
            use_dictionary=False,
        )
        for batch in reader:
            table = pa.Table.from_batches([batch]).cast(schema, safe=True)
            validate_arrow_table(partition.schema_id, table)
            writer.write_table(table, row_group_size=_BATCH_ROWS)
    finally:
        if writer is not None:
            writer.close()
        connection.close()
    os.replace(sorted_path, partition.path)


class _JsonArrayDigest:
    def __init__(self, *, preserve_stdlib_float_format: bool) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b"[")
        self._has_rows = False
        self._preserve_stdlib_float_format = preserve_stdlib_float_format

    def update(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        payload = (
            _canonical_json_bytes(rows)
            if self._preserve_stdlib_float_format
            else orjson.dumps(rows, option=orjson.OPT_SORT_KEYS)
        )
        if payload[:1] != b"[" or payload[-1:] != b"]":
            raise ValidationError("Canonical Arrow batch serialization changed")
        body = payload[1:-1]
        if self._has_rows:
            self._digest.update(b",")
        self._digest.update(body)
        self._has_rows = True

    def hexdigest(self) -> str:
        result = self._digest.copy()
        result.update(b"]")
        return result.hexdigest()


def _logical_rows(schema: pa.Schema, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    timestamp_names = {field.name for field in schema if pa.types.is_timestamp(field.type)}
    date_names = {field.name for field in schema if pa.types.is_date32(field.type)}
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for name in timestamp_names:
            value = item[name]
            if value is not None:
                item[name] = _cached_utc_text(value, name)
        for name in date_names:
            value = item[name]
            if value is not None:
                item[name] = _cached_date_text(value)
        normalized.append(item)
    return normalized


def _contains_floating_type(data_type: pa.DataType) -> bool:
    if pa.types.is_floating(data_type):
        return True
    if pa.types.is_struct(data_type):
        return any(_contains_floating_type(field.type) for field in data_type)
    if pa.types.is_list(data_type):
        return _contains_floating_type(data_type.value_type)
    return False


def _scan_partition(
    path: Path,
    schema_id: str,
) -> tuple[int, str]:
    schema = get_arrow_schema(schema_id)
    parquet = pq.ParquetFile(path)
    try:
        if parquet.schema_arrow != schema:
            raise ValidationError(
                f"Arrow schema mismatch for {schema_id}: "
                f"expected={schema}, actual={parquet.schema_arrow}"
            )
        if not any(_contains_floating_type(field.type) for field in schema):
            expected_rows = parquet.metadata.num_rows
            parquet.close()
            connection = duckdb.connect(database=":memory:")
            try:
                connection.execute("SET TimeZone = 'UTC'")
                connection.execute("SET threads = 8")
                connection.execute("SET preserve_insertion_order = true")
                reader = connection.sql(
                    "SELECT "
                    + _canonical_row_sql(schema_id)
                    + " AS canonical_row FROM read_parquet(?, hive_partitioning=false)",
                    params=[str(path)],
                ).to_arrow_reader(_BATCH_ROWS)
                digest = hashlib.sha256(b"[")
                rows = 0
                first = True
                for batch in reader:
                    values = batch.column(0).to_pylist()
                    if not first:
                        digest.update(b",")
                    digest.update(",".join(values).encode("utf-8"))
                    first = False
                    rows += len(values)
                digest.update(b"]")
            finally:
                connection.close()
            if rows != expected_rows:
                raise ValidationError("Normalized Parquet row count changed during scan")
            return rows, digest.hexdigest()
        rows = 0
        logical = _JsonArrayDigest(preserve_stdlib_float_format=True)
        for batch in parquet.iter_batches(batch_size=_BATCH_ROWS):
            table = pa.Table.from_batches([batch])
            validate_arrow_table(schema_id, table)
            batch_rows = table.to_pylist()
            logical.update(_logical_rows(schema, batch_rows))
            rows += table.num_rows
        return rows, logical.hexdigest()
    finally:
        parquet.close()


def _scan_partition_arrow_ipc(path: Path, schema_id: str) -> tuple[int, str]:
    schema = get_arrow_schema(schema_id)
    parquet = pq.ParquetFile(path)
    digest = hashlib.sha256()
    digest.update(b"puresaber:arrow-ipc-record-batch-v1\0")
    schema_bytes = schema.serialize().to_pybytes()
    digest.update(len(schema_bytes).to_bytes(8, "big"))
    digest.update(schema_bytes)
    rows = 0
    try:
        if parquet.schema_arrow != schema:
            raise ValidationError(
                f"Arrow schema mismatch for {schema_id}: "
                f"expected={schema}, actual={parquet.schema_arrow}"
            )
        for batch in parquet.iter_batches(batch_size=_BATCH_ROWS):
            payload = batch.serialize().to_pybytes()
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
            rows += batch.num_rows
    finally:
        parquet.close()
    digest.update(rows.to_bytes(8, "big"))
    return rows, digest.hexdigest()


def _partition_manifests(
    partitions: _PartitionSet,
    *,
    provider: str,
    venue: str,
    logical_hash_version: str = CANONICAL_JSON_PARTITION_HASH,
) -> tuple[PartitionManifest, ...]:
    manifests: list[PartitionManifest] = []
    for key in sorted(partitions.partitions):
        partition = partitions.partitions[key]
        _sort_partition(partition)
        if logical_hash_version == ARROW_IPC_PARTITION_HASH:
            rows, logical_sha256 = _scan_partition_arrow_ipc(
                partition.path,
                partition.schema_id,
            )
        elif logical_hash_version != CANONICAL_JSON_PARTITION_HASH:
            raise ValidationError(
                f"Unknown normalized partition logical hash: {logical_hash_version}"
            )
        elif partition.monotonic and partition.logical_digest is not None:
            parquet = pq.ParquetFile(partition.path)
            try:
                if parquet.schema_arrow != get_arrow_schema(partition.schema_id):
                    raise ValidationError("Normalized streaming partition schema changed")
                rows = parquet.metadata.num_rows
            finally:
                parquet.close()
            logical_sha256 = partition.logical_digest.hexdigest()
        else:
            rows, logical_sha256 = _scan_partition(partition.path, partition.schema_id)
        if rows != partition.rows:
            raise ValidationError("Normalized streaming partition row count changed")
        event_type, trading_date, instrument_id = key
        manifests.append(
            PartitionManifest(
                relative_path=partition.relative_path.as_posix(),
                provider=provider,
                venue=venue,
                event_type=event_type,
                trading_date=trading_date,
                instrument_id=instrument_id,
                schema_id=partition.schema_id,
                rows=rows,
                logical_sha256=logical_sha256,
                content_sha256=_sha256_file(partition.path),
            )
        )
    return tuple(manifests)


def _duplicate_event_ids(paths: tuple[Path, ...]) -> set[str]:
    if not paths:
        return set()
    connection = duckdb.connect(database=":memory:")
    try:
        query = (
            "SELECT event_id FROM read_parquet("
            + _sql_path_list(paths)
            + ", union_by_name=true, hive_partitioning=false) "
            "GROUP BY event_id HAVING count(*) > 1"
        )
        return {str(row[0]) for row in connection.execute(query).fetchall()}
    finally:
        connection.close()


def _publish_quarantine_file(
    root: Path,
    source_path: Path,
    *,
    provider: str,
    venue: str,
    rows: int,
    policy: StoragePolicy,
) -> Path | None:
    if rows == 0:
        return None
    content_sha256 = _sha256_file(source_path)
    batch_id = f"sha256-{content_sha256}"
    batch_dir = root / "quarantine" / batch_id
    manifest = {
        "schema_version": "2.0.0",
        "layer": "quarantine",
        "batch_id": batch_id,
        "provider": provider,
        "venue": venue,
        "rows": rows,
        "content_sha256": content_sha256,
        "data_path": "records.jsonl",
    }
    manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    with _lake_lock(root, "quarantine-batch", {"batch_id": batch_id}):
        if batch_dir.exists():
            return _validate_quarantine_batch(root, batch_dir, manifest)
        require_collection_capacity(
            root,
            projected_write_bytes=source_path.stat().st_size + len(manifest_bytes),
            policy=policy,
        )
        staging_root = _mkdir_in_lake(root, root / "quarantine" / ".staging")
        stage = staging_root / f"{batch_id}-{uuid.uuid4().hex}"
        stage.mkdir(exist_ok=False)
        try:
            with source_path.open("rb") as source, (stage / "records.jsonl").open("wb") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)
                target.flush()
                os.fsync(target.fileno())
            (stage / "manifest.json").write_bytes(manifest_bytes)
            _validate_quarantine_batch(root, stage, manifest)
            _mkdir_in_lake(root, batch_dir.parent)
            _publish_tree_entry(root, stage, batch_dir, policy=policy)
            return _validate_quarantine_batch(root, batch_dir, manifest)
        finally:
            if stage.exists():
                shutil.rmtree(stage)


def _rebuild_after_exclusions(
    snapshot_root: Path,
    spool_path: Path,
    *,
    provider: str,
    venue: str,
    invalid_streams: Mapping[tuple[str, str, str], str],
    duplicate_event_ids: set[str],
    quarantine_path: Path,
) -> tuple[_PartitionSet, int, int, str | None]:
    if snapshot_root.exists():
        shutil.rmtree(snapshot_root)
    snapshot_root.mkdir()
    partitions = _PartitionSet(snapshot_root, provider, venue)
    accepted = 0
    quarantined = 0
    latest_available: datetime | None = None
    with quarantine_path.open("xb") as quarantine:
        for index, record in enumerate(_iter_spooled_records(spool_path)):
            event_id = record.get("event_id")
            if isinstance(event_id, str) and event_id in duplicate_event_ids:
                reason = "global_duplicate_event_id"
            else:
                reason = invalid_streams.get(_stream_key(record, index))
            if reason is None:
                partitions.append(record)
                accepted += 1
                available = _utc_datetime(record["available_at"], "available_at")
                if latest_available is None or available > latest_available:
                    latest_available = available
                continue
            entry = QuarantineEntry(
                input_index=index,
                reason=reason,
                record=record,
            )
            quarantine.write(_canonical_json_bytes(_json_evidence(asdict(entry))))
            quarantine.write(b"\n")
            quarantined += 1
        quarantine.flush()
        os.fsync(quarantine.fileno())
    partitions.close()
    created_at = _utc_text(latest_available, "available_at") if latest_available else None
    return partitions, accepted, quarantined, created_at


def _claim_database(
    database_path: Path,
    snapshot_root: Path,
    partitions: tuple[PartitionManifest, ...],
) -> duckdb.DuckDBPyConnection:
    temporary_root = database_path.with_suffix(".tmp")
    temporary_root.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(database=str(database_path))
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute("SET threads = 8")
    connection.execute("SET memory_limit = '2GB'")
    connection.execute("SET temp_directory = " + _sql_text(temporary_root))
    connection.execute(
        "CREATE TABLE claims ("
        "shard VARCHAR NOT NULL, event_id_hash VARCHAR NOT NULL, "
        "event_id VARCHAR NOT NULL, schema_id VARCHAR NOT NULL, "
        "event_sha256 VARCHAR NOT NULL, claim_sha256 VARCHAR NOT NULL)"
    )
    for partition in partitions:
        path = snapshot_root / partition.relative_path
        schema = get_arrow_schema(partition.schema_id)
        if any(_contains_floating_type(field.type) for field in schema):
            parquet = pq.ParquetFile(path)
            try:
                for batch in parquet.iter_batches(batch_size=_BATCH_ROWS):
                    rows = _logical_rows(schema, pa.Table.from_batches([batch]).to_pylist())
                    claims = [_event_claim_reference(partition.schema_id, row) for row in rows]
                    connection.executemany(
                        "INSERT INTO claims VALUES (?, ?, ?, ?, ?, ?)",
                        [
                            (
                                claim.event_id_hash[:_CLAIM_SHARD_PREFIX_LENGTH],
                                claim.event_id_hash,
                                claim.event_id,
                                claim.schema_id,
                                claim.event_sha256,
                                claim.claim_sha256,
                            )
                            for claim in claims
                        ],
                    )
            finally:
                parquet.close()
            continue
        canonical_event = _canonical_event_sql(partition.schema_id)
        connection.execute(
            "INSERT INTO claims "
            "WITH event_rows AS ("
            "SELECT event_id, sha256(event_id) AS event_id_hash, "
            "sha256(" + canonical_event + ") AS event_sha256 "
            "FROM read_parquet(?, hive_partitioning=false) t"
            ") "
            "SELECT substr(event_id_hash, 1, ?), event_id_hash, event_id, ?, "
            "event_sha256, sha256(json_object("
            "'event_id', event_id, 'event_id_hash', event_id_hash, "
            "'event_sha256', event_sha256, 'layer', 'normalized-event-claim', "
            "'schema_id', ?, 'schema_version', '2.0.0')) FROM event_rows",
            [
                str(path),
                _CLAIM_SHARD_PREFIX_LENGTH,
                partition.schema_id,
                partition.schema_id,
            ],
        )
    repeated = connection.execute(
        "SELECT event_id_hash, min(event_id), max(event_id), count(*) FROM claims "
        "GROUP BY event_id_hash HAVING count(*) > 1 LIMIT 1"
    ).fetchone()
    if repeated is not None:
        connection.close()
        if repeated[1] != repeated[2]:
            raise ValidationError("Normalized batch contains colliding event_id hashes")
        raise ValidationError(
            f"Normalized accepted set still contains duplicate event_id: {repeated[1]}"
        )
    return connection


def _claim_shard_manifests(
    connection: duckdb.DuckDBPyConnection,
) -> tuple[EventClaimShardManifest, ...]:
    rows = connection.execute(_claim_multiset_manifest_sql("claims")).fetchall()
    return tuple(
        EventClaimShardManifest(
            shard=str(shard),
            rows=int(count),
            logical_sha256=str(logical_sha256),
        )
        for shard, count, logical_sha256 in rows
    )


def _claim_multiset_manifest_sql(source: str) -> str:
    chunks = [
        f"cast('0x' || substr(claim_sha256, {offset}, 16) as ubigint)" for offset in (1, 17, 33, 49)
    ]
    aggregate_fields: list[str] = [
        "'algorithm', 'sha256-multiset-u64x4-v1'",
        "'rows', count(*)",
    ]
    for index, chunk in enumerate(chunks):
        aggregate_fields.extend(
            [
                _sql_text(f"xor_{index}"),
                f"printf('%016x', bit_xor({chunk}))",
                _sql_text(f"sum_{index}"),
                f"cast(sum(cast({chunk} as hugeint)) as varchar)",
            ]
        )
    return (
        "SELECT substr(event_id_hash, 1, "
        + str(_CLAIM_SHARD_PREFIX_LENGTH)
        + ") AS shard, count(*) AS rows, sha256(json_object("
        + ",".join(aggregate_fields)
        + ")) AS logical_sha256 FROM "
        + source
        + " GROUP BY substr(event_id_hash, 1, "
        + str(_CLAIM_SHARD_PREFIX_LENGTH)
        + ") ORDER BY shard"
    )


def _export_claim_index(
    connection: duckdb.DuckDBPyConnection,
    index_root: Path,
    *,
    snapshot_id: str,
    snapshot_logical_sha256: str,
    shards: tuple[EventClaimShardManifest, ...],
) -> None:
    if index_root.exists():
        shutil.rmtree(index_root)
    index_root.mkdir(parents=True)
    connection.execute("SET threads = 1")
    connection.execute(
        "COPY (SELECT event_id_hash, event_id, schema_id, event_sha256, claim_sha256 "
        "FROM claims) TO "
        + _sql_text(index_root / "claims.parquet")
        + " (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 65536)"
    )
    files = []
    for path in sorted(index_root.rglob("*.parquet")):
        table = pq.ParquetFile(path).schema_arrow
        if table != _CLAIM_FILE_SCHEMA:
            raise ValidationError("Normalized event-claim index Arrow schema changed")
        files.append(
            {
                "relative_path": path.relative_to(index_root).as_posix(),
                "content_sha256": _sha256_file(path),
            }
        )
    manifest = {
        "schema_version": LAYOUT_VERSION,
        "layer": "normalized-event-claim-index",
        "snapshot_id": snapshot_id,
        "snapshot_logical_sha256": snapshot_logical_sha256,
        "format": CLAIM_FORMAT,
        "claim_version": CLAIM_VERSION,
        "rows": sum(item.rows for item in shards),
        "shards": [asdict(item) for item in shards],
        "files": files,
    }
    manifest["manifest_sha256"] = _sha256_bytes(_canonical_json_bytes(manifest))
    (index_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _claim_select_sql(snapshot_root: Path, partition: PartitionManifest) -> str:
    path = snapshot_root / partition.relative_path
    canonical_event = _canonical_event_sql(partition.schema_id)
    return (
        "SELECT substr(event_id_hash, 1, "
        + str(_CLAIM_SHARD_PREFIX_LENGTH)
        + ") AS shard, event_id_hash, event_id, "
        + _sql_text(partition.schema_id)
        + " AS schema_id, event_sha256, sha256(json_object("
        "'event_id', event_id, 'event_id_hash', event_id_hash, "
        "'event_sha256', event_sha256, 'layer', 'normalized-event-claim', "
        "'schema_id', "
        + _sql_text(partition.schema_id)
        + ", 'schema_version', '2.0.0')) AS claim_sha256 FROM ("
        "SELECT event_id, sha256(event_id) AS event_id_hash, sha256("
        + canonical_event
        + ") AS event_sha256 FROM read_parquet("
        + _sql_text(path)
        + ", hive_partitioning=false)) event_rows"
    )


def _claim_file_order(path: Path) -> tuple[str, int, str]:
    suffix = path.stem.rsplit("-", 1)[-1]
    return path.parent.name, int(suffix) if suffix.isdigit() else -1, path.name


def _claim_shard_manifests_from_paths(
    paths: Iterable[Path],
) -> tuple[EventClaimShardManifest, ...]:
    all_paths = tuple(paths)
    if not all_paths:
        return ()
    for path in sorted(all_paths, key=_claim_file_order):
        parquet = pq.ParquetFile(path)
        try:
            if parquet.schema_arrow != _CLAIM_FILE_SCHEMA:
                raise ValidationError("Normalized event-claim index Arrow schema changed")
        finally:
            parquet.close()
    relation = (
        "read_parquet("
        + _sql_path_list(all_paths)
        + ", union_by_name=true, hive_partitioning=false)"
    )
    connection = duckdb.connect(database=":memory:")
    try:
        rows = connection.execute(_claim_multiset_manifest_sql(relation)).fetchall()
    finally:
        connection.close()
    return tuple(
        EventClaimShardManifest(
            shard=str(shard),
            rows=int(count),
            logical_sha256=str(logical_sha256),
        )
        for shard, count, logical_sha256 in rows
    )


def _write_claim_index_manifest(
    index_root: Path,
    *,
    snapshot_id: str,
    snapshot_logical_sha256: str,
    shards: tuple[EventClaimShardManifest, ...],
) -> None:
    files = [
        {
            "relative_path": path.relative_to(index_root).as_posix(),
            "content_sha256": _sha256_file(path),
        }
        for path in sorted(_claim_index_files(index_root), key=_claim_file_order)
    ]
    manifest = {
        "schema_version": LAYOUT_VERSION,
        "layer": "normalized-event-claim-index",
        "snapshot_id": snapshot_id,
        "snapshot_logical_sha256": snapshot_logical_sha256,
        "format": CLAIM_FORMAT,
        "claim_version": CLAIM_VERSION,
        "rows": sum(item.rows for item in shards),
        "shards": [asdict(item) for item in shards],
        "files": files,
    }
    manifest["manifest_sha256"] = _sha256_bytes(_canonical_json_bytes(manifest))
    (index_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _build_claim_index_files(
    snapshot_root: Path,
    partitions: tuple[PartitionManifest, ...],
    index_root: Path,
    temporary_root: Path,
) -> tuple[EventClaimShardManifest, ...]:
    if not partitions:
        return ()
    contains_float = any(
        _contains_floating_type(field.type)
        for partition in partitions
        for field in get_arrow_schema(partition.schema_id)
    )
    if index_root.exists():
        shutil.rmtree(index_root)
    temporary_root.mkdir(parents=True, exist_ok=True)
    index_root.mkdir(parents=True)
    if contains_float:
        connection = _claim_database(
            temporary_root / "floating-claims.duckdb",
            snapshot_root,
            partitions,
        )
        try:
            connection.execute("SET threads = 1")
            connection.execute(
                "COPY (SELECT event_id_hash, event_id, schema_id, event_sha256, claim_sha256 "
                "FROM claims) TO "
                + _sql_text(index_root / "claims.parquet")
                + " (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 65536)"
            )
        finally:
            connection.close()
        paths = _claim_index_files(index_root)
        if not paths:
            raise ValidationError("Normalized event-claim export produced no segments")
        return _claim_shard_manifests_from_paths(paths)
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute("SET TimeZone = 'UTC'")
        connection.execute("SET threads = 8")
        connection.execute("SET preserve_insertion_order = true")
        connection.execute("SET memory_limit = '2GB'")
        connection.execute("SET temp_directory = " + _sql_text(temporary_root))
        union = " UNION ALL ".join(
            _claim_select_sql(snapshot_root, partition) for partition in partitions
        )
        connection.execute(
            "COPY (SELECT event_id_hash, event_id, schema_id, event_sha256, claim_sha256 "
            "FROM ("
            + union
            + ") claims) TO "
            + _sql_text(index_root / "claims.parquet")
            + " (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 65536)"
        )
    finally:
        connection.close()
    paths = _claim_index_files(index_root)
    if not paths:
        raise ValidationError("Normalized event-claim export produced no segments")
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute("SET memory_limit = '2GB'")
        connection.execute("SET temp_directory = " + _sql_text(temporary_root))
        repeated = connection.execute(
            "SELECT event_id_hash FROM read_parquet(?, hive_partitioning=false) "
            "GROUP BY event_id_hash HAVING count(*) > 1 LIMIT 1",
            [str(index_root / "claims.parquet")],
        ).fetchone()
    finally:
        connection.close()
    if repeated is not None:
        raise ValidationError("Normalized event-claim index contains duplicate claims")
    return _claim_shard_manifests_from_paths(paths)


def _claim_index_root(root: Path, snapshot_id: str) -> Path:
    return root / "normalized" / "event-claim-index-v3" / f"snapshot={snapshot_id}"


def _claim_index_files(index_root: Path) -> tuple[Path, ...]:
    return tuple(sorted(index_root.rglob("*.parquet")))


def _validate_claim_index(
    root: Path,
    snapshot_id: str,
    snapshot_logical_sha256: str,
    expected: EventClaimIndexManifest,
) -> tuple[Path, ...]:
    unresolved_root = _claim_index_root(root, snapshot_id)
    if not unresolved_root.exists():
        raise _ClaimIndexMissingError("Normalized event-claim index is missing")
    index_root = _validate_lake_path(root, unresolved_root, allow_missing=False)
    unresolved_manifest = index_root / "manifest.json"
    if not unresolved_manifest.exists():
        raise _ClaimIndexMissingError("Normalized event-claim index manifest is missing")
    manifest_path = _validate_lake_path(root, unresolved_manifest, allow_missing=False)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError("Normalized event-claim index manifest is unreadable") from exc
    anchor = manifest.pop("manifest_sha256", None)
    if anchor != _sha256_bytes(_canonical_json_bytes(manifest)):
        raise ValidationError("Normalized event-claim index manifest integrity changed")
    if (
        manifest.get("schema_version") != LAYOUT_VERSION
        or manifest.get("layer") != "normalized-event-claim-index"
        or manifest.get("snapshot_id") != snapshot_id
        or manifest.get("snapshot_logical_sha256") != snapshot_logical_sha256
        or manifest.get("format") != expected.format
        or manifest.get("claim_version") != expected.claim_version
        or int(manifest.get("rows", -1)) != expected.rows
        or tuple(EventClaimShardManifest(**item) for item in manifest.get("shards", []))
        != expected.shards
    ):
        raise ValidationError("Normalized event-claim index identity changed")
    expected_files: set[Path] = {Path("manifest.json")}
    parquet_paths: list[Path] = []
    missing_files: list[Path] = []
    for item in manifest.get("files", []):
        relative = Path(str(item["relative_path"]))
        if relative.is_absolute() or ".." in relative.parts or relative in expected_files:
            raise ValidationError("Normalized event-claim index path is unsafe or duplicated")
        unresolved_path = index_root / relative
        expected_files.add(relative)
        if not unresolved_path.exists():
            missing_files.append(relative)
            continue
        path = _validate_lake_path(root, unresolved_path, allow_missing=False)
        if not path.is_file() or _sha256_file(path) != item["content_sha256"]:
            raise ValidationError("Normalized event-claim index physical content changed")
        if pq.ParquetFile(path).schema_arrow != _CLAIM_FILE_SCHEMA:
            raise ValidationError("Normalized event-claim index Arrow schema changed")
        parquet_paths.append(path)
    actual_files = {
        path.relative_to(index_root) for path in index_root.rglob("*") if path.is_file()
    }
    unexpected_files = actual_files.difference(expected_files)
    if unexpected_files:
        raise ValidationError("Normalized event-claim index has unexpected or missing files")
    if missing_files or actual_files != expected_files:
        raise _ClaimIndexMissingError("Normalized event-claim index segment is missing")
    if not parquet_paths and expected.rows:
        raise _ClaimIndexMissingError("Normalized event-claim index has no claim segments")

    actual_shards = _claim_shard_manifests_from_paths(parquet_paths)
    if actual_shards != expected.shards:
        raise ValidationError("Normalized event-claim index logical content changed")
    return tuple(parquet_paths)


def _publish_or_validate_claim_index(
    root: Path,
    staged_index: Path,
    *,
    snapshot_id: str,
    snapshot_logical_sha256: str,
    expected: EventClaimIndexManifest,
    policy: StoragePolicy | None = None,
) -> tuple[Path, ...]:
    final = _claim_index_root(root, snapshot_id)
    with _lake_lock(root, "normalized-claim-index", {"snapshot_id": snapshot_id}):
        if final.exists():
            try:
                return _validate_claim_index(
                    root,
                    snapshot_id,
                    snapshot_logical_sha256,
                    expected,
                )
            except _ClaimIndexMissingError:
                if policy is None:
                    raise ValidationError(
                        "Normalized claim-index recovery requires an explicit StoragePolicy"
                    )
                evidence_root = _mkdir_in_lake(
                    root,
                    root / "normalized" / "event-claim-index-v3" / "recovery-evidence",
                )
                evidence = evidence_root / f"{snapshot_id}-{uuid.uuid4().hex}"
                _replace_tree_entry(root, final, evidence)
                _publish_tree_entry(root, staged_index, final, policy=policy)
                return _validate_claim_index(
                    root,
                    snapshot_id,
                    snapshot_logical_sha256,
                    expected,
                )
        _mkdir_in_lake(root, final.parent)
        _validate_lake_path(root, final, allow_missing=True)
        if policy is None:
            raise ValidationError(
                "Normalized claim-index recovery requires an explicit StoragePolicy"
            )
        _publish_tree_entry(root, staged_index, final, policy=policy)
        return _validate_claim_index(
            root,
            snapshot_id,
            snapshot_logical_sha256,
            expected,
        )


def _snapshot_payload_v3(
    *,
    provider: str,
    venue: str,
    created_at: str,
    upstream_raw_references: tuple[RawObjectReference, ...],
    partitions: tuple[PartitionManifest, ...],
    event_claim_index: EventClaimIndexManifest,
    l2_checkpoints: tuple[L2CheckpointManifest, ...],
    partition_logical_hash_version: str = CANONICAL_JSON_PARTITION_HASH,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION_V2,
        "layout_version": LAYOUT_VERSION,
        "layer": "normalized",
        "provider": provider,
        "venue": venue,
        "created_at": created_at,
        "partition_logical_hash_version": partition_logical_hash_version,
        "upstream_raw_references": [asdict(item) for item in upstream_raw_references],
        "partitions": [asdict(item) for item in partitions],
        "event_claim_index": asdict(event_claim_index),
        "l2_checkpoints": [asdict(item) for item in l2_checkpoints],
    }


def _index_for_snapshot(
    root: Path,
    snapshot_root: Path,
    partitions: tuple[PartitionManifest, ...],
    *,
    snapshot_id: str,
    snapshot_logical_sha256: str,
    expected: EventClaimIndexManifest,
    policy: StoragePolicy | None = None,
) -> tuple[Path, ...]:
    final = _claim_index_root(root, snapshot_id)
    verification_root = _mkdir_in_lake(
        root,
        root / "normalized" / "event-claim-index-v3" / ".staging",
    )
    operation = verification_root / f"verify-{uuid.uuid4().hex}"
    operation.mkdir(exist_ok=False)
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = _claim_database(operation / "claims.duckdb", snapshot_root, partitions)
        actual_shards = _claim_shard_manifests(connection)
        actual = EventClaimIndexManifest(
            format=CLAIM_FORMAT,
            claim_version=CLAIM_VERSION,
            rows=sum(item.rows for item in actual_shards),
            shards=actual_shards,
        )
        if actual != expected:
            raise ValidationError("Normalized snapshot event claims changed")
        if final.exists():
            try:
                return _validate_claim_index(
                    root,
                    snapshot_id,
                    snapshot_logical_sha256,
                    expected,
                )
            except _ClaimIndexMissingError:
                pass
        staged_index = operation / "index"
        _export_claim_index(
            connection,
            staged_index,
            snapshot_id=snapshot_id,
            snapshot_logical_sha256=snapshot_logical_sha256,
            shards=actual_shards,
        )
        connection.close()
        connection = None
        return _publish_or_validate_claim_index(
            root,
            staged_index,
            snapshot_id=snapshot_id,
            snapshot_logical_sha256=snapshot_logical_sha256,
            expected=expected,
            policy=policy,
        )
    finally:
        if connection is not None:
            connection.close()
        if operation.exists():
            shutil.rmtree(operation)


def iter_event_claims_v3(
    root: Path,
    snapshot_id: str,
    expected: EventClaimIndexManifest,
) -> Iterator[EventClaimReference]:
    snapshot_dir = root / "normalized" / "snapshots" / snapshot_id
    manifest = json.loads((snapshot_dir / "manifest.json").read_text(encoding="utf-8"))
    paths = _validate_claim_index(
        root,
        snapshot_id,
        str(manifest["logical_sha256"]),
        expected,
    )
    connection = duckdb.connect(database=":memory:")
    try:
        reader = connection.execute(
            "SELECT event_id_hash, event_id, schema_id, event_sha256, claim_sha256 "
            "FROM read_parquet("
            + _sql_path_list(paths)
            + ", union_by_name=true, hive_partitioning=false) "
            "ORDER BY event_id_hash, event_id"
        ).to_arrow_reader(batch_size=_BATCH_ROWS)
        for batch in reader:
            columns = batch.to_pydict()
            for values in zip(
                columns["event_id_hash"],
                columns["event_id"],
                columns["schema_id"],
                columns["event_sha256"],
                columns["claim_sha256"],
                strict=True,
            ):
                yield EventClaimReference(*map(str, values))
    finally:
        connection.close()


def _historical_index_paths(
    root: Path,
    candidate_snapshot_id: str,
    *,
    policy: StoragePolicy | None = None,
) -> tuple[Path, ...]:
    snapshots_root = root / "normalized" / "snapshots"
    paths: list[Path] = []
    if not snapshots_root.exists():
        return ()
    from quant_data_kit.data_lake import _load_normalized_snapshot

    for snapshot_dir in sorted(snapshots_root.glob("sha256-*")):
        if not snapshot_dir.is_dir() or snapshot_dir.name == candidate_snapshot_id:
            continue
        snapshot = _load_normalized_snapshot(
            root,
            snapshot_dir.name,
            verify_event_claim_files=True,
            recovery_policy=policy,
        )
        if snapshot.layout_version != LAYOUT_VERSION or snapshot.event_claim_index is None:
            legacy_root = _mkdir_in_lake(
                root,
                root / "normalized" / "event-claim-index-v3" / ".legacy-staging",
            )
            operation = legacy_root / f"legacy-{uuid.uuid4().hex}"
            operation.mkdir(exist_ok=False)
            connection: duckdb.DuckDBPyConnection | None = None
            try:
                connection = _claim_database(
                    operation / "claims.duckdb",
                    snapshot_dir,
                    snapshot.partitions,
                )
                shards = _claim_shard_manifests(connection)
                expected = EventClaimIndexManifest(
                    CLAIM_FORMAT,
                    CLAIM_VERSION,
                    sum(item.rows for item in shards),
                    shards,
                )
                staged = operation / "index"
                _export_claim_index(
                    connection,
                    staged,
                    snapshot_id=snapshot.snapshot_id,
                    snapshot_logical_sha256=snapshot.logical_sha256,
                    shards=shards,
                )
                connection.close()
                connection = None
                paths.extend(
                    _publish_or_validate_claim_index(
                        root,
                        staged,
                        snapshot_id=snapshot.snapshot_id,
                        snapshot_logical_sha256=snapshot.logical_sha256,
                        expected=expected,
                        policy=policy,
                    )
                )
            finally:
                if connection is not None:
                    connection.close()
                if operation.exists():
                    shutil.rmtree(operation)
            continue
        paths.extend(
            _validate_claim_index(
                root,
                snapshot.snapshot_id,
                snapshot.logical_sha256,
                snapshot.event_claim_index,
            )
        )
    return tuple(paths)


def _assert_lake_wide_claims(
    root: Path,
    candidate_paths: tuple[Path, ...],
    *,
    candidate_snapshot_id: str,
    policy: StoragePolicy | None = None,
) -> None:
    historical_paths = _historical_index_paths(
        root,
        candidate_snapshot_id,
        policy=policy,
    )
    if not historical_paths:
        return
    connection = duckdb.connect(database=":memory:")
    try:
        conflict = connection.execute(
            "SELECT c.event_id FROM read_parquet("
            + _sql_path_list(candidate_paths)
            + ", union_by_name=true, hive_partitioning=false) c "
            "JOIN read_parquet("
            + _sql_path_list(historical_paths)
            + ", union_by_name=true, hive_partitioning=false) h "
            "ON c.event_id_hash = h.event_id_hash "
            "WHERE c.event_id <> h.event_id OR c.event_sha256 <> h.event_sha256 LIMIT 1"
        ).fetchone()
    finally:
        connection.close()
    if conflict is not None:
        raise ValidationError(f"Conflicting lake event_id claim: {conflict[0]}")


def load_normalized_snapshot_v3(
    root: Path,
    snapshot_id: str,
    *,
    payload: Mapping[str, Any] | None = None,
    recovery_policy: StoragePolicy | None = None,
    _trusted_publish: bool = False,
) -> NormalizedSnapshot:
    lake_root = _resolved_lake_root(root, create=False)
    snapshot_id = _segment(snapshot_id, "snapshot_id")
    snapshot_root = _validate_lake_path(
        lake_root,
        lake_root / "normalized" / "snapshots" / snapshot_id,
        allow_missing=False,
    )
    manifest_path = _validate_lake_path(
        lake_root,
        snapshot_root / "manifest.json",
        allow_missing=False,
    )
    if not manifest_path.is_file():
        raise ValidationError(f"Normalized snapshot manifest missing: {manifest_path}")
    manifest = dict(
        payload if payload is not None else json.loads(manifest_path.read_text(encoding="utf-8"))
    )
    try:
        raw_references = tuple(
            RawObjectReference(**item) for item in manifest["upstream_raw_references"]
        )
        partitions = tuple(PartitionManifest(**item) for item in manifest["partitions"])
        index_payload = dict(manifest["event_claim_index"])
        index_payload["shards"] = tuple(
            EventClaimShardManifest(**item) for item in index_payload["shards"]
        )
        claim_index = EventClaimIndexManifest(**index_payload)
        checkpoints = tuple(
            L2CheckpointManifest(**item) for item in manifest.get("l2_checkpoints", [])
        )
    except (KeyError, TypeError) as exc:
        raise ValidationError("Normalized-v3 snapshot manifest is malformed") from exc
    if (
        manifest.get("schema_version") != SCHEMA_VERSION_V2
        or manifest.get("layout_version") != LAYOUT_VERSION
        or manifest.get("layer") != "normalized"
        or manifest.get("snapshot_id") != snapshot_id
        or claim_index.format != CLAIM_FORMAT
        or claim_index.claim_version != CLAIM_VERSION
    ):
        raise ValidationError("Normalized snapshot identity mismatch")
    created_at = _utc_text(str(manifest["created_at"]), "created_at")
    identity = _snapshot_payload_v3(
        provider=str(manifest["provider"]),
        venue=str(manifest["venue"]),
        created_at=created_at,
        upstream_raw_references=raw_references,
        partitions=partitions,
        event_claim_index=claim_index,
        l2_checkpoints=checkpoints,
        partition_logical_hash_version=str(
            manifest.get(
                "partition_logical_hash_version",
                CANONICAL_JSON_PARTITION_HASH,
            )
        ),
    )
    logical_sha256 = _sha256_bytes(_canonical_json_bytes(identity))
    if (
        manifest.get("logical_sha256") != logical_sha256
        or snapshot_id != f"sha256-{logical_sha256}"
    ):
        raise ValidationError("Normalized snapshot logical hash changed")
    provider = str(manifest["provider"])
    venue = str(manifest["venue"])
    for reference in raw_references:
        if reference.source != provider:
            raise ValidationError("Normalized provider does not match its Raw source")
        validate_raw_reference(lake_root, reference, allow_archived=True)

    expected_files: set[Path] = {Path("manifest.json")}
    seen_paths: set[str] = set()
    actual_rows = 0
    for partition in partitions:
        if partition.relative_path in seen_paths:
            raise ValidationError("Normalized snapshot contains duplicate partition paths")
        seen_paths.add(partition.relative_path)
        if partition.provider != provider or partition.venue != venue:
            raise ValidationError("Normalized partition provider/venue mismatch")
        expected_relative = Path(
            f"provider={provider}/venue={venue}/event_type={partition.event_type}/"
            f"date={partition.trading_date}/"
            f"instrument={_partition_segment(partition.instrument_id, 'instrument_id')}/"
            "data.parquet"
        ).as_posix()
        if partition.relative_path != expected_relative:
            raise ValidationError("Normalized partition path metadata mismatch")
        if _event_schema_id({"event_type": partition.event_type}) != partition.schema_id:
            raise ValidationError("Normalized partition event/schema mismatch")
        path = _safe_snapshot_partition(lake_root, snapshot_root, partition.relative_path)
        expected_files.add(Path(partition.relative_path))
        if _sha256_file(path) != partition.content_sha256:
            raise ValidationError(f"Normalized partition hash changed: {path}")
        if _trusted_publish:
            parquet = pq.ParquetFile(path)
            try:
                if parquet.schema_arrow != get_arrow_schema(partition.schema_id):
                    raise ValidationError(f"Normalized partition Arrow schema changed: {path}")
                rows = parquet.metadata.num_rows
            finally:
                parquet.close()
            logical = partition.logical_sha256
        else:
            hash_version = str(
                manifest.get(
                    "partition_logical_hash_version",
                    CANONICAL_JSON_PARTITION_HASH,
                )
            )
            if hash_version == ARROW_IPC_PARTITION_HASH:
                rows, logical = _scan_partition_arrow_ipc(path, partition.schema_id)
            elif hash_version == CANONICAL_JSON_PARTITION_HASH:
                rows, logical = _scan_partition(path, partition.schema_id)
            else:
                raise ValidationError(f"Unknown normalized partition logical hash: {hash_version}")
        if rows != partition.rows:
            raise ValidationError(f"Normalized partition row count changed: {path}")
        if logical != partition.logical_sha256:
            raise ValidationError(f"Normalized partition logical content changed: {path}")
        actual_rows += rows
    if actual_rows != int(manifest.get("rows", -1)):
        raise ValidationError("Normalized snapshot total row count changed")
    if actual_rows != claim_index.rows:
        raise ValidationError("Normalized snapshot event claims changed")
    actual_files = {
        path.relative_to(snapshot_root) for path in snapshot_root.rglob("*") if path.is_file()
    }
    if actual_files != expected_files:
        raise ValidationError("Normalized snapshot contains an unexpected or missing file")
    if _trusted_publish:
        _validate_claim_index(
            lake_root,
            snapshot_id,
            logical_sha256,
            claim_index,
        )
    else:
        _index_for_snapshot(
            lake_root,
            snapshot_root,
            partitions,
            snapshot_id=snapshot_id,
            snapshot_logical_sha256=logical_sha256,
            expected=claim_index,
            policy=recovery_policy,
        )
    claims = EventClaimSequence(lake_root, snapshot_id, claim_index)
    return NormalizedSnapshot(
        schema_version=SCHEMA_VERSION_V2,
        layer="normalized",
        snapshot_id=snapshot_id,
        provider=provider,
        venue=venue,
        created_at=created_at,
        logical_sha256=logical_sha256,
        rows=actual_rows,
        upstream_raw_references=raw_references,
        event_claims=claims,
        partitions=partitions,
        layout_version=LAYOUT_VERSION,
        partition_logical_hash_version=str(
            manifest.get(
                "partition_logical_hash_version",
                CANONICAL_JSON_PARTITION_HASH,
            )
        ),
        event_claim_index=claim_index,
        l2_checkpoints=checkpoints,
    )


def _all_duplicate_ids_from_spool(path: Path) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for record in _iter_spooled_records(path):
        event_id = record.get("event_id")
        if not isinstance(event_id, str):
            continue
        if event_id in seen:
            duplicates.add(event_id)
        else:
            seen.add(event_id)
    return duplicates


def _validate_stream_record(
    record: dict[str, Any],
    *,
    provider: str,
    stream_key: tuple[str, str, str],
    last_sequences: dict[tuple[str, str, str, str], int],
    books: dict[tuple[str, str, str], L2BookReconstructor],
    expected_l2: Mapping[tuple[str, str, str], Mapping[int, str]],
    reached_l2: dict[tuple[str, str, str], set[int]],
) -> None:
    if str(record.get("source")) != provider:
        raise ValidationError(f"stream source does not match provider {provider}")
    schema_id = _event_schema_id(record)
    _validate_event_record(schema_id, record)
    event_type = str(record["event_type"])
    domain = "book" if event_type in {"book_snapshot", "book_delta"} else event_type
    sequence_key = (*stream_key, domain)
    sequence = int(record["sequence"])
    previous = last_sequences.get(sequence_key)
    if previous is not None and sequence <= previous:
        raise ValidationError(
            f"Sequence must be strictly increasing for stream {sequence_key}: "
            f"previous={previous}, current={sequence}"
        )
    if (
        event_type == "book_delta"
        and previous is not None
        and int(record["previous_sequence"]) != previous
    ):
        raise ValidationError(
            "book_delta previous_sequence must equal prior stream sequence: "
            f"expected={previous}, actual={record['previous_sequence']}"
        )
    if event_type in {"book_snapshot", "book_delta"}:
        book = books.setdefault(stream_key, L2BookReconstructor())
        book._apply_validated_without_checkpoint(record)
        expected = expected_l2.get(stream_key, {})
        if sequence in expected:
            checkpoint = book.checkpoint()
            if checkpoint.state_sha256 != expected[sequence]:
                raise L2ReplayError(
                    f"L2 checkpoint hash mismatch at sequence {sequence}: "
                    f"expected={expected[sequence]}, actual={checkpoint.state_sha256}"
                )
            reached_l2.setdefault(stream_key, set()).add(sequence)
    last_sequences[sequence_key] = sequence


def _revalidate_streams_after_duplicate_exclusions(
    spool_path: Path,
    *,
    provider: str,
    invalid_streams: Mapping[tuple[str, str, str], str],
    duplicate_event_ids: set[str],
    expected_l2: Mapping[tuple[str, str, str], Mapping[int, str]],
) -> dict[tuple[str, str, str], str]:
    """Revalidate survivors because removing duplicate IDs can create L2 gaps."""
    revised = dict(invalid_streams)
    last_sequences: dict[tuple[str, str, str, str], int] = {}
    books: dict[tuple[str, str, str], L2BookReconstructor] = {}
    reached_l2: dict[tuple[str, str, str], set[int]] = {}
    for index, record in enumerate(_iter_spooled_records(spool_path)):
        stream_key = _stream_key(record, index)
        if stream_key in revised or record.get("event_id") in duplicate_event_ids:
            continue
        try:
            _validate_stream_record(
                record,
                provider=provider,
                stream_key=stream_key,
                last_sequences=last_sequences,
                books=books,
                expected_l2=expected_l2,
                reached_l2=reached_l2,
            )
        except (ValidationError, ValueError, TypeError, OverflowError) as exc:
            revised[stream_key] = f"stream_validation_failed: {exc}"
    for stream_key, expected in expected_l2.items():
        if stream_key in revised:
            continue
        if stream_key not in books:
            if expected:
                revised[stream_key] = (
                    "stream_validation_failed: Expected L2 stream was removed by "
                    "duplicate event_id quarantine"
                )
            continue
        missing = set(expected).difference(reached_l2.get(stream_key, set()))
        if missing:
            revised[stream_key] = (
                "stream_validation_failed: Expected L2 checkpoints were not reached: "
                f"{sorted(missing)}"
            )
    return revised


@dataclass(frozen=True)
class _ArrowBatchIdentity:
    schema_id: str
    event_type: str
    trading_date: str
    instrument_id: str
    latest_available: datetime


def _all_true(values: pa.Array | pa.ChunkedArray) -> bool:
    result = pc.all(values).as_py()
    return result is True


def _require_all(values: pa.Array | pa.ChunkedArray, message: str) -> None:
    if not _all_true(values):
        raise ValidationError(message)


def _uniform_value(values: pa.Array, field_name: str) -> Any:
    if len(values) == 0 or values.null_count:
        raise ValidationError(f"Arrow {field_name} must be non-null and non-empty")
    first = values[0]
    if not _all_true(pc.equal(values, first)):
        raise ValidationError(f"Arrow batch must contain one {field_name}")
    return first.as_py()


def _validate_fixed_point_array(values: pa.StructArray, field_name: str) -> None:
    units = values.field("units")
    scale = values.field("scale")
    if values.null_count or units.null_count or scale.null_count:
        raise ValidationError(f"Arrow {field_name} contains null fixed-point values")
    _require_all(
        pc.greater_equal(scale, pa.scalar(0, pa.int16())), f"{field_name}.scale is negative"
    )
    _require_all(
        pc.less_equal(scale, pa.scalar(18, pa.int16())),
        f"{field_name}.scale exceeds 18",
    )


def _validate_common_arrow_batch(
    batch: pa.RecordBatch,
    *,
    provider: str,
) -> _ArrowBatchIdentity:
    try:
        batch.validate(full=True)
    except pa.ArrowInvalid as exc:
        raise ValidationError(f"Arrow batch is structurally invalid: {exc}") from exc
    event_type_index = batch.schema.get_field_index("event_type")
    if event_type_index < 0 or batch.num_rows == 0:
        raise ValidationError("Arrow event batch must contain at least one event_type")
    event_type = str(_uniform_value(batch.column(event_type_index), "event_type"))
    schema_id = _event_schema_id({"event_type": event_type})
    expected = get_arrow_schema(schema_id)
    if batch.schema != expected:
        raise ValidationError(
            f"Arrow schema mismatch for {schema_id}: expected={expected}, actual={batch.schema}"
        )
    for schema_field, values in zip(expected, batch.columns, strict=True):
        if not schema_field.nullable and values.null_count:
            raise ValidationError(f"Arrow required field contains nulls: {schema_field.name}")
    for name in ("event_id", "instrument_id", "source", "session_id"):
        values = batch.column(expected.get_field_index(name))
        _require_all(
            pc.greater(pc.utf8_length(values), pa.scalar(0, pa.int32())),
            f"Arrow {name} must be non-empty",
        )
    source = str(_uniform_value(batch.column(expected.get_field_index("source")), "source"))
    if source != provider:
        raise ValidationError(f"stream source does not match provider {provider}")
    instrument_id = str(
        _uniform_value(batch.column(expected.get_field_index("instrument_id")), "instrument_id")
    )
    trading_day = _uniform_value(
        batch.column(expected.get_field_index("trading_day")),
        "trading_day",
    )
    event_time = batch.column(expected.get_field_index("event_time"))
    received_at = batch.column(expected.get_field_index("received_at"))
    available_at = batch.column(expected.get_field_index("available_at"))
    for name, values in (
        ("event_time", event_time),
        ("received_at", received_at),
        ("available_at", available_at),
    ):
        try:
            pc.cast(values, pa.timestamp("us", tz="UTC"), safe=True)
        except pa.ArrowInvalid as exc:
            raise ValidationError(
                f"Arrow {name} has nanoseconds that frozen JSON cannot preserve"
            ) from exc
    _require_all(
        pc.less_equal(event_time, received_at),
        "received_at must not be earlier than event_time",
    )
    _require_all(
        pc.less_equal(received_at, available_at),
        "available_at must not be earlier than received_at",
    )
    sequence = batch.column(expected.get_field_index("sequence"))
    _require_all(
        pc.greater_equal(sequence, pa.scalar(0, pa.int64())),
        "Arrow sequence must be non-negative",
    )
    latest_available = pc.max(available_at).as_py()
    if not isinstance(latest_available, datetime):
        raise ValidationError("Arrow available_at maximum is not a datetime")
    return _ArrowBatchIdentity(
        schema_id=schema_id,
        event_type=event_type,
        trading_date=_cached_date_text(trading_day),
        instrument_id=instrument_id,
        latest_available=latest_available,
    )


def _logical_batch_rows(batch: pa.RecordBatch, schema_id: str) -> list[dict[str, Any]]:
    return _logical_rows(get_arrow_schema(schema_id), batch.to_pylist())


def _sort_keys(rows: list[dict[str, Any]]) -> tuple[_SortKey, _SortKey]:
    keys = [
        (
            _cached_timestamp(str(row["event_time"])),
            int(row["sequence"]),
            str(row["event_id"]),
        )
        for row in rows
    ]
    if any(current < previous for previous, current in pairwise(keys)):
        raise ValidationError("Arrow event batch sort order moved backwards")
    return keys[0], keys[-1]


def _validate_l2_delta_columns(batch: pa.RecordBatch) -> None:
    schema = get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID)
    side = batch.column(schema.get_field_index("side"))
    action = batch.column(schema.get_field_index("action"))
    price = batch.column(schema.get_field_index("price"))
    quantity = batch.column(schema.get_field_index("quantity"))
    previous = batch.column(schema.get_field_index("previous_sequence"))
    sequence = batch.column(schema.get_field_index("sequence"))
    _validate_fixed_point_array(price, "price")
    _validate_fixed_point_array(quantity, "quantity")
    _require_all(pc.is_in(side, value_set=pa.array(["bid", "ask"])), "Arrow side is invalid")
    _require_all(
        pc.is_in(action, value_set=pa.array(["upsert", "delete"])),
        "Arrow action is invalid",
    )
    price_units = price.field("units")
    quantity_units = quantity.field("units")
    _require_all(
        pc.greater(price_units, pa.scalar(0, pa.int64())),
        "price must be positive",
    )
    _require_all(
        pc.greater_equal(quantity_units, pa.scalar(0, pa.int64())),
        "quantity must be non-negative",
    )
    _require_all(
        pc.greater_equal(previous, pa.scalar(0, pa.int64())),
        "previous_sequence must be non-negative",
    )
    _require_all(pc.less(previous, sequence), "previous_sequence must precede sequence")
    delete_ok = pc.and_(
        pc.equal(action, pa.scalar("delete")),
        pc.equal(quantity_units, pa.scalar(0, pa.int64())),
    )
    upsert_ok = pc.and_(
        pc.equal(action, pa.scalar("upsert")),
        pc.greater(quantity_units, pa.scalar(0, pa.int64())),
    )
    _require_all(pc.or_(delete_ok, upsert_ok), "Arrow delta action and quantity disagree")


def _encoded_l2_group_ranges(batch: pa.RecordBatch) -> tuple[tuple[int, int], ...] | None:
    """Return contiguous provider-update ranges for the adapter sequence encoding."""
    schema = get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID)
    if batch.schema != schema or batch.num_rows == 0:
        return None
    sequences = batch.column(schema.get_field_index("sequence")).to_numpy(zero_copy_only=False)
    first_provider, first_level = divmod(int(sequences[0]), BOOK_SEQUENCE_FACTOR)
    if first_provider <= 0 or first_level <= 0:
        return None
    ranges: list[tuple[int, int]] = []
    start = 0
    provider = first_provider
    level = first_level
    for index in range(1, len(sequences)):
        current_provider, current_level = divmod(int(sequences[index]), BOOK_SEQUENCE_FACTOR)
        if current_provider == provider:
            if current_level != level + 1:
                return None
        else:
            if current_provider <= 0 or current_level != 1:
                return None
            ranges.append((start, index))
            start = index
            provider = current_provider
        level = current_level
    ranges.append((start, len(sequences)))
    return tuple(ranges)


def _try_apply_atomic_l2_delta_batch(
    batch: pa.RecordBatch,
    *,
    last_sequences: dict[tuple[str, str, str, str], int],
    books: dict[tuple[str, str, str], L2BookReconstructor],
    expected_l2: Mapping[tuple[str, str, str], Mapping[int, str]],
    reached_l2: dict[tuple[str, str, str], set[int]],
) -> tuple[_SortKey, _SortKey] | None:
    """Apply complete multi-level provider updates atomically without row materialization."""
    ranges = _encoded_l2_group_ranges(batch)
    if ranges is None:
        return None
    schema = get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID)
    source = str(_uniform_value(batch.column(schema.get_field_index("source")), "source"))
    instrument_id = str(
        _uniform_value(batch.column(schema.get_field_index("instrument_id")), "instrument_id")
    )
    session_id = str(
        _uniform_value(batch.column(schema.get_field_index("session_id")), "session_id")
    )
    stream_key = (source, instrument_id, session_id)
    sequence = batch.column(schema.get_field_index("sequence"))
    previous = batch.column(schema.get_field_index("previous_sequence"))
    event_time = batch.column(schema.get_field_index("event_time"))
    if batch.num_rows > 1:
        _require_all(
            pc.equal(previous.slice(1), sequence.slice(0, batch.num_rows - 1)),
            "book_delta previous_sequence must equal prior stream sequence",
        )
        _require_all(
            pc.greater_equal(event_time.slice(1), event_time.slice(0, batch.num_rows - 1)),
            "L2 event_time moved backwards",
        )

    sequence_values = sequence.to_numpy(zero_copy_only=False)
    previous_values = previous.to_numpy(zero_copy_only=False)
    expected = expected_l2.get(stream_key, {})
    for start, end in ranges:
        first_sequence = int(sequence_values[start])
        final_sequence = int(sequence_values[end - 1])
        if first_sequence % BOOK_SEQUENCE_FACTOR != 1:
            return None
        if any(first_sequence <= checkpoint < final_sequence for checkpoint in expected):
            return None
        if event_time[start].as_py() != event_time[end - 1].as_py():
            return None

    price = batch.column(schema.get_field_index("price"))
    quantity = batch.column(schema.get_field_index("quantity"))
    sides = batch.column(schema.get_field_index("side")).to_pylist()
    actions = batch.column(schema.get_field_index("action")).to_pylist()
    price_units = price.field("units").to_numpy(zero_copy_only=False)
    price_scales = price.field("scale").to_numpy(zero_copy_only=False)
    quantity_units = quantity.field("units").to_numpy(zero_copy_only=False)
    quantity_scales = quantity.field("scale").to_numpy(zero_copy_only=False)
    book = books.setdefault(stream_key, L2BookReconstructor())
    sequence_key = (*stream_key, "book")
    for start, end in ranges:
        event_time_text = _cached_utc_text(event_time[start].as_py(), "event_time")
        book._apply_validated_atomic_delta_group(
            source=source,
            instrument_id=instrument_id,
            session_id=session_id,
            event_time=event_time_text,
            sequences=sequence_values[start:end],
            previous_sequences=previous_values[start:end],
            sides=sides[start:end],
            actions=actions[start:end],
            price_units=price_units[start:end],
            price_scales=price_scales[start:end],
            quantity_units=quantity_units[start:end],
            quantity_scales=quantity_scales[start:end],
        )
        final_sequence = int(sequence_values[end - 1])
        last_sequences[sequence_key] = final_sequence
        if final_sequence in expected:
            checkpoint = book.checkpoint()
            if checkpoint.state_sha256 != expected[final_sequence]:
                raise L2ReplayError(
                    f"L2 checkpoint hash mismatch at sequence {final_sequence}: "
                    f"expected={expected[final_sequence]}, actual={checkpoint.state_sha256}"
                )
            reached_l2.setdefault(stream_key, set()).add(final_sequence)

    first_sequence = int(sequence_values[0])
    final_sequence = int(sequence_values[-1])
    first_key = (
        _utc_datetime(event_time[0].as_py(), "event_time"),
        first_sequence,
        str(batch.column(schema.get_field_index("event_id"))[0].as_py()),
    )
    last_key = (
        _utc_datetime(event_time[-1].as_py(), "event_time"),
        final_sequence,
        str(batch.column(schema.get_field_index("event_id"))[-1].as_py()),
    )
    return first_key, last_key


def _try_apply_uniform_l2_delta_batch(
    batch: pa.RecordBatch,
    *,
    identity: _ArrowBatchIdentity,
    last_sequences: dict[tuple[str, str, str, str], int],
    books: dict[tuple[str, str, str], L2BookReconstructor],
    expected_l2: Mapping[tuple[str, str, str], Mapping[int, str]],
    reached_l2: dict[tuple[str, str, str], set[int]],
) -> tuple[_SortKey, _SortKey] | None:
    schema = get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID)
    source = str(_uniform_value(batch.column(schema.get_field_index("source")), "source"))
    instrument_id = str(
        _uniform_value(batch.column(schema.get_field_index("instrument_id")), "instrument_id")
    )
    session_id = str(
        _uniform_value(batch.column(schema.get_field_index("session_id")), "session_id")
    )
    stream_key = (source, instrument_id, session_id)
    sequence = batch.column(schema.get_field_index("sequence"))
    previous = batch.column(schema.get_field_index("previous_sequence"))
    event_time = batch.column(schema.get_field_index("event_time"))
    if batch.num_rows > 1:
        _require_all(
            pc.equal(previous.slice(1), sequence.slice(0, batch.num_rows - 1)),
            "book_delta previous_sequence must equal prior stream sequence",
        )
        _require_all(
            pc.greater_equal(event_time.slice(1), event_time.slice(0, batch.num_rows - 1)),
            "L2 event_time moved backwards",
        )
    action = batch.column(schema.get_field_index("action"))
    side = batch.column(schema.get_field_index("side"))
    price = batch.column(schema.get_field_index("price"))
    price_units = price.field("units")
    price_scale = price.field("scale")
    if not (
        _all_true(pc.equal(action, pa.scalar("upsert")))
        and _all_true(pc.equal(side, side[0]))
        and _all_true(pc.equal(price_units, price_units[0]))
        and _all_true(pc.equal(price_scale, price_scale[0]))
    ):
        return None
    first_sequence = int(sequence[0].as_py())
    final_sequence = int(sequence[-1].as_py())
    expected = expected_l2.get(stream_key, {})
    if any(first_sequence <= checkpoint < final_sequence for checkpoint in expected):
        return None
    book = books.setdefault(stream_key, L2BookReconstructor())
    final_event_time = _cached_utc_text(event_time[-1].as_py(), "event_time")
    quantity = batch.column(schema.get_field_index("quantity"))
    book._apply_validated_uniform_upsert_batch(
        source=source,
        instrument_id=instrument_id,
        session_id=session_id,
        first_previous_sequence=int(previous[0].as_py()),
        final_sequence=final_sequence,
        first_event_time=_cached_utc_text(event_time[0].as_py(), "event_time"),
        final_event_time=final_event_time,
        side=str(side[0].as_py()),
        price_units=int(price_units[0].as_py()),
        price_scale=int(price_scale[0].as_py()),
        quantity_units=int(quantity.field("units")[-1].as_py()),
        quantity_scale=int(quantity.field("scale")[-1].as_py()),
    )
    sequence_key = (*stream_key, "book")
    last_sequences[sequence_key] = final_sequence
    if final_sequence in expected:
        checkpoint = book.checkpoint()
        if checkpoint.state_sha256 != expected[final_sequence]:
            raise L2ReplayError(
                f"L2 checkpoint hash mismatch at sequence {final_sequence}: "
                f"expected={expected[final_sequence]}, actual={checkpoint.state_sha256}"
            )
        reached_l2.setdefault(stream_key, set()).add(final_sequence)
    first_key = (
        _utc_datetime(event_time[0].as_py(), "event_time"),
        first_sequence,
        str(batch.column(schema.get_field_index("event_id"))[0].as_py()),
    )
    last_key = (
        _utc_datetime(event_time[-1].as_py(), "event_time"),
        final_sequence,
        str(batch.column(schema.get_field_index("event_id"))[-1].as_py()),
    )
    return first_key, last_key


def _validate_record_batch(
    batch: pa.RecordBatch,
    *,
    provider: str,
    input_offset: int,
    last_sequences: dict[tuple[str, str, str, str], int],
    books: dict[tuple[str, str, str], L2BookReconstructor],
    expected_l2: Mapping[tuple[str, str, str], Mapping[int, str]],
    reached_l2: dict[tuple[str, str, str], set[int]],
) -> tuple[_ArrowBatchIdentity, _SortKey, _SortKey]:
    identity = _validate_common_arrow_batch(batch, provider=provider)
    if identity.schema_id == BOOK_DELTA_EVENT_SCHEMA_ID:
        _validate_l2_delta_columns(batch)
        sort_keys = _try_apply_atomic_l2_delta_batch(
            batch,
            last_sequences=last_sequences,
            books=books,
            expected_l2=expected_l2,
            reached_l2=reached_l2,
        )
        if sort_keys is None:
            sort_keys = _try_apply_uniform_l2_delta_batch(
                batch,
                identity=identity,
                last_sequences=last_sequences,
                books=books,
                expected_l2=expected_l2,
                reached_l2=reached_l2,
            )
        if sort_keys is not None:
            return identity, *sort_keys
    rows = _logical_batch_rows(batch, identity.schema_id)
    first_key, last_key = _sort_keys(rows)
    for index, record in enumerate(rows, start=input_offset):
        stream_key = _stream_key(record, index)
        _validate_stream_record(
            record,
            provider=provider,
            stream_key=stream_key,
            last_sequences=last_sequences,
            books=books,
            expected_l2=expected_l2,
            reached_l2=reached_l2,
        )
    return identity, first_key, last_key


def _iter_record_batches(
    batches: Iterable[pa.RecordBatch] | pa.RecordBatchReader,
) -> Iterator[pa.RecordBatch]:
    source = batches if isinstance(batches, pa.RecordBatchReader) else iter(batches)
    for batch in source:
        if not isinstance(batch, pa.RecordBatch):
            raise ValidationError("Normalized Arrow input must yield RecordBatch objects")
        if batch.num_rows:
            yield batch


def _l2_group_key(batch: pa.RecordBatch, index: int) -> tuple[str, str, str, int]:
    schema = get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID)
    sequence = int(batch.column(schema.get_field_index("sequence"))[index].as_py())
    return (
        str(batch.column(schema.get_field_index("source"))[index].as_py()),
        str(batch.column(schema.get_field_index("instrument_id"))[index].as_py()),
        str(batch.column(schema.get_field_index("session_id"))[index].as_py()),
        sequence // BOOK_SEQUENCE_FACTOR,
    )


def _concat_record_batches(batches: Sequence[pa.RecordBatch]) -> pa.RecordBatch:
    if not batches:
        raise ValidationError("Cannot concatenate an empty Arrow batch sequence")
    if len(batches) == 1:
        return batches[0]
    schema = batches[0].schema
    if any(batch.schema != schema for batch in batches[1:]):
        raise ValidationError("Cannot concatenate Arrow batches with different schemas")
    return pa.RecordBatch.from_arrays(
        [
            pa.concat_arrays([batch.column(index) for batch in batches])
            for index in range(len(schema))
        ],
        schema=schema,
    )


def _iter_atomic_l2_record_batches(
    batches: Iterable[pa.RecordBatch] | pa.RecordBatchReader,
) -> Iterator[pa.RecordBatch]:
    """Keep one encoded provider update intact across arbitrary input batch cuts."""
    pending: pa.RecordBatch | None = None
    for batch in _iter_record_batches(batches):
        ranges = _encoded_l2_group_ranges(batch)
        if ranges is None:
            if pending is not None:
                yield pending
                pending = None
            yield batch
            continue

        first_end = ranges[0][1]
        last_start = ranges[-1][0]
        first_piece = batch.slice(0, first_end)
        joined_pending = False
        if pending is not None:
            pending_sequence = int(
                pending.column(pending.schema.get_field_index("sequence"))[-1].as_py()
            )
            first_sequence = int(
                first_piece.column(first_piece.schema.get_field_index("sequence"))[0].as_py()
            )
            if (
                _l2_group_key(pending, pending.num_rows - 1) == _l2_group_key(first_piece, 0)
                and first_sequence == pending_sequence + 1
            ):
                first_piece = _concat_record_batches((pending, first_piece))
                joined_pending = True
            else:
                yield pending
            pending = None

        if len(ranges) == 1:
            pending = first_piece
            continue
        if joined_pending:
            complete = [first_piece]
            if first_end < last_start:
                complete.append(batch.slice(first_end, last_start - first_end))
            yield _concat_record_batches(complete)
        else:
            yield batch.slice(0, last_start)
        pending = batch.slice(last_start)
    if pending is not None:
        yield pending


def _finalize_strict_batch_snapshot(
    lake_root: Path,
    stage: Path,
    snapshot_stage: Path,
    partitions: _PartitionSet,
    *,
    provider: str,
    venue: str,
    raw_references: tuple[RawObjectReference, ...],
    accepted_rows: int,
    created_at: str,
    books: Mapping[tuple[str, str, str], L2BookReconstructor],
    policy: StoragePolicy,
) -> NormalizationResult:
    partition_manifests = _partition_manifests(
        partitions,
        provider=provider,
        venue=venue,
        logical_hash_version=ARROW_IPC_PARTITION_HASH,
    )
    staged_index = stage / "claim-index"
    claim_shards = _build_claim_index_files(
        snapshot_stage,
        partition_manifests,
        staged_index,
        stage / "duckdb-temporary",
    )
    claim_index = EventClaimIndexManifest(
        format=CLAIM_FORMAT,
        claim_version=CLAIM_VERSION,
        rows=sum(item.rows for item in claim_shards),
        shards=claim_shards,
    )
    if claim_index.rows != accepted_rows:
        raise ValidationError("Normalized event-claim row count changed")
    checkpoints = tuple(
        L2CheckpointManifest(
            source=stream_key[0],
            instrument_id=stream_key[1],
            session_id=stream_key[2],
            sequence=int(book.sequence),
            state_sha256=book.checkpoint().state_sha256,
        )
        for stream_key, book in sorted(books.items())
        if book.sequence is not None
    )
    identity = _snapshot_payload_v3(
        provider=provider,
        venue=venue,
        created_at=created_at,
        upstream_raw_references=raw_references,
        partitions=partition_manifests,
        event_claim_index=claim_index,
        l2_checkpoints=checkpoints,
        partition_logical_hash_version=ARROW_IPC_PARTITION_HASH,
    )
    logical_sha256 = _sha256_bytes(_canonical_json_bytes(identity))
    snapshot_id = f"sha256-{logical_sha256}"
    snapshot_manifest = {
        **identity,
        "snapshot_id": snapshot_id,
        "logical_sha256": logical_sha256,
        "rows": accepted_rows,
    }
    (snapshot_stage / "manifest.json").write_text(
        json.dumps(snapshot_manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_claim_index_manifest(
        staged_index,
        snapshot_id=snapshot_id,
        snapshot_logical_sha256=logical_sha256,
        shards=claim_shards,
    )
    candidate_paths = _claim_index_files(staged_index)
    snapshot_dir = lake_root / "normalized" / "snapshots" / snapshot_id
    with _lake_lock(lake_root, "normalized-commit", {"scope": "lake-wide"}):
        if snapshot_dir.exists():
            from quant_data_kit.data_lake import load_normalized_snapshot

            existing = load_normalized_snapshot(lake_root, snapshot_id)
            if existing.logical_sha256 != logical_sha256:
                raise ValidationError(f"Normalized snapshot collision: {snapshot_dir}")
            return NormalizationResult(
                snapshot=existing,
                accepted_rows=existing.rows,
                quarantined_rows=0,
                quarantine_manifest=None,
            )
        _assert_lake_wide_claims(
            lake_root,
            candidate_paths,
            candidate_snapshot_id=snapshot_id,
            policy=policy,
        )
        require_collection_capacity(lake_root, projected_write_bytes=0, policy=policy)
        _mkdir_in_lake(lake_root, snapshot_dir.parent)
        _validate_lake_path(lake_root, snapshot_dir, allow_missing=True)
        index_root = _claim_index_root(lake_root, snapshot_id)
        index_preexisted = index_root.exists()
        _publish_or_validate_claim_index(
            lake_root,
            staged_index,
            snapshot_id=snapshot_id,
            snapshot_logical_sha256=logical_sha256,
            expected=claim_index,
            policy=policy,
        )
        try:
            _publish_tree_entry(lake_root, snapshot_stage, snapshot_dir, policy=policy)
        except OSError:
            if not index_preexisted and index_root.exists():
                _replace_tree_entry(lake_root, index_root, staged_index)
            raise
    verified = load_normalized_snapshot_v3(
        lake_root,
        snapshot_id,
        _trusted_publish=True,
    )
    return NormalizationResult(
        snapshot=verified,
        accepted_rows=verified.rows,
        quarantined_rows=0,
        quarantine_manifest=None,
    )


def write_normalized_batches_v3(
    root: Path,
    batches: Iterable[pa.RecordBatch] | pa.RecordBatchReader,
    *,
    provider: str,
    venue: str,
    upstream_raw_references: Iterable[RawObjectReference],
    expected_l2_checkpoint_hashes: Mapping[tuple[str, str, str], Mapping[int, str]] | None,
    policy: StoragePolicy,
) -> NormalizationResult:
    """Write already-standardized Arrow batches with strict, bounded-memory gates.

    Unlike the Mapping compatibility entrypoint, a malformed Arrow batch fails the
    entire operation closed. No partial snapshot or quarantine object is published.
    Batches must be homogeneous by event type, instrument, and trading day and must
    arrive in normalized partition order.
    """
    lake_root = _resolved_lake_root(root, create=True)
    provider = _segment(provider, "provider")
    venue = _segment(venue, "venue")
    raw_references = tuple(
        sorted(
            upstream_raw_references,
            key=lambda item: (
                item.source,
                item.collection_date,
                item.idempotency_key,
                item.object_id,
            ),
        )
    )
    if not raw_references:
        raise ValidationError("Normalized data requires at least one trusted Raw reference")
    if len(set(raw_references)) != len(raw_references):
        raise ValidationError("Normalized data contains duplicate Raw references")
    for reference in raw_references:
        if reference.source != provider:
            raise ValidationError("Normalized provider does not match its Raw source")
        validate_raw_reference(lake_root, reference, allow_archived=False)
    require_collection_capacity(lake_root, projected_write_bytes=1, policy=policy)

    expected_l2 = dict(expected_l2_checkpoint_hashes or {})
    with _streaming_stage(lake_root) as stage:
        snapshot_stage = stage / "snapshot"
        snapshot_stage.mkdir()
        partitions = _PartitionSet(snapshot_stage, provider, venue)
        last_sequences: dict[tuple[str, str, str, str], int] = {}
        books: dict[tuple[str, str, str], L2BookReconstructor] = {}
        reached_l2: dict[tuple[str, str, str], set[int]] = {}
        latest_available: datetime | None = None
        input_rows = 0
        next_capacity_check = _CAPACITY_CHECK_ROWS
        try:
            for batch in _iter_atomic_l2_record_batches(batches):
                projected_rows = input_rows + batch.num_rows
                if projected_rows >= next_capacity_check:
                    require_collection_capacity(
                        lake_root,
                        projected_write_bytes=max(batch.nbytes, 1),
                        policy=policy,
                    )
                    next_capacity_check = (
                        projected_rows // _CAPACITY_CHECK_ROWS + 1
                    ) * _CAPACITY_CHECK_ROWS
                identity, first_sort_key, last_sort_key = _validate_record_batch(
                    batch,
                    provider=provider,
                    input_offset=input_rows,
                    last_sequences=last_sequences,
                    books=books,
                    expected_l2=expected_l2,
                    reached_l2=reached_l2,
                )
                partitions.append_record_batch(
                    batch,
                    schema_id=identity.schema_id,
                    event_type=identity.event_type,
                    trading_date=identity.trading_date,
                    instrument_id=identity.instrument_id,
                    first_sort_key=first_sort_key,
                    last_sort_key=last_sort_key,
                )
                input_rows = projected_rows
                if latest_available is None or identity.latest_available > latest_available:
                    latest_available = identity.latest_available
        finally:
            partitions.close()
        for stream_key, expected in expected_l2.items():
            if stream_key not in books:
                if expected:
                    raise L2ReplayError(f"Expected L2 stream was not observed: {stream_key}")
                continue
            missing = set(expected).difference(reached_l2.get(stream_key, set()))
            if missing:
                raise L2ReplayError(f"Expected L2 checkpoints were not reached: {sorted(missing)}")
        if input_rows == 0:
            return NormalizationResult(
                snapshot=None,
                accepted_rows=0,
                quarantined_rows=0,
                quarantine_manifest=None,
            )
        if latest_available is None:
            raise ValidationError("Normalized accepted rows have no available_at maximum")
        return _finalize_strict_batch_snapshot(
            lake_root,
            stage,
            snapshot_stage,
            partitions,
            provider=provider,
            venue=venue,
            raw_references=raw_references,
            accepted_rows=input_rows,
            created_at=_cached_utc_text(latest_available, "available_at"),
            books=books,
            policy=policy,
        )


def write_normalized_events_v3(
    root: Path,
    records: Iterable[Mapping[str, Any]],
    *,
    provider: str,
    venue: str,
    upstream_raw_references: Iterable[RawObjectReference],
    expected_l2_checkpoint_hashes: Mapping[tuple[str, str, str], Mapping[int, str]] | None,
    policy: StoragePolicy,
) -> NormalizationResult:
    lake_root = _resolved_lake_root(root, create=True)
    provider = _segment(provider, "provider")
    venue = _segment(venue, "venue")
    raw_references = tuple(
        sorted(
            upstream_raw_references,
            key=lambda item: (
                item.source,
                item.collection_date,
                item.idempotency_key,
                item.object_id,
            ),
        )
    )
    if not raw_references:
        raise ValidationError("Normalized data requires at least one trusted Raw reference")
    if len(set(raw_references)) != len(raw_references):
        raise ValidationError("Normalized data contains duplicate Raw references")
    for reference in raw_references:
        if reference.source != provider:
            raise ValidationError("Normalized provider does not match its Raw source")
        validate_raw_reference(lake_root, reference, allow_archived=False)
    require_collection_capacity(lake_root, projected_write_bytes=1, policy=policy)

    expected_l2 = dict(expected_l2_checkpoint_hashes or {})
    with _streaming_stage(lake_root) as stage:
        snapshot_stage = stage / "snapshot"
        snapshot_stage.mkdir()
        spool_path = stage / "input.json-seq"
        partitions = _PartitionSet(snapshot_stage, provider, venue)
        invalid_streams: dict[tuple[str, str, str], str] = {}
        last_sequences: dict[tuple[str, str, str, str], int] = {}
        books: dict[tuple[str, str, str], L2BookReconstructor] = {}
        reached_l2: dict[tuple[str, str, str], set[int]] = {}
        latest_available: datetime | None = None
        input_rows = 0
        with _InputSpool(spool_path) as spool:
            for index, source_record in enumerate(records):
                record = dict(source_record)
                input_rows += 1
                stream_key = _stream_key(record, index)
                if stream_key in invalid_streams:
                    spool.append(record, requires_evidence_encoding=_has_non_finite(record))
                    continue
                try:
                    _validate_stream_record(
                        record,
                        provider=provider,
                        stream_key=stream_key,
                        last_sequences=last_sequences,
                        books=books,
                        expected_l2=expected_l2,
                        reached_l2=reached_l2,
                    )
                    partitions.append(record)
                    available = _utc_datetime(record["available_at"], "available_at")
                    if latest_available is None or available > latest_available:
                        latest_available = available
                except (ValidationError, ValueError, TypeError, OverflowError) as exc:
                    invalid_streams[stream_key] = f"stream_validation_failed: {exc}"
                    spool.append(record, requires_evidence_encoding=True)
                else:
                    spool.append(record)
                if input_rows % _CAPACITY_CHECK_ROWS == 0:
                    partitions.flush_all()
                    require_collection_capacity(
                        lake_root,
                        projected_write_bytes=1,
                        policy=policy,
                    )
        partitions.close()

        for stream_key, expected in expected_l2.items():
            if stream_key not in books:
                if expected:
                    raise L2ReplayError(f"Expected L2 stream was not observed: {stream_key}")
                continue
            if stream_key in invalid_streams:
                continue
            missing = set(expected).difference(reached_l2.get(stream_key, set()))
            if missing:
                invalid_streams[stream_key] = (
                    "stream_validation_failed: Expected L2 checkpoints were not reached: "
                    f"{sorted(missing)}"
                )

        duplicate_ids = _duplicate_event_ids(partitions.paths())
        if invalid_streams:
            duplicate_ids.update(_all_duplicate_ids_from_spool(spool_path))
        if duplicate_ids:
            invalid_streams = _revalidate_streams_after_duplicate_exclusions(
                spool_path,
                provider=provider,
                invalid_streams=invalid_streams,
                duplicate_event_ids=duplicate_ids,
                expected_l2=expected_l2,
            )
        quarantine_temp = stage / "quarantine.records.jsonl"
        quarantine_rows = 0
        accepted_rows = sum(item.rows for item in partitions.partitions.values())
        created_at = (
            _utc_text(latest_available, "available_at") if latest_available is not None else None
        )
        if invalid_streams or duplicate_ids:
            partitions, accepted_rows, quarantine_rows, created_at = _rebuild_after_exclusions(
                snapshot_stage,
                spool_path,
                provider=provider,
                venue=venue,
                invalid_streams=invalid_streams,
                duplicate_event_ids=duplicate_ids,
                quarantine_path=quarantine_temp,
            )
        quarantine_manifest = (
            _publish_quarantine_file(
                lake_root,
                quarantine_temp,
                provider=provider,
                venue=venue,
                rows=quarantine_rows,
                policy=policy,
            )
            if quarantine_rows
            else None
        )
        if accepted_rows == 0:
            return NormalizationResult(
                snapshot=None,
                accepted_rows=0,
                quarantined_rows=quarantine_rows or input_rows,
                quarantine_manifest=quarantine_manifest,
            )
        if created_at is None:
            raise ValidationError("Normalized accepted rows have no available_at maximum")

        partition_manifests = _partition_manifests(
            partitions,
            provider=provider,
            venue=venue,
        )
        claims_db = stage / "claims.duckdb"
        connection = _claim_database(claims_db, snapshot_stage, partition_manifests)
        try:
            claim_shards = _claim_shard_manifests(connection)
            claim_index = EventClaimIndexManifest(
                format=CLAIM_FORMAT,
                claim_version=CLAIM_VERSION,
                rows=sum(item.rows for item in claim_shards),
                shards=claim_shards,
            )
            if claim_index.rows != accepted_rows:
                raise ValidationError("Normalized event-claim row count changed")
            checkpoints = tuple(
                L2CheckpointManifest(
                    source=stream_key[0],
                    instrument_id=stream_key[1],
                    session_id=stream_key[2],
                    sequence=int(book.sequence),
                    state_sha256=book.checkpoint().state_sha256,
                )
                for stream_key, book in sorted(books.items())
                if stream_key not in invalid_streams and book.sequence is not None
            )
            identity = _snapshot_payload_v3(
                provider=provider,
                venue=venue,
                created_at=created_at,
                upstream_raw_references=raw_references,
                partitions=partition_manifests,
                event_claim_index=claim_index,
                l2_checkpoints=checkpoints,
            )
            logical_sha256 = _sha256_bytes(_canonical_json_bytes(identity))
            snapshot_id = f"sha256-{logical_sha256}"
            snapshot_manifest = {
                **identity,
                "snapshot_id": snapshot_id,
                "logical_sha256": logical_sha256,
                "rows": accepted_rows,
            }
            (snapshot_stage / "manifest.json").write_text(
                json.dumps(snapshot_manifest, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            staged_index = stage / "claim-index"
            _export_claim_index(
                connection,
                staged_index,
                snapshot_id=snapshot_id,
                snapshot_logical_sha256=logical_sha256,
                shards=claim_shards,
            )
        finally:
            connection.close()
        claims_db.unlink(missing_ok=True)
        candidate_paths = _claim_index_files(staged_index)
        snapshot_dir = lake_root / "normalized" / "snapshots" / snapshot_id
        with _lake_lock(lake_root, "normalized-commit", {"scope": "lake-wide"}):
            if snapshot_dir.exists():
                from quant_data_kit.data_lake import load_normalized_snapshot

                existing = load_normalized_snapshot(lake_root, snapshot_id)
                if existing.logical_sha256 != logical_sha256:
                    raise ValidationError(f"Normalized snapshot collision: {snapshot_dir}")
                return NormalizationResult(
                    snapshot=existing,
                    accepted_rows=existing.rows,
                    quarantined_rows=quarantine_rows,
                    quarantine_manifest=quarantine_manifest,
                )
            _assert_lake_wide_claims(
                lake_root,
                candidate_paths,
                candidate_snapshot_id=snapshot_id,
                policy=policy,
            )
            require_collection_capacity(lake_root, projected_write_bytes=0, policy=policy)
            _mkdir_in_lake(lake_root, snapshot_dir.parent)
            _validate_lake_path(lake_root, snapshot_dir, allow_missing=True)
            index_root = _claim_index_root(lake_root, snapshot_id)
            index_preexisted = index_root.exists()
            _publish_or_validate_claim_index(
                lake_root,
                staged_index,
                snapshot_id=snapshot_id,
                snapshot_logical_sha256=logical_sha256,
                expected=claim_index,
                policy=policy,
            )
            try:
                _publish_tree_entry(lake_root, snapshot_stage, snapshot_dir, policy=policy)
            except OSError:
                if not index_preexisted and index_root.exists():
                    _replace_tree_entry(lake_root, index_root, staged_index)
                raise
        verified = load_normalized_snapshot_v3(
            lake_root,
            snapshot_id,
            _trusted_publish=True,
        )
        return NormalizationResult(
            snapshot=verified,
            accepted_rows=verified.rows,
            quarantined_rows=quarantine_rows,
            quarantine_manifest=quarantine_manifest,
        )
