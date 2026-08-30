"""Durable Normalized epoch journal backed by immutable Raw segment references."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Executor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from quant_data_kit import normalized_v3
from quant_data_kit.capture_v2.models import canonical_json_bytes, utc_text
from quant_data_kit.capture_v2.storage import (
    CaptureStorageGuard,
    RawSegment,
    _atomic_immutable_write,
    _fsync_directory,
    _safe_mkdir,
    _validate_safe_path,
)
from quant_data_kit.data_lake import (
    RawObjectReference,
    StoragePolicy,
    _capacity_tree_lock,
    validate_raw_reference,
    write_normalized_batches,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.schemas_v2 import (
    BOOK_DELTA_EVENT_SCHEMA_ID,
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    get_arrow_schema,
)

UTC = timezone.utc
_DEFAULT_POLICY = StoragePolicy()
_ARROW_BATCH_ROWS = 32_768
_CAPTURE_EVENT_SCHEMAS = {
    "book_delta": BOOK_DELTA_EVENT_SCHEMA_ID,
    "book_snapshot": BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
}
_SHA256_HEX_LENGTH = 64
_PREPARED_FIELDS = frozenset(
    {
        "schema_version",
        "transaction_state",
        "attempt",
        "epoch_id",
        "stream_id",
        "provider",
        "venue",
        "created_at",
        "records",
        "raw_references",
        "journal_parts",
        "policy",
        "prepared_sha256",
    }
)
_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "transaction_state",
        "prepared_sha256",
        "epoch_id",
        "stream_id",
        "provider",
        "venue",
        "created_at",
        "records",
        "raw_segments",
        "raw_references",
        "journal_parts",
        "policy",
        "normalized_snapshot_id",
        "accepted_rows",
        "quarantined_rows",
        "receipt_sha256",
    }
)
_ABORT_FIELDS = frozenset(
    {
        "schema_version",
        "transaction_state",
        "prepared_sha256",
        "epoch_id",
        "stream_id",
        "provider",
        "venue",
        "created_at",
        "reason",
        "records",
        "raw_references",
        "journal_parts",
        "policy",
        "retryable",
        "abort_sha256",
    }
)
_FAILURE_FIELDS = frozenset(
    {
        "schema_version",
        "transaction_state",
        "prepared_sha256",
        "epoch_id",
        "stream_id",
        "provider",
        "venue",
        "created_at",
        "attempt",
        "exception",
        "message",
        "records",
        "raw_references",
        "journal_parts",
        "policy",
        "published_snapshot_id",
        "accepted_rows",
        "quarantined_rows",
        "retryable_in_process",
        "restart_recovery",
        "failure_sha256",
    }
)
_TERMINAL_FIELDS = {
    "puresaber.normalized-epoch-receipt@1.3.0": _RECEIPT_FIELDS,
    "puresaber.normalized-epoch-abort@1.3.0": _ABORT_FIELDS,
    "puresaber.normalized-epoch-finalize-failure@1.2.0": _FAILURE_FIELDS,
}
_RESTART_RECOVERY = "reconcile PREPARED/ABORTED transaction idempotently"


@dataclass(frozen=True)
class EpochPart:
    relative_path: str
    rows: int
    content_sha256: str
    byte_length: int


@dataclass(frozen=True)
class NormalizedEpochReceipt:
    schema_version: str
    epoch_id: str
    stream_id: str
    provider: str
    venue: str
    created_at: str
    records: int
    raw_segments: int
    raw_references: tuple[RawObjectReference, ...]
    journal_parts: tuple[EpochPart, ...]
    normalized_snapshot_id: str | None
    accepted_rows: int
    quarantined_rows: int
    transaction_state: str
    prepared_sha256: str
    receipt_sha256: str
    receipt_path: str


@dataclass(frozen=True)
class _NormalizationSummary:
    snapshot_id: str | None
    accepted_rows: int
    quarantined_rows: int


def _sha256_file(path: Path, *, trusted_root: Path | None = None) -> str:
    if trusted_root is not None:
        path = _validate_safe_path(trusted_root, path, allow_missing=False)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _journal_root(hot_root: Path, stream_id: str, epoch_id: str) -> Path:
    return (
        Path(hot_root)
        / "capture"
        / "normalized-epoch-journal"
        / f"stream={stream_id}"
        / f"epoch={epoch_id}"
    )


def _validated_glob(hot_root: Path, root: Path, pattern: str) -> tuple[Path, ...]:
    checked_root = _validate_safe_path(hot_root, root, allow_missing=False)
    paths = tuple(sorted(checked_root.glob(pattern)))
    return tuple(_validate_safe_path(hot_root, path, allow_missing=False) for path in paths)


def _load_hashed_json(
    hot_root: Path,
    path: Path,
    *,
    hash_field: str,
    description: str,
    filename_prefix: str | None = None,
    sequence_field: str | None = None,
) -> dict[str, Any]:
    checked = _validate_safe_path(hot_root, path, allow_missing=False)
    try:
        payload = json.loads(checked.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError(f"{description} is unreadable: {checked}") from exc
    if not isinstance(payload, dict):
        raise ValidationError(f"{description} must be a JSON object")
    digest = payload.pop(hash_field, None)
    if not _is_sha256_hex(digest):
        raise ValidationError(f"{description} hash changed")
    if digest != hashlib.sha256(canonical_json_bytes(payload)).hexdigest():
        raise ValidationError(f"{description} hash changed")
    if filename_prefix is None:
        expected_name = None
    elif sequence_field is None:
        expected_name = f"{filename_prefix}sha256-{digest}.json"
    else:
        sequence = payload.get(sequence_field)
        expected_name = (
            f"{filename_prefix}{sequence:04d}-sha256-{digest}.json"
            if _is_positive_int(sequence)
            else None
        )
    if expected_name is not None and checked.name != expected_name:
        raise ValidationError(f"{description} filename hash changed")
    payload[hash_field] = digest
    return payload


def _is_positive_int(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value > 0


def _is_nonnegative_int(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= 0


def _is_sha256_hex(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == _SHA256_HEX_LENGTH
        and all(character in "0123456789abcdef" for character in value)
    )


def _is_snapshot_id(value: object) -> bool:
    return isinstance(value, str) and value.startswith("sha256-") and _is_sha256_hex(value[7:])


def _require_closed_fields(
    payload: Mapping[str, Any], expected: frozenset[str], description: str
) -> None:
    actual = frozenset(payload)
    if actual != expected:
        missing = sorted(expected.difference(actual))
        unknown = sorted(actual.difference(expected))
        raise ValidationError(f"{description} fields changed: missing={missing}, unknown={unknown}")


def _load_epoch_parts(hot_root: Path, root: Path) -> tuple[EpochPart, ...]:
    parts: list[EpochPart] = []
    for path in _validated_glob(hot_root, root, "part-*-sha256-*.ndjson"):
        digest = _sha256_file(path, trusted_root=hot_root)
        if f"sha256-{digest}.ndjson" not in path.name:
            raise ValidationError(f"Normalized recovery part hash changed: {path}")
        with _open_journal_file(hot_root, path, "rb") as stream:
            rows = sum(1 for line in stream if line.strip())
        parts.append(EpochPart(path.name, rows, digest, path.stat().st_size))
    return tuple(parts)


def _parse_created_at(value: object, description: str) -> str:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        normalized = utc_text(parsed, f"{description} created_at")
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{description} created_at is malformed") from exc
    if normalized != value:
        raise ValidationError(f"{description} created_at is not canonical UTC")
    return normalized


def _validate_raw_references(
    hot_root: Path,
    value: object,
    *,
    description: str,
) -> tuple[RawObjectReference, ...]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ValidationError(f"{description} Raw lineage is malformed")
    try:
        references = tuple(RawObjectReference(**item) for item in value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{description} Raw lineage is malformed") from exc
    if any(
        not all(
            isinstance(field, str)
            for field in (
                reference.source,
                reference.collection_date,
                reference.idempotency_key,
                reference.object_id,
                reference.content_sha256,
                reference.manifest_sha256,
            )
        )
        for reference in references
    ):
        raise ValidationError(f"{description} Raw lineage is malformed")
    hashes = [item.manifest_sha256 for item in references]
    if len(hashes) != len(set(hashes)):
        raise ValidationError(f"{description} Raw lineage contains duplicates")
    for reference in references:
        validate_raw_reference(hot_root, reference, allow_archived=True)
    return references


def _validate_prepared_binding(
    hot_root: Path,
    payload: Mapping[str, Any],
    *,
    epoch_id: str,
    stream_id: str,
    provider: str,
    venue: str,
    policy: StoragePolicy,
    actual_parts: tuple[EpochPart, ...],
) -> tuple[RawObjectReference, ...]:
    _require_closed_fields(
        payload,
        _PREPARED_FIELDS,
        "Normalized epoch PREPARED transaction",
    )
    expected_parts = [asdict(item) for item in actual_parts]
    attempt = payload.get("attempt")
    if (
        payload.get("schema_version") != "puresaber.normalized-epoch-transaction@1.0.0"
        or payload.get("transaction_state") != "PREPARED"
        or payload.get("epoch_id") != epoch_id
        or payload.get("stream_id") != stream_id
        or payload.get("provider") != provider
        or payload.get("venue") != venue
        or payload.get("policy") != asdict(policy)
        or not _is_positive_int(attempt)
        or not _is_sha256_hex(payload.get("prepared_sha256"))
    ):
        raise ValidationError("Normalized epoch PREPARED transaction identity changed")
    _parse_created_at(payload.get("created_at"), "Normalized epoch PREPARED transaction")
    if payload.get("journal_parts") != expected_parts:
        raise ValidationError("Normalized recovery journal part manifest changed")
    records = sum(item.rows for item in actual_parts)
    recorded = payload.get("records")
    if isinstance(recorded, bool) or not isinstance(recorded, int) or recorded != records:
        raise ValidationError("Normalized recovery record count changed")
    references = _validate_raw_references(
        hot_root,
        payload.get("raw_references"),
        description="Normalized epoch PREPARED transaction",
    )
    if records and not references:
        raise ValidationError("Normalized epoch records require Raw segment lineage")
    return references


def _validate_terminal_binding(
    hot_root: Path,
    payload: Mapping[str, Any],
    *,
    prepared: Mapping[str, Any] | None,
    epoch_id: str,
    stream_id: str,
    provider: str,
    venue: str,
    policy: StoragePolicy,
    actual_parts: tuple[EpochPart, ...],
    schema_version: str,
    description: str,
) -> tuple[RawObjectReference, ...]:
    expected_fields = _TERMINAL_FIELDS.get(schema_version)
    if expected_fields is None:
        raise ValidationError(f"{description} schema version is unsupported")
    _require_closed_fields(payload, expected_fields, description)
    expected_identity = {
        "epoch_id": epoch_id,
        "stream_id": stream_id,
        "provider": provider,
        "venue": venue,
        "policy": asdict(policy),
    }
    if prepared is not None:
        expected_identity["created_at"] = prepared.get("created_at")
    if payload.get("schema_version") != schema_version or any(
        payload.get(key) != value for key, value in expected_identity.items()
    ):
        raise ValidationError(f"{description} identity changed")
    prepared_sha256 = payload.get("prepared_sha256")
    if prepared_sha256 is not None and not _is_sha256_hex(prepared_sha256):
        raise ValidationError(f"{description} PREPARED hash is malformed")
    if schema_version == "puresaber.normalized-epoch-receipt@1.3.0":
        if (
            payload.get("transaction_state") != "COMMITTED"
            or not _is_sha256_hex(prepared_sha256)
            or not _is_sha256_hex(payload.get("receipt_sha256"))
        ):
            raise ValidationError(f"{description} identity changed")
    elif schema_version == "puresaber.normalized-epoch-abort@1.3.0":
        if (
            payload.get("transaction_state") != "ABORTED"
            or payload.get("retryable") is not False
            or not isinstance(payload.get("reason"), str)
            or not payload.get("reason")
            or not _is_sha256_hex(payload.get("abort_sha256"))
        ):
            raise ValidationError(f"{description} identity changed")
    else:
        if (
            payload.get("transaction_state") != "ABORTED"
            or payload.get("retryable_in_process") is not True
            or not _is_positive_int(payload.get("attempt"))
            or not isinstance(payload.get("exception"), str)
            or not payload.get("exception")
            or not isinstance(payload.get("message"), str)
            or payload.get("restart_recovery") != _RESTART_RECOVERY
            or not _is_sha256_hex(payload.get("failure_sha256"))
        ):
            raise ValidationError(f"{description} is not recoverable")
        published_snapshot_id = payload.get("published_snapshot_id")
        if published_snapshot_id is not None and not _is_snapshot_id(published_snapshot_id):
            raise ValidationError(f"{description} snapshot identity is malformed")
        row_state = (payload.get("accepted_rows"), payload.get("quarantined_rows"))
        if published_snapshot_id is None:
            if row_state != (None, None):
                raise ValidationError(f"{description} snapshot state changed")
        elif not all(_is_nonnegative_int(value) for value in row_state):
            raise ValidationError(f"{description} snapshot state changed")
    _parse_created_at(payload.get("created_at"), description)
    records = sum(item.rows for item in actual_parts)
    recorded = payload.get("records")
    if isinstance(recorded, bool) or not isinstance(recorded, int) or recorded != records:
        raise ValidationError("Normalized recovery record count changed")
    if payload.get("journal_parts") != [asdict(item) for item in actual_parts]:
        raise ValidationError("Normalized recovery journal part manifest changed")
    if prepared is not None and payload.get("raw_references") != prepared.get("raw_references"):
        raise ValidationError(f"{description} Raw lineage changed")
    references = _validate_raw_references(
        hot_root,
        payload.get("raw_references"),
        description=description,
    )
    if records and not references:
        raise ValidationError("Normalized epoch records require Raw segment lineage")
    return references


class _CanonicalRowsDigest:
    def __init__(self) -> None:
        self._digest = hashlib.sha256(b"[")
        self._has_rows = False

    def update(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        encoded = canonical_json_bytes(rows)
        if encoded[:1] != b"[" or encoded[-1:] != b"]":
            raise ValidationError("Normalized epoch canonical row encoding changed")
        if self._has_rows:
            self._digest.update(b",")
        self._digest.update(encoded[1:-1])
        self._has_rows = True

    def hexdigest(self) -> str:
        digest = self._digest.copy()
        digest.update(b"]")
        return digest.hexdigest()


@dataclass
class _ExpectedPartitionBinding:
    rows: int
    digest: _CanonicalRowsDigest


def _expected_snapshot_binding(
    hot_root: Path,
    journal_root: Path,
    parts: tuple[EpochPart, ...],
    *,
    provider: str,
) -> tuple[
    dict[tuple[str, str, str, str], tuple[int, str]],
    str | None,
    tuple[tuple[str, str, str, int, str], ...],
]:
    expected: dict[tuple[str, str, str, str], _ExpectedPartitionBinding] = {}
    last_sequences: dict[tuple[str, str, str, str], int] = {}
    books: dict[tuple[str, str, str], Any] = {}
    reached_l2: dict[tuple[str, str, str], set[int]] = {}
    latest_available: datetime | None = None
    input_rows = 0
    source_batches = _iter_epoch_record_batches(
        journal_root,
        parts,
        trusted_root=hot_root,
    )
    for batch in normalized_v3._iter_atomic_l2_record_batches(source_batches):
        identity, _first_sort_key, _last_sort_key = normalized_v3._validate_record_batch(
            batch,
            provider=provider,
            input_offset=input_rows,
            last_sequences=last_sequences,
            books=books,
            expected_l2={},
            reached_l2=reached_l2,
        )
        key = (
            identity.schema_id,
            identity.event_type,
            identity.trading_date,
            identity.instrument_id,
        )
        binding = expected.get(key)
        if binding is None:
            binding = _ExpectedPartitionBinding(0, _CanonicalRowsDigest())
            expected[key] = binding
        binding.digest.update(normalized_v3._logical_batch_rows(batch, identity.schema_id))
        binding.rows += batch.num_rows
        input_rows += batch.num_rows
        if latest_available is None or identity.latest_available > latest_available:
            latest_available = identity.latest_available
    partitions = {
        key: (binding.rows, binding.digest.hexdigest()) for key, binding in expected.items()
    }
    checkpoints = tuple(
        (
            stream_key[0],
            stream_key[1],
            stream_key[2],
            int(book.sequence),
            book.checkpoint().state_sha256,
        )
        for stream_key, book in sorted(books.items())
        if book.sequence is not None
    )
    created_at = (
        utc_text(latest_available, "available_at") if latest_available is not None else None
    )
    return partitions, created_at, checkpoints


def _actual_snapshot_partitions(
    hot_root: Path,
    snapshot_id: str,
    snapshot: Any,
) -> dict[tuple[str, str, str, str], tuple[int, str]]:
    actual: dict[tuple[str, str, str, str], tuple[int, str]] = {}
    snapshot_root = Path(hot_root) / "normalized" / "snapshots" / snapshot_id
    for partition in snapshot.partitions:
        path = _validate_safe_path(
            hot_root,
            snapshot_root / partition.relative_path,
            allow_missing=False,
        )
        digest = _CanonicalRowsDigest()
        rows = 0
        parquet = pq.ParquetFile(path)
        try:
            for batch in parquet.iter_batches(batch_size=_ARROW_BATCH_ROWS):
                digest.update(normalized_v3._logical_batch_rows(batch, partition.schema_id))
                rows += batch.num_rows
        finally:
            parquet.close()
        key = (
            partition.schema_id,
            partition.event_type,
            partition.trading_date,
            partition.instrument_id,
        )
        if key in actual:
            raise ValidationError("Normalized snapshot contains duplicate logical partitions")
        actual[key] = (rows, digest.hexdigest())
    return actual


def _validate_snapshot_binding(
    hot_root: Path,
    payload: Mapping[str, Any],
    *,
    references: tuple[RawObjectReference, ...],
    description: str,
    journal_root: Path | None = None,
    parts: tuple[EpochPart, ...] | None = None,
) -> None:
    values = tuple(payload.get(key) for key in ("records", "accepted_rows", "quarantined_rows"))
    if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
        raise ValidationError(f"{description} row counts are malformed")
    records, accepted, quarantined = values
    if min(records, accepted, quarantined) < 0 or accepted + quarantined != records:
        raise ValidationError(f"{description} row counts changed")
    snapshot_id = payload.get("normalized_snapshot_id")
    if snapshot_id is None:
        if accepted:
            raise ValidationError(f"{description} snapshot identity is missing")
        return
    if not _is_snapshot_id(snapshot_id):
        raise ValidationError(f"{description} snapshot identity is malformed")
    snapshot = normalized_v3.load_normalized_snapshot_v3(hot_root, snapshot_id)
    if (
        snapshot.provider != payload.get("provider")
        or snapshot.venue != payload.get("venue")
        or snapshot.rows != accepted
        or snapshot.upstream_raw_references != references
    ):
        raise ValidationError(f"{description} snapshot identity changed")
    if journal_root is None or parts is None:
        raise ValidationError(f"{description} lacks its journal content binding")
    expected_partitions, expected_created_at, expected_checkpoints = _expected_snapshot_binding(
        hot_root,
        journal_root,
        parts,
        provider=str(payload.get("provider")),
    )
    actual_partitions = _actual_snapshot_partitions(hot_root, snapshot_id, snapshot)
    actual_checkpoints = tuple(
        (
            item.source,
            item.instrument_id,
            item.session_id,
            item.sequence,
            item.state_sha256,
        )
        for item in snapshot.l2_checkpoints
    )
    if (
        expected_partitions != actual_partitions
        or expected_created_at != snapshot.created_at
        or expected_checkpoints != actual_checkpoints
    ):
        raise ValidationError(f"{description} does not match its journal content")


def _open_journal_file(hot_root: Path, path: Path, mode: str):
    allow_missing = "x" in mode
    checked = _validate_safe_path(hot_root, path, allow_missing=allow_missing)
    _validate_safe_path(hot_root, checked.parent, allow_missing=False)
    stream = checked.open(mode)
    try:
        _validate_safe_path(hot_root, checked, allow_missing=False)
    except Exception:
        stream.close()
        raise
    return stream


class NormalizedEpochJournal:
    """Spool live v2 records with bounded memory, then publish one replay-complete epoch."""

    def __init__(
        self,
        hot_root: Path,
        *,
        epoch_id: str,
        stream_id: str,
        provider: str,
        venue: str,
        storage_guard: CaptureStorageGuard,
        policy: StoragePolicy = _DEFAULT_POLICY,
        max_part_rows: int = 50_000,
        flush_records: int = 256,
        flush_bytes: int = 1024 * 1024,
        flush_seconds: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
        normalization_executor: Executor | None = None,
    ) -> None:
        if max_part_rows < 1:
            raise ValidationError("max_part_rows must be positive")
        if flush_records < 1 or flush_bytes < 1 or flush_seconds <= 0:
            raise ValidationError("journal flush limits must be positive")
        self.hot_root = Path(hot_root)
        self.epoch_id = epoch_id
        self.stream_id = stream_id
        self.provider = provider
        self.venue = venue
        self.storage_guard = storage_guard
        self.policy = policy
        self.max_part_rows = max_part_rows
        self.flush_records = flush_records
        self.flush_bytes = flush_bytes
        self.flush_seconds = flush_seconds
        self.monotonic = monotonic
        self.normalization_executor = normalization_executor
        self.root = _journal_root(self.hot_root, stream_id, epoch_id)
        _safe_mkdir(self.hot_root, self.root, exist_ok=False)
        self._open_path = self.root / "part-open.ndjson"
        self._open_stream = _open_journal_file(self.hot_root, self._open_path, "xb")
        self._open_rows = 0
        self._unflushed_rows = 0
        self._unflushed_bytes = 0
        self._last_flush = monotonic()
        self._records = 0
        self._parts: list[EpochPart] = []
        self._raw_references: list[RawObjectReference] = []
        self._raw_manifest_hashes: set[str] = set()
        self._state = "OPEN"
        self._finalize_failures = 0
        self._finalize_attempts = 0
        self._prepared_transaction: dict[str, Any] | None = None

    def append(self, records: Iterable[Mapping[str, Any]]) -> None:
        self._require_open()
        bodies: list[bytes] = []
        body_bytes = 0
        for record in records:
            value = record if isinstance(record, dict) else dict(record)
            body = orjson.dumps(value, option=orjson.OPT_SORT_KEYS) + b"\n"
            if bodies and (
                len(bodies) >= self.flush_records or body_bytes + len(body) > self.flush_bytes
            ):
                self._append_bodies(bodies, body_bytes)
                bodies, body_bytes = [], 0
            bodies.append(body)
            body_bytes += len(body)
        if bodies:
            self._append_bodies(bodies, body_bytes)

    def _append_bodies(self, bodies: list[bytes], body_bytes: int) -> None:
        offset = 0
        while offset < len(bodies):
            available_rows = self.max_part_rows - self._open_rows
            selected = bodies[offset : offset + available_rows]
            selected_bytes = (
                body_bytes
                if offset == 0 and len(selected) == len(bodies)
                else sum(len(body) for body in selected)
            )
            self.storage_guard.require_hot_capacity(projected_write_bytes=selected_bytes)
            self._open_stream.writelines(selected)
            written = len(selected)
            self._open_rows += written
            self._records += written
            self._unflushed_rows += written
            self._unflushed_bytes += selected_bytes
            offset += written
            if (
                self._unflushed_rows >= self.flush_records
                or self._unflushed_bytes >= self.flush_bytes
                or self.monotonic() - self._last_flush >= self.flush_seconds
            ):
                self.flush()
            if self._open_rows >= self.max_part_rows:
                self._seal_part()

    def flush(self) -> None:
        self._require_open()
        if self._unflushed_rows == 0:
            return
        self._open_stream.flush()
        os.fsync(self._open_stream.fileno())
        self._unflushed_rows = 0
        self._unflushed_bytes = 0
        self._last_flush = self.monotonic()

    def record_segment(self, segment: RawSegment) -> None:
        self._require_open()
        reference = segment.raw_manifest.reference()
        if reference.manifest_sha256 in self._raw_manifest_hashes:
            raise ValidationError("Normalized epoch received a duplicate Raw segment reference")
        self._raw_manifest_hashes.add(reference.manifest_sha256)
        self._raw_references.append(reference)

    @property
    def records(self) -> int:
        return self._records

    @classmethod
    def recover(
        cls,
        hot_root: Path,
        *,
        epoch_id: str,
        stream_id: str,
        provider: str,
        venue: str,
        storage_guard: CaptureStorageGuard,
        policy: StoragePolicy = _DEFAULT_POLICY,
        max_part_rows: int = 50_000,
        flush_records: int = 256,
        flush_bytes: int = 1024 * 1024,
        flush_seconds: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
        normalization_executor: Executor | None = None,
    ) -> NormalizedEpochJournal:
        """Reload a pending PREPARED or retryable ABORTED transaction."""

        hot_root = Path(hot_root)
        root = _journal_root(hot_root, stream_id, epoch_id)
        _validate_safe_path(hot_root, root, allow_missing=True)
        if not root.exists():
            raise ValidationError("Normalized epoch is not a retryable failed finalize")
        _validate_safe_path(hot_root, root, allow_missing=False)
        receipts = _validated_glob(hot_root, root, "receipt-sha256-*.json")
        explicit_aborts = _validated_glob(hot_root, root, "aborted-sha256-*.json")
        failures = _validated_glob(hot_root, root, "finalize-failure-*.json")
        prepared_paths = _validated_glob(hot_root, root, "transaction-prepared-*.json")
        actual_parts = _load_epoch_parts(hot_root, root)
        prepared_payloads = tuple(
            _load_hashed_json(
                hot_root,
                path,
                hash_field="prepared_sha256",
                description="Normalized epoch PREPARED transaction",
                filename_prefix="transaction-prepared-",
                sequence_field="attempt",
            )
            for path in prepared_paths
        )
        for item in prepared_payloads:
            _validate_prepared_binding(
                hot_root,
                item,
                epoch_id=epoch_id,
                stream_id=stream_id,
                provider=provider,
                venue=venue,
                policy=policy,
                actual_parts=actual_parts,
            )
        prepared_by_hash = {str(item["prepared_sha256"]): item for item in prepared_payloads}
        if len(prepared_by_hash) != len(prepared_payloads):
            raise ValidationError("Normalized epoch contains duplicate PREPARED transactions")
        receipt_payloads = tuple(
            _load_hashed_json(
                hot_root,
                path,
                hash_field="receipt_sha256",
                description="Normalized epoch receipt",
                filename_prefix="receipt-",
            )
            for path in receipts
        )
        terminal_prepared: list[str] = []
        for item in receipt_payloads:
            prepared = prepared_by_hash.get(str(item.get("prepared_sha256")))
            if (
                item.get("transaction_state") != "COMMITTED"
                or item.get("epoch_id") != epoch_id
                or item.get("stream_id") != stream_id
                or prepared is None
            ):
                raise ValidationError("Normalized epoch COMMITTED receipt identity changed")
            references = _validate_terminal_binding(
                hot_root,
                item,
                prepared=prepared,
                epoch_id=epoch_id,
                stream_id=stream_id,
                provider=provider,
                venue=venue,
                policy=policy,
                actual_parts=actual_parts,
                schema_version="puresaber.normalized-epoch-receipt@1.3.0",
                description="Normalized epoch COMMITTED receipt",
            )
            raw_segments = item.get("raw_segments")
            if (
                isinstance(raw_segments, bool)
                or not isinstance(raw_segments, int)
                or raw_segments != len(references)
            ):
                raise ValidationError("Normalized epoch COMMITTED receipt Raw count changed")
            _validate_snapshot_binding(
                hot_root,
                item,
                references=references,
                description="Normalized epoch COMMITTED receipt",
                journal_root=root,
                parts=actual_parts,
            )
            terminal_prepared.append(str(item["prepared_sha256"]))
        abort_payloads = tuple(
            _load_hashed_json(
                hot_root,
                path,
                hash_field="abort_sha256",
                description="Normalized epoch explicit ABORTED transaction",
                filename_prefix="aborted-",
            )
            for path in explicit_aborts
        )
        for item in abort_payloads:
            prepared_sha256 = item.get("prepared_sha256")
            prepared = (
                prepared_by_hash.get(str(prepared_sha256)) if prepared_sha256 is not None else None
            )
            if (
                item.get("transaction_state") != "ABORTED"
                or item.get("epoch_id") != epoch_id
                or item.get("stream_id") != stream_id
                or (prepared_sha256 is not None and prepared is None)
                or item.get("retryable") is not False
            ):
                raise ValidationError("Normalized epoch explicit ABORTED identity changed")
            _validate_terminal_binding(
                hot_root,
                item,
                prepared=prepared,
                epoch_id=epoch_id,
                stream_id=stream_id,
                provider=provider,
                venue=venue,
                policy=policy,
                actual_parts=actual_parts,
                schema_version="puresaber.normalized-epoch-abort@1.3.0",
                description="Normalized epoch explicit ABORTED transaction",
            )
            terminal_prepared.append(
                str(prepared_sha256) if prepared_sha256 is not None else "ABORTED_WITHOUT_PREPARED"
            )
        if (
            len(receipt_payloads) > 1
            or len(abort_payloads) > 1
            or (receipt_payloads and abort_payloads)
        ):
            raise ValidationError("Normalized epoch contains conflicting terminal transactions")
        failure_payloads = tuple(
            _load_hashed_json(
                hot_root,
                path,
                hash_field="failure_sha256",
                description="Normalized epoch failure record",
                filename_prefix="finalize-failure-",
                sequence_field="attempt",
            )
            for path in failures
        )
        for item in failure_payloads:
            prepared_sha256 = item.get("prepared_sha256")
            prepared = (
                prepared_by_hash.get(str(prepared_sha256)) if prepared_sha256 is not None else None
            )
            if prepared_sha256 is not None and prepared is None:
                raise ValidationError("Normalized epoch failure transaction identity changed")
            if (
                item.get("transaction_state") != "ABORTED"
                or item.get("epoch_id") != epoch_id
                or item.get("stream_id") != stream_id
                or item.get("retryable_in_process") is not True
                or isinstance(item.get("attempt"), bool)
                or not isinstance(item.get("attempt"), int)
                or item.get("attempt", 0) < 1
            ):
                raise ValidationError("Normalized epoch failure record is not recoverable")
            references = _validate_terminal_binding(
                hot_root,
                item,
                prepared=prepared,
                epoch_id=epoch_id,
                stream_id=stream_id,
                provider=provider,
                venue=venue,
                policy=policy,
                actual_parts=actual_parts,
                schema_version="puresaber.normalized-epoch-finalize-failure@1.2.0",
                description="Normalized epoch failure record",
            )
            if item.get("published_snapshot_id") is not None:
                _validate_snapshot_binding(
                    hot_root,
                    {
                        **item,
                        "normalized_snapshot_id": item.get("published_snapshot_id"),
                    },
                    references=references,
                    description="Normalized epoch failure record",
                    journal_root=root,
                    parts=actual_parts,
                )
            elif item.get("accepted_rows") is not None or item.get("quarantined_rows") is not None:
                raise ValidationError("Normalized epoch failure snapshot state changed")
            terminal_prepared.append(
                str(prepared_sha256) if prepared_sha256 is not None else "FAILURE_WITHOUT_PREPARED"
            )
        if len(terminal_prepared) != len(set(terminal_prepared)):
            raise ValidationError("Normalized epoch contains conflicting terminal transactions")
        if receipts or explicit_aborts:
            raise ValidationError("Normalized epoch is not a retryable failed finalize")
        failed_prepared = {
            str(item["prepared_sha256"]) for item in failure_payloads if item.get("prepared_sha256")
        }
        pending = tuple(
            item
            for item in prepared_payloads
            if str(item["prepared_sha256"]) not in failed_prepared
        )
        if len(pending) > 1:
            raise ValidationError("Normalized epoch contains multiple pending transactions")
        failure = None if pending else failure_payloads[-1] if failure_payloads else None
        if not pending and (failure is None or not failure.get("retryable_in_process")):
            raise ValidationError("Normalized epoch is not a retryable failed finalize")
        transaction = (
            pending[0] if pending else prepared_payloads[-1] if prepared_payloads else None
        )
        if transaction is None:
            raise ValidationError("Normalized epoch failure has no durable PREPARED transaction")
        self = cls.__new__(cls)
        self.hot_root = hot_root
        self.epoch_id = epoch_id
        self.stream_id = stream_id
        self.provider = provider
        self.venue = venue
        self.storage_guard = storage_guard
        self.policy = policy
        self.max_part_rows = max_part_rows
        self.flush_records = flush_records
        self.flush_bytes = flush_bytes
        self.flush_seconds = flush_seconds
        self.monotonic = monotonic
        self.normalization_executor = normalization_executor
        self.root = _validate_safe_path(hot_root, root, allow_missing=False)
        self._parts = list(actual_parts)
        self._records = sum(item.rows for item in actual_parts)
        self._raw_references = list(
            _validate_raw_references(
                hot_root,
                transaction.get("raw_references"),
                description="Normalized epoch recovery transaction",
            )
        )
        self._raw_manifest_hashes = {item.manifest_sha256 for item in self._raw_references}
        if int(transaction.get("records", -1)) != self._records:
            raise ValidationError("Normalized recovery record count changed")
        expected_parts = transaction.get("journal_parts")
        if expected_parts != [asdict(item) for item in self._parts]:
            raise ValidationError("Normalized recovery journal part manifest changed")
        self._open_path = root / "part-open.ndjson"
        _validate_safe_path(hot_root, self._open_path, allow_missing=True)
        if self._open_path.exists() and (
            _validate_safe_path(hot_root, self._open_path, allow_missing=False).stat().st_size
        ):
            raise ValidationError("Normalized recovery found an unsealed open part")
        mode = "ab" if self._open_path.exists() else "xb"
        self._open_stream = _open_journal_file(hot_root, self._open_path, mode)
        self._open_rows = 0
        self._unflushed_rows = 0
        self._unflushed_bytes = 0
        self._last_flush = monotonic()
        self._state = "OPEN"
        self._finalize_failures = len(failures)
        self._finalize_attempts = max(
            (int(item.get("attempt", 0)) for item in prepared_payloads), default=0
        )
        self._prepared_transaction = pending[0] if pending else None
        return self

    @classmethod
    def reconcile_pending(
        cls,
        hot_root: Path,
        *,
        expected_streams: Mapping[str, tuple[str, str]],
        storage_guard: CaptureStorageGuard,
        policy: StoragePolicy = _DEFAULT_POLICY,
        normalization_executor: Executor | None = None,
    ) -> tuple[NormalizedEpochReceipt, ...]:
        """Finish every persisted PREPARED/retryable ABORTED epoch before network startup."""

        hot_root = Path(hot_root)
        base = hot_root / "capture" / "normalized-epoch-journal"
        _validate_safe_path(hot_root, base, allow_missing=True)
        if not base.exists():
            return ()
        reconciled: list[NormalizedEpochReceipt] = []
        for stream_root in _validated_glob(hot_root, base, "stream=*"):
            if not stream_root.is_dir():
                raise ValidationError("Normalized epoch stream journal is not a directory")
            stream_id = stream_root.name.removeprefix("stream=")
            identity = expected_streams.get(stream_id)
            if identity is None or len(identity) != 2:
                raise ValidationError(
                    "Normalized epoch stream lacks an independent identity anchor"
                )
            provider, venue = identity
            for epoch_root in _validated_glob(hot_root, stream_root, "epoch=*"):
                if not epoch_root.is_dir():
                    raise ValidationError("Normalized epoch journal is not a directory")
                epoch_id = epoch_root.name.removeprefix("epoch=")
                actual_parts = _load_epoch_parts(hot_root, epoch_root)
                open_part = epoch_root / "part-open.ndjson"
                _validate_safe_path(hot_root, open_part, allow_missing=True)
                if (
                    open_part.exists()
                    and _validate_safe_path(hot_root, open_part, allow_missing=False).stat().st_size
                ):
                    raise ValidationError("Normalized recovery found an unsealed open part")
                prepared_paths = _validated_glob(
                    hot_root, epoch_root, "transaction-prepared-*.json"
                )
                prepared_payloads = tuple(
                    _load_hashed_json(
                        hot_root,
                        path,
                        hash_field="prepared_sha256",
                        description="Normalized epoch PREPARED transaction",
                        filename_prefix="transaction-prepared-",
                        sequence_field="attempt",
                    )
                    for path in prepared_paths
                )
                for item in prepared_payloads:
                    _validate_prepared_binding(
                        hot_root,
                        item,
                        epoch_id=epoch_id,
                        stream_id=stream_id,
                        provider=provider,
                        venue=venue,
                        policy=policy,
                        actual_parts=actual_parts,
                    )
                prepared_by_hash = {
                    str(item["prepared_sha256"]): item for item in prepared_payloads
                }
                if len(prepared_by_hash) != len(prepared_payloads):
                    raise ValidationError(
                        "Normalized epoch contains duplicate PREPARED transactions"
                    )
                terminal_prepared: list[str] = []
                receipt_paths = _validated_glob(hot_root, epoch_root, "receipt-sha256-*.json")
                receipt_payloads: list[dict[str, Any]] = []
                for path in receipt_paths:
                    item = _load_hashed_json(
                        hot_root,
                        path,
                        hash_field="receipt_sha256",
                        description="Normalized epoch receipt",
                        filename_prefix="receipt-",
                    )
                    prepared = prepared_by_hash.get(str(item.get("prepared_sha256")))
                    if (
                        item.get("transaction_state") != "COMMITTED"
                        or prepared is None
                        or item.get("stream_id") != stream_id
                        or item.get("epoch_id") != epoch_id
                    ):
                        raise ValidationError("Normalized epoch COMMITTED receipt identity changed")
                    references = _validate_terminal_binding(
                        hot_root,
                        item,
                        prepared=prepared,
                        epoch_id=epoch_id,
                        stream_id=stream_id,
                        provider=provider,
                        venue=venue,
                        policy=policy,
                        actual_parts=actual_parts,
                        schema_version="puresaber.normalized-epoch-receipt@1.3.0",
                        description="Normalized epoch COMMITTED receipt",
                    )
                    raw_segments = item.get("raw_segments")
                    if (
                        isinstance(raw_segments, bool)
                        or not isinstance(raw_segments, int)
                        or raw_segments != len(references)
                    ):
                        raise ValidationError(
                            "Normalized epoch COMMITTED receipt Raw count changed"
                        )
                    _validate_snapshot_binding(
                        hot_root,
                        item,
                        references=references,
                        description="Normalized epoch COMMITTED receipt",
                        journal_root=epoch_root,
                        parts=actual_parts,
                    )
                    receipt_payloads.append(item)
                    terminal_prepared.append(str(item["prepared_sha256"]))
                explicit_aborts = _validated_glob(hot_root, epoch_root, "aborted-sha256-*.json")
                abort_payloads: list[dict[str, Any]] = []
                for path in explicit_aborts:
                    item = _load_hashed_json(
                        hot_root,
                        path,
                        hash_field="abort_sha256",
                        description="Normalized epoch explicit ABORTED transaction",
                        filename_prefix="aborted-",
                    )
                    prepared_sha256 = item.get("prepared_sha256")
                    prepared = (
                        prepared_by_hash.get(str(prepared_sha256))
                        if prepared_sha256 is not None
                        else None
                    )
                    if (
                        item.get("transaction_state") != "ABORTED"
                        or item.get("stream_id") != stream_id
                        or item.get("epoch_id") != epoch_id
                        or (prepared_sha256 is not None and prepared is None)
                        or item.get("retryable") is not False
                    ):
                        raise ValidationError("Normalized epoch explicit ABORTED identity changed")
                    _validate_terminal_binding(
                        hot_root,
                        item,
                        prepared=prepared,
                        epoch_id=epoch_id,
                        stream_id=stream_id,
                        provider=provider,
                        venue=venue,
                        policy=policy,
                        actual_parts=actual_parts,
                        schema_version="puresaber.normalized-epoch-abort@1.3.0",
                        description="Normalized epoch explicit ABORTED transaction",
                    )
                    abort_payloads.append(item)
                    terminal_prepared.append(
                        str(prepared_sha256)
                        if prepared_sha256 is not None
                        else "ABORTED_WITHOUT_PREPARED"
                    )
                failure_paths = _validated_glob(hot_root, epoch_root, "finalize-failure-*.json")
                failure_payloads: list[dict[str, Any]] = []
                for path in failure_paths:
                    item = _load_hashed_json(
                        hot_root,
                        path,
                        hash_field="failure_sha256",
                        description="Normalized epoch failure record",
                        filename_prefix="finalize-failure-",
                        sequence_field="attempt",
                    )
                    prepared_sha256 = item.get("prepared_sha256")
                    prepared = (
                        prepared_by_hash.get(str(prepared_sha256))
                        if prepared_sha256 is not None
                        else None
                    )
                    if prepared_sha256 is not None and prepared is None:
                        raise ValidationError(
                            "Normalized epoch failure transaction identity changed"
                        )
                    if (
                        item.get("transaction_state") != "ABORTED"
                        or item.get("stream_id") != stream_id
                        or item.get("epoch_id") != epoch_id
                        or item.get("retryable_in_process") is not True
                        or isinstance(item.get("attempt"), bool)
                        or not isinstance(item.get("attempt"), int)
                        or item.get("attempt", 0) < 1
                    ):
                        raise ValidationError("Normalized epoch failure record is not recoverable")
                    references = _validate_terminal_binding(
                        hot_root,
                        item,
                        prepared=prepared,
                        epoch_id=epoch_id,
                        stream_id=stream_id,
                        provider=provider,
                        venue=venue,
                        policy=policy,
                        actual_parts=actual_parts,
                        schema_version="puresaber.normalized-epoch-finalize-failure@1.2.0",
                        description="Normalized epoch failure record",
                    )
                    if item.get("published_snapshot_id") is not None:
                        _validate_snapshot_binding(
                            hot_root,
                            {
                                **item,
                                "normalized_snapshot_id": item.get("published_snapshot_id"),
                            },
                            references=references,
                            description="Normalized epoch failure record",
                            journal_root=epoch_root,
                            parts=actual_parts,
                        )
                    elif (
                        item.get("accepted_rows") is not None
                        or item.get("quarantined_rows") is not None
                    ):
                        raise ValidationError("Normalized epoch failure snapshot state changed")
                    failure_payloads.append(item)
                    terminal_prepared.append(
                        str(prepared_sha256)
                        if prepared_sha256 is not None
                        else "FAILURE_WITHOUT_PREPARED"
                    )
                if (
                    len(receipt_payloads) > 1
                    or len(abort_payloads) > 1
                    or (receipt_payloads and abort_payloads)
                ):
                    raise ValidationError(
                        "Normalized epoch contains conflicting terminal transactions"
                    )
                if len(terminal_prepared) != len(set(terminal_prepared)):
                    raise ValidationError(
                        "Normalized epoch contains conflicting terminal transactions"
                    )
                terminal_hashes = {
                    str(item["prepared_sha256"])
                    for item in (*receipt_payloads, *abort_payloads, *failure_payloads)
                    if item.get("prepared_sha256") is not None
                }
                pending = tuple(
                    item
                    for item in prepared_payloads
                    if str(item["prepared_sha256"]) not in terminal_hashes
                )
                if len(pending) > 1:
                    raise ValidationError("Normalized epoch contains multiple pending transactions")
                if receipt_payloads or abort_payloads:
                    if pending:
                        raise ValidationError(
                            "Normalized epoch contains a terminal state followed by pending work"
                        )
                    continue
                if any(item.get("prepared_sha256") is None for item in failure_payloads):
                    raise ValidationError(
                        "Normalized epoch failure has no durable PREPARED transaction"
                    )
                if not pending and not failure_payloads:
                    raise ValidationError(
                        "Normalized epoch journal has no durable transaction state"
                    )
                latest = pending[0] if pending else prepared_payloads[-1]
                try:
                    created_at = datetime.fromisoformat(
                        _parse_created_at(
                            latest.get("created_at"), "Normalized epoch recovery transaction"
                        ).replace("Z", "+00:00")
                    )
                    journal = cls.recover(
                        hot_root,
                        epoch_id=epoch_id,
                        stream_id=stream_id,
                        provider=provider,
                        venue=venue,
                        storage_guard=storage_guard,
                        policy=policy,
                        normalization_executor=normalization_executor,
                    )
                    reconciled.append(journal.finalize(created_at=created_at))
                except (KeyError, TypeError, ValueError) as exc:
                    raise ValidationError(
                        f"Normalized epoch PREPARED transaction is malformed: {epoch_root}"
                    ) from exc
        return tuple(reconciled)

    def finalize(self, *, created_at: datetime | None = None) -> NormalizedEpochReceipt:
        self._require_open()
        prepared: dict[str, Any] | None = None
        result: _NormalizationSummary | None = None
        created = utc_text(created_at or datetime.now(tz=UTC), "epoch created_at")
        try:
            self._seal_part()
            if self._records and not self._raw_references:
                raise ValidationError("Normalized epoch records require Raw segment lineage")
            prepared = self._prepare_transaction(created)
            result = (
                self._publish_normalized() if self._records else _NormalizationSummary(None, 0, 0)
            )
            receipt = self._write_committed_receipt(prepared, result)
        except Exception as exc:
            try:
                self._record_finalize_failure(
                    exc,
                    prepared=prepared,
                    result=result,
                    created_at=created,
                )
            except Exception as abort_exc:  # noqa: BLE001 - both failures must be preserved
                raise ValidationError(
                    "Normalized finalize failed and its durable failure record also failed; "
                    f"primary={type(exc).__name__}: {exc}; "
                    f"audit={type(abort_exc).__name__}: {abort_exc}"
                ) from exc
            self._prepared_transaction = None
            self._state = "OPEN"
            raise
        self._open_stream.close()
        self._state = "FINALIZED"
        self._prepared_transaction = None
        return receipt

    def _prepare_transaction(self, created_at: str) -> dict[str, Any]:
        if self._prepared_transaction is not None:
            expected = {
                "epoch_id": self.epoch_id,
                "stream_id": self.stream_id,
                "provider": self.provider,
                "venue": self.venue,
                "records": self._records,
                "raw_references": [asdict(item) for item in self._raw_references],
                "journal_parts": [asdict(item) for item in self._parts],
                "policy": asdict(self.policy),
            }
            if any(self._prepared_transaction.get(key) != value for key, value in expected.items()):
                raise ValidationError("Normalized epoch changed after PREPARED")
            self._state = "PREPARED"
            return self._prepared_transaction
        attempt = self._finalize_attempts + 1
        identity = {
            "schema_version": "puresaber.normalized-epoch-transaction@1.0.0",
            "transaction_state": "PREPARED",
            "attempt": attempt,
            "epoch_id": self.epoch_id,
            "stream_id": self.stream_id,
            "provider": self.provider,
            "venue": self.venue,
            "created_at": created_at,
            "records": self._records,
            "raw_references": [asdict(item) for item in self._raw_references],
            "journal_parts": [asdict(item) for item in self._parts],
            "policy": asdict(self.policy),
        }
        digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        payload = {**identity, "prepared_sha256": digest}
        path = self.root / f"transaction-prepared-{attempt:04d}-sha256-{digest}.json"
        _atomic_immutable_write(path, canonical_json_bytes(payload), root=self.hot_root)
        reloaded = _load_hashed_json(
            self.hot_root,
            path,
            hash_field="prepared_sha256",
            description="Normalized epoch PREPARED transaction",
            filename_prefix="transaction-prepared-",
            sequence_field="attempt",
        )
        if reloaded != payload:
            raise ValidationError("Normalized epoch PREPARED transaction reload mismatch")
        self._finalize_attempts = attempt
        self._prepared_transaction = payload
        self._state = "PREPARED"
        return payload

    def _write_committed_receipt(
        self,
        prepared: Mapping[str, Any],
        result: _NormalizationSummary,
    ) -> NormalizedEpochReceipt:
        identity = {
            "schema_version": "puresaber.normalized-epoch-receipt@1.3.0",
            "transaction_state": "COMMITTED",
            "prepared_sha256": str(prepared["prepared_sha256"]),
            "epoch_id": self.epoch_id,
            "stream_id": self.stream_id,
            "provider": self.provider,
            "venue": self.venue,
            "created_at": str(prepared["created_at"]),
            "records": self._records,
            "raw_segments": len(self._raw_references),
            "raw_references": [asdict(item) for item in self._raw_references],
            "journal_parts": [asdict(item) for item in self._parts],
            "policy": asdict(self.policy),
            "normalized_snapshot_id": result.snapshot_id,
            "accepted_rows": result.accepted_rows,
            "quarantined_rows": result.quarantined_rows,
        }
        receipt_hash = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        payload = {**identity, "receipt_sha256": receipt_hash}
        receipt_path = self.root / f"receipt-sha256-{receipt_hash}.json"
        _atomic_immutable_write(receipt_path, canonical_json_bytes(payload), root=self.hot_root)
        reloaded = _load_hashed_json(
            self.hot_root,
            receipt_path,
            hash_field="receipt_sha256",
            description="Normalized epoch receipt",
            filename_prefix="receipt-",
        )
        if reloaded != payload:
            raise ValidationError("Normalized epoch receipt reload mismatch")
        return NormalizedEpochReceipt(
            schema_version=identity["schema_version"],
            epoch_id=self.epoch_id,
            stream_id=self.stream_id,
            provider=self.provider,
            venue=self.venue,
            created_at=identity["created_at"],
            records=self._records,
            raw_segments=len(self._raw_references),
            raw_references=tuple(self._raw_references),
            journal_parts=tuple(self._parts),
            normalized_snapshot_id=result.snapshot_id,
            accepted_rows=result.accepted_rows,
            quarantined_rows=result.quarantined_rows,
            transaction_state="COMMITTED",
            prepared_sha256=identity["prepared_sha256"],
            receipt_sha256=receipt_hash,
            receipt_path=str(receipt_path),
        )

    def abort_visible(self, reason: str) -> Path:
        if self._state == "FINALIZED":
            raise ValidationError("cannot abort a finalized Normalized epoch")
        if self._state == "ABORTED":
            raise ValidationError("Normalized epoch is already aborted")
        if self._state in {"OPEN", "PREPARED"}:
            if self._state == "OPEN":
                self._seal_part()
            self._open_stream.close()
        prepared = self._prepared_transaction
        payload = {
            "schema_version": "puresaber.normalized-epoch-abort@1.3.0",
            "transaction_state": "ABORTED",
            "prepared_sha256": (str(prepared["prepared_sha256"]) if prepared is not None else None),
            "epoch_id": self.epoch_id,
            "stream_id": self.stream_id,
            "provider": self.provider,
            "venue": self.venue,
            "created_at": (
                str(prepared["created_at"])
                if prepared is not None
                else utc_text(datetime.now(tz=UTC), "epoch abort created_at")
            ),
            "reason": reason,
            "records": self._records,
            "raw_references": [asdict(item) for item in self._raw_references],
            "journal_parts": [asdict(item) for item in self._parts],
            "policy": asdict(self.policy),
            "retryable": False,
        }
        payload["abort_sha256"] = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        path = self.root / f"aborted-sha256-{payload['abort_sha256']}.json"
        _atomic_immutable_write(path, canonical_json_bytes(payload), root=self.hot_root)
        self._state = "ABORTED"
        self._prepared_transaction = None
        return path

    def _seal_part(self) -> None:
        if self._open_rows == 0:
            return
        self.flush()
        self._open_stream.close()
        _validate_safe_path(self.hot_root, self.root, allow_missing=False)
        _validate_safe_path(self.hot_root, self._open_path, allow_missing=False)
        digest = _sha256_file(self._open_path, trusted_root=self.hot_root)
        index = len(self._parts) + 1
        final_name = f"part-{index:08d}-sha256-{digest}.ndjson"
        final_path = self.root / final_name
        checked_final = _validate_safe_path(self.hot_root, final_path, allow_missing=True)
        with _capacity_tree_lock(self.hot_root):
            try:
                os.link(self._open_path, checked_final)
            except FileExistsError as exc:
                raise ValidationError(
                    f"Normalized journal part already exists: {checked_final}"
                ) from exc
            self._open_path.unlink()
        _validate_safe_path(self.hot_root, checked_final, allow_missing=False)
        if _sha256_file(checked_final, trusted_root=self.hot_root) != digest:
            raise ValidationError(f"Normalized sealed journal part hash changed: {checked_final}")
        _fsync_directory(self.root)
        self._parts.append(
            EpochPart(
                relative_path=final_name,
                rows=self._open_rows,
                content_sha256=digest,
                byte_length=final_path.stat().st_size,
            )
        )
        self._open_rows = 0
        self._unflushed_rows = 0
        self._unflushed_bytes = 0
        self._open_path = self.root / "part-open.ndjson"
        self._open_stream = _open_journal_file(self.hot_root, self._open_path, "xb")

    def _record_finalize_failure(
        self,
        exc: Exception,
        *,
        prepared: Mapping[str, Any] | None,
        result: _NormalizationSummary | None,
        created_at: str,
    ) -> Path:
        self._finalize_failures += 1
        identity = {
            "schema_version": "puresaber.normalized-epoch-finalize-failure@1.2.0",
            "transaction_state": "ABORTED",
            "prepared_sha256": (str(prepared["prepared_sha256"]) if prepared is not None else None),
            "epoch_id": self.epoch_id,
            "stream_id": self.stream_id,
            "provider": self.provider,
            "venue": self.venue,
            "created_at": (str(prepared["created_at"]) if prepared is not None else created_at),
            "attempt": self._finalize_failures,
            "exception": type(exc).__name__,
            "message": str(exc),
            "records": self._records,
            "raw_references": [asdict(item) for item in self._raw_references],
            "journal_parts": [asdict(item) for item in self._parts],
            "policy": asdict(self.policy),
            "published_snapshot_id": result.snapshot_id if result is not None else None,
            "accepted_rows": result.accepted_rows if result is not None else None,
            "quarantined_rows": result.quarantined_rows if result is not None else None,
            "retryable_in_process": True,
            "restart_recovery": _RESTART_RECOVERY,
        }
        digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        path = self.root / (f"finalize-failure-{self._finalize_failures:04d}-sha256-{digest}.json")
        _atomic_immutable_write(
            path,
            canonical_json_bytes({**identity, "failure_sha256": digest}),
            root=self.hot_root,
        )
        return path

    def _publish_normalized(self) -> _NormalizationSummary:
        arguments = (
            self.hot_root,
            self.root,
            tuple(self._parts),
            self.provider,
            self.venue,
            tuple(self._raw_references),
            self.policy,
        )
        if self.normalization_executor is not None:
            return self.normalization_executor.submit(_publish_epoch_parts, *arguments).result()
        return _publish_epoch_parts(*arguments)

    def _iter_record_batches(self) -> Iterable[pa.RecordBatch]:
        return _iter_epoch_record_batches(self.root, tuple(self._parts), trusted_root=self.hot_root)

    @staticmethod
    def _record_batch(
        identity: tuple[str, str, str] | None,
        records: list[dict[str, Any]],
    ) -> pa.RecordBatch:
        if identity is None or not records:
            raise ValidationError("capture epoch Arrow batch identity is missing")
        schema = get_arrow_schema(identity[0])
        prepared = normalized_v3._arrow_ready_rows(schema, records)
        return pa.RecordBatch.from_pylist(prepared, schema=schema)

    def _iter_records(self) -> Iterable[dict[str, Any]]:
        return _iter_epoch_records(self.root, tuple(self._parts), trusted_root=self.hot_root)

    def _require_open(self) -> None:
        if self._state != "OPEN":
            raise ValidationError("Normalized epoch journal is already closed")

    def __del__(self) -> None:
        stream = getattr(self, "_open_stream", None)
        if stream is not None and not stream.closed:
            try:
                stream.flush()
                os.fsync(stream.fileno())
            except OSError:
                pass
            stream.close()


def _iter_epoch_records(
    root: Path,
    parts: tuple[EpochPart, ...],
    *,
    trusted_root: Path,
) -> Iterable[dict[str, Any]]:
    root = _validate_safe_path(trusted_root, root, allow_missing=False)
    for part in parts:
        path = _validate_safe_path(trusted_root, root / part.relative_path, allow_missing=False)
        if (
            path.stat().st_size != part.byte_length
            or _sha256_file(path, trusted_root=trusted_root) != part.content_sha256
        ):
            raise ValidationError(f"Normalized epoch journal part integrity changed: {path}")
        with _open_journal_file(trusted_root, path, "rb") as stream:
            for line_number, line in enumerate(stream, start=1):
                try:
                    value = orjson.loads(line)
                except (UnicodeError, orjson.JSONDecodeError) as exc:
                    raise ValidationError(
                        f"Normalized epoch journal line is malformed: {path}:{line_number}"
                    ) from exc
                if not isinstance(value, dict):
                    raise ValidationError("Normalized epoch journal record must be an object")
                yield value


def _iter_epoch_record_batches(
    root: Path,
    parts: tuple[EpochPart, ...],
    *,
    trusted_root: Path,
) -> Iterable[pa.RecordBatch]:
    pending: list[dict[str, Any]] = []
    pending_identity: tuple[str, str, str] | None = None
    for record in _iter_epoch_records(root, parts, trusted_root=trusted_root):
        try:
            schema_id = _CAPTURE_EVENT_SCHEMAS[str(record["event_type"])]
            identity = (
                schema_id,
                str(record["instrument_id"]),
                str(record["trading_day"]),
            )
        except KeyError as exc:
            raise ValidationError(
                f"capture epoch contains an unsupported or incomplete event: {record}"
            ) from exc
        if pending and (identity != pending_identity or len(pending) >= _ARROW_BATCH_ROWS):
            yield NormalizedEpochJournal._record_batch(pending_identity, pending)
            pending = []
        pending_identity = identity
        pending.append(record)
    if pending:
        yield NormalizedEpochJournal._record_batch(pending_identity, pending)


def _publish_epoch_parts(
    hot_root: Path,
    journal_root: Path,
    parts: tuple[EpochPart, ...],
    provider: str,
    venue: str,
    raw_references: tuple[RawObjectReference, ...],
    policy: StoragePolicy,
) -> _NormalizationSummary:
    result = write_normalized_batches(
        hot_root,
        _iter_epoch_record_batches(journal_root, parts, trusted_root=hot_root),
        provider=provider,
        venue=venue,
        upstream_raw_references=raw_references,
        policy=policy,
    )
    return _NormalizationSummary(
        snapshot_id=result.snapshot.snapshot_id if result.snapshot else None,
        accepted_rows=result.accepted_rows,
        quarantined_rows=result.quarantined_rows,
    )


def _publish_epoch_group(
    jobs: tuple[
        tuple[
            Path,
            Path,
            tuple[EpochPart, ...],
            str,
            str,
            tuple[RawObjectReference, ...],
            StoragePolicy,
        ],
        ...,
    ],
) -> tuple[_NormalizationSummary, ...]:
    if not jobs:
        raise ValidationError("Normalized epoch group must not be empty")
    hot_root = jobs[0][0]
    provider = jobs[0][3]
    venue = jobs[0][4]
    policy = jobs[0][6]
    if any(
        job[0] != hot_root or job[3] != provider or job[4] != venue or job[6] != policy
        for job in jobs
    ):
        raise ValidationError("Normalized epoch group identity or policy changed")
    expected_rows = tuple(sum(part.rows for part in job[2]) for job in jobs)

    def batches() -> Iterable[pa.RecordBatch]:
        for job in jobs:
            yield from _iter_epoch_record_batches(job[1], job[2], trusted_root=hot_root)

    raw_references = tuple(reference for job in jobs for reference in job[5])
    result = write_normalized_batches(
        hot_root,
        batches(),
        provider=provider,
        venue=venue,
        upstream_raw_references=raw_references,
        policy=policy,
    )
    if result.accepted_rows != sum(expected_rows) or result.quarantined_rows:
        raise ValidationError("Normalized epoch group row accounting changed")
    snapshot_id = result.snapshot.snapshot_id if result.snapshot else None
    return tuple(_NormalizationSummary(snapshot_id, rows, 0) for rows in expected_rows)
