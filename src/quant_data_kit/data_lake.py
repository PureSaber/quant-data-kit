"""Immutable Raw/Normalized data-lake storage and pinned DuckDB catalog access."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from typing_extensions import Self

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.l2_replay import replay_l2
from quant_data_kit.schemas_v2 import (
    BAR_EVENT_SCHEMA_ID,
    BOOK_DELTA_EVENT_SCHEMA_ID,
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    CORPORATE_ACTION_EVENT_SCHEMA_ID,
    FUNDING_RATE_EVENT_SCHEMA_ID,
    MARK_PRICE_EVENT_SCHEMA_ID,
    QUOTE_EVENT_SCHEMA_ID,
    SCHEMA_VERSION_V2,
    STATUS_EVENT_SCHEMA_ID,
    TRADE_EVENT_SCHEMA_ID,
    get_arrow_schema,
    validate_arrow_table,
    validate_event_stream,
    validate_json_record,
)

_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_EVENT_SCHEMAS = {
    "bar": BAR_EVENT_SCHEMA_ID,
    "book_delta": BOOK_DELTA_EVENT_SCHEMA_ID,
    "book_snapshot": BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    "corporate_action": CORPORATE_ACTION_EVENT_SCHEMA_ID,
    "funding_rate": FUNDING_RATE_EVENT_SCHEMA_ID,
    "mark_price": MARK_PRICE_EVENT_SCHEMA_ID,
    "quote": QUOTE_EVENT_SCHEMA_ID,
    "status": STATUS_EVENT_SCHEMA_ID,
    "trade": TRADE_EVENT_SCHEMA_ID,
}
_GIB = 1024**3


class CollectionStoppedError(ValidationError):
    """Raised when explicit storage safety policy stops acquisition."""


@dataclass(frozen=True)
class StoragePolicy:
    hot_retention_days: int = 30
    hot_quota_bytes: int = 150 * _GIB
    minimum_free_bytes: int = 100 * _GIB
    minimum_free_fraction: float = 0.20

    def __post_init__(self) -> None:
        if self.hot_retention_days != 30:
            raise ValidationError("M2 hot_retention_days must remain 30")
        if self.hot_quota_bytes <= 0 or self.minimum_free_bytes <= 0:
            raise ValidationError("storage byte thresholds must be positive")
        if not 0 < self.minimum_free_fraction < 1:
            raise ValidationError("minimum_free_fraction must be between zero and one")


_DEFAULT_STORAGE_POLICY = StoragePolicy()


@dataclass(frozen=True)
class CapacityDecision:
    allowed: bool
    hot_bytes: int
    projected_hot_bytes: int
    free_bytes_after_write: int
    minimum_free_bytes: int
    reasons: tuple[str, ...]
    alert: str | None


@dataclass(frozen=True)
class RawObjectManifest:
    schema_version: str
    layer: str
    object_id: str
    source: str
    request: dict[str, Any]
    request_sha256: str
    collected_at: str
    collection_date: str
    content_sha256: str
    byte_length: int
    hot_retention_days: int
    hot_until: str
    data_path: str = "payload.bin"


@dataclass(frozen=True)
class ArchiveReceipt:
    object_id: str
    archive_uri: str
    source_sha256: str
    archive_sha256: str
    restored_sha256: str
    verified_at: str


@dataclass(frozen=True)
class PartitionManifest:
    relative_path: str
    provider: str
    venue: str
    event_type: str
    trading_date: str
    instrument_id: str
    schema_id: str
    rows: int
    logical_sha256: str
    content_sha256: str


@dataclass(frozen=True)
class NormalizedSnapshot:
    schema_version: str
    layer: str
    snapshot_id: str
    provider: str
    venue: str
    created_at: str
    logical_sha256: str
    rows: int
    upstream_raw_ids: tuple[str, ...]
    partitions: tuple[PartitionManifest, ...]


@dataclass(frozen=True)
class QuarantineEntry:
    input_index: int
    reason: str
    record: dict[str, Any]


@dataclass(frozen=True)
class NormalizationResult:
    snapshot: NormalizedSnapshot | None
    accepted_rows: int
    quarantined_rows: int
    quarantine_manifest: Path | None


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _json_evidence(value: Any) -> Any:
    """Preserve invalid input as deterministic, standards-compliant quarantine evidence."""
    if isinstance(value, datetime):
        return _utc_text(value, "quarantine_datetime")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_evidence(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_evidence(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return {"invalid_float": repr(value)}
    if isinstance(value, bytes):
        return {"bytes": len(value), "sha256": _sha256_bytes(value)}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return {"invalid_type": type(value).__name__, "repr": repr(value)}


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_datetime(value: datetime | str, field_name: str) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValidationError(f"{field_name} must be UTC-aware")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime | str, field_name: str) -> str:
    return _utc_datetime(value, field_name).isoformat().replace("+00:00", "Z")


def _segment(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not _SAFE_SEGMENT.fullmatch(value):
        raise ValidationError(f"{field_name} is not a safe path segment: {value!r}")
    if value in {".", "..", "latest"}:
        raise ValidationError(f"{field_name} is reserved: {value!r}")
    return value


def _partition_segment(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty partition value")
    if value in {".", "..", "latest"}:
        raise ValidationError(f"{field_name} is not a safe partition value: {value!r}")
    return quote(value, safe="-._")


def _tree_size(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _disk_probe_path(root: Path) -> Path:
    candidate = root.resolve()
    while not candidate.exists() and candidate.parent != candidate:
        candidate = candidate.parent
    return candidate


def evaluate_capacity(
    root: Path,
    *,
    projected_write_bytes: int = 0,
    policy: StoragePolicy = _DEFAULT_STORAGE_POLICY,
    current_hot_bytes: int | None = None,
    disk_total_bytes: int | None = None,
    disk_free_bytes: int | None = None,
) -> CapacityDecision:
    """Evaluate the 150GB hot quota and max(20% volume, 100GB) free-space gate."""
    if projected_write_bytes < 0:
        raise ValidationError("projected_write_bytes must be non-negative")
    hot_bytes = _tree_size(Path(root)) if current_hot_bytes is None else current_hot_bytes
    if hot_bytes < 0:
        raise ValidationError("current_hot_bytes must be non-negative")
    if disk_total_bytes is None or disk_free_bytes is None:
        usage = shutil.disk_usage(_disk_probe_path(Path(root)))
        disk_total_bytes = usage.total
        disk_free_bytes = usage.free
    if disk_total_bytes <= 0 or disk_free_bytes < 0:
        raise ValidationError("disk capacity values are invalid")
    minimum_free = max(
        int(disk_total_bytes * policy.minimum_free_fraction),
        policy.minimum_free_bytes,
    )
    projected_hot = hot_bytes + projected_write_bytes
    free_after = disk_free_bytes - projected_write_bytes
    reasons: list[str] = []
    if projected_hot > policy.hot_quota_bytes:
        reasons.append(
            f"hot quota exceeded: projected={projected_hot}, quota={policy.hot_quota_bytes}"
        )
    if free_after < minimum_free:
        reasons.append(
            f"free-space floor breached: projected={free_after}, minimum={minimum_free}"
        )
    alert = None
    if reasons:
        alert = "COLLECTION_STOPPED: " + "; ".join(reasons)
    return CapacityDecision(
        allowed=not reasons,
        hot_bytes=hot_bytes,
        projected_hot_bytes=projected_hot,
        free_bytes_after_write=free_after,
        minimum_free_bytes=minimum_free,
        reasons=tuple(reasons),
        alert=alert,
    )


def require_collection_capacity(
    root: Path,
    *,
    projected_write_bytes: int,
    policy: StoragePolicy = _DEFAULT_STORAGE_POLICY,
    current_hot_bytes: int | None = None,
    disk_total_bytes: int | None = None,
    disk_free_bytes: int | None = None,
) -> CapacityDecision:
    decision = evaluate_capacity(
        root,
        projected_write_bytes=projected_write_bytes,
        policy=policy,
        current_hot_bytes=current_hot_bytes,
        disk_total_bytes=disk_total_bytes,
        disk_free_bytes=disk_free_bytes,
    )
    if not decision.allowed:
        raise CollectionStoppedError(decision.alert or "COLLECTION_STOPPED")
    return decision


def _raw_object_dir(
    root: Path,
    source: str,
    collection_date: str,
    object_id: str,
) -> Path:
    return (
        Path(root)
        / "raw"
        / f"source={_segment(source, 'source')}"
        / f"date={_segment(collection_date, 'collection_date')}"
        / _segment(object_id, "object_id")
    )


def _load_raw_from_dir(object_dir: Path) -> RawObjectManifest:
    manifest_path = object_dir / "manifest.json"
    if not manifest_path.is_file():
        raise ValidationError(f"Raw manifest missing: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest = RawObjectManifest(**payload)
    if manifest.schema_version != "2.0.0" or manifest.layer != "raw":
        raise ValidationError("Raw manifest schema/layer mismatch")
    if object_dir.name != manifest.object_id:
        raise ValidationError("Raw object_id does not match its immutable directory")
    if _sha256_bytes(_canonical_json_bytes(manifest.request)) != manifest.request_sha256:
        raise ValidationError("Raw request hash changed")
    expected_hot_until = _utc_text(
        _utc_datetime(manifest.collected_at, "collected_at")
        + timedelta(days=manifest.hot_retention_days),
        "hot_until",
    )
    if manifest.hot_retention_days != 30 or manifest.hot_until != expected_hot_until:
        raise ValidationError("Raw hot-retention metadata changed")
    data_path = object_dir / manifest.data_path
    if data_path.parent.resolve() != object_dir.resolve() or not data_path.is_file():
        raise ValidationError(f"Raw payload path is invalid: {data_path}")
    if data_path.stat().st_size != manifest.byte_length:
        raise ValidationError(f"Raw payload length changed: {data_path}")
    if _sha256_file(data_path) != manifest.content_sha256:
        raise ValidationError(f"Raw payload hash changed: {data_path}")
    return manifest


def write_raw_bytes(
    root: Path,
    *,
    source: str,
    request: Mapping[str, Any],
    collected_at: datetime | str,
    payload: bytes,
    object_id: str | None = None,
    policy: StoragePolicy = _DEFAULT_STORAGE_POLICY,
) -> RawObjectManifest:
    """Write exact provider bytes once; repeated identical writes are idempotent."""
    if not isinstance(payload, bytes):
        raise ValidationError("Raw payload must be bytes")
    collected_text = _utc_text(collected_at, "collected_at")
    collection_date = collected_text[:10]
    request_dict = dict(request)
    try:
        request_hash = _sha256_bytes(_canonical_json_bytes(request_dict))
    except (TypeError, ValueError) as exc:
        raise ValidationError("Raw request metadata must be canonical JSON") from exc
    derived_id = _sha256_bytes(
        _canonical_json_bytes(
            {"source": source, "request_sha256": request_hash, "collected_at": collected_text}
        )
    )[:32]
    resolved_id = object_id or derived_id
    object_dir = _raw_object_dir(root, source, collection_date, resolved_id)
    tombstone = object_dir.parent / f"{resolved_id}.cleanup.json"
    if tombstone.exists():
        raise ValidationError(f"Raw object was archived and cleaned; key cannot be reused: {resolved_id}")
    content_hash = _sha256_bytes(payload)
    manifest = RawObjectManifest(
        schema_version="2.0.0",
        layer="raw",
        object_id=resolved_id,
        source=source,
        request=request_dict,
        request_sha256=request_hash,
        collected_at=collected_text,
        collection_date=collection_date,
        content_sha256=content_hash,
        byte_length=len(payload),
        hot_retention_days=policy.hot_retention_days,
        hot_until=_utc_text(
            _utc_datetime(collected_text, "collected_at")
            + timedelta(days=policy.hot_retention_days),
            "hot_until",
        ),
    )
    if object_dir.exists():
        existing = _load_raw_from_dir(object_dir)
        if existing != manifest:
            raise ValidationError(f"Conflicting immutable Raw object: {object_dir}")
        return existing
    manifest_bytes = json.dumps(asdict(manifest), indent=2, ensure_ascii=False).encode("utf-8")
    require_collection_capacity(
        root,
        projected_write_bytes=len(payload) + len(manifest_bytes),
        policy=policy,
    )
    object_dir.mkdir(parents=True, exist_ok=False)
    try:
        (object_dir / manifest.data_path).write_bytes(payload)
        (object_dir / "manifest.json").write_bytes(manifest_bytes)
    except BaseException:
        for child in object_dir.iterdir():
            if child.is_file():
                child.unlink()
        object_dir.rmdir()
        raise
    return _load_raw_from_dir(object_dir)


def load_raw_object(manifest_path: Path) -> tuple[RawObjectManifest, bytes]:
    object_dir = Path(manifest_path).resolve().parent
    manifest = _load_raw_from_dir(object_dir)
    return manifest, (object_dir / manifest.data_path).read_bytes()


def cleanup_archived_raw_object(
    manifest_path: Path,
    receipt: ArchiveReceipt,
    *,
    confirm: bool = False,
    now: datetime | str | None = None,
) -> Path:
    """Remove one verified Raw object and retain an immutable cleanup audit record."""
    if not confirm:
        raise ValidationError("Raw cleanup requires explicit confirm=True")
    object_dir = Path(manifest_path).resolve().parent
    manifest = _load_raw_from_dir(object_dir)
    current_time = _utc_datetime(now or datetime.now(timezone.utc), "now")
    if current_time < _utc_datetime(manifest.hot_until, "hot_until"):
        raise ValidationError(
            f"Raw object is still inside its {manifest.hot_retention_days}-day hot-retention window"
        )
    verified_at = _utc_datetime(receipt.verified_at, "verified_at")
    collected_at = _utc_datetime(manifest.collected_at, "collected_at")
    if verified_at < collected_at or verified_at > current_time:
        raise ValidationError("Archive verification time is outside the valid cleanup interval")
    if not receipt.archive_uri.strip():
        raise ValidationError("archive_uri must be non-empty")
    expected = manifest.content_sha256
    if receipt.object_id != manifest.object_id or {
        receipt.source_sha256,
        receipt.archive_sha256,
        receipt.restored_sha256,
    } != {expected}:
        raise ValidationError("Archive receipt does not prove hash-verified restoration")
    expected_files = {"manifest.json", manifest.data_path}
    actual_files = {path.name for path in object_dir.iterdir()}
    if actual_files != expected_files:
        raise ValidationError("Raw object contains unexpected files; cleanup refused")
    tombstone = object_dir.parent / f"{manifest.object_id}.cleanup.json"
    if tombstone.exists():
        raise ValidationError(f"Cleanup audit already exists: {tombstone}")
    audit = {
        "schema_version": "2.0.0",
        "action": "verified_archive_cleanup",
        "raw_manifest": asdict(manifest),
        "archive_receipt": asdict(receipt),
    }
    tombstone.write_text(json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8")
    (object_dir / manifest.data_path).unlink()
    (object_dir / "manifest.json").unlink()
    object_dir.rmdir()
    return tombstone


def _event_schema_id(record: Mapping[str, Any]) -> str:
    try:
        return _EVENT_SCHEMAS[str(record["event_type"])]
    except (KeyError, TypeError) as exc:
        raise ValidationError(f"Unknown event_type: {record.get('event_type')!r}") from exc


def _stream_key(record: Mapping[str, Any], index: int) -> tuple[str, str, str]:
    try:
        return (
            str(record["source"]),
            str(record["instrument_id"]),
            str(record["session_id"]),
        )
    except KeyError:
        return (f"invalid-{index}", f"invalid-{index}", f"invalid-{index}")


def _arrow_ready(record: Mapping[str, Any], schema: pa.Schema) -> dict[str, Any]:
    result = dict(record)
    for field_definition in schema:
        value = result[field_definition.name]
        if value is None:
            continue
        if pa.types.is_timestamp(field_definition.type):
            result[field_definition.name] = _utc_datetime(value, field_definition.name)
        elif pa.types.is_date32(field_definition.type):
            result[field_definition.name] = date.fromisoformat(str(value))
    return result


def _validated_table(schema_id: str, records: list[dict[str, Any]]) -> pa.Table:
    schema = get_arrow_schema(schema_id)
    table = pa.Table.from_pylist([_arrow_ready(record, schema) for record in records], schema=schema)
    validate_arrow_table(schema_id, table)
    return table


def _normalized_snapshot_payload(
    *,
    provider: str,
    venue: str,
    upstream_raw_ids: tuple[str, ...],
    partitions: tuple[PartitionManifest, ...],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION_V2,
        "layer": "normalized",
        "provider": provider,
        "venue": venue,
        "upstream_raw_ids": upstream_raw_ids,
        "partitions": [
            {
                "relative_path": item.relative_path,
                "provider": item.provider,
                "venue": item.venue,
                "event_type": item.event_type,
                "trading_date": item.trading_date,
                "instrument_id": item.instrument_id,
                "schema_id": item.schema_id,
                "rows": item.rows,
                "logical_sha256": item.logical_sha256,
            }
            for item in partitions
        ],
    }


def _write_quarantine(
    root: Path,
    provider: str,
    venue: str,
    entries: list[QuarantineEntry],
) -> Path | None:
    if not entries:
        return None
    body = (
        b"\n".join(_canonical_json_bytes(_json_evidence(asdict(entry))) for entry in entries)
        + b"\n"
    )
    batch_id = f"sha256-{_sha256_bytes(body)[:24]}"
    batch_dir = Path(root) / "quarantine" / batch_id
    records_path = batch_dir / "records.jsonl"
    manifest_path = batch_dir / "manifest.json"
    manifest = {
        "schema_version": "2.0.0",
        "layer": "quarantine",
        "batch_id": batch_id,
        "provider": provider,
        "venue": venue,
        "rows": len(entries),
        "content_sha256": _sha256_bytes(body),
        "data_path": "records.jsonl",
    }
    if batch_dir.exists():
        if not records_path.is_file() or _sha256_file(records_path) != manifest["content_sha256"]:
            raise ValidationError(f"Quarantine batch changed: {batch_dir}")
        if not manifest_path.is_file() or json.loads(
            manifest_path.read_text(encoding="utf-8")
        ) != manifest:
            raise ValidationError(f"Quarantine manifest changed: {batch_dir}")
        return manifest_path
    batch_dir.mkdir(parents=True, exist_ok=False)
    records_path.write_bytes(body)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest_path


def write_normalized_events(
    root: Path,
    records: Iterable[Mapping[str, Any]],
    *,
    provider: str,
    venue: str,
    upstream_raw_ids: Iterable[str] = (),
    expected_l2_checkpoint_hashes: Mapping[
        tuple[str, str, str], Mapping[int, str]
    ] | None = None,
) -> NormalizationResult:
    """Validate full streams, quarantine failures, then immutably partition strict v2 Parquet."""
    provider = _segment(provider, "provider")
    venue = _segment(venue, "venue")
    indexed = [(index, dict(record)) for index, record in enumerate(records)]
    streams: dict[tuple[str, str, str], list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    for index, record in indexed:
        streams[_stream_key(record, index)].append((index, record))
    accepted: list[tuple[int, dict[str, Any]]] = []
    quarantined: list[QuarantineEntry] = []
    expected_l2 = dict(expected_l2_checkpoint_hashes or {})
    for stream_key, stream_records in streams.items():
        try:
            if any(str(record.get("source")) != provider for _, record in stream_records):
                raise ValidationError(f"stream source does not match provider {provider}")
            validate_event_stream(record for _, record in stream_records)
            l2_records = [
                record
                for _, record in stream_records
                if record.get("event_type") in {"book_snapshot", "book_delta"}
            ]
            if l2_records:
                replay_l2(
                    l2_records,
                    expected_checkpoint_hashes=expected_l2.get(stream_key),
                    capture_all_checkpoints=False,
                )
        except (ValidationError, ValueError, TypeError, OverflowError) as exc:
            reason = f"stream_validation_failed: {exc}"
            quarantined.extend(
                QuarantineEntry(input_index=index, reason=reason, record=record)
                for index, record in stream_records
            )
        else:
            accepted.extend(stream_records)
    accepted.sort(key=lambda item: item[0])
    quarantine_manifest = _write_quarantine(root, provider, venue, quarantined)
    if not accepted:
        return NormalizationResult(
            snapshot=None,
            accepted_rows=0,
            quarantined_rows=len(quarantined),
            quarantine_manifest=quarantine_manifest,
        )

    partitioned: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for _, record in accepted:
        event_type = _segment(str(record["event_type"]), "event_type")
        trading_date = _segment(str(record["trading_day"]), "trading_day")
        instrument_id = str(record["instrument_id"])
        _partition_segment(instrument_id, "instrument_id")
        partitioned[(event_type, trading_date, instrument_id)].append(record)

    normalized_root = Path(root) / "normalized"
    staging_root = normalized_root / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix="m2-", dir=staging_root))
    partition_manifests: list[PartitionManifest] = []
    try:
        for event_type, trading_date, instrument_id in sorted(partitioned):
            rows = sorted(
                partitioned[(event_type, trading_date, instrument_id)],
                key=lambda row: (
                    str(row["event_time"]),
                    -1 if row["sequence"] is None else int(row["sequence"]),
                    str(row["event_id"]),
                ),
            )
            schema_id = _event_schema_id(rows[0])
            for row in rows:
                if _event_schema_id(row) != schema_id:
                    raise ValidationError("Mixed schemas entered one normalized partition")
                validate_json_record(schema_id, row)
            table = _validated_table(schema_id, rows)
            partition_logical_sha256 = _sha256_bytes(_canonical_json_bytes(rows))
            relative = Path(
                f"provider={provider}/venue={venue}/event_type={event_type}/"
                f"date={trading_date}/"
                f"instrument={_partition_segment(instrument_id, 'instrument_id')}/data.parquet"
            )
            target = stage / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, target, compression="zstd", use_dictionary=False)
            partition_manifests.append(
                PartitionManifest(
                    relative_path=relative.as_posix(),
                    provider=provider,
                    venue=venue,
                    event_type=event_type,
                    trading_date=trading_date,
                    instrument_id=instrument_id,
                    schema_id=schema_id,
                    rows=table.num_rows,
                    logical_sha256=partition_logical_sha256,
                    content_sha256=_sha256_file(target),
                )
            )
        partitions = tuple(partition_manifests)
        raw_ids = tuple(sorted(_segment(item, "upstream_raw_id") for item in upstream_raw_ids))
        identity = _normalized_snapshot_payload(
            provider=provider,
            venue=venue,
            upstream_raw_ids=raw_ids,
            partitions=partitions,
        )
        logical_sha256 = _sha256_bytes(_canonical_json_bytes(identity))
        snapshot_id = f"sha256-{logical_sha256[:24]}"
        snapshot = NormalizedSnapshot(
            schema_version=SCHEMA_VERSION_V2,
            layer="normalized",
            snapshot_id=snapshot_id,
            provider=provider,
            venue=venue,
            created_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            logical_sha256=logical_sha256,
            rows=sum(item.rows for item in partitions),
            upstream_raw_ids=raw_ids,
            partitions=partitions,
        )
        (stage / "manifest.json").write_text(
            json.dumps(asdict(snapshot), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        snapshot_dir = normalized_root / "snapshots" / snapshot_id
        snapshot_dir.parent.mkdir(parents=True, exist_ok=True)
        if snapshot_dir.exists():
            existing = load_normalized_snapshot(root, snapshot_id)
            if existing.logical_sha256 != logical_sha256:
                raise ValidationError(f"Normalized snapshot collision: {snapshot_dir}")
            return NormalizationResult(
                snapshot=existing,
                accepted_rows=existing.rows,
                quarantined_rows=len(quarantined),
                quarantine_manifest=quarantine_manifest,
            )
        os.replace(stage, snapshot_dir)
        stage = snapshot_dir
        verified = load_normalized_snapshot(root, snapshot_id)
        return NormalizationResult(
            snapshot=verified,
            accepted_rows=verified.rows,
            quarantined_rows=len(quarantined),
            quarantine_manifest=quarantine_manifest,
        )
    finally:
        if stage.exists() and stage.parent == staging_root:
            shutil.rmtree(stage)


def _safe_snapshot_partition(snapshot_dir: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValidationError(f"Unsafe partition path: {relative_path}")
    candidate = snapshot_dir / relative
    if candidate.is_symlink() or not candidate.is_file():
        raise ValidationError(f"Snapshot partition missing or linked: {candidate}")
    if snapshot_dir.resolve() not in candidate.resolve().parents:
        raise ValidationError(f"Snapshot partition escapes root: {candidate}")
    return candidate


def load_normalized_snapshot(root: Path, snapshot_id: str) -> NormalizedSnapshot:
    snapshot_id = _segment(snapshot_id, "snapshot_id")
    snapshot_dir = Path(root) / "normalized" / "snapshots" / snapshot_id
    manifest_path = snapshot_dir / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValidationError(f"Normalized snapshot manifest missing: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["upstream_raw_ids"] = tuple(payload["upstream_raw_ids"])
    payload["partitions"] = tuple(PartitionManifest(**item) for item in payload["partitions"])
    snapshot = NormalizedSnapshot(**payload)
    if (
        snapshot.snapshot_id != snapshot_id
        or snapshot.layer != "normalized"
        or snapshot.schema_version != SCHEMA_VERSION_V2
    ):
        raise ValidationError("Normalized snapshot identity mismatch")
    _utc_datetime(snapshot.created_at, "created_at")
    identity = _normalized_snapshot_payload(
        provider=snapshot.provider,
        venue=snapshot.venue,
        upstream_raw_ids=snapshot.upstream_raw_ids,
        partitions=snapshot.partitions,
    )
    logical_sha256 = _sha256_bytes(_canonical_json_bytes(identity))
    if logical_sha256 != snapshot.logical_sha256 or snapshot_id != f"sha256-{logical_sha256[:24]}":
        raise ValidationError("Normalized snapshot logical hash changed")
    rows = 0
    expected_files = {Path("manifest.json")}
    seen_paths: set[str] = set()
    for partition in snapshot.partitions:
        if partition.relative_path in seen_paths:
            raise ValidationError("Normalized snapshot contains duplicate partition paths")
        seen_paths.add(partition.relative_path)
        if partition.provider != snapshot.provider or partition.venue != snapshot.venue:
            raise ValidationError("Normalized partition provider/venue mismatch")
        if _EVENT_SCHEMAS.get(partition.event_type) != partition.schema_id:
            raise ValidationError("Normalized partition event/schema mismatch")
        expected_relative = Path(
            f"provider={partition.provider}/venue={partition.venue}/"
            f"event_type={partition.event_type}/date={partition.trading_date}/"
            f"instrument={_partition_segment(partition.instrument_id, 'instrument_id')}/"
            "data.parquet"
        ).as_posix()
        if partition.relative_path != expected_relative:
            raise ValidationError("Normalized partition path metadata mismatch")
        path = _safe_snapshot_partition(snapshot_dir, partition.relative_path)
        expected_files.add(Path(partition.relative_path))
        if _sha256_file(path) != partition.content_sha256:
            raise ValidationError(f"Normalized partition hash changed: {path}")
        table = pq.ParquetFile(path).read()
        validate_arrow_table(partition.schema_id, table)
        if table.num_rows != partition.rows:
            raise ValidationError(f"Normalized partition row count changed: {path}")
        logical_rows = [_json_evidence(item) for item in table.to_pylist()]
        if _sha256_bytes(_canonical_json_bytes(logical_rows)) != partition.logical_sha256:
            raise ValidationError(f"Normalized partition logical content changed: {path}")
        rows += table.num_rows
    if rows != snapshot.rows:
        raise ValidationError("Normalized snapshot total row count changed")
    actual_files = {
        path.relative_to(snapshot_dir)
        for path in snapshot_dir.rglob("*")
        if path.is_file()
    }
    if actual_files != expected_files:
        raise ValidationError("Normalized snapshot contains an unexpected or missing file")
    return snapshot


class DuckDBSnapshot:
    """In-memory read-only query facade over one already verified snapshot."""

    def __init__(self, root: Path, snapshot: NormalizedSnapshot) -> None:
        self.snapshot = snapshot
        self._connection = duckdb.connect(database=":memory:")
        snapshot_dir = Path(root) / "normalized" / "snapshots" / snapshot.snapshot_id
        paths_by_event: dict[str, list[str]] = defaultdict(list)
        for partition in snapshot.partitions:
            path = _safe_snapshot_partition(snapshot_dir, partition.relative_path)
            paths_by_event[partition.event_type].append(path.as_posix())
        for event_type, paths in sorted(paths_by_event.items()):
            view_name = f"event_{event_type}"
            literals = ", ".join("'" + path.replace("'", "''") + "'" for path in sorted(paths))
            self._connection.execute(
                f'CREATE VIEW "{view_name}" AS '
                f"SELECT * FROM read_parquet([{literals}], hive_partitioning = false)"
            )

    def query(self, sql: str, parameters: Iterable[Any] | None = None) -> pa.Table:
        statement = sql.strip().lower()
        if not statement.startswith(("select", "with", "explain")):
            raise ValidationError("DuckDB snapshot catalog accepts read-only queries only")
        without_trailing_semicolon = statement.removesuffix(";")
        if ";" in without_trailing_semicolon or re.search(
            r"\b(attach|call|copy|create|delete|drop|export|import|insert|install|load|pragma|"
            r"update|alter)\b",
            without_trailing_semicolon,
        ):
            raise ValidationError("DuckDB snapshot query contains a non-read-only operation")
        relation = self._connection.execute(sql, list(parameters or ()))
        return relation.to_arrow_table()

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class DuckDBCatalog:
    """Resolve only explicit immutable snapshot IDs; there is no floating latest alias."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def open_snapshot(self, snapshot_id: str) -> DuckDBSnapshot:
        snapshot = load_normalized_snapshot(self.root, snapshot_id)
        return DuckDBSnapshot(self.root, snapshot)
