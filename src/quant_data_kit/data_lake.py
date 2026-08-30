"""Immutable Raw/Normalized data-lake storage and pinned DuckDB catalog access."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import stat
import threading
import uuid
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse
from urllib.request import url2pathname

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from typing_extensions import Self

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.l2_replay import replay_l2
from quant_data_kit.process_lock import process_file_lock
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

_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_WINDOWS_RESERVED_NAMES = {
    "AUX",
    "CON",
    "NUL",
    "PRN",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}
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
_CAPACITY_TREE_LOCK_STATE = threading.local()
_TRANSIENT_LAKE_RELATIVE_DIRECTORIES = {
    (".locks",),
    ("normalized", ".stage-owners"),
    ("normalized", "event-claim-index-v3", ".legacy-staging"),
    ("normalized", "event-claim-index-v3", ".staging"),
    ("normalized", "staging"),
    ("quarantine", ".staging"),
    ("raw", ".staging"),
}


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
    idempotency_key: str
    source: str
    request: dict[str, Any]
    request_sha256: str
    collected_at: str
    collection_date: str
    content_sha256: str
    byte_length: int
    hot_retention_days: int
    hot_until: str
    manifest_sha256: str
    data_path: str = "payload.bin"

    def reference(self) -> RawObjectReference:
        return RawObjectReference(
            source=self.source,
            collection_date=self.collection_date,
            idempotency_key=self.idempotency_key,
            object_id=self.object_id,
            content_sha256=self.content_sha256,
            manifest_sha256=self.manifest_sha256,
        )


@dataclass(frozen=True)
class RawObjectReference:
    source: str
    collection_date: str
    idempotency_key: str
    object_id: str
    content_sha256: str
    manifest_sha256: str


@dataclass(frozen=True)
class RawKeyClaim:
    schema_version: str
    layer: str
    source: str
    collection_date: str
    idempotency_key: str
    object_id: str
    manifest_sha256: str
    claim_sha256: str


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
class EventClaimReference:
    event_id_hash: str
    event_id: str
    schema_id: str
    event_sha256: str
    claim_sha256: str


@dataclass(frozen=True)
class EventClaimShardManifest:
    shard: str
    rows: int
    logical_sha256: str


@dataclass(frozen=True)
class EventClaimIndexManifest:
    format: str
    claim_version: str
    rows: int
    shards: tuple[EventClaimShardManifest, ...]


@dataclass(frozen=True)
class L2CheckpointManifest:
    source: str
    instrument_id: str
    session_id: str
    sequence: int
    state_sha256: str


class EventClaimSequence(Sequence[EventClaimReference]):
    """Lazy, immutable view of one normalized-v3 sharded claim index."""

    def __init__(
        self,
        root: Path,
        snapshot_id: str,
        index: EventClaimIndexManifest,
    ) -> None:
        self._root = Path(root)
        self._snapshot_id = snapshot_id
        self._index = index

    def __len__(self) -> int:
        return self._index.rows

    def __iter__(self) -> Iterator[EventClaimReference]:
        from quant_data_kit.normalized_v3 import iter_event_claims_v3

        return iter_event_claims_v3(self._root, self._snapshot_id, self._index)

    def __getitem__(
        self, index: int | slice
    ) -> EventClaimReference | tuple[EventClaimReference, ...]:
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            if step < 0:
                requested = range(start, stop, step)
                positions = set(requested)
                selected = {
                    position: claim for position, claim in enumerate(self) if position in positions
                }
                return tuple(selected[position] for position in requested)
            selected: list[EventClaimReference] = []
            for position, claim in enumerate(self):
                if position >= stop:
                    break
                if position >= start and (position - start) % step == 0:
                    selected.append(claim)
            return tuple(selected)
        position = index if index >= 0 else len(self) + index
        if position < 0 or position >= len(self):
            raise IndexError(index)
        for current, claim in enumerate(self):
            if current == position:
                return claim
        raise IndexError(index)

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, Sequence) or len(self) != len(other):
            return False
        return all(left == right for left, right in zip(self, other, strict=True))

    def __repr__(self) -> str:
        return f"EventClaimSequence(snapshot_id={self._snapshot_id!r}, rows={self._index.rows})"


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
    upstream_raw_references: tuple[RawObjectReference, ...]
    event_claims: Sequence[EventClaimReference]
    partitions: tuple[PartitionManifest, ...]
    layout_version: str = "2.0.0"
    partition_logical_hash_version: str = "canonical-json-array-v1"
    event_claim_index: EventClaimIndexManifest | None = None
    l2_checkpoints: tuple[L2CheckpointManifest, ...] = ()


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
    value_type = type(value)
    return {
        "invalid_type": value_type.__qualname__,
        "type_module": value_type.__module__,
    }


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
    if (
        value in {".", "..", "latest"}
        or value.rstrip(". ") != value
        or value.split(".", 1)[0].upper() in _WINDOWS_RESERVED_NAMES
    ):
        raise ValidationError(f"{field_name} is reserved: {value!r}")
    return value


def _partition_segment(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty partition value")
    if value in {".", "..", "latest"}:
        raise ValidationError(f"{field_name} is not a safe partition value: {value!r}")
    return quote(value, safe="-._")


def _is_reparse_point(path: Path) -> bool:
    if path.is_symlink():
        return True
    try:
        attributes = getattr(os.lstat(path), "st_file_attributes", 0)
    except FileNotFoundError:
        return False
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _resolved_lake_root(root: Path, *, create: bool) -> Path:
    lexical = Path(root).absolute()
    if create:
        lexical.mkdir(parents=True, exist_ok=True)
    if not lexical.is_dir():
        raise ValidationError(f"Data-lake root is not a directory: {lexical}")
    if _is_reparse_point(lexical):
        raise ValidationError(f"Data-lake root cannot be a reparse point: {lexical}")
    return lexical.resolve(strict=True)


def _validate_lake_path(root: Path, candidate: Path, *, allow_missing: bool) -> Path:
    """Reject lexical/resolved escapes and all reparse points below the trusted root."""
    resolved_root = _resolved_lake_root(root, create=False)
    lexical_candidate = Path(candidate).absolute()
    try:
        relative = lexical_candidate.relative_to(Path(root).absolute())
    except ValueError as exc:
        raise ValidationError(f"Path escapes data-lake root: {candidate}") from exc
    current = Path(root).absolute()
    for part in relative.parts:
        current = current / part
        if not current.exists():
            if allow_missing:
                continue
            raise ValidationError(f"Data-lake path is missing: {current}")
        if _is_reparse_point(current):
            raise ValidationError(f"Data-lake path contains a reparse point: {current}")
        try:
            current.resolve(strict=True).relative_to(resolved_root)
        except ValueError as exc:
            raise ValidationError(f"Resolved path escapes data-lake root: {current}") from exc
    resolved_candidate = lexical_candidate.resolve(strict=not allow_missing)
    try:
        resolved_candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValidationError(f"Resolved path escapes data-lake root: {candidate}") from exc
    return lexical_candidate


def _mkdir_in_lake(root: Path, path: Path) -> Path:
    bootstrap_path = _validate_lake_path(
        root,
        Path(root) / ".lock-bootstrap",
        allow_missing=True,
    )
    with process_file_lock(bootstrap_path):
        _validate_lake_path(root, bootstrap_path, allow_missing=False)
        checked = _validate_lake_path(root, path, allow_missing=True)
        checked.mkdir(parents=True, exist_ok=True)
        return _validate_lake_path(root, checked, allow_missing=False)


def _atomic_write_bytes(root: Path, target: Path, body: bytes) -> None:
    """Atomically replace one file while its target-specific caller lock is held."""
    parent = _mkdir_in_lake(root, target.parent)
    checked_target = _validate_lake_path(root, target, allow_missing=True)
    target_identity = checked_target.relative_to(Path(root).absolute()).as_posix()
    temporary_prefix = f".atomic-{_sha256_bytes(target_identity.encode('utf-8'))}-"
    for stale in parent.glob(f"{temporary_prefix}*.tmp"):
        checked_stale = _validate_lake_path(root, stale, allow_missing=False)
        if not checked_stale.is_file():
            raise ValidationError(f"Atomic staging entry is not a file: {checked_stale}")
        _unlink_tree_entry(root, checked_stale)
    temporary = parent / f"{temporary_prefix}{uuid.uuid4().hex}.tmp"
    _validate_lake_path(root, temporary, allow_missing=True)
    try:
        with temporary.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        if _sha256_file(temporary) != _sha256_bytes(body):
            raise ValidationError(f"Atomic staging verification failed: {temporary}")
        _replace_tree_entry(root, temporary, checked_target)
    finally:
        if temporary.exists():
            _unlink_tree_entry(root, temporary)


@contextmanager
def _lake_lock(root: Path, namespace: str, identity: Mapping[str, Any]) -> Iterable[None]:
    namespace = _segment(namespace, "lock_namespace")
    lock_id = _sha256_bytes(_canonical_json_bytes(dict(identity)))
    lock_root = _mkdir_in_lake(root, Path(root) / ".locks" / namespace)
    lock_path = _validate_lake_path(root, lock_root / f"{lock_id}.lock", allow_missing=True)
    with process_file_lock(lock_path):
        yield


@contextmanager
def _capacity_tree_lock(root: Path) -> Iterable[None]:
    """Serialize lake-wide capacity scans with topology-removing mutations."""
    key = str(Path(root).absolute())
    depths = getattr(_CAPACITY_TREE_LOCK_STATE, "depths", None)
    if depths is None:
        depths = {}
        _CAPACITY_TREE_LOCK_STATE.depths = depths
    if depths.get(key, 0):
        depths[key] += 1
        try:
            yield
        finally:
            depths[key] -= 1
        return
    with _lake_lock(root, "capacity-tree", {"scope": "lake-wide"}):
        depths[key] = 1
        try:
            yield
        finally:
            depths.pop(key, None)


def _replace_tree_entry(root: Path, source: Path, target: Path) -> None:
    """Atomically replace one lake entry without racing a capacity tree scan."""
    with _capacity_tree_lock(root):
        os.replace(source, target)


def _publish_tree_entry(
    root: Path,
    source: Path,
    target: Path,
    *,
    policy: StoragePolicy,
) -> CapacityDecision:
    """Capacity-check and publish one staged tree as a single lake-wide transaction."""
    with _capacity_tree_lock(root):
        decision = require_collection_capacity(
            root,
            projected_write_bytes=_tree_size(source),
            policy=policy,
        )
        os.replace(source, target)
        return decision


def _remove_tree(root: Path, target: Path) -> None:
    """Remove one lake subtree without racing a capacity tree scan."""
    with _capacity_tree_lock(root):
        shutil.rmtree(target)


def _remove_empty_tree(root: Path, target: Path) -> None:
    """Remove one empty lake directory without racing a capacity tree scan."""
    with _capacity_tree_lock(root):
        target.rmdir()


def _unlink_tree_entry(root: Path, target: Path, *, missing_ok: bool = False) -> None:
    """Remove one lake file without racing a capacity tree scan."""
    with _capacity_tree_lock(root):
        target.unlink(missing_ok=missing_ok)


@contextmanager
def _stable_staging_directory(
    root: Path,
    staging_root: Path,
    *,
    namespace: str,
    identity: Mapping[str, Any],
) -> Iterable[Path]:
    """Own and recover staging for one stable operation identity."""
    operation_id = _sha256_bytes(_canonical_json_bytes(dict(identity)))
    prefix = f"{_segment(namespace, 'staging_namespace')}-{operation_id}-"
    checked_root = _mkdir_in_lake(root, staging_root)
    with _lake_lock(root, namespace, identity):
        for stale in sorted(checked_root.glob(f"{prefix}*")):
            checked_stale = _validate_lake_path(root, stale, allow_missing=False)
            if not checked_stale.is_dir():
                raise ValidationError(f"Stable staging entry is not a directory: {checked_stale}")
            _remove_tree(root, checked_stale)
        stage = checked_root / f"{prefix}{uuid.uuid4().hex}"
        _validate_lake_path(root, stage, allow_missing=True)
        stage.mkdir(exist_ok=False)
        try:
            yield stage
        finally:
            if stage.exists() and stage.parent == checked_root:
                _remove_tree(root, stage)


def _tree_size(root: Path) -> int:
    if not root.exists():
        return 0
    total = 0
    for directory, child_directories, filenames in os.walk(root):
        relative_directory = Path(directory).relative_to(root)
        child_directories[:] = [
            name
            for name in child_directories
            if not _is_transient_lake_directory(relative_directory / name)
        ]
        for filename in filenames:
            if filename.startswith(".atomic-") and filename.endswith(".tmp"):
                continue
            total += (Path(directory) / filename).stat().st_size
    return total


def _is_transient_lake_directory(relative_path: Path) -> bool:
    parts = relative_path.parts
    return parts in _TRANSIENT_LAKE_RELATIVE_DIRECTORIES or (
        len(parts) == 3 and parts[0] == "curated" and parts[2] == "staging"
    )


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
    if current_hot_bytes is None:
        with _capacity_tree_lock(Path(root)):
            hot_bytes = _tree_size(Path(root))
    else:
        hot_bytes = current_hot_bytes
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
        reasons.append(f"free-space floor breached: projected={free_after}, minimum={minimum_free}")
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


def _raw_manifest_identity(manifest: RawObjectManifest) -> dict[str, Any]:
    return {
        "schema_version": manifest.schema_version,
        "layer": manifest.layer,
        "idempotency_key": manifest.idempotency_key,
        "source": manifest.source,
        "request": manifest.request,
        "request_sha256": manifest.request_sha256,
        "collected_at": manifest.collected_at,
        "collection_date": manifest.collection_date,
        "content_sha256": manifest.content_sha256,
        "byte_length": manifest.byte_length,
        "hot_retention_days": manifest.hot_retention_days,
        "hot_until": manifest.hot_until,
        "data_path": manifest.data_path,
    }


def _raw_key_dir(
    root: Path,
    source: str,
    collection_date: str,
    idempotency_key: str,
) -> Path:
    return (
        Path(root)
        / "raw"
        / f"source={_segment(source, 'source')}"
        / f"date={_segment(collection_date, 'collection_date')}"
        / f"key={_segment(idempotency_key, 'idempotency_key')}"
    )


def _raw_object_dir(root: Path, reference: RawObjectReference) -> Path:
    return (
        _raw_key_dir(
            root,
            reference.source,
            reference.collection_date,
            reference.idempotency_key,
        )
        / f"object={_segment(reference.object_id, 'object_id')}"
    )


def _raw_tombstone_path(root: Path, reference: RawObjectReference) -> Path:
    return (
        _raw_key_dir(
            root,
            reference.source,
            reference.collection_date,
            reference.idempotency_key,
        )
        / f"object={_segment(reference.object_id, 'object_id')}.cleanup.json"
    )


def _raw_deleting_dir(root: Path, reference: RawObjectReference) -> Path:
    return (
        _raw_key_dir(
            root,
            reference.source,
            reference.collection_date,
            reference.idempotency_key,
        )
        / f"deleting={_segment(reference.object_id, 'object_id')}"
    )


def _raw_claim_path(root: Path, reference: RawObjectReference) -> Path:
    return (
        Path(root)
        / "raw"
        / "key-claims"
        / f"source={_segment(reference.source, 'source')}"
        / f"key-sha256={_sha256_bytes(reference.idempotency_key.encode('utf-8'))}.json"
    )


def _raw_lock_identity(reference: RawObjectReference) -> dict[str, str]:
    return {
        "source": reference.source,
        "idempotency_key": reference.idempotency_key,
    }


def _raw_key_claim(reference: RawObjectReference) -> RawKeyClaim:
    identity = {
        "schema_version": "2.0.0",
        "layer": "raw-key-claim",
        **_raw_lock_identity(reference),
        "collection_date": reference.collection_date,
        "object_id": reference.object_id,
        "manifest_sha256": reference.manifest_sha256,
    }
    return RawKeyClaim(
        **identity,
        claim_sha256=_sha256_bytes(_canonical_json_bytes(identity)),
    )


def _load_raw_key_claim(root: Path, reference: RawObjectReference) -> RawKeyClaim:
    claim_path = _validate_lake_path(root, _raw_claim_path(root, reference), allow_missing=False)
    try:
        claim = RawKeyClaim(**json.loads(claim_path.read_text(encoding="utf-8")))
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError) as exc:
        raise ValidationError("Raw idempotency-key claim is unreadable or malformed") from exc
    identity = asdict(claim)
    anchor = identity.pop("claim_sha256")
    if anchor != _sha256_bytes(_canonical_json_bytes(identity)):
        raise ValidationError("Raw idempotency-key claim integrity changed")
    expected = _raw_key_claim(reference)
    if claim != expected:
        raise ValidationError(
            f"Conflicting immutable Raw idempotency key: {reference.idempotency_key}"
        )
    return claim


def _ensure_raw_key_claim(root: Path, reference: RawObjectReference) -> RawKeyClaim:
    claim_path = _raw_claim_path(root, reference)
    if claim_path.exists():
        return _load_raw_key_claim(root, reference)
    claim = _raw_key_claim(reference)
    _atomic_write_bytes(
        root,
        claim_path,
        json.dumps(asdict(claim), indent=2, ensure_ascii=False).encode("utf-8"),
    )
    return _load_raw_key_claim(root, reference)


def _raw_stage_prefix(reference: RawObjectReference) -> str:
    lock_id = _sha256_bytes(_canonical_json_bytes(_raw_lock_identity(reference)))
    return f"raw-{lock_id}-"


def _validate_raw_manifest(manifest: RawObjectManifest) -> None:
    if manifest.schema_version != "2.0.0" or manifest.layer != "raw":
        raise ValidationError("Raw manifest schema/layer mismatch")
    for value, field_name in (
        (manifest.source, "source"),
        (manifest.collection_date, "collection_date"),
        (manifest.idempotency_key, "idempotency_key"),
        (manifest.object_id, "object_id"),
    ):
        _segment(value, field_name)
    if manifest.data_path != "payload.bin":
        raise ValidationError("Raw data_path is not the frozen payload path")
    if _sha256_bytes(_canonical_json_bytes(manifest.request)) != manifest.request_sha256:
        raise ValidationError("Raw request hash changed")
    expected_hot_until = _utc_text(
        _utc_datetime(manifest.collected_at, "collected_at")
        + timedelta(days=manifest.hot_retention_days),
        "hot_until",
    )
    if (
        manifest.collection_date != manifest.collected_at[:10]
        or manifest.hot_retention_days != 30
        or manifest.hot_until != expected_hot_until
    ):
        raise ValidationError("Raw collection/retention metadata changed")
    anchor = _sha256_bytes(_canonical_json_bytes(_raw_manifest_identity(manifest)))
    if manifest.manifest_sha256 != anchor or manifest.object_id != f"sha256-{anchor}":
        raise ValidationError("Raw manifest integrity anchor changed")


def _load_raw_from_dir(
    root: Path,
    object_dir: Path,
    *,
    expected: RawObjectReference | None = None,
    enforce_directory_identity: bool = True,
) -> RawObjectManifest:
    object_dir = _validate_lake_path(root, object_dir, allow_missing=False)
    manifest_path = _validate_lake_path(root, object_dir / "manifest.json", allow_missing=False)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest = RawObjectManifest(**payload)
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError) as exc:
        raise ValidationError(f"Raw manifest is unreadable or malformed: {manifest_path}") from exc
    _validate_raw_manifest(manifest)
    if enforce_directory_identity:
        expected_dir = _raw_object_dir(root, manifest.reference())
        if object_dir != expected_dir:
            raise ValidationError("Raw directory identity does not match anchored manifest")
    if expected is not None and manifest.reference() != expected:
        raise ValidationError("Raw object does not match the trusted reference")
    data_path = _validate_lake_path(root, object_dir / manifest.data_path, allow_missing=False)
    if data_path.stat().st_size != manifest.byte_length:
        raise ValidationError(f"Raw payload length changed: {data_path}")
    if _sha256_file(data_path) != manifest.content_sha256:
        raise ValidationError(f"Raw payload hash changed: {data_path}")
    actual_files = {path.name for path in object_dir.iterdir()}
    if actual_files != {"manifest.json", manifest.data_path}:
        raise ValidationError("Raw object contains unexpected or missing files")
    return manifest


def _relocate_invalid_raw(root: Path, object_dir: Path, *, reason: str) -> Path:
    object_dir = _validate_lake_path(root, object_dir, allow_missing=False)
    quarantine_root = _mkdir_in_lake(root, Path(root) / "quarantine" / "raw-unpublished")
    target = quarantine_root / f"{uuid.uuid4().hex}-{object_dir.name}"
    _validate_lake_path(root, target, allow_missing=True)
    _replace_tree_entry(root, object_dir, target)
    evidence = {
        "schema_version": "2.0.0",
        "layer": "quarantine",
        "reason": reason,
        "original_relative_path": object_dir.relative_to(root).as_posix(),
    }
    evidence["evidence_sha256"] = _sha256_bytes(_canonical_json_bytes(evidence))
    (target / "quarantine.json").write_text(
        json.dumps(evidence, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return target


def _remove_staging_directory(root: Path, stage: Path) -> None:
    checked = _validate_lake_path(root, stage, allow_missing=False)
    staging_root = _validate_lake_path(root, Path(root) / "raw" / ".staging", allow_missing=False)
    if checked.parent != staging_root:
        raise ValidationError("Refused to remove a non-staging Raw path")
    _remove_tree(root, checked)


def _recover_raw_staging(
    root: Path,
    reference: RawObjectReference,
    manifest: RawObjectManifest,
    *,
    policy: StoragePolicy = _DEFAULT_STORAGE_POLICY,
) -> RawObjectManifest | None:
    staging_root = _mkdir_in_lake(root, Path(root) / "raw" / ".staging")
    object_dir = _raw_object_dir(root, reference)
    recovered: RawObjectManifest | None = None
    for stage in sorted(staging_root.glob(f"{_raw_stage_prefix(reference)}*")):
        if not stage.is_dir():
            continue
        try:
            staged = _load_raw_from_dir(
                root,
                stage,
                expected=reference,
                enforce_directory_identity=False,
            )
            if staged != manifest:
                raise ValidationError("Raw staging content differs from its immutable key claim")
        except ValidationError as exc:
            _relocate_invalid_raw(root, stage, reason=f"stale_raw_staging: {exc}")
            continue
        if object_dir.exists():
            existing = _load_raw_from_dir(root, object_dir, expected=reference)
            if existing != manifest:
                raise ValidationError(f"Conflicting immutable Raw object: {object_dir}")
            _remove_staging_directory(root, stage)
            recovered = existing
            continue
        _publish_tree_entry(root, stage, object_dir, policy=policy)
        recovered = _load_raw_from_dir(root, object_dir, expected=reference)
    return recovered


def write_raw_bytes(
    root: Path,
    *,
    source: str,
    request: Mapping[str, Any],
    collected_at: datetime | str,
    payload: bytes,
    idempotency_key: str | None = None,
    policy: StoragePolicy = _DEFAULT_STORAGE_POLICY,
) -> RawObjectManifest:
    """Stage, verify and atomically publish one content-addressed Raw object."""
    lake_root = _resolved_lake_root(root, create=True)
    source = _segment(source, "source")
    if not isinstance(payload, bytes):
        raise ValidationError("Raw payload must be bytes")
    collected_text = _utc_text(collected_at, "collected_at")
    collection_date = collected_text[:10]
    request_dict = dict(request)
    try:
        request_hash = _sha256_bytes(_canonical_json_bytes(request_dict))
    except (TypeError, ValueError) as exc:
        raise ValidationError("Raw request metadata must be canonical JSON") from exc
    resolved_key = idempotency_key or (
        "key-"
        + _sha256_bytes(
            _canonical_json_bytes(
                {"source": source, "request_sha256": request_hash, "collected_at": collected_text}
            )
        )[:24]
    )
    resolved_key = _segment(resolved_key, "idempotency_key")
    content_hash = _sha256_bytes(payload)
    provisional = RawObjectManifest(
        schema_version="2.0.0",
        layer="raw",
        object_id="pending",
        idempotency_key=resolved_key,
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
        manifest_sha256="pending",
    )
    anchor = _sha256_bytes(_canonical_json_bytes(_raw_manifest_identity(provisional)))
    manifest = RawObjectManifest(
        **{
            **asdict(provisional),
            "object_id": f"sha256-{anchor}",
            "manifest_sha256": anchor,
        }
    )
    reference = manifest.reference()
    manifest_bytes = json.dumps(asdict(manifest), indent=2, ensure_ascii=False).encode("utf-8")
    with _lake_lock(lake_root, "raw-key", _raw_lock_identity(reference)):
        key_dir = _mkdir_in_lake(
            lake_root,
            _raw_key_dir(lake_root, source, collection_date, resolved_key),
        )
        _ensure_raw_key_claim(lake_root, reference)
        if list(key_dir.glob("object=*.cleanup.json")):
            raise ValidationError(
                f"Raw idempotency key was already archived and cleaned: {resolved_key}"
            )
        if list(key_dir.glob("deleting=*")):
            raise ValidationError(f"Raw idempotency key cleanup is in progress: {resolved_key}")
        object_dir = _raw_object_dir(lake_root, reference)
        recovered = _recover_raw_staging(lake_root, reference, manifest, policy=policy)
        if recovered is not None:
            return recovered
        for existing_dir in key_dir.glob("object=*"):
            if not existing_dir.is_dir():
                continue
            try:
                existing = _load_raw_from_dir(lake_root, existing_dir)
            except ValidationError as exc:
                _relocate_invalid_raw(lake_root, existing_dir, reason=str(exc))
                continue
            if existing.reference() == reference and existing == manifest:
                return existing
            raise ValidationError(f"Conflicting immutable Raw idempotency key: {resolved_key}")

        require_collection_capacity(
            lake_root,
            projected_write_bytes=len(payload) + len(manifest_bytes),
            policy=policy,
        )
        staging_root = _mkdir_in_lake(lake_root, lake_root / "raw" / ".staging")
        stage = staging_root / f"{_raw_stage_prefix(reference)}{uuid.uuid4().hex}"
        stage.mkdir(exist_ok=False)
        try:
            with (stage / manifest.data_path).open("wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            with (stage / "manifest.json").open("wb") as stream:
                stream.write(manifest_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            _load_raw_from_dir(
                lake_root,
                stage,
                expected=reference,
                enforce_directory_identity=False,
            )
            _validate_lake_path(lake_root, object_dir, allow_missing=True)
            _publish_tree_entry(lake_root, stage, object_dir, policy=policy)
            return _load_raw_from_dir(lake_root, object_dir, expected=reference)
        finally:
            if stage.exists():
                _remove_staging_directory(lake_root, stage)


def _local_archive_path(archive_uri: str) -> Path:
    if not archive_uri.strip():
        raise ValidationError("archive_uri must be non-empty")
    direct_path = Path(archive_uri)
    if direct_path.is_absolute():
        path = direct_path
    elif archive_uri.lower().startswith("file:"):
        parsed = urlparse(archive_uri)
        raw_path = url2pathname(unquote(parsed.path))
        if os.name == "nt" and re.match(r"^/[A-Za-z]:", raw_path):
            raw_path = raw_path[1:]
        path = Path(raw_path)
    else:
        parsed = urlparse(archive_uri)
        if parsed.scheme:
            raise ValidationError("Remote archive cleanup is unsupported without a verifier")
        path = Path(archive_uri)
    if not path.is_absolute() or not path.is_file() or _is_reparse_point(path):
        raise ValidationError("Local archive object is missing, unreadable, or a reparse point")
    return path.resolve(strict=True)


def _verify_archive_restore(root: Path, archive_path: Path) -> tuple[str, str]:
    try:
        archive_path.resolve(strict=True).relative_to(_resolved_lake_root(root, create=False))
    except ValueError:
        pass
    else:
        raise ValidationError("Local archive object must be outside the hot data lake")
    archive_hash = _sha256_file(archive_path)
    staging_root = _mkdir_in_lake(root, Path(root) / "raw" / ".staging")
    restore_path = staging_root / f"restore-{uuid.uuid4().hex}.bin"
    try:
        with archive_path.open("rb") as source, restore_path.open("wb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
            target.flush()
            os.fsync(target.fileno())
        return archive_hash, _sha256_file(restore_path)
    finally:
        if restore_path.exists():
            _unlink_tree_entry(root, restore_path)


def _read_cleanup_audit(root: Path, reference: RawObjectReference) -> dict[str, Any]:
    tombstone = _validate_lake_path(root, _raw_tombstone_path(root, reference), allow_missing=False)
    try:
        audit = json.loads(tombstone.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError("Raw cleanup audit is unreadable or malformed") from exc
    audit_hash = audit.pop("audit_sha256", None)
    if audit_hash != _sha256_bytes(_canonical_json_bytes(audit)):
        raise ValidationError("Raw cleanup audit integrity changed")
    return audit


def _load_archived_raw_reference(root: Path, reference: RawObjectReference) -> RawObjectManifest:
    audit = _read_cleanup_audit(root, reference)
    try:
        manifest = RawObjectManifest(**audit["raw_manifest"])
    except (KeyError, TypeError) as exc:
        raise ValidationError("Raw cleanup audit has malformed manifest evidence") from exc
    _validate_raw_manifest(manifest)
    if manifest.reference() != reference:
        raise ValidationError("Archived Raw reference does not match cleanup audit")
    archive_path = _local_archive_path(audit["archive_receipt"]["archive_uri"])
    try:
        archive_path.relative_to(_resolved_lake_root(root, create=False))
    except ValueError:
        pass
    else:
        raise ValidationError("Archived Raw reference points inside the hot data lake")
    if _sha256_file(archive_path) != reference.content_sha256:
        raise ValidationError("Archived Raw object content changed")
    return manifest


def _validate_cleanup_receipt(
    root: Path,
    manifest: RawObjectManifest,
    receipt: ArchiveReceipt,
    *,
    current_time: datetime,
) -> tuple[str, str]:
    if current_time < _utc_datetime(manifest.hot_until, "hot_until"):
        raise ValidationError(
            f"Raw object is still inside its {manifest.hot_retention_days}-day hot-retention window"
        )
    verified_at = _utc_datetime(receipt.verified_at, "verified_at")
    collected_at = _utc_datetime(manifest.collected_at, "collected_at")
    if verified_at < collected_at or verified_at > current_time:
        raise ValidationError("Archive verification time is outside the valid cleanup interval")
    if receipt.object_id != manifest.object_id or receipt.source_sha256 != manifest.content_sha256:
        raise ValidationError("Archive receipt does not identify the trusted Raw object")
    archive_path = _local_archive_path(receipt.archive_uri)
    archive_hash, restored_hash = _verify_archive_restore(root, archive_path)
    if {
        archive_hash,
        restored_hash,
        receipt.archive_sha256,
        receipt.restored_sha256,
    } != {manifest.content_sha256}:
        raise ValidationError("Real archive read/restore hash validation failed")
    return archive_hash, restored_hash


def _finalize_raw_deleting(
    root: Path,
    deleting_dir: Path,
    manifest: RawObjectManifest,
) -> None:
    deleting_dir = _validate_lake_path(root, deleting_dir, allow_missing=False)
    actual_names = {path.name for path in deleting_dir.iterdir()}
    expected_names = {manifest.data_path, "manifest.json"}
    if not actual_names <= expected_names:
        raise ValidationError("Raw deleting state contains unexpected files")
    payload_path = deleting_dir / manifest.data_path
    if payload_path.exists():
        payload_path = _validate_lake_path(root, payload_path, allow_missing=False)
        if payload_path.stat().st_size != manifest.byte_length:
            raise ValidationError("Raw deleting payload length changed")
        if _sha256_file(payload_path) != manifest.content_sha256:
            raise ValidationError("Raw deleting payload hash changed")
        _unlink_tree_entry(root, payload_path)
    manifest_path = deleting_dir / "manifest.json"
    if manifest_path.exists():
        manifest_path = _validate_lake_path(root, manifest_path, allow_missing=False)
        try:
            remaining_manifest = RawObjectManifest(
                **json.loads(manifest_path.read_text(encoding="utf-8"))
            )
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError) as exc:
            raise ValidationError("Raw deleting manifest is unreadable or malformed") from exc
        if remaining_manifest != manifest:
            raise ValidationError("Raw deleting manifest changed")
        _unlink_tree_entry(root, manifest_path)
    _remove_empty_tree(root, deleting_dir)


def validate_raw_reference(
    root: Path,
    reference: RawObjectReference,
    *,
    allow_archived: bool,
) -> RawObjectManifest:
    lake_root = _resolved_lake_root(root, create=False)
    _load_raw_key_claim(lake_root, reference)
    object_dir = _raw_object_dir(lake_root, reference)
    if object_dir.exists():
        return _load_raw_from_dir(lake_root, object_dir, expected=reference)
    if allow_archived and _raw_tombstone_path(lake_root, reference).exists():
        return _load_archived_raw_reference(lake_root, reference)
    deleting_dir = _raw_deleting_dir(lake_root, reference)
    if allow_archived and deleting_dir.exists():
        return _load_raw_from_dir(
            lake_root,
            deleting_dir,
            expected=reference,
            enforce_directory_identity=False,
        )
    raise ValidationError(f"Trusted Raw object is unavailable: {reference.object_id}")


def load_raw_object(
    root: Path,
    reference: RawObjectReference,
) -> tuple[RawObjectManifest, bytes]:
    lake_root = _resolved_lake_root(root, create=False)
    manifest = validate_raw_reference(lake_root, reference, allow_archived=False)
    object_dir = _raw_object_dir(lake_root, reference)
    data_path = _validate_lake_path(lake_root, object_dir / manifest.data_path, allow_missing=False)
    return manifest, data_path.read_bytes()


def cleanup_archived_raw_object(
    root: Path,
    reference: RawObjectReference,
    receipt: ArchiveReceipt,
    *,
    confirm: bool = False,
    now: datetime | str | None = None,
) -> Path:
    """Resume or complete one locked, auditable Raw cleanup transaction."""
    if not confirm:
        raise ValidationError("Raw cleanup requires explicit confirm=True")
    lake_root = _resolved_lake_root(root, create=False)
    current_time = _utc_datetime(now or datetime.now(timezone.utc), "now")
    with _lake_lock(lake_root, "raw-key", _raw_lock_identity(reference)):
        _load_raw_key_claim(lake_root, reference)
        object_dir = _raw_object_dir(lake_root, reference)
        deleting_dir = _raw_deleting_dir(lake_root, reference)
        tombstone = _raw_tombstone_path(lake_root, reference)

        if tombstone.exists():
            audit = _read_cleanup_audit(lake_root, reference)
            try:
                manifest = RawObjectManifest(**audit["raw_manifest"])
            except (KeyError, TypeError) as exc:
                raise ValidationError("Raw cleanup audit has malformed manifest evidence") from exc
            _validate_raw_manifest(manifest)
            if manifest.reference() != reference:
                raise ValidationError("Archived Raw reference does not match cleanup audit")
            if audit.get("archive_receipt") != asdict(receipt):
                raise ValidationError("Cleanup retry receipt differs from immutable audit")
            _validate_cleanup_receipt(
                lake_root,
                manifest,
                receipt,
                current_time=current_time,
            )
            if object_dir.exists():
                if deleting_dir.exists():
                    raise ValidationError("Raw cleanup has both live and deleting states")
                _replace_tree_entry(lake_root, object_dir, deleting_dir)
            if deleting_dir.exists():
                _finalize_raw_deleting(lake_root, deleting_dir, manifest)
            return tombstone

        if deleting_dir.exists():
            manifest = _load_raw_from_dir(
                lake_root,
                deleting_dir,
                expected=reference,
                enforce_directory_identity=False,
            )
            should_rename = False
        elif object_dir.exists():
            manifest = _load_raw_from_dir(lake_root, object_dir, expected=reference)
            should_rename = True
        else:
            raise ValidationError(f"Trusted Raw object is unavailable: {reference.object_id}")

        archive_hash, restored_hash = _validate_cleanup_receipt(
            lake_root,
            manifest,
            receipt,
            current_time=current_time,
        )
        if should_rename:
            _replace_tree_entry(lake_root, object_dir, deleting_dir)
        audit = {
            "schema_version": "2.0.0",
            "action": "verified_local_archive_cleanup",
            "raw_reference": asdict(reference),
            "raw_manifest": asdict(manifest),
            "archive_receipt": asdict(receipt),
            "archive_actual_sha256": archive_hash,
            "restore_actual_sha256": restored_hash,
            "cleaned_at": _utc_text(current_time, "cleaned_at"),
        }
        audit["audit_sha256"] = _sha256_bytes(_canonical_json_bytes(audit))
        _atomic_write_bytes(
            lake_root,
            tombstone,
            json.dumps(audit, indent=2, ensure_ascii=False).encode("utf-8"),
        )
        _finalize_raw_deleting(lake_root, deleting_dir, manifest)
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
    table = pa.Table.from_pylist(
        [_arrow_ready(record, schema) for record in records], schema=schema
    )
    validate_arrow_table(schema_id, table)
    return table


def _normalized_snapshot_payload(
    *,
    provider: str,
    venue: str,
    created_at: str,
    upstream_raw_references: tuple[RawObjectReference, ...],
    event_claims: tuple[EventClaimReference, ...],
    partitions: tuple[PartitionManifest, ...],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION_V2,
        "layer": "normalized",
        "provider": provider,
        "venue": venue,
        "created_at": created_at,
        "upstream_raw_references": [asdict(item) for item in upstream_raw_references],
        "event_claims": [asdict(item) for item in event_claims],
        "partitions": [asdict(item) for item in partitions],
    }


def _validate_quarantine_batch(
    root: Path,
    batch_dir: Path,
    manifest: Mapping[str, Any],
) -> Path:
    batch_dir = _validate_lake_path(root, batch_dir, allow_missing=False)
    records_path = _validate_lake_path(root, batch_dir / "records.jsonl", allow_missing=False)
    manifest_path = _validate_lake_path(root, batch_dir / "manifest.json", allow_missing=False)
    if not records_path.is_file() or _sha256_file(records_path) != manifest["content_sha256"]:
        raise ValidationError(f"Quarantine batch changed: {batch_dir}")
    try:
        stored_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError(f"Quarantine manifest is unreadable: {batch_dir}") from exc
    if stored_manifest != manifest:
        raise ValidationError(f"Quarantine manifest changed: {batch_dir}")
    actual_files = {path.name for path in batch_dir.iterdir()}
    if actual_files != {"records.jsonl", "manifest.json"}:
        raise ValidationError(f"Quarantine batch has unexpected files: {batch_dir}")
    return manifest_path


def _event_claim_reference(schema_id: str, canonical_row: Mapping[str, Any]) -> EventClaimReference:
    event_id = str(canonical_row["event_id"])
    event_id_hash = _sha256_bytes(event_id.encode("utf-8"))
    event_sha256 = _sha256_bytes(
        _canonical_json_bytes({"schema_id": schema_id, "record": dict(canonical_row)})
    )
    identity = {
        "schema_version": "2.0.0",
        "layer": "normalized-event-claim",
        "event_id_hash": event_id_hash,
        "event_id": event_id,
        "schema_id": schema_id,
        "event_sha256": event_sha256,
    }
    return EventClaimReference(
        event_id_hash=event_id_hash,
        event_id=event_id,
        schema_id=schema_id,
        event_sha256=event_sha256,
        claim_sha256=_sha256_bytes(_canonical_json_bytes(identity)),
    )


def _event_claim_path(root: Path, claim: EventClaimReference) -> Path:
    return (
        Path(root)
        / "normalized"
        / "event-claims"
        / f"shard={claim.event_id_hash[:2]}"
        / f"{claim.event_id_hash}.json"
    )


def _event_claim_payload(claim: EventClaimReference) -> dict[str, Any]:
    return {
        "schema_version": "2.0.0",
        "layer": "normalized-event-claim",
        **asdict(claim),
    }


def _validate_event_claim_reference(
    actual: EventClaimReference,
    expected: EventClaimReference,
) -> EventClaimReference:
    identity = {
        "schema_version": "2.0.0",
        "layer": "normalized-event-claim",
        "event_id_hash": actual.event_id_hash,
        "event_id": actual.event_id,
        "schema_id": actual.schema_id,
        "event_sha256": actual.event_sha256,
    }
    if actual.event_id_hash != _sha256_bytes(
        actual.event_id.encode("utf-8")
    ) or actual.claim_sha256 != _sha256_bytes(_canonical_json_bytes(identity)):
        raise ValidationError(f"Normalized event claim integrity changed: {expected.event_id}")
    if actual != expected:
        raise ValidationError(f"Conflicting lake event_id claim: {expected.event_id}")
    return actual


def _validate_event_claim(
    root: Path,
    expected: EventClaimReference,
) -> EventClaimReference:
    path = _validate_lake_path(root, _event_claim_path(root, expected), allow_missing=False)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError(f"Normalized event claim is unreadable: {expected.event_id}") from exc
    if payload.pop("schema_version", None) != "2.0.0" or payload.pop("layer", None) != (
        "normalized-event-claim"
    ):
        raise ValidationError(f"Normalized event claim schema changed: {expected.event_id}")
    try:
        actual = EventClaimReference(**payload)
    except TypeError as exc:
        raise ValidationError(f"Normalized event claim is malformed: {expected.event_id}") from exc
    return _validate_event_claim_reference(actual, expected)


def _recover_missing_event_claim(
    root: Path,
    expected: EventClaimReference,
) -> bool:
    """Recover a missing acceleration index from validated immutable snapshots."""
    snapshots_root = Path(root) / "normalized" / "snapshots"
    if not snapshots_root.exists():
        return False
    matching_snapshot_ids: list[str] = []
    for snapshot_dir in sorted(snapshots_root.glob("sha256-*")):
        if not snapshot_dir.is_dir():
            raise ValidationError(f"Normalized snapshot entry is not a directory: {snapshot_dir}")
        snapshot = _load_normalized_snapshot(
            root,
            snapshot_dir.name,
            verify_event_claim_files=False,
        )
        for actual in snapshot.event_claims:
            if actual.event_id_hash != expected.event_id_hash:
                continue
            _validate_event_claim_reference(actual, expected)
            matching_snapshot_ids.append(snapshot.snapshot_id)
    if not matching_snapshot_ids:
        return False
    _atomic_write_bytes(
        root,
        _event_claim_path(root, expected),
        json.dumps(_event_claim_payload(expected), indent=2, ensure_ascii=False).encode("utf-8"),
    )
    _validate_event_claim(root, expected)
    return True


def _publish_event_claims(
    root: Path,
    claims: tuple[EventClaimReference, ...],
) -> None:
    shards = sorted({claim.event_id_hash[:2] for claim in claims})
    with ExitStack() as stack:
        for shard in shards:
            stack.enter_context(_lake_lock(root, "event-claim-shard", {"shard": shard}))
        missing: list[EventClaimReference] = []
        for claim in claims:
            path = _event_claim_path(root, claim)
            if path.exists():
                _validate_event_claim(root, claim)
            elif not _recover_missing_event_claim(root, claim):
                missing.append(claim)
        for claim in missing:
            _atomic_write_bytes(
                root,
                _event_claim_path(root, claim),
                json.dumps(_event_claim_payload(claim), indent=2, ensure_ascii=False).encode(
                    "utf-8"
                ),
            )
        for claim in claims:
            _validate_event_claim(root, claim)


def _write_quarantine(
    root: Path,
    provider: str,
    venue: str,
    entries: list[QuarantineEntry],
    *,
    policy: StoragePolicy,
) -> Path | None:
    if not entries:
        return None
    body = (
        b"\n".join(_canonical_json_bytes(_json_evidence(asdict(entry))) for entry in entries)
        + b"\n"
    )
    batch_id = f"sha256-{_sha256_bytes(body)}"
    lake_root = _resolved_lake_root(root, create=True)
    batch_dir = lake_root / "quarantine" / batch_id
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
    manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    with _lake_lock(lake_root, "quarantine-batch", {"batch_id": batch_id}):
        if batch_dir.exists():
            return _validate_quarantine_batch(lake_root, batch_dir, manifest)
        staging_root = _mkdir_in_lake(lake_root, lake_root / "quarantine" / ".staging")
        for stale in staging_root.glob(f"{batch_id}-*"):
            if stale.is_dir():
                checked = _validate_lake_path(lake_root, stale, allow_missing=False)
                _remove_tree(lake_root, checked)
        require_collection_capacity(
            lake_root,
            projected_write_bytes=len(body) + len(manifest_bytes),
            policy=policy,
        )
        _mkdir_in_lake(lake_root, batch_dir.parent)
        _validate_lake_path(lake_root, batch_dir, allow_missing=True)
        stage = staging_root / f"{batch_id}-{uuid.uuid4().hex}"
        stage.mkdir(exist_ok=False)
        try:
            with (stage / "records.jsonl").open("wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            with (stage / "manifest.json").open("wb") as stream:
                stream.write(manifest_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            _validate_quarantine_batch(lake_root, stage, manifest)
            _publish_tree_entry(lake_root, stage, batch_dir, policy=policy)
            return _validate_quarantine_batch(lake_root, batch_dir, manifest)
        finally:
            if stage.exists():
                _remove_tree(lake_root, stage)


def _write_normalized_events_legacy(
    root: Path,
    records: Iterable[Mapping[str, Any]],
    *,
    provider: str,
    venue: str,
    upstream_raw_references: Iterable[RawObjectReference],
    expected_l2_checkpoint_hashes: Mapping[tuple[str, str, str], Mapping[int, str]] | None = None,
    policy: StoragePolicy = _DEFAULT_STORAGE_POLICY,
) -> NormalizationResult:
    """Validate full streams, quarantine failures, then immutably partition strict v2 Parquet."""
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

    indexed = [(index, dict(record)) for index, record in enumerate(records)]
    event_id_indices: dict[str, list[int]] = defaultdict(list)
    for index, record in indexed:
        event_id = record.get("event_id")
        if isinstance(event_id, str):
            event_id_indices[event_id].append(index)
    duplicate_indices = {
        index for indices in event_id_indices.values() if len(indices) > 1 for index in indices
    }
    streams: dict[tuple[str, str, str], list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    for index, record in indexed:
        if index in duplicate_indices:
            continue
        streams[_stream_key(record, index)].append((index, record))
    accepted: list[tuple[int, dict[str, Any]]] = []
    quarantined = [
        QuarantineEntry(
            input_index=index,
            reason="global_duplicate_event_id",
            record=record,
        )
        for index, record in indexed
        if index in duplicate_indices
    ]
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
    quarantine_manifest = _write_quarantine(
        lake_root,
        provider,
        venue,
        quarantined,
        policy=policy,
    )
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

    estimated_bytes = sum(len(_canonical_json_bytes(_json_evidence(row))) for _, row in accepted)
    require_collection_capacity(
        lake_root,
        projected_write_bytes=estimated_bytes,
        policy=policy,
    )
    normalized_root = _mkdir_in_lake(lake_root, lake_root / "normalized")
    staging_root = normalized_root / "staging"
    partition_manifests: list[PartitionManifest] = []
    event_claim_items: list[EventClaimReference] = []
    batch_identity = {
        "provider": provider,
        "venue": venue,
        "raw_references": [asdict(item) for item in raw_references],
        "records": [_json_evidence(record) for _, record in accepted],
        "expected_l2_checkpoint_hashes": [
            {
                "stream": list(stream),
                "checkpoints": {
                    str(sequence): checkpoint_hash
                    for sequence, checkpoint_hash in sorted(checkpoints.items())
                },
            }
            for stream, checkpoints in sorted(expected_l2.items())
        ],
    }
    with _stable_staging_directory(
        lake_root,
        staging_root,
        namespace="normalized-batch",
        identity=batch_identity,
    ) as stage:
        for event_type, trading_date, instrument_id in sorted(partitioned):
            rows = sorted(
                partitioned[(event_type, trading_date, instrument_id)],
                key=lambda row: (
                    str(row["event_time"]),
                    int(row["sequence"]),
                    str(row["event_id"]),
                ),
            )
            schema_id = _event_schema_id(rows[0])
            for row in rows:
                if _event_schema_id(row) != schema_id:
                    raise ValidationError("Mixed schemas entered one normalized partition")
                validate_json_record(schema_id, row)
            table = _validated_table(schema_id, rows)
            relative = Path(
                f"provider={provider}/venue={venue}/event_type={event_type}/"
                f"date={trading_date}/"
                f"instrument={_partition_segment(instrument_id, 'instrument_id')}/data.parquet"
            )
            target = stage / relative
            _mkdir_in_lake(lake_root, target.parent)
            pq.write_table(table, target, compression="zstd", use_dictionary=False)
            logical_rows = [_json_evidence(item) for item in table.to_pylist()]
            event_claim_items.extend(
                _event_claim_reference(schema_id, logical_row) for logical_row in logical_rows
            )
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
                    logical_sha256=_sha256_bytes(_canonical_json_bytes(logical_rows)),
                    content_sha256=_sha256_file(target),
                )
            )
        partitions = tuple(partition_manifests)
        event_claims = tuple(
            sorted(event_claim_items, key=lambda item: (item.event_id_hash, item.event_id))
        )
        if len({item.event_id_hash for item in event_claims}) != len(event_claims):
            raise ValidationError("Normalized batch contains colliding event_id hashes")
        created_at = _utc_text(
            max(_utc_datetime(row["available_at"], "available_at") for _, row in accepted),
            "available_at",
        )
        identity = _normalized_snapshot_payload(
            provider=provider,
            venue=venue,
            created_at=created_at,
            upstream_raw_references=raw_references,
            event_claims=event_claims,
            partitions=partitions,
        )
        logical_sha256 = _sha256_bytes(_canonical_json_bytes(identity))
        snapshot_id = f"sha256-{logical_sha256}"
        snapshot = NormalizedSnapshot(
            schema_version=SCHEMA_VERSION_V2,
            layer="normalized",
            snapshot_id=snapshot_id,
            provider=provider,
            venue=venue,
            created_at=created_at,
            logical_sha256=logical_sha256,
            rows=sum(item.rows for item in partitions),
            upstream_raw_references=raw_references,
            event_claims=event_claims,
            partitions=partitions,
        )
        (stage / "manifest.json").write_text(
            json.dumps(asdict(snapshot), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        snapshot_dir = normalized_root / "snapshots" / snapshot_id
        _mkdir_in_lake(lake_root, snapshot_dir.parent)
        _validate_lake_path(lake_root, snapshot_dir, allow_missing=True)
        _publish_event_claims(lake_root, event_claims)
        with _lake_lock(lake_root, "normalized-snapshot", {"snapshot_id": snapshot_id}):
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
            _publish_tree_entry(lake_root, stage, snapshot_dir, policy=policy)
            stage = snapshot_dir
            verified = load_normalized_snapshot(root, snapshot_id)
            return NormalizationResult(
                snapshot=verified,
                accepted_rows=verified.rows,
                quarantined_rows=len(quarantined),
                quarantine_manifest=quarantine_manifest,
            )


def write_normalized_events(
    root: Path,
    records: Iterable[Mapping[str, Any]],
    *,
    provider: str,
    venue: str,
    upstream_raw_references: Iterable[RawObjectReference],
    expected_l2_checkpoint_hashes: Mapping[tuple[str, str, str], Mapping[int, str]] | None = None,
    policy: StoragePolicy = _DEFAULT_STORAGE_POLICY,
) -> NormalizationResult:
    """Stream events into the normalized-v3 layout without changing frozen event schemas."""
    from quant_data_kit.normalized_v3 import write_normalized_events_v3

    return write_normalized_events_v3(
        root,
        records,
        provider=provider,
        venue=venue,
        upstream_raw_references=upstream_raw_references,
        expected_l2_checkpoint_hashes=expected_l2_checkpoint_hashes,
        policy=policy,
    )


def write_normalized_batches(
    root: Path,
    batches: Iterable[pa.RecordBatch] | pa.RecordBatchReader,
    *,
    provider: str,
    venue: str,
    upstream_raw_references: Iterable[RawObjectReference],
    expected_l2_checkpoint_hashes: Mapping[tuple[str, str, str], Mapping[int, str]] | None = None,
    policy: StoragePolicy = _DEFAULT_STORAGE_POLICY,
) -> NormalizationResult:
    """Write homogeneous, already-standardized Arrow event batches fail closed."""
    from quant_data_kit.normalized_v3 import write_normalized_batches_v3

    return write_normalized_batches_v3(
        root,
        batches,
        provider=provider,
        venue=venue,
        upstream_raw_references=upstream_raw_references,
        expected_l2_checkpoint_hashes=expected_l2_checkpoint_hashes,
        policy=policy,
    )


def _safe_snapshot_partition(root: Path, snapshot_dir: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValidationError(f"Unsafe partition path: {relative_path}")
    candidate = _validate_lake_path(root, snapshot_dir / relative, allow_missing=False)
    if not candidate.is_file():
        raise ValidationError(f"Snapshot partition missing: {candidate}")
    if snapshot_dir.resolve(strict=True) not in candidate.resolve(strict=True).parents:
        raise ValidationError(f"Snapshot partition escapes root: {candidate}")
    return candidate


def _load_normalized_snapshot(
    root: Path,
    snapshot_id: str,
    *,
    verify_event_claim_files: bool,
    recovery_policy: StoragePolicy | None = None,
) -> NormalizedSnapshot:
    lake_root = _resolved_lake_root(root, create=False)
    snapshot_id = _segment(snapshot_id, "snapshot_id")
    snapshot_dir = _validate_lake_path(
        lake_root,
        lake_root / "normalized" / "snapshots" / snapshot_id,
        allow_missing=False,
    )
    manifest_path = _validate_lake_path(
        lake_root,
        snapshot_dir / "manifest.json",
        allow_missing=False,
    )
    if not manifest_path.is_file():
        raise ValidationError(f"Normalized snapshot manifest missing: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("layout_version") == "3.0.0":
        from quant_data_kit.normalized_v3 import load_normalized_snapshot_v3

        return load_normalized_snapshot_v3(
            lake_root,
            snapshot_id,
            payload=payload,
            recovery_policy=recovery_policy,
        )
    payload["upstream_raw_references"] = tuple(
        RawObjectReference(**item) for item in payload["upstream_raw_references"]
    )
    payload["event_claims"] = tuple(EventClaimReference(**item) for item in payload["event_claims"])
    payload["partitions"] = tuple(PartitionManifest(**item) for item in payload["partitions"])
    snapshot = NormalizedSnapshot(**payload)
    if (
        snapshot.snapshot_id != snapshot_id
        or snapshot.layer != "normalized"
        or snapshot.schema_version != SCHEMA_VERSION_V2
    ):
        raise ValidationError("Normalized snapshot identity mismatch")
    _utc_datetime(snapshot.created_at, "created_at")
    for reference in snapshot.upstream_raw_references:
        if reference.source != snapshot.provider:
            raise ValidationError("Normalized provider does not match its Raw source")
        validate_raw_reference(lake_root, reference, allow_archived=True)
    if verify_event_claim_files:
        for claim in snapshot.event_claims:
            _validate_event_claim(lake_root, claim)
    identity = _normalized_snapshot_payload(
        provider=snapshot.provider,
        venue=snapshot.venue,
        created_at=snapshot.created_at,
        upstream_raw_references=snapshot.upstream_raw_references,
        event_claims=snapshot.event_claims,
        partitions=snapshot.partitions,
    )
    logical_sha256 = _sha256_bytes(_canonical_json_bytes(identity))
    if logical_sha256 != snapshot.logical_sha256 or snapshot_id != f"sha256-{logical_sha256}":
        raise ValidationError("Normalized snapshot logical hash changed")
    rows = 0
    expected_files = {Path("manifest.json")}
    seen_paths: set[str] = set()
    actual_event_claims: list[EventClaimReference] = []
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
        path = _safe_snapshot_partition(lake_root, snapshot_dir, partition.relative_path)
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
        actual_event_claims.extend(
            _event_claim_reference(partition.schema_id, logical_row) for logical_row in logical_rows
        )
        rows += table.num_rows
    if rows != snapshot.rows:
        raise ValidationError("Normalized snapshot total row count changed")
    if (
        tuple(sorted(actual_event_claims, key=lambda item: (item.event_id_hash, item.event_id)))
        != snapshot.event_claims
    ):
        raise ValidationError("Normalized snapshot event claims changed")
    actual_files = {
        path.relative_to(snapshot_dir) for path in snapshot_dir.rglob("*") if path.is_file()
    }
    if actual_files != expected_files:
        raise ValidationError("Normalized snapshot contains an unexpected or missing file")
    return snapshot


def load_normalized_snapshot(
    root: Path,
    snapshot_id: str,
    *,
    recovery_policy: StoragePolicy | None = None,
) -> NormalizedSnapshot:
    return _load_normalized_snapshot(
        root,
        snapshot_id,
        verify_event_claim_files=True,
        recovery_policy=recovery_policy,
    )


def read_normalized_events(
    root: Path,
    snapshot_id: str,
    *,
    event_type: str | None = None,
) -> tuple[dict[str, Any], ...]:
    """Read canonical rows only after the fixed snapshot and all Raw lineage verify."""
    lake_root = _resolved_lake_root(root, create=False)
    snapshot = load_normalized_snapshot(lake_root, snapshot_id)
    selected_type = _segment(event_type, "event_type") if event_type is not None else None
    rows: list[dict[str, Any]] = []
    snapshot_dir = lake_root / "normalized" / "snapshots" / snapshot.snapshot_id
    for partition in snapshot.partitions:
        if selected_type is not None and partition.event_type != selected_type:
            continue
        path = _safe_snapshot_partition(lake_root, snapshot_dir, partition.relative_path)
        rows.extend(_json_evidence(item) for item in pq.ParquetFile(path).read().to_pylist())
    return tuple(rows)


class DuckDBSnapshot:
    """In-memory read-only query facade over one already verified snapshot."""

    def __init__(self, root: Path, snapshot: NormalizedSnapshot) -> None:
        lake_root = _resolved_lake_root(root, create=False)
        self.snapshot = load_normalized_snapshot(lake_root, snapshot.snapshot_id)
        if self.snapshot != snapshot:
            raise ValidationError("DuckDB snapshot object differs from the verified manifest")
        self._connection = duckdb.connect(database=":memory:")
        snapshot_dir = lake_root / "normalized" / "snapshots" / snapshot.snapshot_id
        tables_by_event: dict[str, list[pa.Table]] = defaultdict(list)
        for partition in self.snapshot.partitions:
            path = _safe_snapshot_partition(lake_root, snapshot_dir, partition.relative_path)
            tables_by_event[partition.event_type].append(pq.ParquetFile(path).read())
        for event_type, tables in sorted(tables_by_event.items()):
            table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
            registration = f"verified_arrow_{event_type}"
            table_name = f"event_{event_type}"
            self._connection.register(registration, table)
            try:
                self._connection.execute(
                    f'CREATE TABLE "{table_name}" AS SELECT * FROM "{registration}"'
                )
            finally:
                self._connection.unregister(registration)
        self._connection.execute("SET enable_external_access = false")

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
        if re.search(
            r"\b(read_(?:text|csv|csv_auto|json|json_auto|parquet)|"
            r"parquet_scan|csv_scan|glob|pragma_[a-z0-9_]+)\s*\(",
            without_trailing_semicolon,
        ):
            raise ValidationError("DuckDB snapshot query cannot access external files")
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
