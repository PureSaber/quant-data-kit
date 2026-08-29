"""Durable Normalized epoch journal backed by immutable Raw segment references."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from quant_data_kit.capture_v2.models import canonical_json_bytes, utc_text
from quant_data_kit.capture_v2.storage import (
    CaptureStorageGuard,
    RawSegment,
    _atomic_immutable_write,
)
from quant_data_kit.data_lake import (
    NormalizationResult,
    RawObjectReference,
    StoragePolicy,
    write_normalized_events,
)
from quant_data_kit.exceptions import ValidationError

UTC = timezone.utc
_DEFAULT_POLICY = StoragePolicy()


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
    receipt_sha256: str
    receipt_path: str


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        self.root = (
            self.hot_root
            / "capture"
            / "normalized-epoch-journal"
            / f"stream={stream_id}"
            / f"epoch={epoch_id}"
        )
        self.root.mkdir(parents=True, exist_ok=False)
        self._open_path = self.root / "part-open.ndjson"
        self._open_stream = self._open_path.open("xb")
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

    def append(self, records: Iterable[Mapping[str, Any]]) -> None:
        self._require_open()
        for record in records:
            body = canonical_json_bytes(dict(record)) + b"\n"
            self.storage_guard.require_hot_capacity(projected_write_bytes=len(body))
            self._open_stream.write(body)
            self._open_rows += 1
            self._records += 1
            self._unflushed_rows += 1
            self._unflushed_bytes += len(body)
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
    ) -> NormalizedEpochJournal:
        """Reload a retryable failed finalize while preserving journal and Raw lineage."""

        root = (
            Path(hot_root)
            / "capture"
            / "normalized-epoch-journal"
            / f"stream={stream_id}"
            / f"epoch={epoch_id}"
        )
        failures = sorted(root.glob("finalize-failure-*.json"))
        if not failures or tuple(root.glob("receipt-sha256-*.json")):
            raise ValidationError("Normalized epoch is not a retryable failed finalize")
        failure = json.loads(failures[-1].read_text(encoding="utf-8"))
        failure_digest = failure.pop("failure_sha256", None)
        if failure_digest != hashlib.sha256(canonical_json_bytes(failure)).hexdigest():
            raise ValidationError("Normalized epoch failure record hash changed")
        if failure.get("stream_id") != stream_id or not failure.get("retryable_in_process"):
            raise ValidationError("Normalized epoch failure record is not recoverable")
        self = cls.__new__(cls)
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
        self.root = root
        self._parts = []
        self._records = 0
        for path in sorted(root.glob("part-*-sha256-*.ndjson")):
            digest = _sha256_file(path)
            if f"sha256-{digest}.ndjson" not in path.name:
                raise ValidationError(f"Normalized recovery part hash changed: {path}")
            with path.open("rb") as stream:
                rows = sum(1 for line in stream if line.strip())
            self._parts.append(EpochPart(path.name, rows, digest, path.stat().st_size))
            self._records += rows
        self._raw_references = [
            RawObjectReference(**item) for item in failure.get("raw_references", [])
        ]
        self._raw_manifest_hashes = {item.manifest_sha256 for item in self._raw_references}
        if int(failure.get("records", -1)) != self._records:
            raise ValidationError("Normalized recovery record count changed")
        self._open_path = root / "part-open.ndjson"
        if self._open_path.exists() and self._open_path.stat().st_size:
            raise ValidationError("Normalized recovery found an unsealed open part")
        self._open_stream = self._open_path.open("ab")
        self._open_rows = 0
        self._unflushed_rows = 0
        self._unflushed_bytes = 0
        self._last_flush = monotonic()
        self._closed = False
        self._state = "OPEN"
        self._finalize_failures = len(failures)
        return self

    def finalize(self, *, created_at: datetime | None = None) -> NormalizedEpochReceipt:
        self._require_open()
        try:
            self._seal_part()
            if self._records and not self._raw_references:
                raise ValidationError("Normalized epoch records require Raw segment lineage")
            result = (
                self._publish_normalized()
                if self._records
                else NormalizationResult(None, 0, 0, None)
            )
            created = utc_text(created_at or datetime.now(tz=UTC), "epoch created_at")
            identity = {
                "schema_version": "puresaber.normalized-epoch-receipt@1.1.0",
                "epoch_id": self.epoch_id,
                "stream_id": self.stream_id,
                "provider": self.provider,
                "venue": self.venue,
                "created_at": created,
                "records": self._records,
                "raw_segments": len(self._raw_references),
                "raw_references": [asdict(item) for item in self._raw_references],
                "journal_parts": [asdict(item) for item in self._parts],
                "normalized_snapshot_id": result.snapshot.snapshot_id if result.snapshot else None,
                "accepted_rows": result.accepted_rows,
                "quarantined_rows": result.quarantined_rows,
            }
            receipt_hash = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
            payload = {**identity, "receipt_sha256": receipt_hash}
            receipt_path = self.root / f"receipt-sha256-{receipt_hash}.json"
            _atomic_immutable_write(receipt_path, canonical_json_bytes(payload), root=self.hot_root)
            reloaded = json.loads(receipt_path.read_text(encoding="utf-8"))
            if reloaded != payload:
                raise ValidationError("Normalized epoch receipt reload mismatch")
        except Exception as exc:
            try:
                self._record_finalize_failure(exc)
            except Exception as abort_exc:  # noqa: BLE001 - both failures must be preserved
                raise ValidationError(
                    "Normalized finalize failed and its durable failure record also failed; "
                    f"primary={type(exc).__name__}: {exc}; "
                    f"audit={type(abort_exc).__name__}: {abort_exc}"
                ) from exc
            raise
        self._open_stream.close()
        self._state = "FINALIZED"
        return NormalizedEpochReceipt(
            schema_version=identity["schema_version"],
            epoch_id=self.epoch_id,
            stream_id=self.stream_id,
            provider=self.provider,
            venue=self.venue,
            created_at=created,
            records=self._records,
            raw_segments=len(self._raw_references),
            raw_references=tuple(self._raw_references),
            journal_parts=tuple(self._parts),
            normalized_snapshot_id=identity["normalized_snapshot_id"],
            accepted_rows=result.accepted_rows,
            quarantined_rows=result.quarantined_rows,
            receipt_sha256=receipt_hash,
            receipt_path=str(receipt_path),
        )

    def abort_visible(self, reason: str) -> Path:
        if self._state == "FINALIZED":
            raise ValidationError("cannot abort a finalized Normalized epoch")
        if self._state == "ABORTED":
            raise ValidationError("Normalized epoch is already aborted")
        if self._state == "OPEN":
            self.flush()
            self._open_stream.close()
        payload = {
            "schema_version": "puresaber.normalized-epoch-abort@1.1.0",
            "epoch_id": self.epoch_id,
            "stream_id": self.stream_id,
            "reason": reason,
            "records": self._records,
            "raw_references": [asdict(item) for item in self._raw_references],
            "retryable": False,
        }
        payload["abort_sha256"] = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        path = self.root / f"aborted-sha256-{payload['abort_sha256']}.json"
        _atomic_immutable_write(path, canonical_json_bytes(payload), root=self.hot_root)
        self._state = "ABORTED"
        return path

    def _seal_part(self) -> None:
        if self._open_rows == 0:
            return
        self.flush()
        self._open_stream.close()
        digest = _sha256_file(self._open_path)
        index = len(self._parts) + 1
        final_name = f"part-{index:08d}-sha256-{digest}.ndjson"
        final_path = self.root / final_name
        os.replace(self._open_path, final_path)
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
        self._open_stream = self._open_path.open("xb")

    def _record_finalize_failure(self, exc: Exception) -> Path:
        self._finalize_failures += 1
        identity = {
            "schema_version": "puresaber.normalized-epoch-finalize-failure@1.0.0",
            "epoch_id": self.epoch_id,
            "stream_id": self.stream_id,
            "attempt": self._finalize_failures,
            "exception": type(exc).__name__,
            "message": str(exc),
            "records": self._records,
            "raw_references": [asdict(item) for item in self._raw_references],
            "retryable_in_process": True,
            "restart_recovery": "journal parts and Raw lineage retained",
        }
        digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        path = self.root / (f"finalize-failure-{self._finalize_failures:04d}-sha256-{digest}.json")
        _atomic_immutable_write(
            path,
            canonical_json_bytes({**identity, "failure_sha256": digest}),
            root=self.hot_root,
        )
        return path

    def _publish_normalized(self) -> NormalizationResult:
        return write_normalized_events(
            self.hot_root,
            self._iter_records(),
            provider=self.provider,
            venue=self.venue,
            upstream_raw_references=self._raw_references,
            policy=self.policy,
        )

    def _iter_records(self) -> Iterable[dict[str, Any]]:
        for part in self._parts:
            path = self.root / part.relative_path
            if path.stat().st_size != part.byte_length or _sha256_file(path) != part.content_sha256:
                raise ValidationError(f"Normalized epoch journal part integrity changed: {path}")
            with path.open("rb") as stream:
                for line_number, line in enumerate(stream, start=1):
                    try:
                        value = json.loads(line)
                    except (UnicodeError, json.JSONDecodeError) as exc:
                        raise ValidationError(
                            f"Normalized epoch journal line is malformed: {path}:{line_number}"
                        ) from exc
                    if not isinstance(value, dict):
                        raise ValidationError("Normalized epoch journal record must be an object")
                    yield value

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
