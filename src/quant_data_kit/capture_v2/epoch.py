"""Durable Normalized epoch journal backed by immutable Raw segment references."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from quant_data_kit.capture_v2.models import canonical_json_bytes, utc_text
from quant_data_kit.capture_v2.storage import CaptureStorageGuard, RawSegment
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
    ) -> None:
        if max_part_rows < 1:
            raise ValidationError("max_part_rows must be positive")
        self.hot_root = Path(hot_root)
        self.epoch_id = epoch_id
        self.stream_id = stream_id
        self.provider = provider
        self.venue = venue
        self.storage_guard = storage_guard
        self.policy = policy
        self.max_part_rows = max_part_rows
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
        self._records = 0
        self._parts: list[EpochPart] = []
        self._raw_references: list[RawObjectReference] = []
        self._raw_manifest_hashes: set[str] = set()
        self._closed = False

    def append(self, records: Iterable[Mapping[str, Any]]) -> None:
        self._require_open()
        for record in records:
            body = canonical_json_bytes(dict(record)) + b"\n"
            self.storage_guard.require_hot_capacity(projected_write_bytes=len(body))
            self._open_stream.write(body)
            self._open_stream.flush()
            os.fsync(self._open_stream.fileno())
            self._open_rows += 1
            self._records += 1
            if self._open_rows >= self.max_part_rows:
                self._seal_part()

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

    def finalize(self, *, created_at: datetime | None = None) -> NormalizedEpochReceipt:
        self._require_open()
        self._seal_part()
        self._open_stream.close()
        self._closed = True
        if self._records and not self._raw_references:
            raise ValidationError("Normalized epoch records require Raw segment lineage")
        result = (
            self._publish_normalized() if self._records else NormalizationResult(None, 0, 0, None)
        )
        created = utc_text(created_at or datetime.now(tz=UTC), "epoch created_at")
        identity = {
            "schema_version": "puresaber.normalized-epoch-receipt@1.0.0",
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
        receipt_path = self.root / "receipt.json"
        with receipt_path.open("xb") as stream:
            stream.write(canonical_json_bytes(payload))
            stream.flush()
            os.fsync(stream.fileno())
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
        if not self._closed:
            self._open_stream.flush()
            os.fsync(self._open_stream.fileno())
            self._open_stream.close()
            self._closed = True
        payload = {
            "schema_version": "puresaber.normalized-epoch-abort@1.0.0",
            "epoch_id": self.epoch_id,
            "stream_id": self.stream_id,
            "reason": reason,
            "records": self._records,
            "raw_references": [asdict(item) for item in self._raw_references],
        }
        payload["abort_sha256"] = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        path = self.root / "aborted.json"
        with path.open("xb") as stream:
            stream.write(canonical_json_bytes(payload))
            stream.flush()
            os.fsync(stream.fileno())
        return path

    def _seal_part(self) -> None:
        if self._open_rows == 0:
            return
        self._open_stream.flush()
        os.fsync(self._open_stream.fileno())
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
        self._open_path = self.root / "part-open.ndjson"
        self._open_stream = self._open_path.open("xb")

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
        if self._closed:
            raise ValidationError("Normalized epoch journal is already closed")

    def __del__(self) -> None:
        stream = getattr(self, "_open_stream", None)
        if stream is not None and not stream.closed:
            stream.close()
