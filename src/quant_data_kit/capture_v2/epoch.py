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
) -> dict[str, Any]:
    checked = _validate_safe_path(hot_root, path, allow_missing=False)
    try:
        payload = json.loads(checked.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError(f"{description} is unreadable: {checked}") from exc
    if not isinstance(payload, dict):
        raise ValidationError(f"{description} must be a JSON object")
    digest = payload.pop(hash_field, None)
    if digest != hashlib.sha256(canonical_json_bytes(payload)).hexdigest():
        raise ValidationError(f"{description} hash changed")
    if f"sha256-{digest}" not in checked.name:
        raise ValidationError(f"{description} filename hash changed")
    payload[hash_field] = digest
    return payload


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
        prepared_payloads = tuple(
            _load_hashed_json(
                hot_root,
                path,
                hash_field="prepared_sha256",
                description="Normalized epoch PREPARED transaction",
            )
            for path in prepared_paths
        )
        for item in prepared_payloads:
            if (
                item.get("transaction_state") != "PREPARED"
                or item.get("epoch_id") != epoch_id
                or item.get("stream_id") != stream_id
                or item.get("provider") != provider
                or item.get("venue") != venue
                or item.get("policy") != asdict(policy)
                or int(item.get("attempt", 0)) < 1
            ):
                raise ValidationError("Normalized epoch PREPARED transaction identity changed")
        prepared_by_hash = {str(item["prepared_sha256"]): item for item in prepared_payloads}
        receipt_payloads = tuple(
            _load_hashed_json(
                hot_root,
                path,
                hash_field="receipt_sha256",
                description="Normalized epoch receipt",
            )
            for path in receipts
        )
        for item in receipt_payloads:
            if (
                item.get("transaction_state") != "COMMITTED"
                or item.get("epoch_id") != epoch_id
                or item.get("stream_id") != stream_id
                or str(item.get("prepared_sha256")) not in prepared_by_hash
            ):
                raise ValidationError("Normalized epoch COMMITTED receipt identity changed")
        abort_payloads = tuple(
            _load_hashed_json(
                hot_root,
                path,
                hash_field="abort_sha256",
                description="Normalized epoch explicit ABORTED transaction",
            )
            for path in explicit_aborts
        )
        for item in abort_payloads:
            if (
                item.get("transaction_state") != "ABORTED"
                or item.get("epoch_id") != epoch_id
                or item.get("stream_id") != stream_id
                or (
                    item.get("prepared_sha256") is not None
                    and str(item.get("prepared_sha256")) not in prepared_by_hash
                )
            ):
                raise ValidationError("Normalized epoch explicit ABORTED identity changed")
        if receipts or explicit_aborts:
            raise ValidationError("Normalized epoch is not a retryable failed finalize")
        failure_payloads = tuple(
            _load_hashed_json(
                hot_root,
                path,
                hash_field="failure_sha256",
                description="Normalized epoch failure record",
            )
            for path in failures
        )
        for item in failure_payloads:
            if item.get("stream_id") != stream_id or not item.get("retryable_in_process"):
                raise ValidationError("Normalized epoch failure record is not recoverable")
            prepared_sha256 = item.get("prepared_sha256")
            if not prepared_sha256:
                continue
            referenced = prepared_by_hash.get(str(prepared_sha256))
            if referenced is None:
                raise ValidationError("Normalized epoch failure transaction identity changed")
            if int(item.get("records", -1)) != int(referenced.get("records", -2)):
                raise ValidationError("Normalized recovery record count changed")
            if item.get("raw_references", []) != referenced.get("raw_references", []):
                raise ValidationError("Normalized epoch failure Raw lineage changed")
        terminal_prepared = {
            str(item["prepared_sha256"]) for item in failure_payloads if item.get("prepared_sha256")
        }
        pending = tuple(
            item
            for item in prepared_payloads
            if str(item["prepared_sha256"]) not in terminal_prepared
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
            # Legacy failure records did not persist PREPARED. Preserve manual recovery support.
            transaction = {
                "epoch_id": epoch_id,
                "stream_id": stream_id,
                "provider": provider,
                "venue": venue,
                "records": failure.get("records", -1),
                "raw_references": failure.get("raw_references", []),
                "journal_parts": None,
                "policy": asdict(policy),
                "attempt": len(failure_payloads),
            }
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
        self._parts = []
        self._records = 0
        for path in _validated_glob(hot_root, root, "part-*-sha256-*.ndjson"):
            digest = _sha256_file(path, trusted_root=hot_root)
            if f"sha256-{digest}.ndjson" not in path.name:
                raise ValidationError(f"Normalized recovery part hash changed: {path}")
            with _open_journal_file(hot_root, path, "rb") as stream:
                rows = sum(1 for line in stream if line.strip())
            self._parts.append(EpochPart(path.name, rows, digest, path.stat().st_size))
            self._records += rows
        self._raw_references = [
            RawObjectReference(**item) for item in transaction.get("raw_references", [])
        ]
        self._raw_manifest_hashes = {item.manifest_sha256 for item in self._raw_references}
        if int(transaction.get("records", -1)) != self._records:
            raise ValidationError("Normalized recovery record count changed")
        expected_parts = transaction.get("journal_parts")
        if expected_parts is not None and expected_parts != [asdict(item) for item in self._parts]:
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
            for epoch_root in _validated_glob(hot_root, stream_root, "epoch=*"):
                if not epoch_root.is_dir():
                    raise ValidationError("Normalized epoch journal is not a directory")
                prepared_paths = _validated_glob(
                    hot_root, epoch_root, "transaction-prepared-*.json"
                )
                prepared_payloads = tuple(
                    _load_hashed_json(
                        hot_root,
                        path,
                        hash_field="prepared_sha256",
                        description="Normalized epoch PREPARED transaction",
                    )
                    for path in prepared_paths
                )
                for item in prepared_payloads:
                    if (
                        item.get("transaction_state") != "PREPARED"
                        or stream_root.name != f"stream={item.get('stream_id')}"
                        or epoch_root.name != f"epoch={item.get('epoch_id')}"
                        or item.get("policy") != asdict(policy)
                    ):
                        raise ValidationError(
                            "Normalized epoch PREPARED transaction identity changed"
                        )
                prepared_hashes = {str(item["prepared_sha256"]) for item in prepared_payloads}
                receipt_paths = _validated_glob(hot_root, epoch_root, "receipt-sha256-*.json")
                for path in receipt_paths:
                    item = _load_hashed_json(
                        hot_root,
                        path,
                        hash_field="receipt_sha256",
                        description="Normalized epoch receipt",
                    )
                    if (
                        item.get("transaction_state") != "COMMITTED"
                        or item.get("prepared_sha256") not in prepared_hashes
                        or stream_root.name != f"stream={item.get('stream_id')}"
                        or epoch_root.name != f"epoch={item.get('epoch_id')}"
                    ):
                        raise ValidationError("Normalized epoch COMMITTED receipt identity changed")
                if receipt_paths:
                    continue
                explicit_aborts = _validated_glob(hot_root, epoch_root, "aborted-sha256-*.json")
                for path in explicit_aborts:
                    item = _load_hashed_json(
                        hot_root,
                        path,
                        hash_field="abort_sha256",
                        description="Normalized epoch explicit ABORTED transaction",
                    )
                    if (
                        item.get("transaction_state") != "ABORTED"
                        or (
                            item.get("prepared_sha256") is not None
                            and item.get("prepared_sha256") not in prepared_hashes
                        )
                        or stream_root.name != f"stream={item.get('stream_id')}"
                        or epoch_root.name != f"epoch={item.get('epoch_id')}"
                    ):
                        raise ValidationError("Normalized epoch explicit ABORTED identity changed")
                if explicit_aborts:
                    continue
                if not prepared_paths:
                    continue
                latest = prepared_payloads[-1]
                try:
                    created_at = datetime.fromisoformat(
                        str(latest["created_at"]).replace("Z", "+00:00")
                    )
                    journal = cls.recover(
                        hot_root,
                        epoch_id=str(latest["epoch_id"]),
                        stream_id=str(latest["stream_id"]),
                        provider=str(latest["provider"]),
                        venue=str(latest["venue"]),
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
        try:
            self._seal_part()
            if self._records and not self._raw_references:
                raise ValidationError("Normalized epoch records require Raw segment lineage")
            created = utc_text(created_at or datetime.now(tz=UTC), "epoch created_at")
            prepared = self._prepare_transaction(created)
            result = (
                self._publish_normalized() if self._records else _NormalizationSummary(None, 0, 0)
            )
            receipt = self._write_committed_receipt(prepared, result)
        except Exception as exc:
            try:
                self._record_finalize_failure(exc, prepared=prepared, result=result)
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
            "schema_version": "puresaber.normalized-epoch-receipt@1.2.0",
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
                self.flush()
            self._open_stream.close()
        payload = {
            "schema_version": "puresaber.normalized-epoch-abort@1.2.0",
            "transaction_state": "ABORTED",
            "prepared_sha256": (
                str(self._prepared_transaction["prepared_sha256"])
                if self._prepared_transaction is not None
                else None
            ),
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
        try:
            os.link(self._open_path, checked_final)
        except FileExistsError as exc:
            raise ValidationError(
                f"Normalized journal part already exists: {checked_final}"
            ) from exc
        _validate_safe_path(self.hot_root, checked_final, allow_missing=False)
        if _sha256_file(checked_final, trusted_root=self.hot_root) != digest:
            raise ValidationError(f"Normalized sealed journal part hash changed: {checked_final}")
        self._open_path.unlink()
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
    ) -> Path:
        self._finalize_failures += 1
        identity = {
            "schema_version": "puresaber.normalized-epoch-finalize-failure@1.1.0",
            "transaction_state": "ABORTED",
            "prepared_sha256": (str(prepared["prepared_sha256"]) if prepared is not None else None),
            "epoch_id": self.epoch_id,
            "stream_id": self.stream_id,
            "attempt": self._finalize_failures,
            "exception": type(exc).__name__,
            "message": str(exc),
            "records": self._records,
            "raw_references": [asdict(item) for item in self._raw_references],
            "published_snapshot_id": result.snapshot_id if result is not None else None,
            "retryable_in_process": True,
            "restart_recovery": "reconcile PREPARED/ABORTED transaction idempotently",
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
