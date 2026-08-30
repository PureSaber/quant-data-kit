"""Raw segment rotation, capacity gates, and independently verified local archives."""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import shutil
import stat
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Protocol

from quant_data_kit.capture_v2.models import (
    AuditEvent,
    AuditReference,
    RawFrame,
    SegmentRotation,
    canonical_json_bytes,
    utc_text,
)
from quant_data_kit.data_lake import (
    RawObjectManifest,
    StoragePolicy,
    evaluate_capacity,
    load_raw_object,
    write_raw_bytes,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.process_lock import process_file_lock

UTC = timezone.utc
_GIB = 1024**3
_DEFAULT_POLICY = StoragePolicy()


class CapturePausedError(ValidationError):
    """A fail-closed storage condition that requires operator intervention."""


@dataclass(frozen=True)
class DiskCapacity:
    total_bytes: int
    free_bytes: int


@dataclass(frozen=True)
class VolumeIdentity:
    identity: str
    physical_devices: tuple[str, ...]

    def __post_init__(self) -> None:
        devices = tuple(sorted(set(self.physical_devices)))
        if not self.identity or not devices:
            raise ValidationError("volume identity must include auditable physical devices")
        object.__setattr__(self, "physical_devices", devices)


class VolumeIdentityProbe(Protocol):
    def __call__(self, path: Path) -> VolumeIdentity: ...


class CapacityProbe(Protocol):
    def __call__(self, path: Path) -> DiskCapacity: ...


def default_capacity_probe(path: Path) -> DiskCapacity:
    usage = shutil.disk_usage(path)
    return DiskCapacity(total_bytes=usage.total, free_bytes=usage.free)


def _windows_volume_identity(path: Path) -> VolumeIdentity:
    from ctypes import wintypes

    class DiskExtent(ctypes.Structure):
        _fields_ = [
            ("disk_number", wintypes.DWORD),
            ("starting_offset", ctypes.c_longlong),
            ("extent_length", ctypes.c_longlong),
        ]

    class VolumeDiskExtents(ctypes.Structure):
        _fields_ = [("count", wintypes.DWORD), ("extents", DiskExtent * 1)]

    anchor = Path(path).resolve(strict=True).anchor.rstrip("\\/")
    if len(anchor) != 2 or anchor[1] != ":":
        raise CapturePausedError(f"cannot resolve Windows volume anchor for {path}")
    volume_path = rf"\\.\{anchor}"
    create_file = ctypes.windll.kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    handle = create_file(
        volume_path,
        0,
        0x00000001 | 0x00000002,
        None,
        3,
        0,
        None,
    )
    invalid_handle = wintypes.HANDLE(-1).value
    if handle == invalid_handle:
        raise CapturePausedError(f"cannot open volume for physical identity: {anchor}")
    try:
        output = ctypes.create_string_buffer(4096)
        returned = wintypes.DWORD()
        ok = ctypes.windll.kernel32.DeviceIoControl(
            handle,
            0x00560000,
            None,
            0,
            output,
            len(output),
            ctypes.byref(returned),
            None,
        )
        if not ok or returned.value < VolumeDiskExtents.extents.offset:
            raise CapturePausedError(f"physical disk extents unavailable for {anchor}")
        count = int.from_bytes(output.raw[:4], "little")
        if count < 1:
            raise CapturePausedError(f"volume has no physical disk extents: {anchor}")
        offset = VolumeDiskExtents.extents.offset
        extent_size = ctypes.sizeof(DiskExtent)
        devices = []
        for index in range(count):
            end = offset + (index + 1) * extent_size
            if end > returned.value:
                raise CapturePausedError(f"truncated physical disk extents for {anchor}")
            extent = DiskExtent.from_buffer_copy(output.raw, offset + index * extent_size)
            devices.append(f"windows-physical-disk-{extent.disk_number}")
        return VolumeIdentity(
            identity=f"windows-volume-{anchor.upper()}", physical_devices=tuple(devices)
        )
    finally:
        ctypes.windll.kernel32.CloseHandle(handle)


def _linux_physical_devices(path: Path) -> tuple[str, ...]:
    stat_result = os.stat(path)
    major, minor = os.major(stat_result.st_dev), os.minor(stat_result.st_dev)
    sys_device = Path(f"/sys/dev/block/{major}:{minor}")
    if not sys_device.exists():
        raise CapturePausedError(f"Linux physical block identity unavailable for {path}")
    resolved = sys_device.resolve(strict=True)
    slaves = resolved / "slaves"
    if slaves.is_dir():
        child_devices = tuple(sorted(item.name for item in slaves.iterdir()))
        if child_devices:
            return tuple(f"linux-block-{item}" for item in child_devices)
    parts = resolved.parts
    try:
        block_index = parts.index("block")
        device = parts[block_index + 1]
    except (ValueError, IndexError) as exc:
        raise CapturePausedError(
            f"cannot map Linux volume to a physical block device: {path}"
        ) from exc
    return (f"linux-block-{device}",)


def default_volume_identity(path: Path) -> VolumeIdentity:
    resolved = Path(path).resolve(strict=True)
    if os.name == "nt":
        return _windows_volume_identity(resolved)
    devices = _linux_physical_devices(resolved)
    return VolumeIdentity(
        identity=f"posix-device-{os.stat(resolved).st_dev}",
        physical_devices=devices,
    )


@dataclass(frozen=True)
class StoragePreflight:
    allowed: bool
    hot_identity: VolumeIdentity | None
    archive_identity: VolumeIdentity | None
    hot_capacity: DiskCapacity | None
    archive_capacity: DiskCapacity | None
    minimum_hot_free_bytes: int | None
    minimum_archive_free_bytes: int | None
    reasons: tuple[str, ...]

    @property
    def alert(self) -> str | None:
        return "CAPTURE_PAUSED: " + "; ".join(self.reasons) if self.reasons else None


class CaptureStorageGuard:
    """Check hot and archive volumes without assuming drive letters or path aliases."""

    def __init__(
        self,
        hot_root: Path,
        archive_root: Path,
        *,
        policy: StoragePolicy = _DEFAULT_POLICY,
        archive_reserve_bytes: int = 150 * _GIB,
        volume_identity: VolumeIdentityProbe = default_volume_identity,
        capacity_probe: CapacityProbe = default_capacity_probe,
        hot_size_probe: Callable[[Path], int] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        probe_messages: int = 256,
        probe_bytes: int = 4 * 1024 * 1024,
        probe_seconds: float = 1.0,
    ) -> None:
        self.hot_root = Path(hot_root)
        self.archive_root = Path(archive_root)
        self.policy = policy
        self.archive_reserve_bytes = archive_reserve_bytes
        self.volume_identity = volume_identity
        self.capacity_probe = capacity_probe
        self.hot_size_probe = hot_size_probe or self._tree_size
        if probe_messages < 1 or probe_bytes < 1 or probe_seconds <= 0:
            raise ValidationError("capacity probe intervals must be positive")
        self.monotonic = monotonic
        self.probe_messages = probe_messages
        self.probe_bytes = probe_bytes
        self.probe_seconds = probe_seconds
        self._lock = threading.RLock()
        self._hot_capacity: DiskCapacity | None = None
        self._hot_bytes = 0
        self._hot_unprobed_bytes = 0
        self._hot_unprobed_messages = 0
        self._archive_capacity: DiskCapacity | None = None
        self._archive_unprobed_bytes = 0
        self._archive_unprobed_messages = 0
        self._last_hot_probe = float("-inf")
        self._last_archive_probe = float("-inf")
        self._capacity_probe_count = 0
        self._tree_scan_count = 0
        self._hot_reservation_count = 0
        self._archive_reservation_count = 0

    @staticmethod
    def _tree_size(root: Path) -> int:
        return sum(item.stat().st_size for item in root.rglob("*") if item.is_file())

    @staticmethod
    def _minimum_free(capacity: DiskCapacity, policy: StoragePolicy) -> int:
        return max(
            int(capacity.total_bytes * policy.minimum_free_fraction),
            policy.minimum_free_bytes,
        )

    def preflight(self, *, projected_hot_bytes: int = 0) -> StoragePreflight:
        reasons: list[str] = []
        hot_identity = archive_identity = None
        hot_capacity = archive_capacity = None
        minimum_hot = minimum_archive = None
        for root, name in ((self.hot_root, "hot_root"), (self.archive_root, "archive_root")):
            if not root.is_absolute():
                reasons.append(f"{name} must be an explicit absolute path")
            elif not root.is_dir():
                reasons.append(f"{name} is missing or not a directory: {root}")
            else:
                try:
                    _validate_safe_path(root, root, allow_missing=False)
                except ValidationError as exc:
                    reasons.append(f"{name} is unsafe: {exc}")
        if reasons:
            return StoragePreflight(False, None, None, None, None, None, None, tuple(reasons))
        try:
            hot_identity = self.volume_identity(self.hot_root)
            archive_identity = self.volume_identity(self.archive_root)
        except Exception as exc:  # noqa: BLE001 - injectable OS probe must fail closed
            reasons.append(f"physical volume identity unavailable: {type(exc).__name__}: {exc}")
        if hot_identity and archive_identity:
            overlap = sorted(
                set(hot_identity.physical_devices).intersection(archive_identity.physical_devices)
            )
            if overlap:
                reasons.append(f"hot and archive roots share physical devices: {overlap}")
        try:
            hot_capacity = self._probe_capacity(self.hot_root)
            archive_capacity = self._probe_capacity(self.archive_root)
            minimum_hot = self._minimum_free(hot_capacity, self.policy)
            minimum_archive = self._minimum_free(archive_capacity, self.policy)
            hot_decision = evaluate_capacity(
                self.hot_root,
                projected_write_bytes=projected_hot_bytes,
                policy=self.policy,
                current_hot_bytes=self._probe_hot_size(),
                disk_total_bytes=hot_capacity.total_bytes,
                disk_free_bytes=hot_capacity.free_bytes,
            )
            reasons.extend(hot_decision.reasons)
            archive_after_reserve = archive_capacity.free_bytes - self.archive_reserve_bytes
            if archive_after_reserve < minimum_archive:
                reasons.append(
                    "archive capacity floor breached: "
                    f"projected={archive_after_reserve}, minimum={minimum_archive}, "
                    f"reserve={self.archive_reserve_bytes}"
                )
        except Exception as exc:  # noqa: BLE001 - injectable capacity probe must fail closed
            reasons.append(f"capacity probe failed: {type(exc).__name__}: {exc}")
        result = StoragePreflight(
            allowed=not reasons,
            hot_identity=hot_identity,
            archive_identity=archive_identity,
            hot_capacity=hot_capacity,
            archive_capacity=archive_capacity,
            minimum_hot_free_bytes=minimum_hot,
            minimum_archive_free_bytes=minimum_archive,
            reasons=tuple(reasons),
        )
        if result.allowed:
            assert hot_capacity is not None and archive_capacity is not None
            with self._lock:
                self._hot_capacity = hot_capacity
                self._archive_capacity = archive_capacity
                self._hot_bytes = self._last_hot_size
                self._hot_unprobed_bytes = projected_hot_bytes
                self._hot_unprobed_messages = 0
                self._archive_unprobed_bytes = 0
                self._archive_unprobed_messages = 0
                now = self.monotonic()
                self._last_hot_probe = now
                self._last_archive_probe = now
        return result

    def require_preflight(self, *, projected_hot_bytes: int = 0) -> StoragePreflight:
        decision = self.preflight(projected_hot_bytes=projected_hot_bytes)
        if not decision.allowed:
            raise CapturePausedError(decision.alert or "CAPTURE_PAUSED")
        return decision

    def require_hot_capacity(self, *, projected_write_bytes: int) -> None:
        if projected_write_bytes < 0:
            raise ValidationError("projected_write_bytes cannot be negative")
        with self._lock:
            self._ensure_hot_baseline()
            now = self.monotonic()
            if self._probe_due(
                self._hot_unprobed_messages + 1,
                self._hot_unprobed_bytes + projected_write_bytes,
                now - self._last_hot_probe,
            ):
                self._refresh_hot(now)
            assert self._hot_capacity is not None
            decision = evaluate_capacity(
                self.hot_root,
                projected_write_bytes=projected_write_bytes,
                policy=self.policy,
                current_hot_bytes=self._hot_bytes + self._hot_unprobed_bytes,
                disk_total_bytes=self._hot_capacity.total_bytes,
                disk_free_bytes=self._hot_capacity.free_bytes - self._hot_unprobed_bytes,
            )
            if not decision.allowed:
                raise CapturePausedError(decision.alert or "CAPTURE_PAUSED")
            self._hot_unprobed_bytes += projected_write_bytes
            self._hot_unprobed_messages += 1
            self._hot_reservation_count += 1

    def require_archive_capacity(self, *, projected_write_bytes: int) -> None:
        if projected_write_bytes < 0:
            raise ValidationError("projected_write_bytes cannot be negative")
        with self._lock:
            self._ensure_archive_baseline()
            now = self.monotonic()
            if self._probe_due(
                self._archive_unprobed_messages + 1,
                self._archive_unprobed_bytes + projected_write_bytes,
                now - self._last_archive_probe,
            ):
                self._refresh_archive(now)
            assert self._archive_capacity is not None
            minimum = self._minimum_free(self._archive_capacity, self.policy)
            projected = (
                self._archive_capacity.free_bytes
                - self._archive_unprobed_bytes
                - projected_write_bytes
            )
            if projected < minimum:
                raise CapturePausedError(
                    "CAPTURE_PAUSED: archive free-space floor breached: "
                    f"projected={projected}, minimum={minimum}"
                )
            self._archive_unprobed_bytes += projected_write_bytes
            self._archive_unprobed_messages += 1
            self._archive_reservation_count += 1

    @property
    def probe_statistics(self) -> Mapping[str, int]:
        with self._lock:
            return MappingProxyType(
                {
                    "capacity_probes": self._capacity_probe_count,
                    "tree_scans": self._tree_scan_count,
                    "hot_reservations": self._hot_reservation_count,
                    "archive_reservations": self._archive_reservation_count,
                    "maximum_unprobed_bytes": self.probe_bytes,
                }
            )

    def force_hot_probe(self) -> None:
        with self._lock:
            self._refresh_hot(self.monotonic())

    def _probe_due(self, messages: int, byte_count: int, age: float) -> bool:
        return (
            messages >= self.probe_messages
            or byte_count >= self.probe_bytes
            or age >= self.probe_seconds
        )

    def _probe_capacity(self, path: Path) -> DiskCapacity:
        self._capacity_probe_count += 1
        return self.capacity_probe(path)

    def _probe_hot_size(self) -> int:
        self._tree_scan_count += 1
        value = self.hot_size_probe(self.hot_root)
        self._last_hot_size = value
        return value

    def _ensure_hot_baseline(self) -> None:
        if self._hot_capacity is None:
            self._refresh_hot(self.monotonic())

    def _ensure_archive_baseline(self) -> None:
        if self._archive_capacity is None:
            self._refresh_archive(self.monotonic())

    def _refresh_hot(self, now: float) -> None:
        capacity = self._probe_capacity(self.hot_root)
        hot_bytes = self._probe_hot_size()
        decision = evaluate_capacity(
            self.hot_root,
            projected_write_bytes=0,
            policy=self.policy,
            current_hot_bytes=hot_bytes,
            disk_total_bytes=capacity.total_bytes,
            disk_free_bytes=capacity.free_bytes,
        )
        if not decision.allowed:
            raise CapturePausedError(decision.alert or "CAPTURE_PAUSED")
        self._hot_capacity = capacity
        self._hot_bytes = hot_bytes
        self._hot_unprobed_bytes = 0
        self._hot_unprobed_messages = 0
        self._last_hot_probe = now

    def _refresh_archive(self, now: float) -> None:
        capacity = self._probe_capacity(self.archive_root)
        minimum = self._minimum_free(capacity, self.policy)
        if capacity.free_bytes < minimum:
            raise CapturePausedError(
                "CAPTURE_PAUSED: archive free-space floor breached: "
                f"projected={capacity.free_bytes}, minimum={minimum}"
            )
        self._archive_capacity = capacity
        self._archive_unprobed_bytes = 0
        self._archive_unprobed_messages = 0
        self._last_archive_probe = now


@dataclass(frozen=True)
class RawSegment:
    stream_id: str
    segment_index: int
    raw_manifest: RawObjectManifest
    message_count: int
    wire_byte_count: int
    first_received_at: str
    last_received_at: str
    first_event_time: str | None
    last_event_time: str | None
    native_sequence_range: Mapping[str, tuple[int, int]]
    previous_segment_manifest_sha256: str | None


class RawSegmentWriter:
    """Batch exact Raw frames and publish each segment through the existing Raw contract."""

    def __init__(
        self,
        hot_root: Path,
        stream_id: str,
        source: str,
        *,
        collector_commit: str,
        rotation: SegmentRotation,
        storage_guard: CaptureStorageGuard,
        policy: StoragePolicy = _DEFAULT_POLICY,
    ) -> None:
        self.hot_root = Path(hot_root)
        self.stream_id = stream_id
        self.source = source
        self.collector_commit = collector_commit
        self.rotation = rotation
        self.storage_guard = storage_guard
        self.policy = policy
        self._records: list[bytes] = []
        self._frames: list[RawFrame] = []
        self._wire_bytes = 0
        self._segment_index = 0
        self._previous_manifest_sha256: str | None = None
        self._completed: list[RawSegment] = []

    @property
    def pending_messages(self) -> int:
        return len(self._frames)

    @property
    def segment_count(self) -> int:
        return self._segment_index

    def append(self, frame: RawFrame) -> None:
        if frame.stream_id != self.stream_id or frame.provider != self.source:
            raise ValidationError("Raw frame identity does not match segment writer")
        if self._frames and frame.received_at < self._frames[-1].received_at:
            raise ValidationError("Raw frame received_at order regressed")
        encoded = canonical_json_bytes(frame.record()) + b"\n"
        self.storage_guard.require_hot_capacity(projected_write_bytes=len(encoded))
        if self._frames:
            age = (frame.received_at - self._frames[0].received_at).total_seconds()
            if age >= self.rotation.max_age_seconds:
                self.flush()
        self._frames.append(frame)
        self._records.append(encoded)
        self._wire_bytes += len(frame.payload)
        if (
            len(self._frames) >= self.rotation.max_messages
            or self._wire_bytes >= self.rotation.max_wire_bytes
        ):
            self.flush()

    def drain_completed(self) -> tuple[RawSegment, ...]:
        completed = tuple(self._completed)
        self._completed.clear()
        return completed

    def peek_completed(self) -> tuple[RawSegment, ...]:
        return tuple(self._completed)

    def acknowledge_completed(self, segment: RawSegment) -> None:
        if not self._completed or self._completed[0] != segment:
            raise ValidationError("Raw segment acknowledgement is out of order")
        self._completed.pop(0)

    def flush(self) -> RawSegment | None:
        if not self._frames:
            return None
        payload = b"".join(self._records)
        self.storage_guard.require_hot_capacity(projected_write_bytes=4096)
        sequence_range: dict[str, tuple[int, int]] = {}
        for frame in self._frames:
            for name, value in frame.native_sequence.items():
                previous = sequence_range.get(name)
                sequence_range[name] = (
                    value if previous is None else min(previous[0], value),
                    value if previous is None else max(previous[1], value),
                )
        event_times = [frame.event_time for frame in self._frames if frame.event_time is not None]
        first = self._frames[0]
        last = self._frames[-1]
        body_hash = hashlib.sha256(payload).hexdigest()
        segment_number = self._segment_index + 1
        request = {
            "schema_version": "puresaber.raw-segment-request@1.0.0",
            "source": self.source,
            "stream_id": self.stream_id,
            "segment_index": segment_number,
            "message_count": len(self._frames),
            "wire_byte_count": self._wire_bytes,
            "first_received_at": utc_text(first.received_at, "first_received_at"),
            "last_received_at": utc_text(last.received_at, "last_received_at"),
            "first_event_time": (
                utc_text(min(event_times), "first_event_time") if event_times else None
            ),
            "last_event_time": (
                utc_text(max(event_times), "last_event_time") if event_times else None
            ),
            "native_sequence_range": {
                key: list(value) for key, value in sorted(sequence_range.items())
            },
            "payload_format": "canonical-json-lines+base64-exact-wire-bytes",
            "payload_sha256": body_hash,
            "collector_commit": self.collector_commit,
            "previous_segment_manifest_sha256": self._previous_manifest_sha256,
        }
        safe_stream_hash = hashlib.sha256(self.stream_id.encode("utf-8")).hexdigest()[:12]
        key = f"seg-{safe_stream_hash}-{segment_number:012d}-{body_hash[:16]}"
        raw = write_raw_bytes(
            self.hot_root,
            source=self.source,
            request=request,
            collected_at=first.received_at,
            payload=payload,
            idempotency_key=key,
            policy=self.policy,
        )
        segment = RawSegment(
            stream_id=self.stream_id,
            segment_index=segment_number,
            raw_manifest=raw,
            message_count=len(self._frames),
            wire_byte_count=self._wire_bytes,
            first_received_at=request["first_received_at"],
            last_received_at=request["last_received_at"],
            first_event_time=request["first_event_time"],
            last_event_time=request["last_event_time"],
            native_sequence_range=MappingProxyType(sequence_range),
            previous_segment_manifest_sha256=self._previous_manifest_sha256,
        )
        self._segment_index = segment_number
        self._previous_manifest_sha256 = raw.manifest_sha256
        self._records.clear()
        self._frames.clear()
        self._wire_bytes = 0
        self._completed.append(segment)
        return segment


@dataclass(frozen=True)
class ArchivePreflightReceipt:
    schema_version: str
    verified_at: str
    hot_identity: VolumeIdentity
    archive_identity: VolumeIdentity
    restore_identity: VolumeIdentity
    probe_sha256: str
    restored_sha256: str
    receipt_path: str


@dataclass(frozen=True)
class SegmentArchiveReceipt:
    schema_version: str
    object_id: str
    stream_id: str
    archive_directory: str
    source_content_sha256: str
    archive_content_sha256: str
    source_manifest_sha256: str
    archive_manifest_file_sha256: str
    restored_content_sha256: str
    restored_manifest_file_sha256: str
    hot_volume_identity: str
    archive_volume_identity: str
    hot_physical_devices: tuple[str, ...]
    archive_physical_devices: tuple[str, ...]
    verified_at: str
    archive_restore_verified: bool
    eligible_for_cleanup: bool
    cleanup_performed: bool
    receipt_sha256: str
    receipt_path: str


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_reparse_point(path: Path) -> bool:
    if path.is_symlink():
        return True
    try:
        attributes = getattr(os.lstat(path), "st_file_attributes", 0)
    except FileNotFoundError:
        return False
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _validate_safe_path(root: Path, candidate: Path, *, allow_missing: bool) -> Path:
    lexical_root = Path(root).absolute()
    lexical_candidate = Path(candidate).absolute()
    if not lexical_root.exists() or not lexical_root.is_dir():
        raise ValidationError(f"trusted storage root is missing: {lexical_root}")
    if _is_reparse_point(lexical_root):
        raise ValidationError(f"trusted storage root is a symlink/reparse point: {lexical_root}")
    resolved_root = lexical_root.resolve(strict=True)
    try:
        relative = lexical_candidate.relative_to(lexical_root)
    except ValueError as exc:
        raise ValidationError(f"storage path escapes trusted root: {candidate}") from exc
    current = lexical_root
    for part in relative.parts:
        current = current / part
        if not current.exists():
            if allow_missing:
                continue
            raise ValidationError(f"storage path is missing: {current}")
        if _is_reparse_point(current):
            raise ValidationError(f"storage path contains a symlink/reparse point: {current}")
        try:
            current.resolve(strict=True).relative_to(resolved_root)
        except ValueError as exc:
            raise ValidationError(f"resolved storage path escapes trusted root: {current}") from exc
    resolved_candidate = lexical_candidate.resolve(strict=not allow_missing)
    try:
        resolved_candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValidationError(f"resolved storage path escapes trusted root: {candidate}") from exc
    return lexical_candidate


def _safe_mkdir(root: Path, path: Path, *, exist_ok: bool = True) -> Path:
    checked = _validate_safe_path(root, path, allow_missing=True)
    lock = Path(root) / ".capture-path-bootstrap.lock"
    with process_file_lock(lock):
        _validate_safe_path(root, lock, allow_missing=True)
        checked.mkdir(parents=True, exist_ok=exist_ok)
        return _validate_safe_path(root, checked, allow_missing=False)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_immutable_write(path: Path, body: bytes, *, root: Path | None = None) -> None:
    """Publish create-if-absent across processes; never replace an existing target."""

    if root is None:
        path.parent.mkdir(parents=True, exist_ok=True)
    trusted_root = Path(root) if root is not None else path.parent
    parent = _safe_mkdir(trusted_root, path.parent)
    checked_path = _validate_safe_path(trusted_root, path, allow_missing=True)
    temporary = parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    _validate_safe_path(trusted_root, temporary, allow_missing=True)
    expected_hash = hashlib.sha256(body).hexdigest()
    try:
        with temporary.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        if _sha256_file(temporary) != expected_hash:
            raise ValidationError(f"immutable staging hash mismatch: {temporary}")
        _validate_safe_path(trusted_root, parent, allow_missing=False)
        try:
            os.link(temporary, checked_path)
            _fsync_directory(parent)
        except FileExistsError:
            pass
        _validate_safe_path(trusted_root, checked_path, allow_missing=False)
        if not checked_path.is_file() or _sha256_file(checked_path) != expected_hash:
            raise ValidationError(
                f"immutable archive path already contains different bytes: {checked_path}"
            )
    finally:
        if temporary.exists():
            temporary.unlink()


class DurableAuditStore:
    """Low-frequency, immutable and independently reloadable state/audit journal."""

    def __init__(
        self,
        hot_root: Path,
        *,
        stream_id: str,
        connection_id: str,
        collector_commit: str,
        storage_guard: CaptureStorageGuard,
    ) -> None:
        self.hot_root = Path(hot_root)
        self.stream_id = stream_id
        self.connection_id = connection_id
        self.collector_commit = collector_commit
        self.storage_guard = storage_guard
        stream_hash = hashlib.sha256(stream_id.encode("utf-8")).hexdigest()[:16]
        self.root = (
            self.hot_root
            / "capture"
            / "durable-audit"
            / f"stream={stream_hash}"
            / f"connection={connection_id}"
        )
        _safe_mkdir(self.hot_root, self.root)
        self._previous_sha256: str | None = None

    def append(self, event: AuditEvent) -> AuditReference:
        if event.stream_id != self.stream_id:
            raise ValidationError("audit event stream identity mismatch")
        identity = {
            "schema_version": "puresaber.durable-capture-audit@1.0.0",
            "stream_id": self.stream_id,
            "connection_id": self.connection_id,
            "collector_commit": self.collector_commit,
            "ordinal": event.ordinal,
            "previous_audit_sha256": self._previous_sha256,
            "event": json.loads(event.payload()),
        }
        digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        payload = {**identity, "audit_sha256": digest}
        body = canonical_json_bytes(payload)
        self.storage_guard.require_hot_capacity(projected_write_bytes=len(body) + 4096)
        path = self.root / f"audit-{event.ordinal:012d}-sha256-{digest}.json"
        _atomic_immutable_write(path, body, root=self.hot_root)
        loaded = self.load(path)
        if loaded["audit_sha256"] != digest or loaded["ordinal"] != event.ordinal:
            raise ValidationError("durable audit reload verification failed")
        self._previous_sha256 = digest
        return AuditReference(event.ordinal, digest, str(path))

    def load(self, path: Path) -> Mapping[str, object]:
        checked = _validate_safe_path(self.hot_root, path, allow_missing=False)
        try:
            payload = json.loads(checked.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValidationError(f"durable audit is unreadable: {checked}") from exc
        if not isinstance(payload, dict):
            raise ValidationError("durable audit must be a JSON object")
        digest = payload.get("audit_sha256")
        identity = {key: value for key, value in payload.items() if key != "audit_sha256"}
        if digest != hashlib.sha256(canonical_json_bytes(identity)).hexdigest():
            raise ValidationError("durable audit hash verification failed")
        return MappingProxyType(payload)


class LocalArchiveController:
    """Copy, hash, restore, and mark Raw segments without deleting hot data."""

    def __init__(
        self,
        hot_root: Path,
        archive_root: Path,
        restore_root: Path,
        *,
        storage_guard: CaptureStorageGuard,
        clock: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    ) -> None:
        self.hot_root = Path(hot_root)
        self.archive_root = Path(archive_root)
        self.restore_root = Path(restore_root)
        self.storage_guard = storage_guard
        self.clock = clock
        self._preflight_receipt: ArchivePreflightReceipt | None = None

    def preflight(self) -> ArchivePreflightReceipt:
        for root, name in (
            (self.hot_root, "hot_root"),
            (self.archive_root, "archive_root"),
            (self.restore_root, "restore_root"),
        ):
            if not root.is_absolute() or not root.is_dir():
                raise CapturePausedError(
                    f"CAPTURE_PAUSED: {name} is missing or not absolute: {root}"
                )
            try:
                _validate_safe_path(root, root, allow_missing=False)
            except ValidationError as exc:
                raise CapturePausedError(f"CAPTURE_PAUSED: unsafe {name}: {exc}") from exc
        decision = self.storage_guard.require_preflight(projected_hot_bytes=4096)
        assert decision.hot_identity is not None and decision.archive_identity is not None
        try:
            restore_identity = self.storage_guard.volume_identity(self.restore_root)
        except Exception as exc:
            raise CapturePausedError(
                f"CAPTURE_PAUSED: restore volume identity unavailable: {type(exc).__name__}: {exc}"
            ) from exc
        verified_at = utc_text(self.clock(), "archive preflight clock")
        probe_payload = canonical_json_bytes(
            {
                "schema_version": "puresaber.archive-preflight-probe@1.0.0",
                "verified_at": verified_at,
                "hot_identity": asdict(decision.hot_identity),
                "archive_identity": asdict(decision.archive_identity),
                "restore_identity": asdict(restore_identity),
            }
        )
        probe_hash = hashlib.sha256(probe_payload).hexdigest()
        self.storage_guard.require_archive_capacity(projected_write_bytes=len(probe_payload) + 4096)
        archive_probe = self.archive_root / "preflight" / f"sha256-{probe_hash}.bin"
        _atomic_immutable_write(archive_probe, probe_payload, root=self.archive_root)
        self._assert_identity(self.archive_root, decision.archive_identity, "archive_root")
        restore_dir = Path(tempfile.mkdtemp(prefix="qdk-archive-preflight-", dir=self.restore_root))
        try:
            _validate_safe_path(self.restore_root, restore_dir, allow_missing=False)
            restored = restore_dir / "probe.bin"
            shutil.copy2(archive_probe, restored)
            _validate_safe_path(self.restore_root, restored, allow_missing=False)
            restored_hash = _sha256_file(restored)
        finally:
            self._remove_restore_temp(restore_dir)
        if restored_hash != probe_hash:
            raise CapturePausedError("CAPTURE_PAUSED: archive preflight restore hash mismatch")
        receipt_payload = {
            "schema_version": "puresaber.archive-preflight-receipt@1.0.0",
            "verified_at": verified_at,
            "hot_identity": asdict(decision.hot_identity),
            "archive_identity": asdict(decision.archive_identity),
            "restore_identity": asdict(restore_identity),
            "probe_sha256": probe_hash,
            "restored_sha256": restored_hash,
        }
        receipt_path = self.hot_root / "capture" / "archive-preflight" / f"{probe_hash}.json"
        _atomic_immutable_write(
            receipt_path, canonical_json_bytes(receipt_payload), root=self.hot_root
        )
        receipt = ArchivePreflightReceipt(
            schema_version=receipt_payload["schema_version"],
            verified_at=verified_at,
            hot_identity=decision.hot_identity,
            archive_identity=decision.archive_identity,
            restore_identity=restore_identity,
            probe_sha256=probe_hash,
            restored_sha256=restored_hash,
            receipt_path=str(receipt_path),
        )
        self._preflight_receipt = receipt
        return receipt

    def archive_segment(self, segment: RawSegment) -> SegmentArchiveReceipt:
        preflight = self._preflight_receipt
        if preflight is None:
            raise CapturePausedError("CAPTURE_PAUSED: archive preflight was not completed")
        self._assert_identity(self.hot_root, preflight.hot_identity, "hot_root")
        self._assert_identity(self.archive_root, preflight.archive_identity, "archive_root")
        self._assert_identity(self.restore_root, preflight.restore_identity, "restore_root")
        raw, payload = load_raw_object(self.hot_root, segment.raw_manifest.reference())
        source_dir = self._hot_object_dir(raw)
        source_manifest_path = source_dir / "manifest.json"
        source_manifest_file_hash = _sha256_file(source_manifest_path)
        projected = len(payload) + source_manifest_path.stat().st_size + 8192
        self.storage_guard.require_archive_capacity(projected_write_bytes=projected)
        destination = (
            self.archive_root
            / "raw-segments"
            / f"source={raw.source}"
            / f"date={raw.collection_date}"
            / f"object={raw.object_id}"
        )
        _safe_mkdir(self.archive_root, destination)
        self._assert_identity(self.archive_root, preflight.archive_identity, "archive_root")
        _atomic_immutable_write(destination / raw.data_path, payload, root=self.archive_root)
        self._assert_identity(self.archive_root, preflight.archive_identity, "archive_root")
        _atomic_immutable_write(
            destination / "manifest.json",
            source_manifest_path.read_bytes(),
            root=self.archive_root,
        )
        self._assert_identity(self.archive_root, preflight.archive_identity, "archive_root")
        archive_content_hash = _sha256_file(destination / raw.data_path)
        archive_manifest_hash = _sha256_file(destination / "manifest.json")
        restore_dir = Path(tempfile.mkdtemp(prefix="qdk-segment-restore-", dir=self.restore_root))
        try:
            _validate_safe_path(self.restore_root, restore_dir, allow_missing=False)
            self._assert_identity(self.restore_root, preflight.restore_identity, "restore_root")
            restored_payload = restore_dir / raw.data_path
            restored_manifest = restore_dir / "manifest.json"
            shutil.copy2(destination / raw.data_path, restored_payload)
            shutil.copy2(destination / "manifest.json", restored_manifest)
            restored_content_hash = _sha256_file(restored_payload)
            restored_manifest_hash = _sha256_file(restored_manifest)
            restored_model = RawObjectManifest(
                **json.loads(restored_manifest.read_text(encoding="utf-8"))
            )
            self._assert_identity(self.archive_root, preflight.archive_identity, "archive_root")
            self._assert_identity(self.restore_root, preflight.restore_identity, "restore_root")
        except (OSError, TypeError, json.JSONDecodeError) as exc:
            raise CapturePausedError(
                f"CAPTURE_PAUSED: archive restore validation failed: {type(exc).__name__}: {exc}"
            ) from exc
        finally:
            self._remove_restore_temp(restore_dir)
        expected = (
            raw.content_sha256,
            raw.content_sha256,
            source_manifest_file_hash,
            source_manifest_file_hash,
        )
        actual = (
            archive_content_hash,
            restored_content_hash,
            archive_manifest_hash,
            restored_manifest_hash,
        )
        if actual != expected or restored_model != raw:
            raise CapturePausedError("CAPTURE_PAUSED: archive or restore hash validation failed")
        identity = {
            "schema_version": "puresaber.segment-archive-receipt@1.0.0",
            "object_id": raw.object_id,
            "stream_id": segment.stream_id,
            "archive_directory": str(destination),
            "source_content_sha256": raw.content_sha256,
            "archive_content_sha256": archive_content_hash,
            "source_manifest_sha256": raw.manifest_sha256,
            "archive_manifest_file_sha256": archive_manifest_hash,
            "restored_content_sha256": restored_content_hash,
            "restored_manifest_file_sha256": restored_manifest_hash,
            "hot_volume_identity": preflight.hot_identity.identity,
            "archive_volume_identity": preflight.archive_identity.identity,
            "hot_physical_devices": list(preflight.hot_identity.physical_devices),
            "archive_physical_devices": list(preflight.archive_identity.physical_devices),
            "verified_at": utc_text(self.clock(), "archive verified_at"),
            "archive_restore_verified": True,
            "eligible_for_cleanup": True,
            "cleanup_performed": False,
        }
        receipt_hash = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        receipt_payload = {**identity, "receipt_sha256": receipt_hash}
        receipt_path = (
            self.hot_root
            / "capture"
            / "archive-receipts"
            / (f"object={raw.object_id}-receipt={receipt_hash}.json")
        )
        _atomic_immutable_write(
            receipt_path, canonical_json_bytes(receipt_payload), root=self.hot_root
        )
        return SegmentArchiveReceipt(
            **{
                **receipt_payload,
                "hot_physical_devices": tuple(identity["hot_physical_devices"]),
                "archive_physical_devices": tuple(identity["archive_physical_devices"]),
                "receipt_path": str(receipt_path),
            }
        )

    def _assert_identity(self, root: Path, expected: VolumeIdentity, root_name: str) -> None:
        _validate_safe_path(root, root, allow_missing=False)
        try:
            actual = self.storage_guard.volume_identity(root)
        except Exception as exc:
            raise CapturePausedError(
                f"CAPTURE_PAUSED: {root_name} identity recheck failed: {type(exc).__name__}: {exc}"
            ) from exc
        if actual != expected:
            raise CapturePausedError(
                f"CAPTURE_PAUSED: {root_name} physical identity changed since preflight: "
                f"expected={expected}, actual={actual}"
            )

    def _remove_restore_temp(self, restore_dir: Path) -> None:
        checked = _validate_safe_path(self.restore_root, restore_dir, allow_missing=False)
        if checked.parent.resolve(strict=True) != self.restore_root.resolve(strict=True):
            raise CapturePausedError("CAPTURE_PAUSED: restore temp escaped configured restore_root")
        shutil.rmtree(checked)

    def _hot_object_dir(self, raw: RawObjectManifest) -> Path:
        path = (
            self.hot_root
            / "raw"
            / f"source={raw.source}"
            / f"date={raw.collection_date}"
            / f"key={raw.idempotency_key}"
            / f"object={raw.object_id}"
        )
        return _validate_safe_path(self.hot_root, path, allow_missing=False).resolve(strict=True)


def iter_segment_frames(hot_root: Path, segment: RawSegment) -> Iterable[RawFrame]:
    _, payload = load_raw_object(hot_root, segment.raw_manifest.reference())
    for line_number, line in enumerate(payload.splitlines(), start=1):
        if not line:
            continue
        try:
            record = json.loads(line)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValidationError(f"Raw segment line {line_number} is malformed") from exc
        yield RawFrame.from_record(record)
