"""Local content-addressed artifacts with atomic publication and reference protection.

The root is owned by cooperating local processes. All access (including readers)
uses one OS lock; SQLite records objects, dependencies and durable owner pins.
No URI is interpreted as a filesystem path. Network filesystems are unsupported.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import math
import os
import re
import sqlite3
import stat
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.process_lock import process_file_lock

_HASH = re.compile(r"[0-9a-f]{64}\Z")
_CHUNK = 1024 * 1024
_LOG = logging.getLogger(__name__)


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field} must be a non-empty string")
    return value


def safe_path(path: Path, *, regular: bool = False) -> Path:
    """Reject traversal, symlinks and Windows junctions, including ancestors."""
    path = Path(path)
    if ".." in path.parts:
        raise ValidationError("Path traversal is forbidden")
    path = path.absolute()
    for component in (*reversed(path.parents), path):
        try:
            info = component.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & getattr(
            stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0
        ):
            raise ValidationError(f"Symlink/reparse point is forbidden: {component}")
    if regular and (not path.is_file() or not stat.S_ISREG(path.stat().st_mode)):
        raise ValidationError(f"Expected a regular file: {path}")
    return path


@dataclass(frozen=True)
class ArtifactRef:
    sha256: str
    size_bytes: int
    uri: str
    schema_version: str = "quant.artifact-ref/v1"
    media_type: str | None = None

    def __post_init__(self) -> None:
        if self.schema_version != "quant.artifact-ref/v1":
            raise ValidationError("Unsupported ArtifactRef schema_version")
        if not isinstance(self.sha256, str) or not _HASH.fullmatch(self.sha256):
            raise ValidationError("sha256 must be 64 lowercase hexadecimal characters")
        if type(self.size_bytes) is not int or self.size_bytes < 0:
            raise ValidationError("size_bytes must be a non-negative integer")
        if self.uri != f"qdk+sha256:{self.sha256}":
            raise ValidationError("Artifact URI must match its sha256")
        if self.media_type is not None:
            _text(self.media_type, "media_type")

    def to_dict(self) -> dict:
        result = asdict(self)
        if self.media_type is None:
            result.pop("media_type")
        return result

    @classmethod
    def from_dict(cls, value: Mapping) -> ArtifactRef:
        if not isinstance(value, Mapping) or "schema_version" not in value:
            raise ValidationError("ArtifactRef requires schema_version")
        try:
            return cls(**value)
        except TypeError as exc:
            raise ValidationError("Invalid ArtifactRef fields") from exc


class ArtifactStore:
    """A trusted local store. Use owner pins before publishing references externally."""

    def __init__(self, root: Path, *, context: Mapping[str, str] | None = None):
        self.root = safe_path(root)
        self.context = dict(context or {})
        self.root.mkdir(parents=True, exist_ok=True)
        with self._session() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS objects (
                    sha TEXT PRIMARY KEY, size INTEGER NOT NULL, created REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pins (
                    owner TEXT NOT NULL, sha TEXT NOT NULL REFERENCES objects(sha),
                    PRIMARY KEY(owner, sha)
                );
                CREATE TABLE IF NOT EXISTS edges (
                    parent TEXT NOT NULL REFERENCES objects(sha) ON DELETE CASCADE,
                    child TEXT NOT NULL REFERENCES objects(sha), PRIMARY KEY(parent, child)
                );
                """
            )

    def _path(self, *parts: str) -> Path:
        return safe_path(self.root.joinpath(*parts))

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        with process_file_lock(self._path("store.lock")):
            for suffix in ("", "-journal", "-wal", "-shm"):
                self._path("index.sqlite3" + suffix)
            db = sqlite3.connect(self._path("index.sqlite3"), timeout=60)
            try:
                db.execute("PRAGMA foreign_keys=ON")
                db.execute("PRAGMA synchronous=FULL")
                db.execute("BEGIN IMMEDIATE")
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise
            finally:
                db.close()

    def _event(self, event: str, **details: object) -> None:
        _LOG.info(
            event,
            extra={
                "event": event,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "context": self.context,
                **details,
            },
        )

    def _object_path(self, sha: str) -> Path:
        if not _HASH.fullmatch(sha):
            raise ValidationError("Invalid object hash")
        return self._path("objects", sha[:2], sha)

    def _verify(self, db: sqlite3.Connection, ref: ArtifactRef) -> Path:
        if not isinstance(ref, ArtifactRef):
            raise ValidationError("Expected ArtifactRef; use ArtifactRef.from_dict for JSON")
        record = db.execute("SELECT size FROM objects WHERE sha=?", (ref.sha256,)).fetchone()
        if record != (ref.size_bytes,):
            raise ValidationError("Artifact is missing or size differs from its reference")
        path = self._object_path(ref.sha256)
        self._verify_file(path, ref)
        return path

    @staticmethod
    def _verify_file(path: Path, ref: ArtifactRef) -> None:
        safe_path(path, regular=True)
        if path.stat().st_nlink != 1:
            raise ValidationError("Artifact hard links are forbidden")
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as stream:
            while block := stream.read(_CHUNK):
                size += len(block)
                digest.update(block)
        if size != ref.size_bytes or digest.hexdigest() != ref.sha256:
            raise ValidationError("Artifact integrity check failed")

    def _put_stream(
        self, db: sqlite3.Connection, stream: BinaryIO, media_type: str | None
    ) -> ArtifactRef:
        stage_dir = self._path("staging")
        stage_dir.mkdir(exist_ok=True)
        stage = self._path("staging", uuid4().hex + ".tmp")
        digest = hashlib.sha256()
        size = 0
        try:
            with stage.open("xb") as destination:
                while block := stream.read(_CHUNK):
                    destination.write(block)
                    digest.update(block)
                    size += len(block)
                destination.flush()
                os.fsync(destination.fileno())
            sha = digest.hexdigest()
            ref = ArtifactRef(sha, size, f"qdk+sha256:{sha}", media_type=media_type)
            target = self._object_path(sha)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                self._verify_file(target, ref)
            else:
                os.replace(stage, target)
                # Persist the directory entry on POSIX. Windows os.replace is atomic;
                # sudden power-loss durability beyond fsync is filesystem dependent.
                if os.name == "posix":
                    directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
            db.execute("INSERT OR IGNORE INTO objects VALUES (?, ?, ?)", (sha, size, time.time()))
            return ref
        finally:
            stage.unlink(missing_ok=True)

    def _put_file(self, db: sqlite3.Connection, path: Path, media_type: str | None) -> ArtifactRef:
        path = safe_path(path, regular=True)
        before_path = path.stat()
        with path.open("rb") as source:
            before = os.fstat(source.fileno())
            ref = self._put_stream(db, source, media_type)
            after = os.fstat(source.fileno())
            current = path.stat()
            fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
            # On Windows Python3.12, fstat and stat can report different ctime
            # semantics. Compare each observation through the same syscall;
            # device/inode still bind the opened file to the supplied path.
            if any(
                getattr(before, key) != getattr(after, key)
                or getattr(before_path, key) != getattr(current, key)
                for key in fields
            ) or (before.st_dev, before.st_ino) != (current.st_dev, current.st_ino):
                raise ValidationError("Source changed during artifact import")
        return ref

    def _pin(self, db: sqlite3.Connection, owner: str, refs: Sequence[ArtifactRef]) -> None:
        _text(owner, "owner")
        for ref in refs:
            self._verify_closure(db, ref)
            db.execute("INSERT OR IGNORE INTO pins VALUES (?, ?)", (owner, ref.sha256))

    def put_bytes(
        self, value: bytes, *, media_type: str | None = None, owner: str | None = None
    ) -> ArtifactRef:
        with self._session() as db:
            ref = self._put_stream(db, io.BytesIO(value), media_type)
            if owner is not None:
                self._pin(db, owner, [ref])
        self._event("artifact_published", sha256=ref.sha256)
        return ref

    def put_file(
        self, path: Path, *, media_type: str | None = None, owner: str | None = None
    ) -> ArtifactRef:
        with self._session() as db:
            ref = self._put_file(db, path, media_type)
            if owner is not None:
                self._pin(db, owner, [ref])
        self._event("artifact_published", sha256=ref.sha256)
        return ref

    @contextmanager
    def open(self, ref: ArtifactRef) -> Iterator[BinaryIO]:
        """Hash-check then expose a read-only stream, protected from GC until closed."""
        with self._session() as db, self._verify(db, ref).open("rb") as stream:
            yield stream

    def read_bytes(self, ref: ArtifactRef) -> bytes:
        with self.open(ref) as stream:
            return stream.read()

    def verify(self, ref: ArtifactRef) -> None:
        with self._session() as db:
            self._verify_closure(db, ref)

    def _verify_closure(self, db: sqlite3.Connection, ref: ArtifactRef) -> None:
        """Verify a registered dependency closure without trusting manifest paths."""
        pending, visited = [ref], set()
        while pending:
            current = pending.pop()
            if current.sha256 in visited:
                continue
            self._verify(db, current)
            visited.add(current.sha256)
            for sha, size in db.execute(
                "SELECT objects.sha, size FROM edges JOIN objects ON child=objects.sha "
                "WHERE parent=?",
                (current.sha256,),
            ):
                pending.append(ArtifactRef(sha, size, f"qdk+sha256:{sha}"))

    def retain(self, owner: str, refs: Sequence[ArtifactRef]) -> None:
        """Atomically add durable pins; existing pins for this owner remain intact."""
        with self._session() as db:
            self._pin(db, owner, refs)

    def release(self, owner: str) -> int:
        """Explicitly release all this owner's pins. This does not delete artifacts."""
        _text(owner, "owner")
        with self._session() as db:
            count = db.execute("DELETE FROM pins WHERE owner=?", (owner,)).rowcount
        self._event("artifact_references_released", owner=owner, count=count)
        return count

    @staticmethod
    def _protected(db: sqlite3.Connection, sha: str) -> bool:
        return bool(
            db.execute("SELECT 1 FROM pins WHERE sha=? LIMIT 1", (sha,)).fetchone()
            or db.execute("SELECT 1 FROM edges WHERE child=? LIMIT 1", (sha,)).fetchone()
        )

    def delete(self, ref: ArtifactRef) -> None:
        with self._session() as db:
            self._verify(db, ref)
            if self._protected(db, ref.sha256):
                raise ValidationError("Artifact is protected by references")
            self._object_path(ref.sha256).unlink()
            db.execute("DELETE FROM objects WHERE sha=?", (ref.sha256,))
        self._event("artifact_deleted", sha256=ref.sha256)

    def collect(self, *, min_age_seconds: float = 3600) -> list[str]:
        """Explicit GC of unreferenced objects and interrupted publication files.

        Dependencies are removed only after their parent is collected. An owner
        pin on a snapshot therefore protects all its members transitively.
        """
        if not math.isfinite(min_age_seconds) or min_age_seconds < 0:
            raise ValidationError("min_age_seconds must be finite and non-negative")
        cutoff = time.time() - min_age_seconds
        removed = []
        with self._session() as db:
            while True:
                candidates = db.execute(
                    "SELECT sha FROM objects WHERE created <= ? "
                    "AND NOT EXISTS (SELECT 1 FROM pins WHERE pins.sha=objects.sha) "
                    "AND NOT EXISTS (SELECT 1 FROM edges WHERE child=objects.sha)",
                    (cutoff,),
                ).fetchall()
                if not candidates:
                    break
                for (sha,) in candidates:
                    self._object_path(sha).unlink(missing_ok=True)
                    db.execute("DELETE FROM objects WHERE sha=?", (sha,))
                    removed.append(sha)
            # A killed publisher can leave complete but unindexed objects or staging files.
            objects = self._path("objects")
            if objects.exists():
                for shard in objects.iterdir():
                    safe_path(shard)
                    if not re.fullmatch("[0-9a-f]{2}", shard.name) or not shard.is_dir():
                        raise ValidationError("Unexpected object directory")
                    for path in shard.iterdir():
                        safe_path(path, regular=True)
                        if not _HASH.fullmatch(path.name) or path.name[:2] != shard.name:
                            raise ValidationError("Unexpected object filename")
                        known = db.execute(
                            "SELECT 1 FROM objects WHERE sha=?", (path.name,)
                        ).fetchone()
                        if not known and path.stat().st_mtime <= cutoff:
                            path.unlink()
                            removed.append(path.name)
            staging = self._path("staging")
            if staging.exists():
                for path in staging.iterdir():
                    safe_path(path, regular=True)
                    if not re.fullmatch(r"[0-9a-f]{32}\.tmp", path.name):
                        raise ValidationError("Unexpected staging filename")
                    if path.stat().st_mtime <= cutoff:
                        path.unlink()
        self._event("artifact_gc_completed", count=len(removed))
        return removed
