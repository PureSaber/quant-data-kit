"""Named immutable research inputs, shared across candidates and retries.

Unlike SnapshotStore this layer accepts arbitrary registered file bytes; it makes
no market-data, PIT, or Parquet claims. Readers receive read-only verified streams.
"""

from __future__ import annotations

import io
import json
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from .artifacts import ArtifactRef, ArtifactStore, _text, canonical_json
from .exceptions import ValidationError

_SCHEMA = "quant.input-bundle/v1"
_MEDIA = "application/vnd.quant.input-bundle+json"
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


@dataclass(frozen=True)
class InputBundle:
    artifact: ArtifactRef
    document: dict


class InputBundleStore:
    """Use one content-addressed bundle reference instead of copying per trial.

    Pins protect the manifest and every member. No automatic release, expiration,
    hard links, writable CAS paths, or deletion is performed by this API.
    """

    def __init__(self, artifacts: ArtifactStore):
        self.artifacts = artifacts

    @staticmethod
    def _name(value: str) -> str:
        if not isinstance(value, str) or not _NAME.fullmatch(value):
            raise ValidationError("Input names must be simple identifiers without paths")
        return value

    def publish(
        self, files: Mapping[str, Path], *, owner: str, metadata: Mapping | None = None
    ) -> InputBundle:
        """Stream files once into CAS and atomically publish members, edges and pin.

        Repeated publication reads and hashes bytes again but stores one copy.
        Metadata is part of the immutable identity; source paths are not retained.
        A crash can leave unindexed bytes, handled by ArtifactStore.collect's
        explicit grace policy. It cannot expose a partially published bundle.
        """
        _text(owner, "owner")
        if not isinstance(files, Mapping) or not 1 <= len(files) <= 1000:
            raise ValidationError("Input bundle requires 1 to 1000 files")
        for name in files:
            self._name(name)
        if metadata is not None and not isinstance(metadata, Mapping):
            raise ValidationError("Input metadata must be an object")
        metadata = json.loads(canonical_json(dict(metadata or {})))
        with self.artifacts._session() as db:
            members = [
                {"name": name, "artifact": self.artifacts._put_file(db, path, None).to_dict()}
                for name, path in sorted(files.items())
            ]
            document = {"schema_version": _SCHEMA, "metadata": metadata, "members": members}
            encoded = canonical_json(document)
            if len(encoded) > 1024 * 1024:
                raise ValidationError("Input bundle manifest exceeds 1 MiB")
            ref = self.artifacts._put_stream(db, io.BytesIO(encoded), _MEDIA)
            for member in members:
                db.execute(
                    "INSERT OR IGNORE INTO edges VALUES (?,?)",
                    (ref.sha256, member["artifact"]["sha256"]),
                )
            self.artifacts._pin(db, owner, [ref])
        self.artifacts._event(
            "input_bundle_published", sha256=ref.sha256, members=len(members), owner=owner
        )
        return InputBundle(ref, document)

    def _load(self, db, ref: ArtifactRef) -> InputBundle:
        if ref.size_bytes > 1024 * 1024:
            raise ValidationError("Input bundle manifest exceeds 1 MiB")
        path = self.artifacts._verify(db, ref)
        try:
            document = json.loads(path.read_bytes())
            canonical_json(document)  # Reject forged nonfinite JSON metadata too.
            if (
                set(document) != {"schema_version", "metadata", "members"}
                or document["schema_version"] != _SCHEMA
                or not isinstance(document["metadata"], dict)
                or not isinstance(document["members"], list)
                or not 1 <= len(document["members"]) <= 1000
            ):
                raise ValidationError("Invalid input bundle manifest")
            names, expected_edges = set(), set()
            for member in document["members"]:
                if not isinstance(member, dict) or set(member) != {"name", "artifact"}:
                    raise ValidationError("Invalid input bundle member")
                name = self._name(member["name"])
                if name in names:
                    raise ValidationError("Duplicate input name")
                names.add(name)
                child = ArtifactRef.from_dict(member["artifact"])
                self.artifacts._verify(db, child)
                expected_edges.add(child.sha256)
            actual_edges = {
                row[0]
                for row in db.execute("SELECT child FROM edges WHERE parent=?", (ref.sha256,))
            }
            if actual_edges != expected_edges:
                raise ValidationError("Input bundle dependencies are not registered")
        except ValidationError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationError("Invalid input bundle manifest") from exc
        return InputBundle(ref, document)

    def load(self, ref: ArtifactRef) -> InputBundle:
        with self.artifacts._session() as db:
            return self._load(db, ref)

    @contextmanager
    def open_member(self, ref: ArtifactRef, name: str) -> Iterator[BinaryIO]:
        """Hold the store lock against GC for the entire verified read-only stream.

        This serializes simultaneous readers in the v1 CAS. It is byte reuse, not
        zero I/O or streaming factor computation. Always close readers promptly.
        """
        self._name(name)
        with self.artifacts._session() as db:
            bundle = self._load(db, ref)
            member = next(
                (item for item in bundle.document["members"] if item["name"] == name), None
            )
            if member is None:
                raise ValidationError("Input name does not exist")
            path = self.artifacts._verify(db, ArtifactRef.from_dict(member["artifact"]))
            with path.open("rb") as stream:
                yield stream

    def describe(self, ref: ArtifactRef) -> dict:
        """Report logical and deduplicated payload bytes without changing retention."""
        bundle = self.load(ref)
        refs = [ArtifactRef.from_dict(item["artifact"]) for item in bundle.document["members"]]
        unique = {item.sha256: item.size_bytes for item in refs}
        return {
            "schema_version": "quant.input-bundle-usage/v1",
            "artifact": ref.to_dict(),
            "members": len(refs),
            "logical_input_bytes": sum(item.size_bytes for item in refs),
            "unique_input_bytes": sum(unique.values()),
            "manifest_bytes": ref.size_bytes,
            "retention": "explicit-owner-pins-no-automatic-release",
        }
