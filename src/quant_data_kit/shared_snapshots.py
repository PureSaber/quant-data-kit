"""Hash-bound Parquet snapshots and bounded column/date/security/PIT queries."""

from __future__ import annotations

import base64
import io
import json
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from quant_data_kit.artifacts import ArtifactRef, ArtifactStore, _text, canonical_json, safe_path
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.snapshots import DatasetSnapshot, frame_sha256, schema_sha256
from quant_data_kit.temporal_v2 import ensure_utc_datetime, parse_timestamp_exact

_SCHEMA = "quant.snapshot-manifest/v1"
_MEDIA = "application/vnd.quant.snapshot+json"
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")


def _date(value: str) -> date:
    try:
        result = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError("Dates must use YYYY-MM-DD") from exc
    if result.isoformat() != value:
        raise ValidationError("Dates must use YYYY-MM-DD")
    return result


def _schema_text(schema: pa.Schema) -> str:
    return base64.b64encode(schema.serialize().to_pybytes()).decode("ascii")


@dataclass(frozen=True)
class SnapshotManifest:
    artifact: ArtifactRef
    document: dict

    @property
    def snapshot_id(self) -> str:
        return "sha256-" + self.artifact.sha256

    def to_dict(self) -> dict:
        return {
            "snapshot_id": self.snapshot_id,
            "artifact": self.artifact.to_dict(),
            **self.document,
        }


class SnapshotStore:
    """Publish same-schema Parquet partitions; query them in sorted member order.

    Snapshot identity includes bytes, Arrow schema, dataset/version, query column
    roles and caller-supplied time/availability semantics. Absolute source paths
    and acquisition wall clocks are not implicitly added to that identity.
    """

    def __init__(self, artifacts: ArtifactStore):
        self.artifacts = artifacts

    @staticmethod
    def _validate_roles(schema: pa.Schema, roles: dict) -> None:
        _text(roles["date_column"], "date_column")
        _text(roles["symbol_column"], "symbol_column")
        if roles["available_at_column"] is not None:
            _text(roles["available_at_column"], "available_at_column")
        if len(schema.names) != len(set(schema.names)):
            raise ValidationError("Duplicate Parquet column names")
        for name in roles.values():
            if name is not None and name not in schema.names:
                raise ValidationError(f"Missing query column: {name}")
        dt = schema.field(roles["date_column"]).type
        if not (
            pa.types.is_date(dt)
            or pa.types.is_string(dt)
            or (pa.types.is_timestamp(dt) and dt.tz == "UTC")
        ):
            raise ValidationError("Date column must be a date, ISO date string or UTC timestamp")
        if not pa.types.is_string(schema.field(roles["symbol_column"]).type):
            raise ValidationError("Symbol column must be a string")
        available = roles["available_at_column"]
        if available is not None:
            dt = schema.field(available).type
            if not pa.types.is_timestamp(dt) or dt.tz != "UTC":
                raise ValidationError("Availability column must be a UTC timestamp")

    @staticmethod
    def _validate_values(parquet: pq.ParquetFile, roles: dict) -> None:
        columns = list(dict.fromkeys(name for name in roles.values() if name is not None))
        for batch in parquet.iter_batches(batch_size=65536, columns=None):
            for name in columns:
                values = batch.column(name)
                if values.null_count:
                    raise ValidationError(f"Query column contains nulls: {name}")
                if name == roles["date_column"] and pa.types.is_string(values.type):
                    for value in pc.unique(values).to_pylist():
                        _date(value)

    def publish(
        self,
        files: Mapping[str, Path],
        *,
        dataset: str,
        version: str,
        temporal: Mapping,
        owner: str,
        date_column: str = "date",
        symbol_column: str = "symbol",
        available_at_column: str | None = None,
        _legacy: DatasetSnapshot | None = None,
    ) -> SnapshotManifest:
        """Copy, validate and atomically pin a snapshot and its dependency graph.

        A failed/killed publisher can leave unindexed files, never visible partial
        snapshots. Explicit collect() reclaims those files after its grace period.
        """
        _text(dataset, "dataset")
        _text(version, "version")
        _text(owner, "owner")
        if (
            not isinstance(files, Mapping)
            or not files
            or not isinstance(temporal, Mapping)
            or not temporal
        ):
            raise ValidationError("Non-empty files and explicit temporal semantics are required")
        if any(not isinstance(name, str) for name in files):
            raise ValidationError("Member names must be strings")
        # A JSON copy decouples nested caller metadata before it is hashed.
        temporal = json.loads(canonical_json(dict(temporal)))
        roles = {
            "date_column": date_column,
            "symbol_column": symbol_column,
            "available_at_column": available_at_column,
        }
        members = []
        common_schema = None
        with self.artifacts._session() as db:
            for name, source in sorted(files.items()):
                if not isinstance(name, str) or not _NAME.fullmatch(name) or name == "latest":
                    raise ValidationError("Member names must be simple stable identifiers")
                ref = self.artifacts._put_file(db, source, "application/vnd.apache.parquet")
                path = self.artifacts._verify(db, ref)
                try:
                    with pq.ParquetFile(path) as parquet:
                        schema = parquet.schema_arrow
                        self._validate_roles(schema, roles)
                        self._validate_values(parquet, roles)
                        if common_schema is not None and not schema.equals(
                            common_schema, check_metadata=True
                        ):
                            raise ValidationError("All snapshot partitions must share one schema")
                        common_schema = schema
                        if _legacy is not None:
                            frame = parquet.read().to_pandas()
                            if (
                                frame_sha256(frame) != _legacy.content_sha256
                                or schema_sha256(frame) != _legacy.schema_sha256
                                or len(frame) != _legacy.rows
                                or list(frame.columns) != _legacy.columns
                            ):
                                raise ValidationError("Legacy snapshot content/schema changed")
                        members.append(
                            {
                                "name": name,
                                "artifact": ref.to_dict(),
                                "rows": parquet.metadata.num_rows,
                            }
                        )
                except (pa.ArrowException, OSError) as exc:
                    raise ValidationError("Invalid Parquet snapshot member") from exc
            document = {
                "schema_version": _SCHEMA,
                "dataset": dataset,
                "version": version,
                "temporal": temporal,
                "query_columns": roles,
                "arrow_schema": _schema_text(common_schema),
                "members": members,
            }
            manifest_ref = self.artifacts._put_stream(
                db, io.BytesIO(canonical_json(document)), _MEDIA
            )
            for member in members:
                db.execute(
                    "INSERT OR IGNORE INTO edges VALUES (?, ?)",
                    (manifest_ref.sha256, member["artifact"]["sha256"]),
                )
            self.artifacts._pin(db, owner, [manifest_ref])
        self.artifacts._event("snapshot_published", sha256=manifest_ref.sha256, owner=owner)
        return SnapshotManifest(manifest_ref, document)

    def import_snapshot(self, manifest_path: Path, *, owner: str) -> SnapshotManifest:
        """Import legacy DatasetSnapshot, checking its logical AND schema hashes.

        Validation uses the copied immutable bytes, eliminating the gap between
        source validation and copying. Import may materialize the legacy table;
        subsequent queries always use bounded batches.
        """
        manifest_path = safe_path(manifest_path, regular=True)
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            legacy = DatasetSnapshot(**payload)
        except (ValueError, TypeError) as exc:
            raise ValidationError("Invalid legacy snapshot manifest") from exc
        relative = Path(legacy.data_path)
        if relative.is_absolute() or relative.drive or ".." in relative.parts:
            raise ValidationError("Legacy snapshot data path escapes manifest directory")
        data = safe_path(manifest_path.parent / relative, regular=True)
        return self.publish(
            {"data": data},
            dataset=legacy.dataset,
            version=legacy.schema_version,
            temporal={"legacy_snapshot": payload, "availability": "legacy-as-of-metadata-only"},
            owner=owner,
            _legacy=legacy,
        )

    def _load(self, db, ref: ArtifactRef) -> tuple[SnapshotManifest, list[Path], pa.Schema]:
        path = self.artifacts._verify(db, ref)
        try:
            payload = json.loads(path.read_bytes())
            if payload["schema_version"] != _SCHEMA:
                raise ValidationError("Unsupported snapshot schema_version")
            schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(payload["arrow_schema"])))
            self._validate_roles(schema, payload["query_columns"])
            if not payload["members"]:
                raise ValidationError("Snapshot has no members")
            paths = []
            names = []
            for member in payload["members"]:
                name = member["name"]
                if not _NAME.fullmatch(name) or name in names:
                    raise ValidationError("Invalid snapshot member name")
                names.append(name)
                child = ArtifactRef.from_dict(member["artifact"])
                if not db.execute(
                    "SELECT 1 FROM edges WHERE parent=? AND child=?", (ref.sha256, child.sha256)
                ).fetchone():
                    raise ValidationError("Snapshot dependency is not registered")
                member_path = self.artifacts._verify(db, child)
                with pq.ParquetFile(member_path) as parquet:
                    if not schema.equals(parquet.schema_arrow, check_metadata=True) or (
                        parquet.metadata.num_rows != member["rows"]
                    ):
                        raise ValidationError("Snapshot member schema/row count differs")
                paths.append(member_path)
        except (KeyError, TypeError, ValueError, pa.ArrowException) as exc:
            raise ValidationError("Invalid snapshot manifest") from exc
        return SnapshotManifest(ref, payload), paths, schema

    def load(self, ref: ArtifactRef) -> SnapshotManifest:
        with self.artifacts._session() as db:
            return self._load(db, ref)[0]

    def iter_batches(
        self,
        ref: ArtifactRef,
        *,
        columns: Sequence[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        symbols: Sequence[str] | None = None,
        as_of: str | datetime | None = None,
        batch_size: int = 65536,
    ) -> Iterator[pa.RecordBatch]:
        """Yield projected batches in file order; dates inclusive, availability <= as_of.

        Hash verification reads all member bytes using a bounded buffer before the
        first result. Only projected/filter columns are decoded. The iterator owns
        the store lock until exhausted/closed; use contextlib.closing when stopping
        early. No arbitrary expression, SQL or source path can be supplied here.
        """
        if type(batch_size) is not int or batch_size <= 0:
            raise ValidationError("batch_size must be a positive integer")
        if isinstance(columns, str):
            raise ValidationError("columns must be a sequence of column names")
        start = _date(start_date) if start_date is not None else None
        end = _date(end_date) if end_date is not None else None
        if start and end and start > end:
            raise ValidationError("start_date must not exceed end_date")
        if isinstance(symbols, str) or (
            symbols is not None and any(not isinstance(value, str) for value in symbols)
        ):
            raise ValidationError("symbols must be a sequence of strings")
        with self.artifacts._session() as db:
            manifest, paths, schema = self._load(db, ref)
            roles = manifest.document["query_columns"]
            selected = schema.names if columns is None else list(columns)
            if (
                not selected
                or len(selected) != len(set(selected))
                or any(column not in schema.names for column in selected)
            ):
                raise ValidationError("columns must be unique existing column names")
            needed = list(selected)
            for active, column in (
                (start is not None or end is not None, roles["date_column"]),
                (symbols is not None, roles["symbol_column"]),
                (as_of is not None, roles["available_at_column"]),
            ):
                if active:
                    if column is None:
                        raise ValidationError("PIT query requires an availability column")
                    if column not in needed:
                        needed.append(column)
            cutoff = None
            if as_of is not None:
                instant = ensure_utc_datetime(
                    parse_timestamp_exact(as_of, field="as_of"), field="as_of"
                )
                # Compare in nanoseconds to preserve sub-microsecond PIT boundaries.
                cutoff = pa.scalar(instant, type=pa.timestamp("ns", tz="UTC"))
            security_set = pa.array(symbols, type=pa.string()) if symbols is not None else None
            for path in paths:
                with pq.ParquetFile(path) as parquet:
                    for batch in parquet.iter_batches(batch_size=batch_size, columns=needed):
                        mask = None

                        def combine(value):
                            nonlocal mask
                            mask = value if mask is None else pc.and_(mask, value)

                        if start is not None or end is not None:
                            dates = batch.column(roles["date_column"])
                            dtype = dates.type
                            if pa.types.is_timestamp(dtype):
                                if start:
                                    lower = datetime.combine(
                                        start, datetime.min.time(), timezone.utc
                                    )
                                    combine(pc.greater_equal(dates, pa.scalar(lower, type=dtype)))
                                if end:
                                    upper = datetime.combine(
                                        end + timedelta(days=1), datetime.min.time(), timezone.utc
                                    )
                                    combine(pc.less(dates, pa.scalar(upper, type=dtype)))
                            else:
                                if start:
                                    value = (
                                        start.isoformat() if pa.types.is_string(dtype) else start
                                    )
                                    combine(pc.greater_equal(dates, pa.scalar(value, type=dtype)))
                                if end:
                                    value = end.isoformat() if pa.types.is_string(dtype) else end
                                    combine(pc.less_equal(dates, pa.scalar(value, type=dtype)))
                        if security_set is not None:
                            combine(
                                pc.is_in(
                                    batch.column(roles["symbol_column"]), value_set=security_set
                                )
                            )
                        if cutoff is not None:
                            available = batch.column(roles["available_at_column"])
                            combine(pc.less_equal(pc.cast(available, cutoff.type), cutoff))
                        result = batch if mask is None else batch.filter(mask)
                        if result.num_rows:
                            yield result.select(selected)
            self.artifacts._event("snapshot_query_completed", sha256=ref.sha256)

    def read_table(self, ref: ArtifactRef, **query) -> pa.Table:
        """Explicit full-result materialization convenience; iter_batches stays streaming."""
        batches = list(self.iter_batches(ref, **query))
        if batches:
            return pa.Table.from_batches(batches)
        with self.artifacts._session() as db:
            _, _, schema = self._load(db, ref)
            selected = query.get("columns") or schema.names
            return pa.Table.from_batches([], schema=pa.schema([schema.field(c) for c in selected]))
