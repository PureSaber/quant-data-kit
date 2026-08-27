"""Immutable, content-addressed dataset snapshots."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from quant_data_kit.exceptions import ValidationError


@dataclass(frozen=True)
class DatasetSnapshot:
    schema_version: str
    dataset: str
    snapshot_id: str
    content_sha256: str
    schema_sha256: str
    rows: int
    columns: list[str]
    data_path: str
    source: str
    acquired_at: str
    as_of: str | None = None
    adjustment: str | None = None
    query: dict[str, Any] = field(default_factory=dict)
    upstream: dict[str, str] = field(default_factory=dict)
    code_version: str | None = None


def _canonical_frame_bytes(frame: pd.DataFrame) -> bytes:
    ordered = frame.copy()
    ordered = ordered.reindex(sorted(ordered.columns), axis=1)
    if not ordered.empty:
        ordered = ordered.sort_values(list(ordered.columns), kind="mergesort").reset_index(
            drop=True
        )
    return ordered.to_csv(index=False, date_format="%Y-%m-%dT%H:%M:%S.%f").encode("utf-8")


def frame_sha256(frame: pd.DataFrame) -> str:
    return hashlib.sha256(_canonical_frame_bytes(frame)).hexdigest()


def schema_sha256(frame: pd.DataFrame) -> str:
    schema = [(str(column), str(dtype)) for column, dtype in frame.dtypes.items()]
    payload = json.dumps(schema, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def create_snapshot(
    frame: pd.DataFrame,
    root: Path,
    *,
    dataset: str,
    source: str,
    acquired_at: str | None = None,
    as_of: str | None = None,
    adjustment: str | None = None,
    query: dict[str, Any] | None = None,
    upstream: dict[str, str] | None = None,
    code_version: str | None = None,
) -> DatasetSnapshot:
    """Persist a dataset once under a content hash and refuse hash collisions."""
    if frame.empty:
        raise ValidationError("Cannot snapshot an empty dataset")
    content_hash = frame_sha256(frame)
    snapshot_id = f"sha256-{content_hash[:16]}"
    snapshot_dir = Path(root) / dataset / snapshot_id
    data_path = snapshot_dir / "data.parquet"
    manifest_path = snapshot_dir / "manifest.json"
    snapshot = DatasetSnapshot(
        schema_version="1.0",
        dataset=dataset,
        snapshot_id=snapshot_id,
        content_sha256=content_hash,
        schema_sha256=schema_sha256(frame),
        rows=len(frame),
        columns=list(frame.columns),
        data_path="data.parquet",
        source=source,
        acquired_at=acquired_at or datetime.now(timezone.utc).isoformat(),
        as_of=as_of,
        adjustment=adjustment,
        query=query or {},
        upstream=upstream or {},
        code_version=code_version,
    )
    if snapshot_dir.exists():
        if not data_path.is_file() or not manifest_path.is_file():
            raise ValidationError(f"Incomplete immutable snapshot: {snapshot_dir}")
        existing = pd.read_parquet(data_path)
        if frame_sha256(existing) != content_hash:
            raise ValidationError(f"Snapshot hash collision or mutation: {snapshot_dir}")
        return load_snapshot(manifest_path)
    snapshot_dir.mkdir(parents=True, exist_ok=False)
    frame.to_parquet(data_path, index=False)
    manifest_path.write_text(
        json.dumps(asdict(snapshot), indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return snapshot


def load_snapshot(path: Path) -> DatasetSnapshot:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    snapshot = DatasetSnapshot(**payload)
    data_path = Path(path).parent / snapshot.data_path
    if not data_path.is_file():
        raise ValidationError(f"Snapshot data missing: {data_path}")
    frame = pd.read_parquet(data_path)
    if frame_sha256(frame) != snapshot.content_sha256:
        raise ValidationError(f"Snapshot content changed: {data_path}")
    return snapshot
