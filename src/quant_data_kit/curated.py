"""Session-aware Curated bars and immutable revision snapshots."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint
from quant_data_kit.market_events_v2 import BarEvent, market_event_payload
from quant_data_kit.schemas_v2 import (
    BAR_EVENT_SCHEMA_ID,
    SCHEMA_VERSION_V2,
    TRADE_EVENT_SCHEMA_ID,
    get_arrow_schema,
    validate_arrow_table,
    validate_json_record,
)


class Curator(Protocol):
    """Minimal extension point for future curated datasets."""

    def curate(self, records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class CuratedPartition:
    relative_path: str
    trading_date: str
    instrument_id: str
    schema_id: str
    rows: int
    logical_sha256: str
    content_sha256: str


@dataclass(frozen=True)
class CuratedSnapshot:
    schema_version: str
    layer: str
    dataset: str
    snapshot_id: str
    revision_id: str
    recipe_version: str
    created_at: str
    logical_sha256: str
    rows: int
    lineage: dict[str, str]
    partitions: tuple[CuratedPartition, ...]


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _hash_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return _utc_text(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if hasattr(value, "isoformat") and not isinstance(value, str):
        return value.isoformat()
    return value


def _dataset_segment(dataset: str) -> str:
    if not isinstance(dataset, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", dataset):
        raise ValidationError("dataset must be one Windows-safe path segment")
    if dataset in {".", "..", "latest"}:
        raise ValidationError("dataset name is reserved")
    return dataset


def _utc(value: str | datetime, field_name: str) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(value.replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValidationError(f"{field_name} must be UTC-aware")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _fixed_decimal(value: Mapping[str, Any]) -> Decimal:
    return Decimal(int(value["units"])).scaleb(-int(value["scale"]))


def _fixed(value: Decimal, scale: int) -> FixedPoint:
    return FixedPoint.from_decimal(value, scale)


def build_session_bars(
    records: Iterable[Mapping[str, Any]],
    *,
    interval: timedelta,
    session_starts: Mapping[str, datetime],
    source: str = "curated",
    recipe_version: str = "session-bars-v1",
) -> list[dict[str, Any]]:
    """Aggregate trades without crossing session boundaries or trading-day identities."""
    interval_us = (
        (interval.days * 86_400 + interval.seconds) * 1_000_000 + interval.microseconds
    )
    if interval_us <= 0:
        raise ValidationError("bar interval must be positive")
    grouped: dict[tuple[str, str, str, str, datetime], list[dict[str, Any]]] = defaultdict(list)
    for raw in records:
        record = dict(raw)
        validate_json_record(TRADE_EVENT_SCHEMA_ID, record)
        session_id = str(record["session_id"])
        if session_id not in session_starts:
            raise ValidationError(f"Missing session start for {session_id}")
        start = _utc(session_starts[session_id], f"session_starts[{session_id}]")
        event_time = _utc(str(record["event_time"]), "event_time")
        elapsed_us = int((event_time - start).total_seconds() * 1_000_000)
        if elapsed_us < 0:
            raise ValidationError(f"Trade precedes its session start: {record['event_id']}")
        bucket = start + timedelta(microseconds=(elapsed_us // interval_us) * interval_us)
        key = (
            str(record["instrument_id"]),
            str(record["trading_day"]),
            session_id,
            str(record["source"]),
            bucket,
        )
        grouped[key].append(record)

    bars: list[dict[str, Any]] = []
    for (instrument_id, trading_day, session_id, upstream_source, bar_start), trades in sorted(
        grouped.items(), key=lambda item: item[0]
    ):
        ordered = sorted(trades, key=lambda item: (item["event_time"], item["event_id"]))
        price_scale = max(int(item["price"]["scale"]) for item in ordered)
        quantity_scale = max(int(item["quantity"]["scale"]) for item in ordered)
        prices = [_fixed_decimal(item["price"]) for item in ordered]
        volume = sum((_fixed_decimal(item["quantity"]) for item in ordered), Decimal(0))
        bar_end = bar_start + interval
        received_at = max(
            bar_end,
            max(_utc(str(item["received_at"]), "received_at") for item in ordered),
        )
        available_at = max(
            received_at,
            max(_utc(str(item["available_at"]), "available_at") for item in ordered),
        )
        identity = {
            "recipe_version": recipe_version,
            "upstream_event_ids": [item["event_id"] for item in ordered],
            "bar_start": _utc_text(bar_start),
            "bar_end": _utc_text(bar_end),
        }
        event_id = f"bar-{_hash_bytes(_canonical(identity))[:24]}"
        event = BarEvent(
            event_id=event_id,
            instrument_id=instrument_id,
            event_time=bar_end,
            received_at=received_at,
            available_at=available_at,
            source=source,
            trading_day=datetime.fromisoformat(trading_day).date(),
            session_id=session_id,
            sequence=None,
            bar_start=bar_start,
            bar_end=bar_end,
            open_price=_fixed(prices[0], price_scale),
            high_price=_fixed(max(prices), price_scale),
            low_price=_fixed(min(prices), price_scale),
            close_price=_fixed(prices[-1], price_scale),
            volume=_fixed(volume, quantity_scale),
            is_complete=True,
        )
        payload = market_event_payload(event)
        payload["source"] = source
        validate_json_record(BAR_EVENT_SCHEMA_ID, payload)
        bars.append(payload)
    return bars


def _arrow_ready_bar(record: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(record)
    schema = get_arrow_schema(BAR_EVENT_SCHEMA_ID)
    for field_definition in schema:
        if pa.types.is_timestamp(field_definition.type):
            result[field_definition.name] = _utc(str(result[field_definition.name]), field_definition.name)
        elif pa.types.is_date32(field_definition.type):
            result[field_definition.name] = datetime.fromisoformat(
                str(result[field_definition.name])
            ).date()
    return result


def _snapshot_identity(
    *,
    dataset: str,
    revision_id: str,
    recipe_version: str,
    lineage: Mapping[str, str],
    partitions: tuple[CuratedPartition, ...],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION_V2,
        "layer": "curated",
        "dataset": dataset,
        "revision_id": revision_id,
        "recipe_version": recipe_version,
        "lineage": dict(sorted(lineage.items())),
        "partitions": [
            {
                "relative_path": item.relative_path,
                "trading_date": item.trading_date,
                "instrument_id": item.instrument_id,
                "schema_id": item.schema_id,
                "rows": item.rows,
                "logical_sha256": item.logical_sha256,
            }
            for item in partitions
        ],
    }


def write_curated_bars(
    root: Path,
    bars: Iterable[Mapping[str, Any]],
    *,
    dataset: str,
    revision_id: str,
    recipe_version: str,
    lineage: Mapping[str, str],
) -> CuratedSnapshot:
    """Persist one immutable Curated revision; lineage participates in snapshot identity."""
    dataset = _dataset_segment(dataset)
    if not revision_id.strip() or not recipe_version.strip():
        raise ValidationError("revision_id and recipe_version are required")
    if not lineage or any(not key.strip() or not value.strip() for key, value in lineage.items()):
        raise ValidationError("Curated lineage must contain non-empty immutable references")
    records = [dict(item) for item in bars]
    if not records:
        raise ValidationError("Cannot write an empty Curated snapshot")
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        validate_json_record(BAR_EVENT_SCHEMA_ID, record)
        groups[(str(record["trading_day"]), str(record["instrument_id"]))].append(record)

    curated_root = Path(root) / "curated" / dataset
    staging_root = curated_root / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix="m2-", dir=staging_root))
    partition_items: list[CuratedPartition] = []
    try:
        for (trading_date, instrument_id), group in sorted(groups.items()):
            ordered = sorted(group, key=lambda row: (row["event_time"], row["event_id"]))
            table = pa.Table.from_pylist(
                [_arrow_ready_bar(record) for record in ordered],
                schema=get_arrow_schema(BAR_EVENT_SCHEMA_ID),
            )
            validate_arrow_table(BAR_EVENT_SCHEMA_ID, table)
            partition_logical_sha256 = _hash_bytes(_canonical(ordered))
            relative = Path(
                f"date={trading_date}/instrument={quote(instrument_id, safe='-._')}/data.parquet"
            )
            target = stage / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, target, compression="zstd", use_dictionary=False)
            partition_items.append(
                CuratedPartition(
                    relative_path=relative.as_posix(),
                    trading_date=trading_date,
                    instrument_id=instrument_id,
                    schema_id=BAR_EVENT_SCHEMA_ID,
                    rows=table.num_rows,
                    logical_sha256=partition_logical_sha256,
                    content_sha256=_hash_file(target),
                )
            )
        partitions = tuple(partition_items)
        identity = _snapshot_identity(
            dataset=dataset,
            revision_id=revision_id,
            recipe_version=recipe_version,
            lineage=lineage,
            partitions=partitions,
        )
        logical_sha256 = _hash_bytes(_canonical(identity))
        snapshot_id = f"sha256-{logical_sha256[:24]}"
        snapshot = CuratedSnapshot(
            schema_version=SCHEMA_VERSION_V2,
            layer="curated",
            dataset=dataset,
            snapshot_id=snapshot_id,
            revision_id=revision_id,
            recipe_version=recipe_version,
            created_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            logical_sha256=logical_sha256,
            rows=sum(item.rows for item in partitions),
            lineage=dict(sorted(lineage.items())),
            partitions=partitions,
        )
        (stage / "manifest.json").write_text(
            json.dumps(asdict(snapshot), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        snapshot_dir = curated_root / "snapshots" / snapshot_id
        snapshot_dir.parent.mkdir(parents=True, exist_ok=True)
        if snapshot_dir.exists():
            return load_curated_snapshot(root, dataset, snapshot_id)
        os.replace(stage, snapshot_dir)
        stage = snapshot_dir
        return load_curated_snapshot(root, dataset, snapshot_id)
    finally:
        if stage.exists() and stage.parent == staging_root:
            shutil.rmtree(stage)


def load_curated_snapshot(root: Path, dataset: str, snapshot_id: str) -> CuratedSnapshot:
    dataset = _dataset_segment(dataset)
    if not re.fullmatch(r"sha256-[0-9a-f]{24}", snapshot_id):
        raise ValidationError("Curated reads require an explicit content-addressed snapshot_id")
    snapshot_dir = Path(root) / "curated" / dataset / "snapshots" / snapshot_id
    manifest_path = snapshot_dir / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValidationError(f"Curated snapshot manifest missing: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["partitions"] = tuple(CuratedPartition(**item) for item in payload["partitions"])
    snapshot = CuratedSnapshot(**payload)
    if (
        snapshot.snapshot_id != snapshot_id
        or snapshot.dataset != dataset
        or snapshot.layer != "curated"
        or snapshot.schema_version != SCHEMA_VERSION_V2
    ):
        raise ValidationError("Curated snapshot identity mismatch")
    _utc(snapshot.created_at, "created_at")
    identity = _snapshot_identity(
        dataset=dataset,
        revision_id=snapshot.revision_id,
        recipe_version=snapshot.recipe_version,
        lineage=snapshot.lineage,
        partitions=snapshot.partitions,
    )
    logical_sha256 = _hash_bytes(_canonical(identity))
    if logical_sha256 != snapshot.logical_sha256 or snapshot_id != f"sha256-{logical_sha256[:24]}":
        raise ValidationError("Curated snapshot logical hash changed")
    rows = 0
    expected_files = {Path("manifest.json")}
    seen_paths: set[str] = set()
    for partition in snapshot.partitions:
        if partition.relative_path in seen_paths:
            raise ValidationError("Curated snapshot contains duplicate partition paths")
        seen_paths.add(partition.relative_path)
        if partition.schema_id != BAR_EVENT_SCHEMA_ID:
            raise ValidationError("Curated partition schema is not the frozen Bar schema")
        expected_relative = Path(
            f"date={partition.trading_date}/"
            f"instrument={quote(partition.instrument_id, safe='-._')}/data.parquet"
        ).as_posix()
        if partition.relative_path != expected_relative:
            raise ValidationError("Curated partition path metadata mismatch")
        relative = Path(partition.relative_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValidationError("Unsafe Curated partition path")
        path = snapshot_dir / relative
        expected_files.add(relative)
        if path.is_symlink() or not path.is_file() or snapshot_dir.resolve() not in path.resolve().parents:
            raise ValidationError(f"Curated partition missing or unsafe: {path}")
        if _hash_file(path) != partition.content_sha256:
            raise ValidationError(f"Curated partition hash changed: {path}")
        table = pq.ParquetFile(path).read()
        validate_arrow_table(partition.schema_id, table)
        if table.num_rows != partition.rows:
            raise ValidationError(f"Curated partition row count changed: {path}")
        logical_rows = [_json_value(item) for item in table.to_pylist()]
        if _hash_bytes(_canonical(logical_rows)) != partition.logical_sha256:
            raise ValidationError(f"Curated partition logical content changed: {path}")
        rows += table.num_rows
    if rows != snapshot.rows:
        raise ValidationError("Curated snapshot row count changed")
    actual_files = {
        path.relative_to(snapshot_dir)
        for path in snapshot_dir.rglob("*")
        if path.is_file()
    }
    if actual_files != expected_files:
        raise ValidationError("Curated snapshot contains an unexpected or missing file")
    return snapshot
