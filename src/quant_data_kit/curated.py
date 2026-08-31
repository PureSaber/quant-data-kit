"""Session-aware Curated bars and immutable revision snapshots."""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq

from quant_data_kit.data_lake import (
    StoragePolicy,
    _atomic_write_bytes,
    _mkdir_in_lake,
    _publish_tree_entry,
    _resolved_lake_root,
    _stable_staging_directory,
    _validate_lake_path,
    load_normalized_snapshot,
    read_normalized_events,
    require_collection_capacity,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint
from quant_data_kit.market_events_v2 import BarEvent, market_event_payload
from quant_data_kit.research_contracts_v2 import (
    CuratedAggregation,
    EventBarPartitionEvidence,
    EventSchemaRef,
)
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
    aggregation: CuratedAggregation | None = None


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
    if (
        dataset in {".", "..", "latest", "main"}
        or dataset.rstrip(". ") != dataset
        or dataset.split(".", 1)[0].upper()
        in {
            "AUX",
            "CON",
            "NUL",
            "PRN",
            *(f"COM{i}" for i in range(1, 10)),
            *(f"LPT{i}" for i in range(1, 10)),
        }
    ):
        raise ValidationError("dataset name is reserved")
    return dataset


def _revision_segment(revision_id: str) -> str:
    try:
        return _dataset_segment(revision_id)
    except ValidationError as exc:
        raise ValidationError("revision_id must be one Windows-safe immutable identifier") from exc


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
    interval_us = (interval.days * 86_400 + interval.seconds) * 1_000_000 + interval.microseconds
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
        ordered = sorted(
            trades,
            key=lambda item: (item["event_time"], int(item["sequence"]), item["event_id"]),
        )
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
            sequence=(bar_end - datetime(1970, 1, 1, tzinfo=timezone.utc))
            // timedelta(microseconds=1),
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


def _bar_from_trade_group(
    trades: list[dict[str, Any]],
    *,
    bar_start: datetime,
    bar_end: datetime,
    source: str,
    recipe_version: str,
    identity_extra: Mapping[str, Any],
    output_session_id: str | None = None,
) -> dict[str, Any]:
    if not trades:
        raise ValidationError("Cannot build a Bar from no trades")
    ordered = sorted(
        trades,
        key=lambda item: (
            _utc(str(item["event_time"]), "event_time"),
            int(item["sequence"]),
            str(item["event_id"]),
        ),
    )
    if not bar_start < bar_end:
        raise ValidationError("Bar boundaries must be strictly positive")
    price_scale = max(int(item["price"]["scale"]) for item in ordered)
    quantity_scale = max(int(item["quantity"]["scale"]) for item in ordered)
    prices = [_fixed_decimal(item["price"]) for item in ordered]
    volume = sum((_fixed_decimal(item["quantity"]) for item in ordered), Decimal(0))
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
        **dict(identity_extra),
    }
    event = BarEvent(
        event_id=f"bar-{_hash_bytes(_canonical(identity))[:24]}",
        instrument_id=str(ordered[0]["instrument_id"]),
        event_time=bar_end,
        received_at=received_at,
        available_at=available_at,
        source=source,
        trading_day=datetime.fromisoformat(str(ordered[0]["trading_day"])).date(),
        session_id=output_session_id or str(ordered[-1]["session_id"]),
        sequence=int(ordered[-1]["sequence"]),
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
    return payload


def build_session_rollup_bars(
    records: Iterable[Mapping[str, Any]],
    *,
    session_boundaries: Mapping[str, tuple[datetime, datetime]],
    session_rollup: str,
    trading_day_boundaries: Mapping[tuple[str, str], tuple[datetime, datetime, str]] | None = None,
    instrument_venues: Mapping[str, str] | None = None,
    source: str = "curated",
    recipe_version: str = "session-rollup-v1",
) -> list[dict[str, Any]]:
    """Aggregate complete session or trading-day Bars from frozen trade events."""
    if session_rollup not in {"session", "trading_day"}:
        raise ValidationError("session_rollup must be session or trading_day")
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for raw in records:
        record = dict(raw)
        validate_json_record(TRADE_EVENT_SCHEMA_ID, record)
        session_id = str(record["session_id"])
        if session_id not in session_boundaries:
            raise ValidationError(f"Missing authoritative boundary for {session_id}")
        key = (
            str(record["instrument_id"]),
            str(record["trading_day"]),
            str(record["source"]),
            *([session_id] if session_rollup == "session" else []),
        )
        grouped[key].append(record)
    bars: list[dict[str, Any]] = []
    for key, trades in sorted(grouped.items()):
        referenced = {str(item["session_id"]) for item in trades}
        output_session_id: str | None = None
        if session_rollup == "trading_day":
            instrument_id, trading_day = key[:2]
            venue = (instrument_venues or {}).get(instrument_id)
            boundary = (trading_day_boundaries or {}).get((str(venue), trading_day))
            if venue is None or boundary is None:
                raise ValidationError(
                    f"Missing authoritative trading-day boundary for {instrument_id}/{trading_day}"
                )
            starts = [_utc(boundary[0], f"{trading_day}.opens_at")]
            ends = [_utc(boundary[1], f"{trading_day}.closes_at")]
            output_session_id = boundary[2]
        else:
            starts = [_utc(session_boundaries[item][0], f"{item}.opens_at") for item in referenced]
            ends = [_utc(session_boundaries[item][1], f"{item}.closes_at") for item in referenced]
        bars.append(
            _bar_from_trade_group(
                trades,
                bar_start=min(starts),
                bar_end=max(ends),
                source=source,
                recipe_version=recipe_version,
                identity_extra={"session_rollup": session_rollup, "group": list(key)},
                output_session_id=output_session_id,
            )
        )
    return bars


def build_event_bars(
    records: Iterable[Mapping[str, Any]],
    *,
    basis: str,
    threshold: FixedPoint,
    session_starts: Mapping[str, datetime],
    source: str = "curated",
    recipe_version: str = "event-bars-v1",
    require_complete: bool = True,
) -> list[dict[str, Any]]:
    """Build deterministic trade-count, base-volume, or quote-notional Bars."""
    if basis not in {"trade_count", "base_volume", "quote_notional"}:
        raise ValidationError("unsupported event-bar basis")
    if not isinstance(threshold, FixedPoint) or not threshold.is_positive():
        raise ValidationError("event-bar threshold must be a positive FixedPoint")
    if basis == "trade_count" and threshold.scale != 0:
        raise ValidationError("trade_count threshold must have scale zero")
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for raw in records:
        record = dict(raw)
        validate_json_record(TRADE_EVENT_SCHEMA_ID, record)
        session_id = str(record["session_id"])
        if session_id not in session_starts:
            raise ValidationError(f"Missing session start for {session_id}")
        grouped[
            (
                str(record["source"]),
                str(record["instrument_id"]),
                str(record["trading_day"]),
                session_id,
            )
        ].append(record)

    threshold_value = threshold.to_decimal()
    bars: list[dict[str, Any]] = []
    for stream, trades in sorted(grouped.items()):
        ordered = sorted(
            trades,
            key=lambda item: (
                _utc(str(item["event_time"]), "event_time"),
                int(item["sequence"]),
                str(item["event_id"]),
            ),
        )
        prior_identity: tuple[datetime, int, str] | None = None
        bar_start = _utc(session_starts[stream[3]], f"{stream[3]}.opens_at")
        bucket: list[dict[str, Any]] = []
        accumulated = Decimal(0)
        for trade in ordered:
            identity = (
                _utc(str(trade["event_time"]), "event_time"),
                int(trade["sequence"]),
                str(trade["event_id"]),
            )
            if prior_identity is not None and identity <= prior_identity:
                raise ValidationError(f"event-bar source stream is not strictly ordered: {stream}")
            prior_identity = identity
            bucket.append(trade)
            if basis == "trade_count":
                accumulated += 1
            elif basis == "base_volume":
                accumulated += _fixed_decimal(trade["quantity"])
            else:
                accumulated += _fixed_decimal(trade["price"]) * _fixed_decimal(trade["quantity"])
            if accumulated < threshold_value:
                continue
            bar_end = identity[0]
            bars.append(
                _bar_from_trade_group(
                    bucket,
                    bar_start=bar_start,
                    bar_end=bar_end,
                    source=source,
                    recipe_version=recipe_version,
                    identity_extra={
                        "event_bar_basis": basis,
                        "event_bar_threshold": {
                            "units": str(threshold.units),
                            "scale": threshold.scale,
                        },
                    },
                )
            )
            bar_start = bar_end
            bucket = []
            accumulated = Decimal(0)
        if bucket and require_complete:
            raise ValidationError(f"event-bar source stream ends below threshold: {stream}")
    return bars


def _arrow_ready_bar(record: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(record)
    schema = get_arrow_schema(BAR_EVENT_SCHEMA_ID)
    for field_definition in schema:
        if pa.types.is_timestamp(field_definition.type):
            result[field_definition.name] = _utc(
                str(result[field_definition.name]), field_definition.name
            )
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
    created_at: str,
    lineage: Mapping[str, str],
    partitions: tuple[CuratedPartition, ...],
    aggregation: CuratedAggregation | None = None,
) -> dict[str, Any]:
    identity = {
        "schema_version": SCHEMA_VERSION_V2,
        "layer": "curated",
        "dataset": dataset,
        "revision_id": revision_id,
        "recipe_version": recipe_version,
        "created_at": created_at,
        "lineage": dict(sorted(lineage.items())),
        "partitions": [asdict(item) for item in partitions],
    }
    if aggregation is not None:
        identity["aggregation"] = aggregation.to_contract()
    return identity


def _snapshot_manifest(snapshot: CuratedSnapshot) -> dict[str, Any]:
    return {
        "schema_version": snapshot.schema_version,
        "layer": snapshot.layer,
        "dataset": snapshot.dataset,
        "snapshot_id": snapshot.snapshot_id,
        "revision_id": snapshot.revision_id,
        "recipe_version": snapshot.recipe_version,
        "created_at": snapshot.created_at,
        "logical_sha256": snapshot.logical_sha256,
        "rows": snapshot.rows,
        "lineage": dict(snapshot.lineage),
        "partitions": [asdict(item) for item in snapshot.partitions],
        "aggregation": snapshot.aggregation.to_contract() if snapshot.aggregation else None,
    }


def _revision_record(snapshot: CuratedSnapshot) -> dict[str, str]:
    record = {
        "schema_version": SCHEMA_VERSION_V2,
        "dataset": snapshot.dataset,
        "revision_id": snapshot.revision_id,
        "snapshot_id": snapshot.snapshot_id,
        "logical_sha256": snapshot.logical_sha256,
    }
    record["anchor_sha256"] = _hash_bytes(_canonical(record))
    return record


def _validate_revision_record(
    actual: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> None:
    actual_identity = dict(actual)
    actual_anchor = actual_identity.pop("anchor_sha256", None)
    if actual_anchor != _hash_bytes(_canonical(actual_identity)):
        raise ValidationError("Curated revision registry integrity changed")
    expected_identity = {key: value for key, value in expected.items() if key != "anchor_sha256"}
    if actual_identity != expected_identity:
        raise ValidationError(
            "Curated revision maps to different content: "
            f"{expected_identity['dataset']}/{expected_identity['revision_id']}"
        )


def _recover_missing_revision(
    root: Path,
    dataset: str,
    expected: Mapping[str, str],
    revision_path: Path,
) -> bool:
    """Recover a deleted revision accelerator from immutable snapshot manifests."""
    snapshots_root = Path(root) / "curated" / dataset / "snapshots"
    if not snapshots_root.exists():
        return False
    matched = False
    for snapshot_dir in sorted(snapshots_root.glob("sha256-*")):
        if not snapshot_dir.is_dir():
            raise ValidationError(f"Curated snapshot entry is not a directory: {snapshot_dir}")
        snapshot = _load_curated_snapshot(
            root,
            dataset,
            snapshot_dir.name,
            verify_revision_registry=False,
        )
        if snapshot.revision_id != expected["revision_id"]:
            continue
        _validate_revision_record(_revision_record(snapshot), expected)
        matched = True
    if not matched:
        return False
    _atomic_write_bytes(
        root,
        revision_path,
        json.dumps(expected, indent=2, ensure_ascii=False).encode("utf-8"),
    )
    _validate_revision_record(
        json.loads(revision_path.read_text(encoding="utf-8")),
        expected,
    )
    return True


def _publish_curated_snapshot(
    lake_root: Path,
    *,
    stage: Path,
    snapshot: CuratedSnapshot,
    policy: StoragePolicy,
) -> CuratedSnapshot:
    """Publish one prepared snapshot while its dataset/revision lock is held."""
    curated_root = Path(lake_root) / "curated" / snapshot.dataset
    snapshot_dir = curated_root / "snapshots" / snapshot.snapshot_id
    _mkdir_in_lake(lake_root, snapshot_dir.parent)
    _validate_lake_path(lake_root, snapshot_dir, allow_missing=True)
    revision_root = _mkdir_in_lake(lake_root, curated_root / "revisions")
    revision_path = revision_root / f"{snapshot.revision_id}.json"
    expected_revision = _revision_record(snapshot)
    if revision_path.exists():
        checked_revision = _validate_lake_path(
            lake_root,
            revision_path,
            allow_missing=False,
        )
        _validate_revision_record(
            json.loads(checked_revision.read_text(encoding="utf-8")),
            expected_revision,
        )
    else:
        _recover_missing_revision(
            lake_root,
            snapshot.dataset,
            expected_revision,
            revision_path,
        )
    if snapshot_dir.exists():
        existing = _load_curated_snapshot(
            lake_root,
            snapshot.dataset,
            snapshot.snapshot_id,
            verify_revision_registry=False,
        )
        if existing != snapshot:
            raise ValidationError(f"Curated snapshot collision: {snapshot_dir}")
    else:
        _publish_tree_entry(lake_root, stage, snapshot_dir, policy=policy)
    if not revision_path.exists():
        _atomic_write_bytes(
            lake_root,
            revision_path,
            json.dumps(expected_revision, indent=2, ensure_ascii=False).encode("utf-8"),
        )
    return load_curated_snapshot(lake_root, snapshot.dataset, snapshot.snapshot_id)


def _write_curated_bars(
    root: Path,
    bars: Iterable[Mapping[str, Any]],
    *,
    dataset: str,
    revision_id: str,
    recipe_version: str,
    normalized_snapshot_id: str,
    policy: StoragePolicy,
    aggregation: CuratedAggregation | None = None,
) -> CuratedSnapshot:
    """Persist bars only after loading their exact immutable Normalized lineage."""
    lake_root = _resolved_lake_root(root, create=False)
    dataset = _dataset_segment(dataset)
    revision_id = _revision_segment(revision_id)
    if not recipe_version.strip():
        raise ValidationError("recipe_version is required")
    if aggregation is not None and aggregation.recipe_version != recipe_version:
        raise ValidationError("aggregation recipe_version must match the Curated recipe")
    normalized = load_normalized_snapshot(lake_root, normalized_snapshot_id)
    lineage = {
        "normalized_snapshot_id": normalized.snapshot_id,
        "normalized_logical_sha256": normalized.logical_sha256,
    }
    records = [dict(item) for item in bars]
    if not records:
        raise ValidationError("Cannot write an empty Curated snapshot")
    event_partitioned = aggregation is not None and aggregation.kind == "event_bar"
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        validate_json_record(BAR_EVENT_SCHEMA_ID, record)
        key = (str(record["trading_day"]), str(record["instrument_id"]))
        if event_partitioned:
            key += (str(record["session_id"]),)
        groups[key].append(record)

    estimated_bytes = sum(len(_canonical(_json_value(item))) for item in records)
    curated_root = _mkdir_in_lake(lake_root, lake_root / "curated" / dataset)
    staging_root = curated_root / "staging"
    partition_items: list[CuratedPartition] = []
    revision_identity = {"dataset": dataset, "revision_id": revision_id}
    with _stable_staging_directory(
        lake_root,
        staging_root,
        namespace="curated-revision",
        identity=revision_identity,
    ) as stage:
        require_collection_capacity(
            lake_root,
            projected_write_bytes=estimated_bytes,
            policy=policy,
        )
        for key, group in sorted(groups.items()):
            trading_date, instrument_id = key[:2]
            ordered = sorted(
                group,
                key=lambda row: (row["event_time"], int(row["sequence"]), row["event_id"]),
            )
            table = pa.Table.from_pylist(
                [_arrow_ready_bar(record) for record in ordered],
                schema=get_arrow_schema(BAR_EVENT_SCHEMA_ID),
            )
            validate_arrow_table(BAR_EVENT_SCHEMA_ID, table)
            partition_root = f"date={trading_date}/instrument={quote(instrument_id, safe='-._')}"
            if event_partitioned:
                partition_root += f"/session={quote(key[2], safe='-._')}"
            relative = Path(f"{partition_root}/data.parquet")
            target = stage / relative
            _mkdir_in_lake(lake_root, target.parent)
            pq.write_table(table, target, compression="zstd", use_dictionary=False)
            logical_rows = [_json_value(item) for item in table.to_pylist()]
            partition_items.append(
                CuratedPartition(
                    relative_path=relative.as_posix(),
                    trading_date=trading_date,
                    instrument_id=instrument_id,
                    schema_id=BAR_EVENT_SCHEMA_ID,
                    rows=table.num_rows,
                    logical_sha256=_hash_bytes(_canonical(logical_rows)),
                    content_sha256=_hash_file(target),
                )
            )
        partitions = tuple(partition_items)
        created_at = _utc_text(
            max(_utc(str(record["available_at"]), "available_at") for record in records)
        )
        identity = _snapshot_identity(
            dataset=dataset,
            revision_id=revision_id,
            recipe_version=recipe_version,
            created_at=created_at,
            lineage=lineage,
            partitions=partitions,
            aggregation=aggregation,
        )
        logical_sha256 = _hash_bytes(_canonical(identity))
        snapshot_id = f"sha256-{logical_sha256}"
        snapshot = CuratedSnapshot(
            schema_version=SCHEMA_VERSION_V2,
            layer="curated",
            dataset=dataset,
            snapshot_id=snapshot_id,
            revision_id=revision_id,
            recipe_version=recipe_version,
            created_at=created_at,
            logical_sha256=logical_sha256,
            rows=sum(item.rows for item in partitions),
            lineage=dict(sorted(lineage.items())),
            partitions=partitions,
            aggregation=aggregation,
        )
        (stage / "manifest.json").write_text(
            json.dumps(_snapshot_manifest(snapshot), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return _publish_curated_snapshot(
            lake_root,
            stage=stage,
            snapshot=snapshot,
            policy=policy,
        )


def curate_trade_bars_from_snapshot(
    root: Path,
    *,
    normalized_snapshot_id: str,
    dataset: str,
    revision_id: str,
    recipe_version: str,
    interval: timedelta,
    session_starts: Mapping[str, datetime],
    source: str = "curated",
    market_context_snapshot_id: str | None = None,
    policy: StoragePolicy | None = None,
) -> CuratedSnapshot:
    """Certified public path: fixed Normalized trade snapshot to immutable Curated bars."""
    normalized = load_normalized_snapshot(root, normalized_snapshot_id)
    resolved_policy = policy or StoragePolicy()
    trades = read_normalized_events(root, normalized.snapshot_id, event_type="trade")
    if not trades:
        raise ValidationError("Normalized snapshot contains no trades to curate")
    bars = build_session_bars(
        trades,
        interval=interval,
        session_starts=session_starts,
        source=source,
        recipe_version=recipe_version,
    )
    aggregation: CuratedAggregation | None = None
    if market_context_snapshot_id is not None:
        from quant_data_kit.research_inputs_v2 import load_market_context_snapshot

        context = load_market_context_snapshot(root, market_context_snapshot_id)
        authoritative_starts = {item.session_id: item.opens_at for item in context.sessions}
        used_session_ids = {str(item["session_id"]) for item in trades}
        for session_id in used_session_ids:
            if session_id not in session_starts or session_id not in authoritative_starts:
                raise ValidationError(f"Missing market-context session start for {session_id}")
            if _utc(session_starts[session_id], session_id) != authoritative_starts[session_id]:
                raise ValidationError(
                    f"session_starts differs from market context for {session_id}"
                )
        interval_us = (
            interval.days * 86_400 + interval.seconds
        ) * 1_000_000 + interval.microseconds
        aggregation = CuratedAggregation(
            calendar_id=context.calendar_id,
            session_policy_version=context.session_policy_version,
            kind="fixed_time_bar",
            recipe_version=recipe_version,
            interval_ns=interval_us * 1_000,
            market_context_snapshot_id=context.snapshot_id,
            market_context_logical_sha256=context.logical_sha256,
            source_event_schemas=(EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),),
        )
    return _write_curated_bars(
        root,
        bars,
        dataset=dataset,
        revision_id=revision_id,
        recipe_version=recipe_version,
        normalized_snapshot_id=normalized.snapshot_id,
        policy=resolved_policy,
        aggregation=aggregation,
    )


def curate_session_bars_from_snapshot(
    root: Path,
    *,
    normalized_snapshot_id: str,
    dataset: str,
    revision_id: str,
    recipe_version: str,
    session_rollup: str,
    market_context_snapshot_id: str,
    source: str = "curated",
    policy: StoragePolicy | None = None,
) -> CuratedSnapshot:
    """Create one complete Bar per authoritative session or trading day."""
    from quant_data_kit.research_inputs_v2 import load_market_context_snapshot

    normalized = load_normalized_snapshot(root, normalized_snapshot_id)
    context = load_market_context_snapshot(root, market_context_snapshot_id)
    trades = read_normalized_events(root, normalized.snapshot_id, event_type="trade")
    if not trades:
        raise ValidationError("Normalized snapshot contains no trades to curate")
    boundaries = {item.session_id: (item.opens_at, item.closes_at) for item in context.sessions}
    sessions_by_venue_day: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for session in context.sessions:
        sessions_by_venue_day[(session.venue, session.trading_day.isoformat())].append(session)
    day_boundaries = {
        key: (
            min(item.opens_at for item in values),
            max(item.closes_at for item in values),
            max(values, key=lambda item: item.closes_at).session_id,
        )
        for key, values in sessions_by_venue_day.items()
    }
    instrument_venues = {item.instrument_id: item.venue for item in context.instruments}
    bars = build_session_rollup_bars(
        trades,
        session_boundaries=boundaries,
        session_rollup=session_rollup,
        trading_day_boundaries=day_boundaries,
        instrument_venues=instrument_venues,
        source=source,
        recipe_version=recipe_version,
    )
    aggregation = CuratedAggregation(
        calendar_id=context.calendar_id,
        session_policy_version=context.session_policy_version,
        kind="session_bar",
        recipe_version=recipe_version,
        session_rollup=session_rollup,
        market_context_snapshot_id=context.snapshot_id,
        market_context_logical_sha256=context.logical_sha256,
        source_event_schemas=(EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),),
    )
    return _write_curated_bars(
        root,
        bars,
        dataset=dataset,
        revision_id=revision_id,
        recipe_version=recipe_version,
        normalized_snapshot_id=normalized.snapshot_id,
        policy=policy or StoragePolicy(),
        aggregation=aggregation,
    )


def _source_selection_sha256(records: list[dict[str, Any]]) -> str:
    payload = {
        "algorithm": "puresaber.event-selection-canonical-json@1.0.0",
        "schema_id": TRADE_EVENT_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION_V2,
        "records": records,
    }
    return _hash_bytes(_canonical(payload))


def curate_trade_event_bars_from_snapshot(
    root: Path,
    *,
    normalized_snapshot_id: str,
    dataset: str,
    revision_id: str,
    recipe_version: str,
    basis: str,
    threshold: FixedPoint,
    market_context_snapshot_id: str,
    source: str = "curated",
    policy: StoragePolicy | None = None,
) -> CuratedSnapshot:
    """Create certified event Bars with source-range evidence for every stream."""
    from quant_data_kit.research_inputs_v2 import load_market_context_snapshot

    normalized = load_normalized_snapshot(root, normalized_snapshot_id)
    context = load_market_context_snapshot(root, market_context_snapshot_id)
    trades = [
        dict(item)
        for item in read_normalized_events(root, normalized.snapshot_id, event_type="trade")
    ]
    if not trades:
        raise ValidationError("Normalized snapshot contains no trades to curate")
    upstream_sources = {str(item["source"]) for item in trades}
    if len(upstream_sources) != 1:
        raise ValidationError("certified event Bars require one upstream source per snapshot")
    session_starts = {item.session_id: item.opens_at for item in context.sessions}
    bars = build_event_bars(
        trades,
        basis=basis,
        threshold=threshold,
        session_starts=session_starts,
        source=source,
        recipe_version=recipe_version,
        require_complete=True,
    )
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in trades:
        grouped[
            (
                str(record["source"]),
                str(record["instrument_id"]),
                str(record["trading_day"]),
                str(record["session_id"]),
            )
        ].append(record)
    evidence: list[EventBarPartitionEvidence] = []
    for (upstream_source, instrument_id, trading_day, session_id), rows in sorted(grouped.items()):
        ordered = sorted(
            rows,
            key=lambda item: (
                _utc(str(item["event_time"]), "event_time"),
                int(item["sequence"]),
                str(item["event_id"]),
            ),
        )
        relative_path = Path(
            f"date={trading_day}/instrument={quote(instrument_id, safe='-._')}/"
            f"session={quote(session_id, safe='-._')}/data.parquet"
        ).as_posix()
        evidence.append(
            EventBarPartitionEvidence(
                relative_path=relative_path,
                source=upstream_source,
                instrument_id=instrument_id,
                session_id=session_id,
                first_sequence=int(ordered[0]["sequence"]),
                last_sequence=int(ordered[-1]["sequence"]),
                first_event_id=str(ordered[0]["event_id"]),
                last_event_id=str(ordered[-1]["event_id"]),
                event_count=len(ordered),
                source_selection_sha256=_source_selection_sha256(ordered),
            )
        )
    ordered_evidence = tuple(
        sorted(
            evidence,
            key=lambda item: (
                item.stream_key,
                item.first_sequence,
                item.last_sequence,
                item.relative_path,
            ),
        )
    )
    aggregation = CuratedAggregation(
        calendar_id=context.calendar_id,
        session_policy_version=context.session_policy_version,
        kind="event_bar",
        recipe_version=recipe_version,
        event_bar_basis=basis,
        event_bar_threshold=threshold,
        market_context_snapshot_id=context.snapshot_id,
        market_context_logical_sha256=context.logical_sha256,
        source_event_schemas=(EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),),
        partition_evidence=ordered_evidence,
    )
    return _write_curated_bars(
        root,
        bars,
        dataset=dataset,
        revision_id=revision_id,
        recipe_version=recipe_version,
        normalized_snapshot_id=normalized.snapshot_id,
        policy=policy or StoragePolicy(),
        aggregation=aggregation,
    )


def _validate_curated_partition_table(
    partition: CuratedPartition,
    table: pa.Table,
    aggregation: CuratedAggregation | None,
) -> str | None:
    """Bind partition metadata, source order and event-bar scope to actual rows."""
    validate_arrow_table(partition.schema_id, table)
    if table.num_rows != partition.rows:
        raise ValidationError("Curated partition row count changed")
    rows = table.to_pylist()
    previous: tuple[datetime, int, str] | None = None
    session_ids: set[str] = set()
    for row in rows:
        trading_day = row["trading_day"]
        trading_day_text = (
            trading_day.isoformat() if isinstance(trading_day, date) else str(trading_day)
        )
        if str(row["instrument_id"]) != partition.instrument_id:
            raise ValidationError("Curated row instrument does not match partition metadata")
        if trading_day_text != partition.trading_date:
            raise ValidationError("Curated row trading_day does not match partition metadata")
        identity = (
            _utc(row["event_time"], "event_time"),
            int(row["sequence"]),
            str(row["event_id"]),
        )
        if previous is not None and identity <= previous:
            raise ValidationError("Curated partition rows are not strictly ordered")
        previous = identity
        session_ids.add(str(row["session_id"]))
    if aggregation is not None and aggregation.kind == "event_bar":
        if len(session_ids) != 1:
            raise ValidationError("event-bar partition must contain exactly one session")
        return next(iter(session_ids))
    return None


def _load_curated_snapshot(
    root: Path,
    dataset: str,
    snapshot_id: str,
    *,
    verify_revision_registry: bool,
) -> CuratedSnapshot:
    lake_root = _resolved_lake_root(root, create=False)
    dataset = _dataset_segment(dataset)
    if not re.fullmatch(r"sha256-[0-9a-f]{64}", snapshot_id):
        raise ValidationError("Curated reads require an explicit content-addressed snapshot_id")
    snapshot_dir = _validate_lake_path(
        lake_root,
        lake_root / "curated" / dataset / "snapshots" / snapshot_id,
        allow_missing=False,
    )
    manifest_path = _validate_lake_path(
        lake_root,
        snapshot_dir / "manifest.json",
        allow_missing=False,
    )
    if not manifest_path.is_file():
        raise ValidationError(f"Curated snapshot manifest missing: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["partitions"] = tuple(CuratedPartition(**item) for item in payload["partitions"])
    raw_aggregation = payload.get("aggregation")
    payload["aggregation"] = (
        CuratedAggregation.from_contract(raw_aggregation) if raw_aggregation is not None else None
    )
    snapshot = CuratedSnapshot(**payload)
    if (
        snapshot.snapshot_id != snapshot_id
        or snapshot.dataset != dataset
        or snapshot.layer != "curated"
        or snapshot.schema_version != SCHEMA_VERSION_V2
    ):
        raise ValidationError("Curated snapshot identity mismatch")
    _utc(snapshot.created_at, "created_at")
    if set(snapshot.lineage) != {
        "normalized_snapshot_id",
        "normalized_logical_sha256",
    }:
        raise ValidationError("Curated lineage is not an exact Normalized snapshot reference")
    normalized = load_normalized_snapshot(
        lake_root,
        snapshot.lineage["normalized_snapshot_id"],
    )
    if normalized.logical_sha256 != snapshot.lineage["normalized_logical_sha256"]:
        raise ValidationError("Curated Normalized lineage hash changed")
    identity = _snapshot_identity(
        dataset=dataset,
        revision_id=snapshot.revision_id,
        recipe_version=snapshot.recipe_version,
        created_at=snapshot.created_at,
        lineage=snapshot.lineage,
        partitions=snapshot.partitions,
        aggregation=snapshot.aggregation,
    )
    logical_sha256 = _hash_bytes(_canonical(identity))
    if logical_sha256 != snapshot.logical_sha256 or snapshot_id != f"sha256-{logical_sha256}":
        raise ValidationError("Curated snapshot logical hash changed")
    rows = 0
    expected_files = {Path("manifest.json")}
    seen_paths: set[str] = set()
    event_evidence_by_path = {
        item.relative_path: item
        for item in (
            snapshot.aggregation.partition_evidence
            if snapshot.aggregation is not None
            and snapshot.aggregation.kind == "event_bar"
            and snapshot.aggregation.partition_evidence is not None
            else ()
        )
    }
    for partition in snapshot.partitions:
        if partition.relative_path in seen_paths:
            raise ValidationError("Curated snapshot contains duplicate partition paths")
        seen_paths.add(partition.relative_path)
        if partition.schema_id != BAR_EVENT_SCHEMA_ID:
            raise ValidationError("Curated partition schema is not the frozen Bar schema")
        partition_root = (
            f"date={partition.trading_date}/instrument={quote(partition.instrument_id, safe='-._')}"
        )
        expected_event_session: str | None = None
        if snapshot.aggregation is not None and snapshot.aggregation.kind == "event_bar":
            evidence = event_evidence_by_path.get(partition.relative_path)
            if evidence is None or evidence.instrument_id != partition.instrument_id:
                raise ValidationError("Curated partition path metadata mismatch")
            expected_event_session = evidence.session_id
            partition_root += f"/session={quote(expected_event_session, safe='-._')}"
        expected_relative = Path(f"{partition_root}/data.parquet").as_posix()
        if partition.relative_path != expected_relative:
            raise ValidationError("Curated partition path metadata mismatch")
        relative = Path(partition.relative_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValidationError("Unsafe Curated partition path")
        path = _validate_lake_path(lake_root, snapshot_dir / relative, allow_missing=False)
        expected_files.add(relative)
        if not path.is_file() or snapshot_dir.resolve() not in path.resolve().parents:
            raise ValidationError(f"Curated partition missing or unsafe: {path}")
        if _hash_file(path) != partition.content_sha256:
            raise ValidationError(f"Curated partition hash changed: {path}")
        table = pq.ParquetFile(path).read()
        event_session_id = _validate_curated_partition_table(partition, table, snapshot.aggregation)
        if event_session_id != expected_event_session:
            raise ValidationError("event-bar evidence session differs from partition rows")
        logical_rows = [_json_value(item) for item in table.to_pylist()]
        if _hash_bytes(_canonical(logical_rows)) != partition.logical_sha256:
            raise ValidationError(f"Curated partition logical content changed: {path}")
        rows += table.num_rows
    if event_evidence_by_path and set(event_evidence_by_path) != seen_paths:
        raise ValidationError("event-bar evidence does not cover Curated partitions")
    if rows != snapshot.rows:
        raise ValidationError("Curated snapshot row count changed")
    actual_files = {
        path.relative_to(snapshot_dir) for path in snapshot_dir.rglob("*") if path.is_file()
    }
    if actual_files != expected_files:
        raise ValidationError("Curated snapshot contains an unexpected or missing file")
    if verify_revision_registry:
        revision_path = _validate_lake_path(
            lake_root,
            lake_root / "curated" / dataset / "revisions" / f"{snapshot.revision_id}.json",
            allow_missing=False,
        )
        _validate_revision_record(
            json.loads(revision_path.read_text(encoding="utf-8")),
            _revision_record(snapshot),
        )
    return snapshot


def load_curated_snapshot(root: Path, dataset: str, snapshot_id: str) -> CuratedSnapshot:
    return _load_curated_snapshot(
        root,
        dataset,
        snapshot_id,
        verify_revision_registry=True,
    )
