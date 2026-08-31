"""Fail-closed M8 factories for immutable Curated and Normalized research inputs."""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import ipc

from quant_data_kit.curated import (
    _validate_curated_partition_table,
    build_event_bars,
    load_curated_snapshot,
)
from quant_data_kit.data_lake import (
    StoragePolicy,
    _json_evidence,
    _lake_lock,
    _mkdir_in_lake,
    _publish_tree_entry,
    _resolved_lake_root,
    _stable_staging_directory,
    _validate_lake_path,
    load_normalized_snapshot,
    require_collection_capacity,
)
from quant_data_kit.domain_v2 import (
    AssetClass,
    InstrumentSpec,
    MarginMode,
    SessionPhase,
    TradingSession,
    dataclass_payload,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint
from quant_data_kit.l2_replay import L2ReplayError, replay_l2
from quant_data_kit.market_clock_v2 import MarketClock
from quant_data_kit.research_contracts_v2 import (
    MARKET_CONTEXT_SCHEMA_ID,
    CuratedAggregation,
    EventBarPartitionEvidence,
    EventSchemaRef,
    LineageRef,
    VerifiedFactorInput,
)
from quant_data_kit.schemas_v2 import (
    BAR_EVENT_SCHEMA_ID,
    BOOK_DELTA_EVENT_SCHEMA_ID,
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    CORPORATE_ACTION_EVENT_SCHEMA_ID,
    FUNDING_RATE_EVENT_SCHEMA_ID,
    MARK_PRICE_EVENT_SCHEMA_ID,
    QUOTE_EVENT_SCHEMA_ID,
    SCHEMA_VERSION_V2,
    STATUS_EVENT_SCHEMA_ID,
    TRADE_EVENT_SCHEMA_ID,
    get_arrow_schema,
    validate_arrow_table,
    validate_json_record,
)

_SNAPSHOT_ID = re.compile(r"^sha256-[0-9a-f]{64}$")
_EVENT_TYPE_BY_SCHEMA = {
    BOOK_DELTA_EVENT_SCHEMA_ID: "book_delta",
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID: "book_snapshot",
    CORPORATE_ACTION_EVENT_SCHEMA_ID: "corporate_action",
    FUNDING_RATE_EVENT_SCHEMA_ID: "funding_rate",
    MARK_PRICE_EVENT_SCHEMA_ID: "mark_price",
    QUOTE_EVENT_SCHEMA_ID: "quote",
    STATUS_EVENT_SCHEMA_ID: "status",
    TRADE_EVENT_SCHEMA_ID: "trade",
}
_INT64_MAX = 2**63 - 1


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


def _utc(value: str | datetime, field_name: str) -> datetime:
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(value.replace("Z", "+00:00"))
        )
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be a valid UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValidationError(f"{field_name} must be UTC-aware")
    return parsed.astimezone(timezone.utc)


def _parse_optional_utc(value: Any, field_name: str) -> datetime | None:
    return None if value is None else _utc(value, field_name)


def _fixed(payload: Mapping[str, Any], field_name: str) -> FixedPoint:
    if not isinstance(payload, Mapping) or set(payload) != {"units", "scale"}:
        raise ValidationError(f"{field_name} must be a closed FixedPoint object")
    return FixedPoint(units=payload["units"], scale=payload["scale"])


def _arrow_ready(record: Mapping[str, Any], schema: pa.Schema) -> dict[str, Any]:
    result = dict(record)
    for field_definition in schema:
        value = result.get(field_definition.name)
        if value is None:
            continue
        if pa.types.is_timestamp(field_definition.type):
            result[field_definition.name] = _utc(value, field_definition.name)
        elif pa.types.is_date32(field_definition.type):
            result[field_definition.name] = (
                value if isinstance(value, date) else date.fromisoformat(str(value))
            )
    return result


def _table_logical_sha256(table: pa.Table) -> str:
    combined = table.combine_chunks()
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, combined.schema) as writer:
        writer.write_table(combined)
    return _hash_bytes(sink.getvalue().to_pybytes())


def _file_stamp(path: Path) -> tuple[int, int, int, int, int]:
    stat = path.stat()
    return (
        stat.st_dev,
        stat.st_ino,
        stat.st_size,
        stat.st_mtime_ns,
        stat.st_ctime_ns,
    )


def _read_content_bound_parquet(
    path: Path,
    expected_content_sha256: str,
) -> tuple[pa.Table, tuple[int, int, int, int, int]]:
    """Hash and parse the same in-memory bytes, rejecting concurrent replacement."""
    before = _file_stamp(path)
    payload = path.read_bytes()
    after_read = _file_stamp(path)
    if before != after_read:
        raise ValidationError(f"snapshot partition changed while reading: {path}")
    if _hash_bytes(payload) != expected_content_sha256:
        raise ValidationError(f"snapshot partition bytes differ from its manifest: {path}")
    try:
        table = pq.ParquetFile(pa.BufferReader(payload)).read()
    except (OSError, pa.ArrowException) as exc:
        raise ValidationError(f"snapshot partition is not readable Parquet: {path}") from exc
    after_parse = _file_stamp(path)
    if after_read != after_parse:
        raise ValidationError(f"snapshot partition changed while parsing: {path}")
    return table, after_parse


def _assert_file_stamps(
    stamps: Mapping[Path, tuple[int, int, int, int, int]],
) -> None:
    for path, expected in stamps.items():
        try:
            actual = _file_stamp(path)
        except FileNotFoundError as exc:
            raise ValidationError(f"snapshot partition disappeared while reading: {path}") from exc
        if actual != expected:
            raise ValidationError(f"snapshot partition changed during verified read: {path}")


def _validate_normalized_partition_table(partition: Any, table: pa.Table) -> None:
    validate_arrow_table(partition.schema_id, table)
    if table.num_rows != partition.rows:
        raise ValidationError("Normalized partition row count differs from its manifest")
    previous: tuple[datetime, int, str] | None = None
    previous_sequence_by_session: dict[str, int] = {}
    for row in table.to_pylist():
        trading_day = row["trading_day"]
        trading_day_text = (
            trading_day.isoformat() if isinstance(trading_day, date) else str(trading_day)
        )
        if str(row["event_type"]) != partition.event_type:
            raise ValidationError("Normalized row event_type differs from its partition")
        if str(row["instrument_id"]) != partition.instrument_id:
            raise ValidationError("Normalized row instrument differs from its partition")
        if trading_day_text != partition.trading_date:
            raise ValidationError("Normalized row trading_day differs from its partition")
        if str(row["source"]) != partition.provider:
            raise ValidationError("Normalized row source differs from its partition provider")
        identity = (
            _utc(row["event_time"], "event_time"),
            int(row["sequence"]),
            str(row["event_id"]),
        )
        if previous is not None and identity <= previous:
            raise ValidationError("Normalized partition rows are not strictly ordered")
        session_id = str(row["session_id"])
        previous_sequence = previous_sequence_by_session.get(session_id)
        if previous_sequence is not None and identity[1] <= previous_sequence:
            raise ValidationError("Normalized partition sequence does not advance")
        previous = identity
        previous_sequence_by_session[session_id] = identity[1]


def _read_bound_normalized_records(
    lake_root: Path,
    snapshot: Any,
    refs: tuple[EventSchemaRef, ...],
) -> tuple[
    list[tuple[EventSchemaRef, dict[str, Any]]],
    dict[Path, tuple[int, int, int, int, int]],
]:
    ref_by_schema = {item.schema_id: item for item in refs}
    snapshot_dir = _validate_lake_path(
        lake_root,
        lake_root / "normalized" / "snapshots" / snapshot.snapshot_id,
        allow_missing=False,
    )
    selected: list[tuple[EventSchemaRef, dict[str, Any]]] = []
    stamps: dict[Path, tuple[int, int, int, int, int]] = {}
    for partition in snapshot.partitions:
        schema_ref = ref_by_schema.get(partition.schema_id)
        if schema_ref is None:
            continue
        path = _validate_lake_path(
            lake_root, snapshot_dir / partition.relative_path, allow_missing=False
        )
        table, stamp = _read_content_bound_parquet(path, partition.content_sha256)
        _validate_normalized_partition_table(partition, table)
        stamps[path] = stamp
        selected.extend((schema_ref, _json_evidence(row)) for row in table.to_pylist())
    return selected, stamps


def _read_bound_curated_tables(
    lake_root: Path,
    snapshot: Any,
) -> tuple[
    list[pa.Table],
    dict[str, pa.Table],
    dict[Path, tuple[int, int, int, int, int]],
]:
    snapshot_dir = _validate_lake_path(
        lake_root,
        lake_root / "curated" / snapshot.dataset / "snapshots" / snapshot.snapshot_id,
        allow_missing=False,
    )
    tables: list[pa.Table] = []
    tables_by_path: dict[str, pa.Table] = {}
    stamps: dict[Path, tuple[int, int, int, int, int]] = {}
    for partition in snapshot.partitions:
        path = _validate_lake_path(
            lake_root, snapshot_dir / partition.relative_path, allow_missing=False
        )
        table, stamp = _read_content_bound_parquet(path, partition.content_sha256)
        _validate_curated_partition_table(partition, table, snapshot.aggregation)
        tables.append(table)
        tables_by_path[partition.relative_path] = table
        stamps[path] = stamp
    return tables, tables_by_path, stamps


def _record_selection_sha256(schema_id: str, rows: Sequence[Mapping[str, Any]]) -> str:
    payload = {
        "algorithm": "puresaber.event-selection-canonical-json@1.0.0",
        "schema_id": schema_id,
        "schema_version": SCHEMA_VERSION_V2,
        "records": [dict(row) for row in rows],
    }
    return _hash_bytes(_canonical(payload))


@dataclass(frozen=True)
class MarketContextSnapshot:
    snapshot_id: str
    logical_sha256: str
    calendar_id: str
    session_policy_version: str
    instruments: tuple[InstrumentSpec, ...]
    sessions: tuple[TradingSession, ...]
    schema_id: str = MARKET_CONTEXT_SCHEMA_ID

    def identity(self) -> dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "calendar_id": self.calendar_id,
            "session_policy_version": self.session_policy_version,
            "instruments": [dataclass_payload(item) for item in self.instruments],
            "sessions": [dataclass_payload(item) for item in self.sessions],
        }

    def manifest(self) -> dict[str, Any]:
        return {
            **self.identity(),
            "snapshot_id": self.snapshot_id,
            "logical_sha256": self.logical_sha256,
        }


def _market_context_identity(
    *,
    calendar_id: str,
    session_policy_version: str,
    instruments: tuple[InstrumentSpec, ...],
    sessions: tuple[TradingSession, ...],
) -> dict[str, Any]:
    return {
        "schema_id": MARKET_CONTEXT_SCHEMA_ID,
        "calendar_id": calendar_id,
        "session_policy_version": session_policy_version,
        "instruments": [dataclass_payload(item) for item in instruments],
        "sessions": [dataclass_payload(item) for item in sessions],
    }


def _validate_market_context_values(
    calendar_id: str,
    session_policy_version: str,
    instruments: tuple[InstrumentSpec, ...],
    sessions: tuple[TradingSession, ...],
) -> None:
    if not isinstance(calendar_id, str) or not calendar_id.strip():
        raise ValidationError("calendar_id is required")
    if not isinstance(session_policy_version, str) or not session_policy_version.strip():
        raise ValidationError("session_policy_version is required")
    if not instruments or not sessions:
        raise ValidationError("market context requires instruments and sessions")
    if any(item.calendar_id != calendar_id for item in instruments):
        raise ValidationError("all instruments must use the market-context calendar")
    if any(item.calendar_id != calendar_id for item in sessions):
        raise ValidationError("all sessions must use the market-context calendar")
    instrument_keys = [
        (item.instrument_id, item.effective_from, item.available_at) for item in instruments
    ]
    if len(instrument_keys) != len(set(instrument_keys)):
        raise ValidationError("market context contains duplicate instrument versions")
    session_ids = [item.session_id for item in sessions]
    if len(session_ids) != len(set(session_ids)):
        raise ValidationError("market context contains duplicate session IDs")
    MarketClock(calendar_id, sessions)


def create_market_context_snapshot(
    root: Path,
    *,
    calendar_id: str,
    session_policy_version: str,
    instruments: Iterable[InstrumentSpec],
    sessions: Iterable[TradingSession],
    policy: StoragePolicy | None = None,
) -> MarketContextSnapshot:
    """Persist a deterministic, content-addressed instrument/session context."""
    materialized_instruments = tuple(instruments)
    materialized_sessions = tuple(sessions)
    if not all(isinstance(item, InstrumentSpec) for item in materialized_instruments):
        raise ValidationError("instruments must contain InstrumentSpec values")
    if not all(isinstance(item, TradingSession) for item in materialized_sessions):
        raise ValidationError("sessions must contain TradingSession values")
    ordered_instruments = tuple(
        sorted(
            materialized_instruments,
            key=lambda item: (item.instrument_id, item.effective_from, item.available_at),
        )
    )
    ordered_sessions = tuple(
        sorted(materialized_sessions, key=lambda item: (item.opens_at, item.session_id))
    )
    _validate_market_context_values(
        calendar_id,
        session_policy_version,
        ordered_instruments,
        ordered_sessions,
    )
    identity = _market_context_identity(
        calendar_id=calendar_id,
        session_policy_version=session_policy_version,
        instruments=ordered_instruments,
        sessions=ordered_sessions,
    )
    logical_sha256 = _hash_bytes(_canonical(identity))
    snapshot_id = f"sha256-{logical_sha256}"
    snapshot = MarketContextSnapshot(
        snapshot_id=snapshot_id,
        logical_sha256=logical_sha256,
        calendar_id=calendar_id,
        session_policy_version=session_policy_version,
        instruments=ordered_instruments,
        sessions=ordered_sessions,
    )
    lake_root = _resolved_lake_root(root, create=True)
    context_root = _mkdir_in_lake(lake_root, lake_root / "market-context")
    snapshots_root = _mkdir_in_lake(lake_root, context_root / "snapshots")
    target = snapshots_root / snapshot_id
    resolved_policy = policy or StoragePolicy()
    require_collection_capacity(
        lake_root,
        projected_write_bytes=len(_canonical(snapshot.manifest())),
        policy=resolved_policy,
    )
    with _stable_staging_directory(
        lake_root,
        context_root / "staging",
        namespace="market-context",
        identity=identity,
    ) as stage:
        (stage / "manifest.json").write_text(
            json.dumps(snapshot.manifest(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        with _lake_lock(lake_root, "market-context", {"snapshot_id": snapshot_id}):
            if target.exists():
                existing = load_market_context_snapshot(lake_root, snapshot_id)
                if existing.manifest() != snapshot.manifest():
                    raise ValidationError("market-context snapshot collision")
                return existing
            _publish_tree_entry(lake_root, stage, target, policy=resolved_policy)
            stage = target
    return load_market_context_snapshot(lake_root, snapshot_id)


def _instrument_from_payload(payload: Mapping[str, Any]) -> InstrumentSpec:
    required = {
        "instrument_id",
        "asset_class",
        "product_type",
        "venue",
        "native_symbol",
        "base_currency",
        "quote_currency",
        "settlement_currency",
        "price_tick",
        "quantity_step",
        "contract_multiplier",
        "calendar_id",
        "margin_mode",
        "inverse",
        "effective_from",
        "effective_to",
        "available_at",
        "superseded_at",
        "underlying_id",
        "expiry_date",
        "metadata",
    }
    if not isinstance(payload, Mapping) or set(payload) != required:
        raise ValidationError("instrument context record is not closed")
    metadata = payload["metadata"]
    if not isinstance(metadata, Mapping):
        raise ValidationError("instrument metadata must be an object")
    return InstrumentSpec(
        instrument_id=payload["instrument_id"],
        asset_class=AssetClass(payload["asset_class"]),
        product_type=payload["product_type"],
        venue=payload["venue"],
        native_symbol=payload["native_symbol"],
        base_currency=payload["base_currency"],
        quote_currency=payload["quote_currency"],
        settlement_currency=payload["settlement_currency"],
        price_tick=_fixed(payload["price_tick"], "price_tick"),
        quantity_step=_fixed(payload["quantity_step"], "quantity_step"),
        contract_multiplier=_fixed(payload["contract_multiplier"], "contract_multiplier"),
        calendar_id=payload["calendar_id"],
        margin_mode=MarginMode(payload["margin_mode"]),
        inverse=payload["inverse"],
        effective_from=_utc(payload["effective_from"], "effective_from"),
        effective_to=_parse_optional_utc(payload["effective_to"], "effective_to"),
        available_at=_utc(payload["available_at"], "available_at"),
        superseded_at=_parse_optional_utc(payload["superseded_at"], "superseded_at"),
        underlying_id=payload["underlying_id"],
        expiry_date=(
            date.fromisoformat(payload["expiry_date"])
            if payload["expiry_date"] is not None
            else None
        ),
        metadata=dict(metadata),
    )


def _session_from_payload(payload: Mapping[str, Any]) -> TradingSession:
    required = {
        "session_id",
        "calendar_id",
        "venue",
        "trading_day",
        "phase",
        "opens_at",
        "closes_at",
        "available_at",
        "superseded_at",
    }
    if not isinstance(payload, Mapping) or set(payload) != required:
        raise ValidationError("session context record is not closed")
    return TradingSession(
        session_id=payload["session_id"],
        calendar_id=payload["calendar_id"],
        venue=payload["venue"],
        trading_day=date.fromisoformat(payload["trading_day"]),
        phase=SessionPhase(payload["phase"]),
        opens_at=_utc(payload["opens_at"], "opens_at"),
        closes_at=_utc(payload["closes_at"], "closes_at"),
        available_at=_utc(payload["available_at"], "available_at"),
        superseded_at=_parse_optional_utc(payload["superseded_at"], "superseded_at"),
    )


def load_market_context_snapshot(root: Path, snapshot_id: str) -> MarketContextSnapshot:
    lake_root = _resolved_lake_root(root, create=False)
    if not isinstance(snapshot_id, str) or _SNAPSHOT_ID.fullmatch(snapshot_id) is None:
        raise ValidationError("market-context reads require a content-addressed snapshot ID")
    snapshot_dir = _validate_lake_path(
        lake_root,
        lake_root / "market-context" / "snapshots" / snapshot_id,
        allow_missing=False,
    )
    manifest_path = _validate_lake_path(
        lake_root, snapshot_dir / "manifest.json", allow_missing=False
    )
    if not manifest_path.is_file():
        raise ValidationError("market-context manifest is missing")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError("market-context manifest is malformed") from exc
    fields = {
        "schema_id",
        "snapshot_id",
        "logical_sha256",
        "calendar_id",
        "session_policy_version",
        "instruments",
        "sessions",
    }
    if not isinstance(payload, Mapping) or set(payload) != fields:
        raise ValidationError("market-context manifest is not closed")
    if payload["schema_id"] != MARKET_CONTEXT_SCHEMA_ID or payload["snapshot_id"] != snapshot_id:
        raise ValidationError("market-context identity mismatch")
    try:
        instruments = tuple(_instrument_from_payload(item) for item in payload["instruments"])
        sessions = tuple(_session_from_payload(item) for item in payload["sessions"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError("market-context records are malformed") from exc
    _validate_market_context_values(
        payload["calendar_id"], payload["session_policy_version"], instruments, sessions
    )
    if instruments != tuple(
        sorted(
            instruments,
            key=lambda item: (item.instrument_id, item.effective_from, item.available_at),
        )
    ) or sessions != tuple(sorted(sessions, key=lambda item: (item.opens_at, item.session_id))):
        raise ValidationError("market-context records are not canonically sorted")
    identity = _market_context_identity(
        calendar_id=payload["calendar_id"],
        session_policy_version=payload["session_policy_version"],
        instruments=instruments,
        sessions=sessions,
    )
    logical_sha256 = _hash_bytes(_canonical(identity))
    if payload["logical_sha256"] != logical_sha256 or snapshot_id != f"sha256-{logical_sha256}":
        raise ValidationError("market-context logical hash changed")
    actual_files = {
        path.relative_to(snapshot_dir) for path in snapshot_dir.rglob("*") if path.is_file()
    }
    if actual_files != {Path("manifest.json")}:
        raise ValidationError("market-context snapshot contains unexpected files")
    return MarketContextSnapshot(
        snapshot_id=snapshot_id,
        logical_sha256=logical_sha256,
        calendar_id=payload["calendar_id"],
        session_policy_version=payload["session_policy_version"],
        instruments=instruments,
        sessions=sessions,
    )


def _active_instrument(
    context: MarketContextSnapshot,
    instrument_id: str,
    event_time: datetime,
    available_at: datetime,
) -> InstrumentSpec:
    candidates = [
        item
        for item in context.instruments
        if item.instrument_id == instrument_id
        and item.effective_from <= event_time
        and (item.effective_to is None or event_time < item.effective_to)
        and item.available_at <= available_at
        and (item.superseded_at is None or available_at < item.superseded_at)
    ]
    if not candidates:
        raise ValidationError(f"no PIT-valid InstrumentSpec for {instrument_id}")
    newest_effective = max(item.effective_from for item in candidates)
    selected = [item for item in candidates if item.effective_from == newest_effective]
    if len(selected) != 1:
        raise ValidationError(f"ambiguous PIT InstrumentSpec for {instrument_id}")
    return selected[0]


def _context_session(
    context: MarketContextSnapshot,
    session_id: str,
    event_time: datetime,
    available_at: datetime,
    trading_day: date,
    *,
    allow_close: bool,
) -> TradingSession:
    matches = [item for item in context.sessions if item.session_id == session_id]
    if len(matches) != 1:
        raise ValidationError(f"market context must contain exactly one session {session_id}")
    session = matches[0]
    upper_ok = event_time <= session.closes_at if allow_close else event_time < session.closes_at
    if not session.opens_at <= event_time or not upper_ok:
        raise ValidationError(f"event time is outside session {session_id}")
    if session.trading_day != trading_day:
        raise ValidationError(f"trading_day does not match session {session_id}")
    if session.available_at > available_at or (
        session.superseded_at is not None and available_at >= session.superseded_at
    ):
        raise ValidationError(f"session {session_id} is not PIT-valid")
    return session


def _validate_context_record(
    context: MarketContextSnapshot,
    record: Mapping[str, Any],
    *,
    bar: bool,
) -> tuple[InstrumentSpec, TradingSession]:
    event_time = _utc(record["event_time"], "event_time")
    received_at = _utc(record["received_at"], "received_at")
    available_at = _utc(record["available_at"], "available_at")
    if event_time > received_at or received_at > available_at:
        raise ValidationError("event PIT timestamps are not monotonic")
    trading_day_value = record["trading_day"]
    trading_day = (
        trading_day_value
        if isinstance(trading_day_value, date)
        else date.fromisoformat(str(trading_day_value))
    )
    instrument = _active_instrument(context, str(record["instrument_id"]), event_time, available_at)
    session = _context_session(
        context,
        str(record["session_id"]),
        event_time,
        available_at,
        trading_day,
        allow_close=bar,
    )
    if instrument.calendar_id != context.calendar_id or session.calendar_id != context.calendar_id:
        raise ValidationError("record context calendar mismatch")
    if instrument.venue != session.venue:
        raise ValidationError("instrument venue and session venue differ")
    return instrument, session


def _ordered_records(
    records: Sequence[tuple[EventSchemaRef, dict[str, Any]]],
) -> list[tuple[EventSchemaRef, dict[str, Any]]]:
    return sorted(
        records,
        key=lambda item: (
            str(item[1]["instrument_id"]),
            _utc(item[1]["event_time"], "event_time"),
            int(item[1]["sequence"]),
            str(item[1]["event_id"]),
            str(item[1]["source"]),
            str(item[1]["session_id"]),
            item[0].schema_id,
        ),
    )


def _validate_event_order(records: Sequence[tuple[EventSchemaRef, dict[str, Any]]]) -> None:
    event_ids: set[str] = set()
    previous_by_instrument: dict[str, tuple[datetime, int, str]] = {}
    previous_by_stream: dict[tuple[str, str, str, str], tuple[datetime, int, str]] = {}
    l2_streams: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for schema_ref, record in records:
        event_id = str(record["event_id"])
        if event_id in event_ids:
            raise ValidationError(f"duplicate event_id in verified selection: {event_id}")
        event_ids.add(event_id)
        event_type = str(record["event_type"])
        domain = "book" if event_type in {"book_snapshot", "book_delta"} else schema_ref.schema_id
        stream = (
            str(record["source"]),
            str(record["instrument_id"]),
            str(record["session_id"]),
            domain,
        )
        identity = (
            _utc(record["event_time"], "event_time"),
            int(record["sequence"]),
            event_id,
        )
        instrument_id = str(record["instrument_id"])
        instrument_previous = previous_by_instrument.get(instrument_id)
        if instrument_previous is not None and identity <= instrument_previous:
            raise ValidationError(f"instrument events are not strictly ordered: {instrument_id}")
        previous_by_instrument[instrument_id] = identity
        previous = previous_by_stream.get(stream)
        if previous is not None and identity <= previous:
            raise ValidationError(f"event stream is not strictly ordered: {stream}")
        if previous is not None and int(record["sequence"]) <= previous[1]:
            raise ValidationError(f"event sequence does not advance: {stream}")
        previous_by_stream[stream] = identity
        if domain == "book":
            l2_streams[stream[:3]].append(record)
    for stream, l2_records in l2_streams.items():
        try:
            replay_l2(l2_records, capture_all_checkpoints=False)
        except (L2ReplayError, ValidationError) as exc:
            raise ValidationError(f"L2 selection failed replay for {stream}: {exc}") from exc


def _event_union_table(records: Sequence[tuple[EventSchemaRef, dict[str, Any]]]) -> pa.Table:
    schema_count = len({item.schema_id for item, _ in records})
    field_types: dict[str, pa.DataType] = {}
    field_nullability: dict[str, bool] = {}
    field_occurrences: dict[str, set[str]] = defaultdict(set)
    field_order: list[str] = []
    for schema_ref, _ in records:
        schema = get_arrow_schema(schema_ref.schema_id, schema_ref.schema_version)
        for field_definition in schema:
            existing = field_types.get(field_definition.name)
            if existing is not None and existing != field_definition.type:
                raise ValidationError(f"event schemas disagree on field {field_definition.name}")
            if field_definition.name not in field_types:
                field_order.append(field_definition.name)
                field_types[field_definition.name] = field_definition.type
                field_nullability[field_definition.name] = field_definition.nullable
            else:
                field_nullability[field_definition.name] |= field_definition.nullable
            field_occurrences[field_definition.name].add(schema_ref.schema_id)
    fields = [pa.field("event_schema_id", pa.string(), nullable=False)]
    for name in field_order:
        fields.append(
            pa.field(
                name,
                field_types[name],
                nullable=field_nullability[name] or len(field_occurrences[name]) < schema_count,
            )
        )
    union_schema = pa.schema(fields)
    ready: list[dict[str, Any]] = []
    for schema_ref, record in records:
        source_schema = get_arrow_schema(schema_ref.schema_id, schema_ref.schema_version)
        converted = _arrow_ready(record, source_schema)
        row = {name: converted.get(name) for name in field_order}
        row["event_schema_id"] = schema_ref.schema_id
        ready.append(row)
    return pa.Table.from_pylist(ready, schema=union_schema).combine_chunks()


def _normalize_schema_refs(
    event_schemas: Iterable[EventSchemaRef | Mapping[str, Any]],
) -> tuple[EventSchemaRef, ...]:
    refs = tuple(
        item if isinstance(item, EventSchemaRef) else EventSchemaRef.from_contract(item)
        for item in event_schemas
    )
    ordered = tuple(sorted(set(refs)))
    if not refs or refs != ordered:
        raise ValidationError("event_schemas must be non-empty, unique, and sorted")
    if any(item.schema_version != SCHEMA_VERSION_V2 for item in refs):
        raise ValidationError("only event schema version 2.0.0 is certified")
    if any(
        item.schema_id == BAR_EVENT_SCHEMA_ID or item.schema_id not in _EVENT_TYPE_BY_SCHEMA
        for item in refs
    ):
        raise ValidationError("Normalized verified input accepts only non-Bar market events")
    return refs


def load_verified_normalized_events(
    root: Path,
    snapshot_id: str,
    event_schemas: Iterable[EventSchemaRef | Mapping[str, Any]],
    market_context_snapshot_id: str,
) -> VerifiedFactorInput:
    """Load a frozen market-event selection after complete snapshot and context verification."""
    refs = _normalize_schema_refs(event_schemas)
    lake_root = _resolved_lake_root(root, create=False)
    before = load_normalized_snapshot(lake_root, snapshot_id)
    context_before = load_market_context_snapshot(lake_root, market_context_snapshot_id)
    selected, partition_stamps = _read_bound_normalized_records(lake_root, before, refs)
    counts = {item: 0 for item in refs}
    for schema_ref, record in selected:
        validate_json_record(schema_ref.schema_id, record, schema_ref.schema_version)
        _validate_context_record(context_before, record, bar=False)
        counts[schema_ref] += 1
    missing = [item.schema_id for item, count in counts.items() if count == 0]
    if missing:
        raise ValidationError(f"Normalized snapshot lacks requested event schemas: {missing}")
    ordered = _ordered_records(selected)
    _validate_event_order(ordered)
    table = _event_union_table(ordered)
    after = load_normalized_snapshot(lake_root, snapshot_id)
    context_after = load_market_context_snapshot(lake_root, market_context_snapshot_id)
    if (
        before.snapshot_id,
        before.logical_sha256,
        before.partitions,
    ) != (after.snapshot_id, after.logical_sha256, after.partitions):
        raise ValidationError("Normalized snapshot changed while building verified input")
    if context_before.manifest() != context_after.manifest():
        raise ValidationError("market context changed while building verified input")
    _assert_file_stamps(partition_stamps)
    return VerifiedFactorInput._from_certified_factory(
        layer="normalized",
        source_snapshot_id=before.snapshot_id,
        source_logical_sha256=before.logical_sha256,
        selection_logical_sha256=_table_logical_sha256(table),
        event_schemas=refs,
        table=table,
        calendar_id=context_before.calendar_id,
        session_policy_version=context_before.session_policy_version,
        market_context_snapshot_id=context_before.snapshot_id,
        market_context_logical_sha256=context_before.logical_sha256,
        lineage=tuple(
            sorted(
                (
                    LineageRef("market", before.snapshot_id, before.logical_sha256),
                    LineageRef(
                        "market_context", context_before.snapshot_id, context_before.logical_sha256
                    ),
                )
            )
        ),
    )


def _validate_bar_rows(
    table: pa.Table,
    aggregation: CuratedAggregation,
    context: MarketContextSnapshot,
) -> list[dict[str, Any]]:
    rows = table.to_pylist()
    identities: set[tuple[str, datetime, int, str]] = set()
    previous_by_instrument: dict[str, tuple[datetime, int, str]] = {}
    sessions_by_day: dict[tuple[str, date], list[TradingSession]] = defaultdict(list)
    for session in context.sessions:
        sessions_by_day[(session.venue, session.trading_day)].append(session)
    for row in rows:
        ready = dict(row)
        validate_json_record(
            BAR_EVENT_SCHEMA_ID,
            {
                key: (
                    value.isoformat().replace("+00:00", "Z")
                    if isinstance(value, datetime)
                    else value.isoformat()
                    if isinstance(value, date)
                    else value
                )
                for key, value in ready.items()
            },
        )
        if not bool(row["is_complete"]):
            raise ValidationError("certified factor input rejects incomplete Bars")
        bar_start = _utc(row["bar_start"], "bar_start")
        bar_end = _utc(row["bar_end"], "bar_end")
        event_time = _utc(row["event_time"], "event_time")
        instrument, session = _validate_context_record(context, row, bar=True)
        identity = (
            str(row["instrument_id"]),
            event_time,
            int(row["sequence"]),
            str(row["event_id"]),
        )
        if identity in identities:
            raise ValidationError("Curated input contains duplicate Bar identity")
        identities.add(identity)
        prior = previous_by_instrument.get(str(row["instrument_id"]))
        ordering = identity[1:]
        if prior is not None and ordering <= prior:
            raise ValidationError("Curated Bars are not strictly ordered per instrument")
        previous_by_instrument[str(row["instrument_id"])] = ordering
        if aggregation.kind == "fixed_time_bar":
            duration = bar_end - bar_start
            duration_ns = (
                duration.days * 86_400 + duration.seconds
            ) * 1_000_000_000 + duration.microseconds * 1_000
            if duration_ns != aggregation.interval_ns:
                raise ValidationError("Bar interval differs from Curated aggregation metadata")
            if bar_start < session.opens_at or bar_end > session.closes_at:
                raise ValidationError("fixed-time Bar crosses its session boundary")
        elif aggregation.kind == "session_bar":
            if aggregation.session_rollup == "session":
                if bar_start != session.opens_at or bar_end != session.closes_at:
                    raise ValidationError("session Bar does not match its authoritative session")
            else:
                day_sessions = sessions_by_day[(instrument.venue, session.trading_day)]
                expected_start = min(item.opens_at for item in day_sessions)
                expected_end = max(item.closes_at for item in day_sessions)
                if bar_start != expected_start or bar_end != expected_end:
                    raise ValidationError("trading-day Bar boundaries are not authoritative")
    return rows


def _source_rows_for_evidence(
    all_rows: Sequence[Mapping[str, Any]], evidence: EventBarPartitionEvidence
) -> list[dict[str, Any]]:
    selected = [
        dict(row)
        for row in all_rows
        if row.get("event_type") == "trade"
        and str(row["source"]) == evidence.source
        and str(row["instrument_id"]) == evidence.instrument_id
        and str(row["session_id"]) == evidence.session_id
        and evidence.first_sequence <= int(row["sequence"]) <= evidence.last_sequence
    ]
    selected.sort(
        key=lambda row: (
            _utc(row["event_time"], "event_time"),
            int(row["sequence"]),
            str(row["event_id"]),
        )
    )
    if len(selected) != evidence.event_count:
        raise ValidationError("event-bar evidence event_count does not match Normalized lineage")
    if (
        int(selected[0]["sequence"]) != evidence.first_sequence
        or int(selected[-1]["sequence"]) != evidence.last_sequence
        or str(selected[0]["event_id"]) != evidence.first_event_id
        or str(selected[-1]["event_id"]) != evidence.last_event_id
    ):
        raise ValidationError("event-bar evidence boundaries do not match Normalized lineage")
    if (
        _record_selection_sha256(TRADE_EVENT_SCHEMA_ID, selected)
        != evidence.source_selection_sha256
    ):
        raise ValidationError("event-bar source selection hash changed")
    return selected


def _verify_event_bars(
    aggregation: CuratedAggregation,
    source_rows: Sequence[Mapping[str, Any]],
    table: pa.Table,
    context: MarketContextSnapshot,
) -> None:
    if aggregation.partition_evidence is None or aggregation.event_bar_threshold is None:
        raise ValidationError("event-bar metadata is incomplete")
    if aggregation.source_event_schemas != (
        EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),
    ):
        raise ValidationError("M8 event bars currently require the frozen Trade schema")
    output_sources = set(table.column("source").to_pylist())
    if len(output_sources) != 1:
        raise ValidationError("event Bars must use one deterministic Curated source")
    output_source = str(next(iter(output_sources)))
    session_starts = {item.session_id: item.opens_at for item in context.sessions}
    rebuilt: list[dict[str, Any]] = []
    used_event_ids: set[str] = set()
    for evidence in aggregation.partition_evidence:
        selected = _source_rows_for_evidence(source_rows, evidence)
        selected_ids = {str(item["event_id"]) for item in selected}
        if used_event_ids.intersection(selected_ids):
            raise ValidationError("event-bar evidence reuses source events")
        used_event_ids.update(selected_ids)
        rebuilt.extend(
            build_event_bars(
                selected,
                basis=aggregation.event_bar_basis,
                threshold=aggregation.event_bar_threshold,
                session_starts=session_starts,
                source=output_source,
                recipe_version=aggregation.recipe_version,
                require_complete=True,
            )
        )
    all_event_ids = {str(item["event_id"]) for item in source_rows}
    if used_event_ids != all_event_ids:
        raise ValidationError("event-bar evidence does not cover its complete Trade lineage")
    expected_table = pa.Table.from_pylist(
        [_arrow_ready(item, get_arrow_schema(BAR_EVENT_SCHEMA_ID)) for item in rebuilt],
        schema=get_arrow_schema(BAR_EVENT_SCHEMA_ID),
    )
    sort_keys = [
        ("instrument_id", "ascending"),
        ("event_time", "ascending"),
        ("sequence", "ascending"),
        ("event_id", "ascending"),
    ]
    expected_table = expected_table.take(pc.sort_indices(expected_table, sort_keys=sort_keys))
    actual_table = table.take(pc.sort_indices(table, sort_keys=sort_keys))
    if not expected_table.equals(actual_table, check_metadata=True):
        raise ValidationError("event Bars do not recompute from their Normalized evidence")


def load_verified_curated_bars(
    root: Path,
    dataset: str,
    snapshot_id: str,
) -> VerifiedFactorInput:
    """Load only M8-certified Curated Bars; legacy Curated snapshots fail closed."""
    lake_root = _resolved_lake_root(root, create=False)
    before = load_curated_snapshot(lake_root, dataset, snapshot_id)
    aggregation = before.aggregation
    if not isinstance(aggregation, CuratedAggregation):
        raise ValidationError("legacy-curated-not-m8-certified")
    if aggregation.recipe_version != before.recipe_version:
        raise ValidationError("Curated recipe and aggregation recipe differ")
    context_before = load_market_context_snapshot(lake_root, aggregation.market_context_snapshot_id)
    if (
        aggregation.market_context_logical_sha256 != context_before.logical_sha256
        or aggregation.calendar_id != context_before.calendar_id
        or aggregation.session_policy_version != context_before.session_policy_version
    ):
        raise ValidationError("Curated aggregation market-context binding changed")
    tables, tables_by_path, curated_stamps = _read_bound_curated_tables(lake_root, before)
    if not tables:
        raise ValidationError("Curated snapshot contains no Bar partitions")
    combined = pa.concat_tables(tables).combine_chunks()
    sort_keys = [
        ("instrument_id", "ascending"),
        ("event_time", "ascending"),
        ("sequence", "ascending"),
        ("event_id", "ascending"),
    ]
    table = combined.take(pc.sort_indices(combined, sort_keys=sort_keys))
    _validate_bar_rows(table, aggregation, context_before)
    if aggregation.kind == "event_bar":
        evidence = aggregation.partition_evidence or ()
        partition_by_path = {item.relative_path: item for item in before.partitions}
        evidence_paths = {item.relative_path for item in evidence}
        if evidence_paths != set(partition_by_path):
            raise ValidationError("event-bar evidence does not cover the Curated partition set")
        for item in evidence:
            partition = partition_by_path[item.relative_path]
            if item.instrument_id != partition.instrument_id:
                raise ValidationError("event-bar evidence instrument does not match its partition")
            partition_sessions = set(
                tables_by_path[item.relative_path].column("session_id").to_pylist()
            )
            if partition_sessions != {item.session_id}:
                raise ValidationError("event-bar evidence session does not match its partition")
        normalized_before = load_normalized_snapshot(
            lake_root, before.lineage["normalized_snapshot_id"]
        )
        trade_ref = (EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),)
        source_pairs, normalized_stamps = _read_bound_normalized_records(
            lake_root, normalized_before, trade_ref
        )
        _verify_event_bars(
            aggregation,
            [record for _, record in source_pairs],
            table,
            context_before,
        )
    after = load_curated_snapshot(lake_root, dataset, snapshot_id)
    context_after = load_market_context_snapshot(lake_root, aggregation.market_context_snapshot_id)
    if before != after:
        raise ValidationError("Curated snapshot changed while building verified input")
    if context_before.manifest() != context_after.manifest():
        raise ValidationError("market context changed while building verified input")
    normalized = load_normalized_snapshot(lake_root, before.lineage["normalized_snapshot_id"])
    if normalized.logical_sha256 != before.lineage["normalized_logical_sha256"]:
        raise ValidationError("Curated Normalized lineage hash changed")
    _assert_file_stamps(curated_stamps)
    if aggregation.kind == "event_bar":
        if normalized_before != normalized:
            raise ValidationError("Normalized lineage changed while verifying event Bars")
        _assert_file_stamps(normalized_stamps)
    return VerifiedFactorInput._from_certified_factory(
        layer="curated",
        source_snapshot_id=before.snapshot_id,
        source_logical_sha256=before.logical_sha256,
        selection_logical_sha256=_table_logical_sha256(table),
        event_schemas=(EventSchemaRef(BAR_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),),
        table=table,
        calendar_id=aggregation.calendar_id,
        session_policy_version=aggregation.session_policy_version,
        market_context_snapshot_id=context_before.snapshot_id,
        market_context_logical_sha256=context_before.logical_sha256,
        lineage=tuple(
            sorted(
                (
                    LineageRef("market", before.snapshot_id, before.logical_sha256),
                    LineageRef(
                        "market_context", context_before.snapshot_id, context_before.logical_sha256
                    ),
                    LineageRef("normalized", normalized.snapshot_id, normalized.logical_sha256),
                )
            )
        ),
        aggregation=aggregation,
    )
