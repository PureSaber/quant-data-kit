"""Closed M8 contracts shared by certified research-input factories."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

import pyarrow as pa

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint

VERIFIED_FACTOR_INPUT_SCHEMA_ID = "puresaber.verified-factor-input@1.0.0"
CURATED_AGGREGATION_SCHEMA_ID = "puresaber.curated-aggregation@1.0.0"
MARKET_CONTEXT_SCHEMA_ID = "puresaber.market-context@1.0.0"

_HASH = re.compile(r"^[0-9a-f]{64}$")
_SNAPSHOT_ID = re.compile(r"^sha256-[0-9a-f]{64}$")
_SCHEMA_ID = re.compile(r"^puresaber\.[a-z0-9._-]+$")
_SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_CANONICAL_NONNEGATIVE = re.compile(r"^(?:0|[1-9][0-9]*)$")
_CANONICAL_POSITIVE = re.compile(r"^[1-9][0-9]*$")
_INT64_MAX = 2**63 - 1


def _required_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty string")
    return value


def _require_hash(value: str, field_name: str) -> str:
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        raise ValidationError(f"{field_name} must be a lowercase SHA-256")
    return value


def _require_snapshot_id(value: str, field_name: str) -> str:
    if not isinstance(value, str) or _SNAPSHOT_ID.fullmatch(value) is None:
        raise ValidationError(f"{field_name} must be a content-addressed snapshot ID")
    return value


def _closed_payload(payload: Mapping[str, Any], fields: set[str], name: str) -> None:
    if not isinstance(payload, Mapping) or set(payload) != fields:
        raise ValidationError(f"{name} must contain exactly {sorted(fields)}")


def _canonical_integer(value: Any, field_name: str, *, positive: bool) -> int:
    pattern = _CANONICAL_POSITIVE if positive else _CANONICAL_NONNEGATIVE
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        qualifier = "positive " if positive else "non-negative "
        raise ValidationError(f"{field_name} must be a canonical {qualifier}integer string")
    parsed = int(value)
    if parsed > _INT64_MAX:
        raise ValidationError(f"{field_name} exceeds signed int64")
    return parsed


@dataclass(frozen=True, order=True)
class EventSchemaRef:
    schema_id: str
    schema_version: str

    def __post_init__(self) -> None:
        if not isinstance(self.schema_id, str) or _SCHEMA_ID.fullmatch(self.schema_id) is None:
            raise ValidationError("schema_id must be a puresaber schema ID")
        if (
            not isinstance(self.schema_version, str)
            or _SEMVER.fullmatch(self.schema_version) is None
        ):
            raise ValidationError("schema_version must be semantic version text")

    def to_contract(self) -> dict[str, str]:
        return {"schema_id": self.schema_id, "schema_version": self.schema_version}

    @classmethod
    def from_contract(cls, payload: Mapping[str, Any]) -> EventSchemaRef:
        _closed_payload(payload, {"schema_id", "schema_version"}, "event schema reference")
        return cls(schema_id=payload["schema_id"], schema_version=payload["schema_version"])


@dataclass(frozen=True, order=True)
class LineageRef:
    role: str
    snapshot_id: str
    logical_sha256: str

    def __post_init__(self) -> None:
        _required_text(self.role, "role")
        _require_snapshot_id(self.snapshot_id, "snapshot_id")
        _require_hash(self.logical_sha256, "logical_sha256")

    def to_contract(self) -> dict[str, str]:
        return {
            "role": self.role,
            "snapshot_id": self.snapshot_id,
            "logical_sha256": self.logical_sha256,
        }


@dataclass(frozen=True)
class EventBarPartitionEvidence:
    relative_path: str
    source: str
    instrument_id: str
    session_id: str
    first_sequence: int
    last_sequence: int
    first_event_id: str
    last_event_id: str
    event_count: int
    source_selection_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "relative_path",
            "source",
            "instrument_id",
            "session_id",
            "first_event_id",
            "last_event_id",
        ):
            _required_text(getattr(self, name), name)
        for name in ("first_sequence", "last_sequence", "event_count"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValidationError(f"{name} must be an integer")
            if value < (1 if name == "event_count" else 0) or value > _INT64_MAX:
                raise ValidationError(f"{name} is outside its signed-int64 contract")
        if self.last_sequence < self.first_sequence:
            raise ValidationError("event evidence sequence range is reversed")
        _require_hash(self.source_selection_sha256, "source_selection_sha256")

    @property
    def stream_key(self) -> tuple[str, str, str]:
        return self.source, self.instrument_id, self.session_id

    def to_contract(self) -> dict[str, str]:
        return {
            "relative_path": self.relative_path,
            "source": self.source,
            "instrument_id": self.instrument_id,
            "session_id": self.session_id,
            "first_sequence": str(self.first_sequence),
            "last_sequence": str(self.last_sequence),
            "first_event_id": self.first_event_id,
            "last_event_id": self.last_event_id,
            "event_count": str(self.event_count),
            "source_selection_sha256": self.source_selection_sha256,
        }

    @classmethod
    def from_contract(cls, payload: Mapping[str, Any]) -> EventBarPartitionEvidence:
        fields = {
            "relative_path",
            "source",
            "instrument_id",
            "session_id",
            "first_sequence",
            "last_sequence",
            "first_event_id",
            "last_event_id",
            "event_count",
            "source_selection_sha256",
        }
        _closed_payload(payload, fields, "event-bar partition evidence")
        return cls(
            relative_path=payload["relative_path"],
            source=payload["source"],
            instrument_id=payload["instrument_id"],
            session_id=payload["session_id"],
            first_sequence=_canonical_integer(
                payload["first_sequence"], "first_sequence", positive=False
            ),
            last_sequence=_canonical_integer(
                payload["last_sequence"], "last_sequence", positive=False
            ),
            first_event_id=payload["first_event_id"],
            last_event_id=payload["last_event_id"],
            event_count=_canonical_integer(payload["event_count"], "event_count", positive=True),
            source_selection_sha256=payload["source_selection_sha256"],
        )


@dataclass(frozen=True)
class CuratedAggregation:
    calendar_id: str
    session_policy_version: str
    kind: Literal["fixed_time_bar", "session_bar", "event_bar"]
    recipe_version: str
    market_context_snapshot_id: str
    market_context_logical_sha256: str
    source_event_schemas: tuple[EventSchemaRef, ...]
    interval_ns: int | None = None
    session_rollup: Literal["session", "trading_day"] | None = None
    event_bar_basis: Literal["trade_count", "base_volume", "quote_notional"] | None = None
    event_bar_threshold: FixedPoint | None = None
    partition_evidence: tuple[EventBarPartitionEvidence, ...] | None = None

    def __post_init__(self) -> None:
        for name in ("calendar_id", "session_policy_version", "recipe_version"):
            _required_text(getattr(self, name), name)
        if self.kind not in {"fixed_time_bar", "session_bar", "event_bar"}:
            raise ValidationError("unsupported Curated aggregation kind")
        _require_snapshot_id(self.market_context_snapshot_id, "market_context_snapshot_id")
        _require_hash(self.market_context_logical_sha256, "market_context_logical_sha256")
        schemas = tuple(self.source_event_schemas)
        if not schemas or schemas != tuple(sorted(set(schemas))):
            raise ValidationError("source_event_schemas must be non-empty, unique, and sorted")
        object.__setattr__(self, "source_event_schemas", schemas)

        if self.kind == "fixed_time_bar":
            if (
                isinstance(self.interval_ns, bool)
                or not isinstance(self.interval_ns, int)
                or not 1 <= self.interval_ns <= _INT64_MAX
            ):
                raise ValidationError("fixed_time_bar requires positive int64 interval_ns")
            if any(
                value is not None
                for value in (
                    self.session_rollup,
                    self.event_bar_basis,
                    self.event_bar_threshold,
                    self.partition_evidence,
                )
            ):
                raise ValidationError("fixed_time_bar contains fields for another aggregation kind")
        elif self.kind == "session_bar":
            if self.session_rollup not in {"session", "trading_day"}:
                raise ValidationError("session_bar requires session_rollup")
            if any(
                value is not None
                for value in (
                    self.interval_ns,
                    self.event_bar_basis,
                    self.event_bar_threshold,
                    self.partition_evidence,
                )
            ):
                raise ValidationError("session_bar contains fields for another aggregation kind")
        else:
            if self.event_bar_basis not in {"trade_count", "base_volume", "quote_notional"}:
                raise ValidationError("event_bar requires an event_bar_basis")
            if not isinstance(self.event_bar_threshold, FixedPoint) or not (
                self.event_bar_threshold.is_positive()
            ):
                raise ValidationError("event_bar requires a positive FixedPoint threshold")
            if self.interval_ns is not None or self.session_rollup is not None:
                raise ValidationError("event_bar contains fields for another aggregation kind")
            evidence = tuple(self.partition_evidence or ())
            if not evidence:
                raise ValidationError("event_bar requires partition evidence")
            selection_hashes = [item.source_selection_sha256 for item in evidence]
            if len(selection_hashes) != len(set(selection_hashes)):
                raise ValidationError("event-bar selection hashes must be globally unique")
            ordered = tuple(
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
            if evidence != ordered:
                raise ValidationError("event-bar partition evidence must be canonically sorted")
            previous_by_stream: dict[tuple[str, str, str], EventBarPartitionEvidence] = {}
            for item in evidence:
                previous = previous_by_stream.get(item.stream_key)
                if previous is not None and item.first_sequence <= previous.last_sequence:
                    raise ValidationError("event-bar evidence ranges overlap within one stream")
                previous_by_stream[item.stream_key] = item
            object.__setattr__(self, "partition_evidence", evidence)

    def to_contract(self) -> dict[str, Any]:
        threshold = (
            {
                "units": str(self.event_bar_threshold.units),
                "scale": self.event_bar_threshold.scale,
            }
            if self.event_bar_threshold is not None
            else None
        )
        return {
            "calendar_id": self.calendar_id,
            "session_policy_version": self.session_policy_version,
            "kind": self.kind,
            "recipe_version": self.recipe_version,
            "interval_ns": str(self.interval_ns) if self.interval_ns is not None else None,
            "session_rollup": self.session_rollup,
            "event_bar_basis": self.event_bar_basis,
            "event_bar_threshold": threshold,
            "market_context_snapshot_id": self.market_context_snapshot_id,
            "market_context_logical_sha256": self.market_context_logical_sha256,
            "source_event_schemas": [item.to_contract() for item in self.source_event_schemas],
            "partition_evidence": (
                [item.to_contract() for item in self.partition_evidence]
                if self.partition_evidence is not None
                else None
            ),
        }

    @classmethod
    def from_contract(cls, payload: Mapping[str, Any]) -> CuratedAggregation:
        fields = {
            "calendar_id",
            "session_policy_version",
            "kind",
            "recipe_version",
            "interval_ns",
            "session_rollup",
            "event_bar_basis",
            "event_bar_threshold",
            "market_context_snapshot_id",
            "market_context_logical_sha256",
            "source_event_schemas",
            "partition_evidence",
        }
        _closed_payload(payload, fields, "Curated aggregation")
        interval = payload["interval_ns"]
        threshold = payload["event_bar_threshold"]
        if threshold is not None:
            _closed_payload(threshold, {"units", "scale"}, "event-bar threshold")
            units = _canonical_integer(threshold["units"], "threshold units", positive=True)
            scale = threshold["scale"]
            if isinstance(scale, bool) or not isinstance(scale, int):
                raise ValidationError("threshold scale must be an integer")
            threshold_value: FixedPoint | None = FixedPoint(units=units, scale=scale)
        else:
            threshold_value = None
        raw_schemas = payload["source_event_schemas"]
        if not isinstance(raw_schemas, list):
            raise ValidationError("source_event_schemas must be an array")
        raw_evidence = payload["partition_evidence"]
        if raw_evidence is not None and not isinstance(raw_evidence, list):
            raise ValidationError("partition_evidence must be an array or null")
        return cls(
            calendar_id=payload["calendar_id"],
            session_policy_version=payload["session_policy_version"],
            kind=payload["kind"],
            recipe_version=payload["recipe_version"],
            interval_ns=(
                _canonical_integer(interval, "interval_ns", positive=True)
                if interval is not None
                else None
            ),
            session_rollup=payload["session_rollup"],
            event_bar_basis=payload["event_bar_basis"],
            event_bar_threshold=threshold_value,
            market_context_snapshot_id=payload["market_context_snapshot_id"],
            market_context_logical_sha256=payload["market_context_logical_sha256"],
            source_event_schemas=tuple(EventSchemaRef.from_contract(item) for item in raw_schemas),
            partition_evidence=(
                tuple(EventBarPartitionEvidence.from_contract(item) for item in raw_evidence)
                if raw_evidence is not None
                else None
            ),
        )


@dataclass(frozen=True, eq=False)
class VerifiedFactorInput:
    layer: Literal["curated", "normalized"]
    source_snapshot_id: str
    source_logical_sha256: str
    selection_logical_sha256: str
    event_schemas: tuple[EventSchemaRef, ...]
    table: pa.Table = field(repr=False)
    calendar_id: str = ""
    session_policy_version: str = ""
    market_context_snapshot_id: str = ""
    market_context_logical_sha256: str = ""
    lineage: tuple[LineageRef, ...] = ()
    aggregation: CuratedAggregation | None = None
    schema_id: str = VERIFIED_FACTOR_INPUT_SCHEMA_ID

    def __post_init__(self) -> None:
        if self.schema_id != VERIFIED_FACTOR_INPUT_SCHEMA_ID:
            raise ValidationError("unsupported VerifiedFactorInput schema")
        if self.layer not in {"curated", "normalized"}:
            raise ValidationError("unsupported verified input layer")
        _require_snapshot_id(self.source_snapshot_id, "source_snapshot_id")
        _require_hash(self.source_logical_sha256, "source_logical_sha256")
        _require_hash(self.selection_logical_sha256, "selection_logical_sha256")
        _required_text(self.calendar_id, "calendar_id")
        _required_text(self.session_policy_version, "session_policy_version")
        _require_snapshot_id(self.market_context_snapshot_id, "market_context_snapshot_id")
        _require_hash(self.market_context_logical_sha256, "market_context_logical_sha256")
        schemas = tuple(self.event_schemas)
        if not schemas or schemas != tuple(sorted(set(schemas))):
            raise ValidationError("event_schemas must be non-empty, unique, and sorted")
        if not isinstance(self.table, pa.Table) or self.table.num_rows <= 0:
            raise ValidationError("verified input table must be a non-empty Arrow table")
        lineage = tuple(self.lineage)
        if not lineage or lineage != tuple(sorted(lineage)):
            raise ValidationError("lineage must be non-empty and canonically ordered")
        if self.layer == "curated":
            if self.aggregation is None:
                raise ValidationError("Curated verified input requires aggregation metadata")
        elif self.aggregation is not None:
            raise ValidationError("Normalized verified input cannot contain aggregation metadata")
        object.__setattr__(self, "event_schemas", schemas)
        object.__setattr__(self, "lineage", lineage)

    @property
    def arrow_schema_sha256(self) -> str:
        import hashlib

        return hashlib.sha256(self.table.schema.serialize().to_pybytes()).hexdigest()

    def to_contract(self) -> dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "layer": self.layer,
            "source_snapshot_id": self.source_snapshot_id,
            "source_logical_sha256": self.source_logical_sha256,
            "selection_logical_sha256": self.selection_logical_sha256,
            "event_schemas": [item.to_contract() for item in self.event_schemas],
            "calendar_id": self.calendar_id,
            "session_policy_version": self.session_policy_version,
            "market_context_snapshot_id": self.market_context_snapshot_id,
            "market_context_logical_sha256": self.market_context_logical_sha256,
            "lineage": [item.to_contract() for item in self.lineage],
            "rows": str(self.table.num_rows),
            "arrow_schema_sha256": self.arrow_schema_sha256,
            "aggregation": self.aggregation.to_contract() if self.aggregation else None,
        }
