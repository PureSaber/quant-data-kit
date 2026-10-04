"""Versioned publication-time evidence with explicit interval topology.

The module validates the structure and internal consistency of supplied
evidence.  It does not authenticate publishers, archives, or caller-provided
identifiers, and it performs no I/O.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .common import utc
from .reconciliation import canonical

EVIDENCE_TIMING_SCHEMA_ID_V2 = "puresaber.evidence-timing/2"
_LEGACY_CONTEXT_V2 = object()

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_OFFSET = re.compile(r"([+-])(\d{2}):(\d{2})\Z")
_PLAIN_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")
_PRECISIONS = {"exact_timestamp", "minute", "date", "interval", "unknown"}
_MATERIAL_KINDS = {"official_document", "official_response_metadata", "trusted_archive"}
_ZONE_STATUSES = {"evidenced_iana", "evidenced_offset", "unverified", "not_applicable"}
_DISPLAY_RULES = {
    "exact",
    "truncated_minute",
    "rounded_half_up",
    "rounded_half_down",
    "rounded_half_even",
    "publication_date",
    "explicit_interval",
}
_DERIVATION_RULES = {
    "minute_truncate",
    "minute_round_half_up",
    "minute_round_half_down",
    "minute_round_half_even",
    "publication_date",
    "explicit_interval",
}
_DOWNGRADE_CODES = {
    "ZONE_UNVERIFIED",
    "DISPLAY_RULE_UNVERIFIED",
    "ROUNDING_TIE_UNVERIFIED",
    "ROUNDING_REFERENCE_UNVERIFIED",
    "PUBLICATION_DATE_SEMANTICS_UNVERIFIED",
    "MATERIAL_REFERENCE_INCOMPLETE",
    "MATERIAL_REFERENCE_CONFLICT",
    "DST_LOCAL_TIME_AMBIGUOUS",
    "DST_LOCAL_TIME_NONEXISTENT",
    "DATE_BOUNDARY_AMBIGUOUS",
    "DATE_BOUNDARY_NONEXISTENT",
    "NOMINAL_BOUND_AFTER_CAPTURE",
    "PRECISION_UNKNOWN",
    "POLICY_CAPTURE_ONLY",
}


def _mapping(value: object, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{context} must be an object")
    return value


def _keys(
    value: Mapping[str, Any],
    *,
    required: set[str],
    context: str,
) -> None:
    missing = required - set(value)
    unknown = set(value) - required
    if missing:
        raise ValueError(f"{context} is missing fields: {sorted(missing)}")
    if unknown:
        raise ValueError(f"{context} has unsupported fields: {sorted(unknown)}")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{field} must be a nonempty string")
    return value.strip()


def _optional_text(value: object, field: str) -> str | None:
    return None if value is None else _text(value, field)


def _timestamp(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a timezone-aware timestamp string")
    return utc(value, field).isoformat().replace("+00:00", "Z")


def _instant(value: str):
    return utc(value)


def _plain_minute(value: object, field: str) -> str:
    value = _text(value, field)
    try:
        parsed = _minute_value(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be YYYY-MM-DDTHH:MM without an offset") from exc
    return parsed.strftime("%Y-%m-%dT%H:%M")


def _minute_value(value: str) -> datetime:
    """Parse a deliberately naive source-local wall-clock minute."""

    return datetime.strptime(value, "%Y-%m-%dT%H:%M")  # noqa: DTZ007


def _plain_date(value: object, field: str) -> str:
    value = _text(value, field)
    if _PLAIN_DATE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a plain ISO calendar date")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a plain ISO calendar date") from exc
    return value


def _offset(value: object, field: str) -> str:
    value = _text(value, field)
    match = _OFFSET.fullmatch(value)
    if match is None:
        raise ValueError(f"{field} must be a canonical signed HH:MM offset")
    hours = int(match.group(2))
    minutes = int(match.group(3))
    if hours > 23 or minutes > 59:
        raise ValueError(f"{field} is outside the supported UTC offset range")
    total = hours * 60 + minutes
    if match.group(1) == "-":
        total = -total
    try:
        timezone(timedelta(minutes=total))
    except ValueError as exc:
        raise ValueError(f"{field} is outside the supported UTC offset range") from exc
    return value


def _offset_timezone(value: str) -> timezone:
    match = _OFFSET.fullmatch(value)
    assert match is not None
    total = int(match.group(2)) * 60 + int(match.group(3))
    if match.group(1) == "-":
        total = -total
    return timezone(timedelta(minutes=total))


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{field} must be an array of strings")
    result = tuple(_text(item, field) for item in value)
    if len(result) != len(set(result)):
        raise ValueError(f"{field} must contain unique values")
    return result


@dataclass(frozen=True)
class SourceMaterialReferenceV2:
    material_id: str
    kind: str
    archive_version: str
    sha256: str
    locator: str
    review_citation: str
    acquired_at: str

    def __post_init__(self) -> None:
        material_id = _text(self.material_id, "material_id")
        if self.kind not in _MATERIAL_KINDS:
            raise ValueError("unsupported source material kind")
        digest = _text(self.sha256, "sha256")
        if _SHA256.fullmatch(digest) is None:
            raise ValueError("sha256 must be 64 lowercase hexadecimal characters")
        object.__setattr__(self, "material_id", material_id)
        object.__setattr__(self, "archive_version", _text(self.archive_version, "archive_version"))
        object.__setattr__(self, "sha256", digest)
        object.__setattr__(self, "locator", _text(self.locator, "locator"))
        object.__setattr__(self, "review_citation", _text(self.review_citation, "review_citation"))
        object.__setattr__(self, "acquired_at", _timestamp(self.acquired_at, "acquired_at"))

    def to_dict(self) -> dict[str, str]:
        return {
            "material_id": self.material_id,
            "kind": self.kind,
            "archive_version": self.archive_version,
            "sha256": self.sha256,
            "locator": self.locator,
            "review_citation": self.review_citation,
            "acquired_at": self.acquired_at,
        }

    @classmethod
    def from_dict(cls, value: object) -> SourceMaterialReferenceV2:
        value = _mapping(value, "source material")
        _keys(
            value,
            required={
                "material_id",
                "kind",
                "archive_version",
                "sha256",
                "locator",
                "review_citation",
                "acquired_at",
            },
            context="source material",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class PublicationZoneEvidenceV2:
    status: str
    iana_name: str | None = None
    utc_offset: str | None = None
    fold: int | None = None
    material_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status not in _ZONE_STATUSES:
            raise ValueError("unsupported publication zone status")
        materials = _string_tuple(self.material_ids, "zone material_ids")
        iana_name = _optional_text(self.iana_name, "iana_name")
        offset = _offset(self.utc_offset, "utc_offset") if self.utc_offset is not None else None
        if self.fold not in {None, 0, 1}:
            raise ValueError("fold must be 0, 1, or null")
        if self.status == "evidenced_iana":
            if iana_name is None or not materials:
                raise ValueError("evidenced IANA zone requires a name and material IDs")
            try:
                ZoneInfo(iana_name)
            except (ZoneInfoNotFoundError, ValueError) as exc:
                raise ValueError("iana_name must identify an installed IANA zone") from exc
        elif self.status == "evidenced_offset":
            if offset is None or iana_name is not None or self.fold is not None or not materials:
                raise ValueError("evidenced offset requires only an offset and material IDs")
        elif any(item is not None for item in (iana_name, offset, self.fold)) or materials:
            raise ValueError("unverified or inapplicable zone cannot carry zone claims")
        object.__setattr__(self, "iana_name", iana_name)
        object.__setattr__(self, "utc_offset", offset)
        object.__setattr__(self, "material_ids", materials)

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "iana_name": self.iana_name,
            "utc_offset": self.utc_offset,
            "fold": self.fold,
            "material_ids": list(self.material_ids),
        }

    @classmethod
    def from_dict(cls, value: object) -> PublicationZoneEvidenceV2:
        value = _mapping(value, "publication zone")
        _keys(
            value,
            required={"status", "iana_name", "utc_offset", "fold", "material_ids"},
            context="publication zone",
        )
        return cls(
            status=value["status"],
            iana_name=value["iana_name"],
            utc_offset=value["utc_offset"],
            fold=value["fold"],
            material_ids=_string_tuple(value["material_ids"], "zone material_ids"),
        )


@dataclass(frozen=True)
class PublicationDisplayRuleV2:
    status: str
    value: str | None = None
    parity_reference: str | None = None
    material_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status not in {"evidenced", "unknown"}:
            raise ValueError("unsupported display rule status")
        materials = _string_tuple(self.material_ids, "display material_ids")
        value = _optional_text(self.value, "display rule value")
        parity = _optional_text(self.parity_reference, "parity_reference")
        if self.status == "evidenced":
            if value not in _DISPLAY_RULES or not materials:
                raise ValueError("evidenced display rule requires a supported value and materials")
            if value == "rounded_half_even":
                if parity not in {"minute_of_hour", "minute_index_since_unix_epoch"}:
                    raise ValueError("half-even requires a supported parity_reference")
            elif parity is not None:
                raise ValueError("parity_reference is only valid for half-even rounding")
        elif value is not None or parity is not None or materials:
            raise ValueError("unknown display rule cannot carry an evidenced rule")
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "parity_reference", parity)
        object.__setattr__(self, "material_ids", materials)

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "value": self.value,
            "parity_reference": self.parity_reference,
            "material_ids": list(self.material_ids),
        }

    @classmethod
    def from_dict(cls, value: object) -> PublicationDisplayRuleV2:
        value = _mapping(value, "publication display rule")
        _keys(
            value,
            required={"status", "value", "parity_reference", "material_ids"},
            context="publication display rule",
        )
        return cls(
            status=value["status"],
            value=value["value"],
            parity_reference=value["parity_reference"],
            material_ids=_string_tuple(value["material_ids"], "display material_ids"),
        )


@dataclass(frozen=True)
class PhysicalClockAccuracyV2:
    status: str
    max_error_seconds: int | None = None
    material_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status not in {"evidenced", "source_asserted", "unknown"}:
            raise ValueError("unsupported clock accuracy status")
        materials = _string_tuple(self.material_ids, "clock material_ids")
        if self.status == "unknown":
            if self.max_error_seconds is not None or materials:
                raise ValueError("unknown clock accuracy cannot claim an error bound")
        elif (
            isinstance(self.max_error_seconds, bool)
            or not isinstance(self.max_error_seconds, int)
            or self.max_error_seconds < 0
            or not materials
        ):
            raise ValueError("known clock accuracy requires a nonnegative bound and materials")
        object.__setattr__(self, "material_ids", materials)

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "max_error_seconds": self.max_error_seconds,
            "material_ids": list(self.material_ids),
        }

    @classmethod
    def from_dict(cls, value: object) -> PhysicalClockAccuracyV2:
        value = _mapping(value, "physical clock accuracy")
        _keys(
            value,
            required={"status", "max_error_seconds", "material_ids"},
            context="physical clock accuracy",
        )
        return cls(
            status=value["status"],
            max_error_seconds=value["max_error_seconds"],
            material_ids=_string_tuple(value["material_ids"], "clock material_ids"),
        )


@dataclass(frozen=True)
class NominalPublicationIntervalV2:
    earliest: str
    earliest_inclusive: bool
    latest: str
    latest_inclusive: bool
    derivation_rule: str
    material_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        earliest = _timestamp(self.earliest, "earliest")
        latest = _timestamp(self.latest, "latest")
        if _instant(earliest) >= _instant(latest):
            raise ValueError("nominal publication interval must have positive duration")
        if type(self.earliest_inclusive) is not bool or type(self.latest_inclusive) is not bool:
            raise TypeError("interval endpoint flags must be booleans")
        if self.derivation_rule not in _DERIVATION_RULES:
            raise ValueError("unsupported nominal interval derivation rule")
        materials = _string_tuple(self.material_ids, "interval material_ids")
        if not materials:
            raise ValueError("nominal interval requires material IDs")
        object.__setattr__(self, "earliest", earliest)
        object.__setattr__(self, "latest", latest)
        object.__setattr__(self, "material_ids", materials)

    def to_dict(self) -> dict[str, object]:
        return {
            "earliest": self.earliest,
            "earliest_inclusive": self.earliest_inclusive,
            "latest": self.latest,
            "latest_inclusive": self.latest_inclusive,
            "derivation_rule": self.derivation_rule,
            "material_ids": list(self.material_ids),
        }

    @classmethod
    def from_dict(cls, value: object) -> NominalPublicationIntervalV2:
        value = _mapping(value, "nominal publication interval")
        _keys(
            value,
            required={
                "earliest",
                "earliest_inclusive",
                "latest",
                "latest_inclusive",
                "derivation_rule",
                "material_ids",
            },
            context="nominal publication interval",
        )
        return cls(
            earliest=value["earliest"],
            earliest_inclusive=value["earliest_inclusive"],
            latest=value["latest"],
            latest_inclusive=value["latest_inclusive"],
            derivation_rule=value["derivation_rule"],
            material_ids=_string_tuple(value["material_ids"], "interval material_ids"),
        )


@dataclass(frozen=True)
class DowngradeReasonV2:
    code: str
    field: str
    material_ids: tuple[str, ...]
    detail: str

    def __post_init__(self) -> None:
        if self.code not in _DOWNGRADE_CODES:
            raise ValueError("unsupported downgrade reason code")
        object.__setattr__(self, "field", _text(self.field, "downgrade field"))
        object.__setattr__(self, "material_ids", _string_tuple(self.material_ids, "material_ids"))
        object.__setattr__(self, "detail", _text(self.detail, "downgrade detail"))

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "field": self.field,
            "material_ids": list(self.material_ids),
            "detail": self.detail,
        }

    @classmethod
    def from_dict(cls, value: object) -> DowngradeReasonV2:
        value = _mapping(value, "downgrade reason")
        _keys(
            value,
            required={"code", "field", "material_ids", "detail"},
            context="downgrade reason",
        )
        return cls(
            code=value["code"],
            field=value["field"],
            material_ids=_string_tuple(value["material_ids"], "material_ids"),
            detail=value["detail"],
        )


@dataclass(frozen=True)
class SourcePublicationV2:
    raw_text: str
    precision: str
    exact_at: str | None
    local_minute: str | None
    local_date: str | None
    explicit_interval: NominalPublicationIntervalV2 | None
    zone: PublicationZoneEvidenceV2
    display_rule: PublicationDisplayRuleV2
    clock_accuracy: PhysicalClockAccuracyV2
    nominal_interval: NominalPublicationIntervalV2 | None

    def __post_init__(self) -> None:
        if not isinstance(self.raw_text, str):
            raise TypeError("raw_text must be a string")
        raw = self.raw_text.strip()
        if not raw and self.precision != "unknown":
            raise ValueError("raw_text may be empty only when precision is unknown")
        if self.precision not in _PRECISIONS:
            raise ValueError("unsupported source publication precision")
        exact = _timestamp(self.exact_at, "exact_at") if self.exact_at is not None else None
        minute = (
            _plain_minute(self.local_minute, "local_minute")
            if self.local_minute is not None
            else None
        )
        publication_date = (
            _plain_date(self.local_date, "local_date") if self.local_date is not None else None
        )
        if not isinstance(self.zone, PublicationZoneEvidenceV2):
            raise TypeError("zone must be PublicationZoneEvidenceV2")
        if not isinstance(self.display_rule, PublicationDisplayRuleV2):
            raise TypeError("display_rule must be PublicationDisplayRuleV2")
        if not isinstance(self.clock_accuracy, PhysicalClockAccuracyV2):
            raise TypeError("clock_accuracy must be PhysicalClockAccuracyV2")
        if self.explicit_interval is not None and not isinstance(
            self.explicit_interval, NominalPublicationIntervalV2
        ):
            raise TypeError("explicit_interval must be NominalPublicationIntervalV2 or None")
        if self.nominal_interval is not None and not isinstance(
            self.nominal_interval, NominalPublicationIntervalV2
        ):
            raise TypeError("nominal_interval must be NominalPublicationIntervalV2 or None")
        populated = sum(item is not None for item in (exact, minute, publication_date))
        if self.precision == "exact_timestamp":
            if populated != 1 or exact is None or self.explicit_interval is not None:
                raise ValueError("exact precision requires only exact_at")
        elif self.precision == "minute":
            if populated != 1 or minute is None or self.explicit_interval is not None:
                raise ValueError("minute precision requires only local_minute")
        elif self.precision == "date":
            if populated != 1 or publication_date is None or self.explicit_interval is not None:
                raise ValueError("date precision requires only local_date")
        elif self.precision == "interval":
            if populated or self.explicit_interval is None:
                raise ValueError("interval precision requires only explicit_interval")
            if self.nominal_interval != self.explicit_interval:
                raise ValueError("explicit interval must be the nominal interval")
        elif populated or self.explicit_interval is not None or self.nominal_interval is not None:
            raise ValueError("unknown precision cannot carry normalized publication values")
        object.__setattr__(self, "raw_text", raw)
        object.__setattr__(self, "exact_at", exact)
        object.__setattr__(self, "local_minute", minute)
        object.__setattr__(self, "local_date", publication_date)

    def material_ids(self) -> tuple[str, ...]:
        values = [*self.zone.material_ids, *self.display_rule.material_ids]
        values.extend(self.clock_accuracy.material_ids)
        if self.explicit_interval is not None:
            values.extend(self.explicit_interval.material_ids)
        if self.nominal_interval is not None:
            values.extend(self.nominal_interval.material_ids)
        return tuple(dict.fromkeys(values))

    def to_dict(self) -> dict[str, object]:
        return {
            "raw_text": self.raw_text,
            "precision": self.precision,
            "exact_at": self.exact_at,
            "local_minute": self.local_minute,
            "local_date": self.local_date,
            "explicit_interval": (
                self.explicit_interval.to_dict() if self.explicit_interval is not None else None
            ),
            "zone": self.zone.to_dict(),
            "display_rule": self.display_rule.to_dict(),
            "clock_accuracy": self.clock_accuracy.to_dict(),
            "nominal_interval": (
                self.nominal_interval.to_dict() if self.nominal_interval is not None else None
            ),
        }

    @classmethod
    def from_dict(cls, value: object) -> SourcePublicationV2:
        value = _mapping(value, "source publication")
        _keys(
            value,
            required={
                "raw_text",
                "precision",
                "exact_at",
                "local_minute",
                "local_date",
                "explicit_interval",
                "zone",
                "display_rule",
                "clock_accuracy",
                "nominal_interval",
            },
            context="source publication",
        )
        explicit = value["explicit_interval"]
        nominal = value["nominal_interval"]
        return cls(
            raw_text=value["raw_text"],
            precision=value["precision"],
            exact_at=value["exact_at"],
            local_minute=value["local_minute"],
            local_date=value["local_date"],
            explicit_interval=(
                NominalPublicationIntervalV2.from_dict(explicit) if explicit is not None else None
            ),
            zone=PublicationZoneEvidenceV2.from_dict(value["zone"]),
            display_rule=PublicationDisplayRuleV2.from_dict(value["display_rule"]),
            clock_accuracy=PhysicalClockAccuracyV2.from_dict(value["clock_accuracy"]),
            nominal_interval=(
                NominalPublicationIntervalV2.from_dict(nominal) if nominal is not None else None
            ),
        )


@dataclass(frozen=True)
class AvailabilityDecisionV2:
    boundary_at: str
    admission_relation: str
    mode: str
    basis: str
    trust_model: str
    usage_scope: str
    physical_clock_accuracy: str
    risk_note: str | None
    derivation_version: str
    material_ids: tuple[str, ...]
    downgrade_reasons: tuple[DowngradeReasonV2, ...]
    legacy_timing_pointer: str | None = None

    def __post_init__(self) -> None:
        boundary = _timestamp(self.boundary_at, "boundary_at")
        if self.admission_relation not in {"at_or_after", "strictly_after"}:
            raise ValueError("unsupported admission relation")
        if self.mode not in {
            "source_declared_nominal",
            "conservative_nominal_bound",
            "captured_only",
        }:
            raise ValueError("unsupported availability mode")
        if self.basis not in {
            "source_declared_instant",
            "source_interval_upper",
            "local_capture",
            "legacy_available_at",
        }:
            raise ValueError("unsupported availability basis")
        if self.trust_model not in {"trust_source_declared_time", "capture_receipt_only"}:
            raise ValueError("unsupported availability trust model")
        if self.usage_scope not in {"retrospective_only", "forward_and_retrospective"}:
            raise ValueError("unsupported availability usage scope")
        if self.physical_clock_accuracy not in {"known", "source_asserted", "unknown"}:
            raise ValueError("unsupported physical clock accuracy")
        materials = _string_tuple(self.material_ids, "availability material_ids")
        reasons = tuple(self.downgrade_reasons)
        if not all(isinstance(item, DowngradeReasonV2) for item in reasons):
            raise TypeError("downgrade_reasons must contain DowngradeReasonV2 values")
        pointer = _optional_text(self.legacy_timing_pointer, "legacy_timing_pointer")
        risk_note = _optional_text(self.risk_note, "risk_note")
        if self.mode == "captured_only":
            if (
                self.basis != "local_capture"
                or self.admission_relation != "at_or_after"
                or self.trust_model != "capture_receipt_only"
                or self.usage_scope != "forward_and_retrospective"
                or materials
                or not reasons
                or pointer is not None
            ):
                raise ValueError("captured_only availability has inconsistent fields")
        else:
            if (
                self.trust_model != "trust_source_declared_time"
                or self.usage_scope != "retrospective_only"
                or reasons
            ):
                raise ValueError("nominal availability requires retrospective source trust")
            if self.basis == "legacy_available_at":
                if pointer is None or materials:
                    raise ValueError("legacy availability requires only a legacy timing pointer")
            elif pointer is not None or not materials:
                raise ValueError("nominal availability requires materials and no legacy pointer")
            if self.physical_clock_accuracy == "unknown" and risk_note is None:
                raise ValueError("unknown physical accuracy requires a risk note")
        object.__setattr__(self, "boundary_at", boundary)
        object.__setattr__(self, "risk_note", risk_note)
        object.__setattr__(
            self, "derivation_version", _text(self.derivation_version, "derivation_version")
        )
        object.__setattr__(self, "material_ids", materials)
        object.__setattr__(self, "downgrade_reasons", reasons)
        object.__setattr__(self, "legacy_timing_pointer", pointer)

    def to_dict(self) -> dict[str, object]:
        return {
            "boundary_at": self.boundary_at,
            "admission_relation": self.admission_relation,
            "mode": self.mode,
            "basis": self.basis,
            "trust_model": self.trust_model,
            "usage_scope": self.usage_scope,
            "physical_clock_accuracy": self.physical_clock_accuracy,
            "risk_note": self.risk_note,
            "derivation_version": self.derivation_version,
            "material_ids": list(self.material_ids),
            "downgrade_reasons": [item.to_dict() for item in self.downgrade_reasons],
            "legacy_timing_pointer": self.legacy_timing_pointer,
        }

    @classmethod
    def from_dict(cls, value: object) -> AvailabilityDecisionV2:
        value = _mapping(value, "availability decision")
        _keys(
            value,
            required={
                "boundary_at",
                "admission_relation",
                "mode",
                "basis",
                "trust_model",
                "usage_scope",
                "physical_clock_accuracy",
                "risk_note",
                "derivation_version",
                "material_ids",
                "downgrade_reasons",
                "legacy_timing_pointer",
            },
            context="availability decision",
        )
        reasons = value["downgrade_reasons"]
        if not isinstance(reasons, list):
            raise TypeError("downgrade_reasons must be an array")
        return cls(
            boundary_at=value["boundary_at"],
            admission_relation=value["admission_relation"],
            mode=value["mode"],
            basis=value["basis"],
            trust_model=value["trust_model"],
            usage_scope=value["usage_scope"],
            physical_clock_accuracy=value["physical_clock_accuracy"],
            risk_note=value["risk_note"],
            derivation_version=value["derivation_version"],
            material_ids=_string_tuple(value["material_ids"], "availability material_ids"),
            downgrade_reasons=tuple(DowngradeReasonV2.from_dict(item) for item in reasons),
            legacy_timing_pointer=value["legacy_timing_pointer"],
        )


@dataclass(frozen=True)
class EvidenceRevisionV2:
    revision_id: str
    supersedes_timing_sha256: str | None
    acquired_at: str
    usage_scope: str

    def __post_init__(self) -> None:
        revision_id = _text(self.revision_id, "revision_id")
        supersedes = self.supersedes_timing_sha256
        if supersedes is not None and (
            not isinstance(supersedes, str) or _SHA256.fullmatch(supersedes) is None
        ):
            raise ValueError("supersedes_timing_sha256 must be a lowercase SHA-256")
        if self.usage_scope not in {"retrospective_only", "forward_and_retrospective"}:
            raise ValueError("unsupported revision usage scope")
        object.__setattr__(self, "revision_id", revision_id)
        object.__setattr__(
            self, "acquired_at", _timestamp(self.acquired_at, "revision acquired_at")
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "revision_id": self.revision_id,
            "supersedes_timing_sha256": self.supersedes_timing_sha256,
            "acquired_at": self.acquired_at,
            "usage_scope": self.usage_scope,
        }

    @classmethod
    def from_dict(cls, value: object) -> EvidenceRevisionV2:
        value = _mapping(value, "evidence revision")
        _keys(
            value,
            required={"revision_id", "supersedes_timing_sha256", "acquired_at", "usage_scope"},
            context="evidence revision",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class AdmissionBoundaryV2:
    boundary_at: str
    relation: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "boundary_at", _timestamp(self.boundary_at, "boundary_at"))
        if self.relation not in {"at_or_after", "strictly_after"}:
            raise ValueError("unsupported admission boundary relation")

    def admits(self, cutoff: str | datetime) -> bool:
        point = utc(cutoff, "cutoff")
        boundary = _instant(self.boundary_at)
        return point >= boundary if self.relation == "at_or_after" else point > boundary


@dataclass(frozen=True)
class EvidenceTimingV2:
    effective_at: str
    captured_at: str
    source_materials: tuple[SourceMaterialReferenceV2, ...]
    source_publication: SourcePublicationV2
    availability: AvailabilityDecisionV2
    revision: EvidenceRevisionV2
    schema: str = EVIDENCE_TIMING_SCHEMA_ID_V2
    _legacy_context: object | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.schema != EVIDENCE_TIMING_SCHEMA_ID_V2:
            raise ValueError("unsupported evidence timing schema")
        effective = _timestamp(self.effective_at, "effective_at")
        captured = _timestamp(self.captured_at, "captured_at")
        materials = tuple(self.source_materials)
        if not all(isinstance(item, SourceMaterialReferenceV2) for item in materials):
            raise TypeError("source_materials must contain SourceMaterialReferenceV2 values")
        material_ids = [item.material_id for item in materials]
        if len(material_ids) != len(set(material_ids)):
            raise ValueError("source material IDs must be unique")
        if not isinstance(self.source_publication, SourcePublicationV2):
            raise TypeError("source_publication must be SourcePublicationV2")
        if not isinstance(self.availability, AvailabilityDecisionV2):
            raise TypeError("availability must be AvailabilityDecisionV2")
        if not isinstance(self.revision, EvidenceRevisionV2):
            raise TypeError("revision must be EvidenceRevisionV2")
        known = set(material_ids)
        referenced = set(self.source_publication.material_ids()) | set(
            self.availability.material_ids
        )
        for reason in self.availability.downgrade_reasons:
            referenced.update(reason.material_ids)
        if not referenced <= known:
            raise ValueError("timing contains dangling source material IDs")
        revision_at = _instant(self.revision.acquired_at)
        if revision_at < _instant(captured):
            raise ValueError("revision acquired_at cannot precede captured_at")
        if any(_instant(item.acquired_at) > revision_at for item in materials):
            raise ValueError("source material cannot be acquired after the revision")
        boundary = _instant(self.availability.boundary_at)
        if (
            self.availability.basis == "legacy_available_at"
            and self._legacy_context is not _LEGACY_CONTEXT_V2
        ):
            raise ValueError("legacy_available_at requires a validated lifecycle migration binding")
        if self.availability.mode == "captured_only":
            if boundary != _instant(captured):
                raise ValueError("captured_only boundary must equal captured_at")
        elif boundary > _instant(captured):
            raise ValueError("nominal availability boundary cannot follow captured_at")
        self._validate_publication_decision()
        expected_accuracy = {
            "evidenced": "known",
            "source_asserted": "source_asserted",
            "unknown": "unknown",
        }[self.source_publication.clock_accuracy.status]
        if self.availability.physical_clock_accuracy != expected_accuracy:
            raise ValueError("availability physical clock accuracy conflicts with source evidence")
        if self.source_publication.nominal_interval is not None:
            expected_interval = _derive_nominal_interval_from_source(self.source_publication)
            if expected_interval != self.source_publication.nominal_interval:
                raise ValueError(
                    "nominal publication interval does not match its source derivation"
                )
        if self.revision.usage_scope != self.availability.usage_scope:
            raise ValueError("revision and availability usage scopes must match")
        object.__setattr__(self, "effective_at", effective)
        object.__setattr__(self, "captured_at", captured)
        object.__setattr__(self, "source_materials", materials)

    def _validate_publication_decision(self) -> None:
        source = self.source_publication
        decision = self.availability
        interval = source.nominal_interval
        if decision.mode == "captured_only":
            return
        if decision.basis == "legacy_available_at":
            if source.precision not in {"exact_timestamp", "date", "unknown"}:
                raise ValueError("legacy availability has an unsupported source precision")
            return
        if source.precision == "exact_timestamp":
            if (
                decision.mode != "source_declared_nominal"
                or decision.basis != "source_declared_instant"
                or decision.admission_relation != "at_or_after"
                or decision.boundary_at != source.exact_at
            ):
                raise ValueError("exact source timestamp has an inconsistent availability")
            return
        if interval is None:
            raise ValueError("nominal availability requires a derived interval")
        expected_relation = "strictly_after" if interval.latest_inclusive else "at_or_after"
        if (
            decision.mode != "conservative_nominal_bound"
            or decision.basis != "source_interval_upper"
            or decision.boundary_at != interval.latest
            or decision.admission_relation != expected_relation
        ):
            raise ValueError("interval availability has an inconsistent upper boundary")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "effective_at": self.effective_at,
            "captured_at": self.captured_at,
            "source_materials": [item.to_dict() for item in self.source_materials],
            "source_publication": self.source_publication.to_dict(),
            "availability": self.availability.to_dict(),
            "revision": self.revision.to_dict(),
        }

    def to_json(self) -> str:
        return canonical(self.to_dict()).decode("utf-8")

    def fingerprint(self) -> str:
        return hashlib.sha256(canonical(self.to_dict())).hexdigest()

    @classmethod
    def from_dict(cls, value: object) -> EvidenceTimingV2:
        return cls._from_dict(value)

    @classmethod
    def _from_dict(cls, value: object, *, legacy_context: object | None = None) -> EvidenceTimingV2:
        value = _mapping(value, "evidence timing v2")
        _keys(
            value,
            required={
                "schema",
                "effective_at",
                "captured_at",
                "source_materials",
                "source_publication",
                "availability",
                "revision",
            },
            context="evidence timing v2",
        )
        materials = value["source_materials"]
        if not isinstance(materials, list):
            raise TypeError("source_materials must be an array")
        return cls(
            schema=value["schema"],
            effective_at=value["effective_at"],
            captured_at=value["captured_at"],
            source_materials=tuple(SourceMaterialReferenceV2.from_dict(item) for item in materials),
            source_publication=SourcePublicationV2.from_dict(value["source_publication"]),
            availability=AvailabilityDecisionV2.from_dict(value["availability"]),
            revision=EvidenceRevisionV2.from_dict(value["revision"]),
            _legacy_context=legacy_context,
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> EvidenceTimingV2:
        import json

        if not isinstance(value, (str, bytes)):
            raise TypeError("serialized evidence timing must be str or bytes")
        try:
            payload = json.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError("invalid evidence timing JSON") from exc
        return cls.from_dict(payload)

    def admission_boundary(self, *, study_mode: str, trust_model: str) -> AdmissionBoundaryV2:
        if study_mode not in {"natural_forward", "retrospective"}:
            raise ValueError("unsupported study_mode")
        if trust_model not in {"trust_source_declared_time", "capture_receipt_only"}:
            raise ValueError("unsupported trust_model")
        if study_mode == "natural_forward":
            points = [_instant(self.captured_at), _instant(self.revision.acquired_at)]
            points.extend(_instant(item.acquired_at) for item in self.source_materials)
            return AdmissionBoundaryV2(max(points).isoformat(), "at_or_after")
        if self.availability.mode != "captured_only":
            if trust_model != "trust_source_declared_time":
                raise ValueError("nominal availability requires trust_source_declared_time")
            return AdmissionBoundaryV2(
                self.availability.boundary_at, self.availability.admission_relation
            )
        return AdmissionBoundaryV2(self.captured_at, "at_or_after")

    def is_admitted(self, cutoff: str | datetime, *, study_mode: str, trust_model: str) -> bool:
        return self.admission_boundary(study_mode=study_mode, trust_model=trust_model).admits(
            cutoff
        )


@dataclass(frozen=True)
class EvidenceTimingBuildResultV2:
    timing: EvidenceTimingV2
    downgrade_reasons: tuple[DowngradeReasonV2, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.timing, EvidenceTimingV2):
            raise TypeError("timing must be EvidenceTimingV2")
        reasons = tuple(self.downgrade_reasons)
        if reasons != self.timing.availability.downgrade_reasons:
            raise ValueError("build-result downgrade reasons must match the timing")
        object.__setattr__(self, "downgrade_reasons", reasons)


class _LocalResolutionError(ValueError):
    def __init__(self, code: str, detail: str):
        self.code = code
        super().__init__(detail)


def _valid_zone_instants(local_value: datetime, zone: ZoneInfo) -> list[tuple[int, datetime, str]]:
    candidates: dict[datetime, tuple[int, datetime, str]] = {}
    for fold in (0, 1):
        aware = local_value.replace(tzinfo=zone, fold=fold)
        instant = aware.astimezone(timezone.utc)
        round_trip = instant.astimezone(zone)
        if round_trip.replace(tzinfo=None) == local_value:
            offset = aware.strftime("%z")
            offset = f"{offset[:3]}:{offset[3:]}"
            candidates.setdefault(instant, (fold, instant, offset))
    return list(candidates.values())


def _resolve_local(
    local_value: datetime,
    zone: PublicationZoneEvidenceV2,
    *,
    ambiguous_code: str,
    nonexistent_code: str,
) -> datetime:
    if zone.status == "evidenced_offset":
        assert zone.utc_offset is not None
        return local_value.replace(tzinfo=_offset_timezone(zone.utc_offset)).astimezone(
            timezone.utc
        )
    if zone.status != "evidenced_iana":
        raise _LocalResolutionError("ZONE_UNVERIFIED", "source timezone is not evidenced")
    assert zone.iana_name is not None
    candidates = _valid_zone_instants(local_value, ZoneInfo(zone.iana_name))
    if not candidates:
        raise _LocalResolutionError(nonexistent_code, "local source time does not exist")
    if zone.utc_offset is not None:
        candidates = [item for item in candidates if item[2] == zone.utc_offset]
        if not candidates:
            raise _LocalResolutionError(
                "MATERIAL_REFERENCE_CONFLICT", "IANA zone and exact offset conflict"
            )
    if zone.fold is not None:
        candidates = [item for item in candidates if item[0] == zone.fold]
        if not candidates:
            raise _LocalResolutionError(
                "MATERIAL_REFERENCE_CONFLICT", "IANA zone and fold conflict"
            )
    instants = {item[1] for item in candidates}
    if len(instants) != 1:
        raise _LocalResolutionError(ambiguous_code, "local source time is ambiguous")
    return next(iter(instants))


def _derive_nominal_interval_from_source(
    source: SourcePublicationV2,
) -> NominalPublicationIntervalV2:
    if source.precision == "interval":
        if source.explicit_interval is None:
            raise ValueError("interval source lacks an explicit interval")
        return source.explicit_interval
    if source.precision == "minute":
        if source.local_minute is None or source.display_rule.status != "evidenced":
            raise ValueError("minute interval lacks a local minute or display rule")
        anchor = _resolve_local(
            _minute_value(source.local_minute),
            source.zone,
            ambiguous_code="DST_LOCAL_TIME_AMBIGUOUS",
            nonexistent_code="DST_LOCAL_TIME_NONEXISTENT",
        )
        materials = tuple(
            dict.fromkeys((*source.zone.material_ids, *source.display_rule.material_ids))
        )
        rule = source.display_rule.value
        if rule == "truncated_minute":
            return NominalPublicationIntervalV2(
                anchor.isoformat(),
                True,
                (anchor + timedelta(minutes=1)).isoformat(),
                False,
                "minute_truncate",
                materials,
            )
        if rule not in {"rounded_half_up", "rounded_half_down", "rounded_half_even"}:
            raise ValueError("minute interval has an incompatible display rule")
        lower_inclusive = rule == "rounded_half_up"
        upper_inclusive = rule == "rounded_half_down"
        if rule == "rounded_half_even":
            local = _minute_value(source.local_minute)
            if source.display_rule.parity_reference == "minute_of_hour":
                even = local.minute % 2 == 0
            elif source.display_rule.parity_reference == "minute_index_since_unix_epoch":
                even = int(anchor.timestamp()) // 60 % 2 == 0
            else:
                raise ValueError("half-even interval lacks a parity reference")
            lower_inclusive = upper_inclusive = even
        return NominalPublicationIntervalV2(
            (anchor - timedelta(seconds=30)).isoformat(),
            lower_inclusive,
            (anchor + timedelta(seconds=30)).isoformat(),
            upper_inclusive,
            f"minute_round_{rule.removeprefix('rounded_')}",
            materials,
        )
    if source.precision == "date":
        if (
            source.local_date is None
            or source.zone.status != "evidenced_iana"
            or source.zone.iana_name is None
            or source.display_rule.value != "publication_date"
        ):
            raise ValueError("date interval lacks evidenced date semantics and IANA zone")
        current = datetime.combine(date.fromisoformat(source.local_date), datetime.min.time())
        following = datetime.combine(
            date.fromisoformat(source.local_date) + timedelta(days=1), datetime.min.time()
        )
        midnight_zone = PublicationZoneEvidenceV2(
            "evidenced_iana", source.zone.iana_name, None, None, source.zone.material_ids
        )
        start = _resolve_local(
            current,
            midnight_zone,
            ambiguous_code="DATE_BOUNDARY_AMBIGUOUS",
            nonexistent_code="DATE_BOUNDARY_NONEXISTENT",
        )
        end = _resolve_local(
            following,
            midnight_zone,
            ambiguous_code="DATE_BOUNDARY_AMBIGUOUS",
            nonexistent_code="DATE_BOUNDARY_NONEXISTENT",
        )
        return NominalPublicationIntervalV2(
            start.isoformat(),
            True,
            end.isoformat(),
            False,
            "publication_date",
            tuple(dict.fromkeys((*source.zone.material_ids, *source.display_rule.material_ids))),
        )
    raise ValueError("source precision cannot derive a nominal interval")


def _reason(
    code: str, field: str, detail: str, material_ids: Sequence[str] = ()
) -> DowngradeReasonV2:
    return DowngradeReasonV2(code, field, tuple(material_ids), detail)


def _complete_material_ids(
    material_ids: Sequence[str], known: set[str]
) -> tuple[tuple[str, ...], DowngradeReasonV2 | None]:
    normalized = tuple(dict.fromkeys(_text(item, "material_id") for item in material_ids))
    if not normalized or not set(normalized) <= known:
        return (), _reason(
            "MATERIAL_REFERENCE_INCOMPLETE",
            "source_materials",
            "the asserted evidence lacks a complete material reference",
            tuple(item for item in normalized if item in known),
        )
    return normalized, None


def build_evidence_timing_v2(
    *,
    effective_at: str,
    captured_at: str,
    raw_text: str,
    precision: str,
    revision_id: str,
    source_materials: Sequence[SourceMaterialReferenceV2] = (),
    exact_at: str | None = None,
    local_minute: str | None = None,
    local_date: str | None = None,
    explicit_interval: NominalPublicationIntervalV2 | None = None,
    zone_status: str = "unverified",
    iana_name: str | None = None,
    utc_offset: str | None = None,
    fold: int | None = None,
    zone_material_ids: Sequence[str] = (),
    display_rule: str | None = None,
    parity_reference: str | None = None,
    display_material_ids: Sequence[str] = (),
    clock_accuracy_status: str = "unknown",
    max_error_seconds: int | None = None,
    clock_material_ids: Sequence[str] = (),
    study_mode: str = "natural_forward",
    trust_model: str = "capture_receipt_only",
    risk_note: str | None = None,
    supersedes_timing_sha256: str | None = None,
    acquired_at: str | None = None,
) -> EvidenceTimingBuildResultV2:
    """Build a strict timing, explicitly downgrading incomplete claims."""

    materials = tuple(source_materials)
    if not all(isinstance(item, SourceMaterialReferenceV2) for item in materials):
        raise TypeError("source_materials must contain SourceMaterialReferenceV2 values")
    known = {item.material_id for item in materials}
    if len(known) != len(materials):
        raise ValueError("source material IDs must be unique")
    captured = _timestamp(captured_at, "captured_at")
    effective = _timestamp(effective_at, "effective_at")
    revision_acquired = _timestamp(acquired_at or captured, "acquired_at")
    material_latest = max(
        (_instant(item.acquired_at) for item in materials), default=_instant(captured)
    )
    if _instant(revision_acquired) < material_latest:
        revision_acquired = material_latest.isoformat().replace("+00:00", "Z")
    reasons: list[DowngradeReasonV2] = []

    zone_ids, zone_material_error = _complete_material_ids(zone_material_ids, known)
    if zone_status in {"evidenced_iana", "evidenced_offset"} and zone_material_error:
        reasons.append(zone_material_error)
        zone_status = "unverified"
        iana_name = None
        utc_offset = None
        fold = None
    try:
        zone = PublicationZoneEvidenceV2(
            zone_status,
            iana_name,
            utc_offset,
            fold,
            zone_ids if zone_status.startswith("evidenced") else (),
        )
    except (TypeError, ValueError) as exc:
        reasons.append(
            _reason("MATERIAL_REFERENCE_CONFLICT", "source_publication.zone", str(exc), zone_ids)
        )
        zone = PublicationZoneEvidenceV2("unverified")

    display_ids, display_material_error = _complete_material_ids(display_material_ids, known)
    display_status = "evidenced" if display_rule is not None else "unknown"
    if display_status == "evidenced" and display_material_error:
        reasons.append(display_material_error)
        display_status = "unknown"
        display_rule = None
        parity_reference = None
    try:
        display = PublicationDisplayRuleV2(
            display_status,
            display_rule,
            parity_reference,
            display_ids if display_status == "evidenced" else (),
        )
    except (TypeError, ValueError) as exc:
        code = (
            "ROUNDING_REFERENCE_UNVERIFIED"
            if display_rule == "rounded_half_even"
            else "MATERIAL_REFERENCE_CONFLICT"
        )
        reasons.append(_reason(code, "source_publication.display_rule", str(exc), display_ids))
        display = PublicationDisplayRuleV2("unknown")

    clock_ids, clock_material_error = _complete_material_ids(clock_material_ids, known)
    if clock_accuracy_status != "unknown" and clock_material_error:
        reasons.append(clock_material_error)
        clock_accuracy_status = "unknown"
        max_error_seconds = None
    try:
        clock = PhysicalClockAccuracyV2(
            clock_accuracy_status,
            max_error_seconds,
            clock_ids if clock_accuracy_status != "unknown" else (),
        )
    except (TypeError, ValueError) as exc:
        reasons.append(
            _reason("MATERIAL_REFERENCE_CONFLICT", "source_publication.clock_accuracy", str(exc))
        )
        clock = PhysicalClockAccuracyV2("unknown")

    nominal: NominalPublicationIntervalV2 | None = None
    normalized_precision = precision
    normalized_explicit_interval = explicit_interval
    exact = _timestamp(exact_at, "exact_at") if exact_at is not None else None
    minute = _plain_minute(local_minute, "local_minute") if local_minute is not None else None
    pub_date = _plain_date(local_date, "local_date") if local_date is not None else None
    if precision == "minute":
        if zone.status not in {"evidenced_iana", "evidenced_offset"}:
            reasons.append(
                _reason(
                    "ZONE_UNVERIFIED",
                    "source_publication.zone",
                    "minute value lacks an evidenced zone",
                )
            )
        if display.status != "evidenced":
            reasons.append(
                _reason(
                    "DISPLAY_RULE_UNVERIFIED",
                    "source_publication.display_rule",
                    "minute value lacks an evidenced display rule",
                )
            )
        elif display.value not in {
            "truncated_minute",
            "rounded_half_up",
            "rounded_half_down",
            "rounded_half_even",
        }:
            reasons.append(
                _reason(
                    "DISPLAY_RULE_UNVERIFIED",
                    "source_publication.display_rule",
                    "display rule is incompatible with minute precision",
                    display.material_ids,
                )
            )
        if minute is None:
            raise ValueError("minute precision requires local_minute")
        if not reasons:
            try:
                anchor = _resolve_local(
                    _minute_value(minute),
                    zone,
                    ambiguous_code="DST_LOCAL_TIME_AMBIGUOUS",
                    nonexistent_code="DST_LOCAL_TIME_NONEXISTENT",
                )
                interval_materials = tuple(
                    dict.fromkeys((*zone.material_ids, *display.material_ids))
                )
                if display.value == "truncated_minute":
                    bounds = (anchor, True, anchor + timedelta(minutes=1), False)
                    derivation = "minute_truncate"
                else:
                    assert display.value is not None
                    lower_inclusive = display.value == "rounded_half_up"
                    upper_inclusive = display.value == "rounded_half_down"
                    if display.value == "rounded_half_even":
                        if display.parity_reference == "minute_of_hour":
                            even = _minute_value(minute).minute % 2 == 0
                        else:
                            even = int(anchor.timestamp()) // 60 % 2 == 0
                        lower_inclusive = upper_inclusive = even
                    bounds = (
                        anchor - timedelta(seconds=30),
                        lower_inclusive,
                        anchor + timedelta(seconds=30),
                        upper_inclusive,
                    )
                    derivation = f"minute_round_{display.value.removeprefix('rounded_')}"
                nominal = NominalPublicationIntervalV2(
                    bounds[0].isoformat(),
                    bounds[1],
                    bounds[2].isoformat(),
                    bounds[3],
                    derivation,
                    interval_materials,
                )
            except _LocalResolutionError as exc:
                reasons.append(
                    _reason(
                        exc.code, "source_publication.local_minute", str(exc), zone.material_ids
                    )
                )
    elif precision == "date":
        if pub_date is None:
            raise ValueError("date precision requires local_date")
        if zone.status != "evidenced_iana":
            reasons.append(
                _reason(
                    "ZONE_UNVERIFIED",
                    "source_publication.zone",
                    "publication date requires an evidenced IANA zone",
                )
            )
        if display.status != "evidenced" or display.value != "publication_date":
            reasons.append(
                _reason(
                    "PUBLICATION_DATE_SEMANTICS_UNVERIFIED",
                    "source_publication.display_rule",
                    "the date is not evidenced as a publication date",
                    display.material_ids,
                )
            )
        if not reasons:
            assert zone.iana_name is not None
            current = datetime.combine(date.fromisoformat(pub_date), datetime.min.time())
            following = datetime.combine(
                date.fromisoformat(pub_date) + timedelta(days=1), datetime.min.time()
            )
            midnight_zone = PublicationZoneEvidenceV2(
                "evidenced_iana", zone.iana_name, None, None, zone.material_ids
            )
            try:
                start = _resolve_local(
                    current,
                    midnight_zone,
                    ambiguous_code="DATE_BOUNDARY_AMBIGUOUS",
                    nonexistent_code="DATE_BOUNDARY_NONEXISTENT",
                )
                end = _resolve_local(
                    following,
                    midnight_zone,
                    ambiguous_code="DATE_BOUNDARY_AMBIGUOUS",
                    nonexistent_code="DATE_BOUNDARY_NONEXISTENT",
                )
                nominal = NominalPublicationIntervalV2(
                    start.isoformat(),
                    True,
                    end.isoformat(),
                    False,
                    "publication_date",
                    tuple(dict.fromkeys((*zone.material_ids, *display.material_ids))),
                )
            except _LocalResolutionError as exc:
                reasons.append(
                    _reason(exc.code, "source_publication.local_date", str(exc), zone.material_ids)
                )
    elif precision == "interval":
        if explicit_interval is None:
            raise ValueError("interval precision requires explicit_interval")
        interval_evidence_complete = set(explicit_interval.material_ids) <= known
        if not interval_evidence_complete:
            reasons.append(
                _reason(
                    "MATERIAL_REFERENCE_INCOMPLETE",
                    "source_publication.explicit_interval",
                    "explicit interval has dangling material references",
                    tuple(item for item in explicit_interval.material_ids if item in known),
                )
            )
        display_evidence_complete = (
            display.status == "evidenced" and display.value == "explicit_interval"
        )
        if not display_evidence_complete:
            reasons.append(
                _reason(
                    "DISPLAY_RULE_UNVERIFIED",
                    "source_publication.display_rule",
                    "explicit interval lacks complete source-format evidence",
                    display.material_ids,
                )
            )
        if interval_evidence_complete and display_evidence_complete:
            nominal = explicit_interval
        else:
            normalized_precision = "unknown"
            normalized_explicit_interval = None
    elif precision == "exact_timestamp":
        if exact is None:
            raise ValueError("exact timestamp precision requires exact_at")
        if display.status != "evidenced" or display.value != "exact":
            reasons.append(
                _reason(
                    "DISPLAY_RULE_UNVERIFIED",
                    "source_publication.display_rule",
                    "exact timestamp lacks complete source-format evidence",
                    display.material_ids,
                )
            )
    elif precision == "unknown":
        reasons.append(
            _reason(
                "PRECISION_UNKNOWN", "source_publication.precision", "source precision is unknown"
            )
        )
    else:
        raise ValueError("unsupported source publication precision")

    nominal_boundary: str | None = None
    nominal_relation = "at_or_after"
    nominal_basis = "source_interval_upper"
    if precision == "exact_timestamp" and not reasons:
        nominal_boundary = exact
        nominal_basis = "source_declared_instant"
    elif nominal is not None and not reasons:
        nominal_boundary = nominal.latest
        nominal_relation = "strictly_after" if nominal.latest_inclusive else "at_or_after"
    if nominal_boundary is not None and _instant(nominal_boundary) > _instant(captured):
        reasons.append(
            _reason(
                "NOMINAL_BOUND_AFTER_CAPTURE",
                "availability.boundary_at",
                "the nominal upper boundary follows the local capture",
            )
        )
    if (
        (study_mode != "retrospective" or trust_model != "trust_source_declared_time")
        and nominal_boundary is not None
        and not reasons
    ):
        reasons.append(
            _reason(
                "POLICY_CAPTURE_ONLY",
                "availability.trust_model",
                "nominal source time was not explicitly trusted for a retrospective study",
            )
        )
    if reasons:
        availability = AvailabilityDecisionV2(
            boundary_at=captured,
            admission_relation="at_or_after",
            mode="captured_only",
            basis="local_capture",
            trust_model="capture_receipt_only",
            usage_scope="forward_and_retrospective",
            physical_clock_accuracy=("known" if clock.status == "evidenced" else clock.status),
            risk_note=risk_note,
            derivation_version="1",
            material_ids=(),
            downgrade_reasons=tuple(dict.fromkeys(reasons)),
        )
        revision_scope = "forward_and_retrospective"
    else:
        assert nominal_boundary is not None
        material_ids = (
            display.material_ids
            if precision == "exact_timestamp"
            else nominal.material_ids
            if nominal is not None
            else ()
        )
        availability = AvailabilityDecisionV2(
            boundary_at=nominal_boundary,
            admission_relation=nominal_relation,
            mode=(
                "source_declared_nominal"
                if precision == "exact_timestamp"
                else "conservative_nominal_bound"
            ),
            basis=nominal_basis,
            trust_model="trust_source_declared_time",
            usage_scope="retrospective_only",
            physical_clock_accuracy=("known" if clock.status == "evidenced" else clock.status),
            risk_note=(
                risk_note or "source-declared nominal boundary; physical clock accuracy is unknown"
            ),
            derivation_version="1",
            material_ids=material_ids,
            downgrade_reasons=(),
        )
        revision_scope = "retrospective_only"
    source = SourcePublicationV2(
        raw_text=raw_text,
        precision=normalized_precision,
        exact_at=exact,
        local_minute=minute,
        local_date=pub_date,
        explicit_interval=normalized_explicit_interval,
        zone=zone,
        display_rule=display,
        clock_accuracy=clock,
        nominal_interval=nominal,
    )
    revision = EvidenceRevisionV2(
        revision_id=revision_id,
        supersedes_timing_sha256=supersedes_timing_sha256,
        acquired_at=revision_acquired,
        usage_scope=revision_scope,
    )
    timing = EvidenceTimingV2(
        effective_at=effective,
        captured_at=captured,
        source_materials=materials,
        source_publication=source,
        availability=availability,
        revision=revision,
    )
    return EvidenceTimingBuildResultV2(timing, availability.downgrade_reasons)


__all__ = [
    "EVIDENCE_TIMING_SCHEMA_ID_V2",
    "AdmissionBoundaryV2",
    "AvailabilityDecisionV2",
    "DowngradeReasonV2",
    "EvidenceRevisionV2",
    "EvidenceTimingBuildResultV2",
    "EvidenceTimingV2",
    "NominalPublicationIntervalV2",
    "PhysicalClockAccuracyV2",
    "PublicationDisplayRuleV2",
    "PublicationZoneEvidenceV2",
    "SourceMaterialReferenceV2",
    "SourcePublicationV2",
    "build_evidence_timing_v2",
]
