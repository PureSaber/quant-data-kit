"""Explicit, closed migration from dividend lifecycle v1 bytes to v2."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .common import utc
from .dividends import DividendLifecycle, EvidenceTiming
from .dividends_v2 import (
    DIVIDEND_LIFECYCLE_SCHEMA_ID_V2,
    DividendEntitlementV2,
    DividendLifecycleV2,
    DividendPaymentElectionV2,
    DividendPaymentV2,
    DividendProposalV2,
    IssuerFixingTimeV2,
    IssuerFxConversionV2,
    PaymentPolicyV2,
    PhaseEvidenceV2,
)
from .publication_time_v2 import (
    _LEGACY_CONTEXT_V2,
    AvailabilityDecisionV2,
    DowngradeReasonV2,
    EvidenceRevisionV2,
    EvidenceTimingV2,
    PhysicalClockAccuracyV2,
    PublicationDisplayRuleV2,
    PublicationZoneEvidenceV2,
    SourcePublicationV2,
)
from .reconciliation import canonical

DIVIDEND_LIFECYCLE_MIGRATION_SCHEMA_ID_V2 = "puresaber.dividend-lifecycle-v1-to-v2/1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _mapping(value: object, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{context} must be an object")
    return value


def _keys(value: Mapping[str, Any], *, required: set[str], context: str) -> None:
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


def _timestamp(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a timezone-aware timestamp string")
    return utc(value, field).isoformat().replace("+00:00", "Z")


def _plain(value):
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


@dataclass(frozen=True)
class LegacyTimingBindingV2:
    source_pointer: str
    target_event_id: str
    source_available_at: str
    source_captured_at: str

    def __post_init__(self) -> None:
        pointer = _text(self.source_pointer, "source_pointer")
        if not pointer.startswith("/") or "~" in pointer:
            raise ValueError("source_pointer must be a simple absolute JSON pointer")
        object.__setattr__(self, "source_pointer", pointer)
        object.__setattr__(self, "target_event_id", _text(self.target_event_id, "target_event_id"))
        object.__setattr__(
            self,
            "source_available_at",
            _timestamp(self.source_available_at, "source_available_at"),
        )
        object.__setattr__(
            self,
            "source_captured_at",
            _timestamp(self.source_captured_at, "source_captured_at"),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "source_pointer": self.source_pointer,
            "target_event_id": self.target_event_id,
            "source_available_at": self.source_available_at,
            "source_captured_at": self.source_captured_at,
        }

    @classmethod
    def from_dict(cls, value: object) -> LegacyTimingBindingV2:
        value = _mapping(value, "legacy timing binding v2")
        _keys(
            value,
            required={
                "source_pointer",
                "target_event_id",
                "source_available_at",
                "source_captured_at",
            },
            context="legacy timing binding v2",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class LegacyV1BindingV2:
    source_v1_payload: Mapping[str, object]
    source_fingerprint: str
    timing_bindings: tuple[LegacyTimingBindingV2, ...]
    rule_version: str
    migrated_at: str

    def __post_init__(self) -> None:
        payload = _plain(_mapping(self.source_v1_payload, "source_v1_payload"))
        fingerprint = _text(self.source_fingerprint, "source_fingerprint")
        if _SHA256.fullmatch(fingerprint) is None:
            raise ValueError("source_fingerprint must be a lowercase SHA-256")
        bindings = tuple(self.timing_bindings)
        if not bindings or not all(isinstance(item, LegacyTimingBindingV2) for item in bindings):
            raise TypeError("timing_bindings must contain LegacyTimingBindingV2 values")
        pointers = [item.source_pointer for item in bindings]
        events = [item.target_event_id for item in bindings]
        if len(pointers) != len(set(pointers)) or len(events) != len(set(events)):
            raise ValueError("legacy timing pointers and target events must be unique")
        if _text(self.rule_version, "rule_version") != "1":
            raise ValueError("unsupported dividend lifecycle migration rule")
        object.__setattr__(self, "source_v1_payload", payload)
        object.__setattr__(self, "source_fingerprint", fingerprint)
        object.__setattr__(self, "timing_bindings", bindings)
        object.__setattr__(self, "rule_version", "1")
        object.__setattr__(self, "migrated_at", _timestamp(self.migrated_at, "migrated_at"))

    def to_dict(self) -> dict[str, object]:
        return {
            "source_v1_payload": _plain(self.source_v1_payload),
            "source_fingerprint": self.source_fingerprint,
            "timing_bindings": [item.to_dict() for item in self.timing_bindings],
            "rule_version": self.rule_version,
            "migrated_at": self.migrated_at,
        }

    @classmethod
    def from_dict(cls, value: object) -> LegacyV1BindingV2:
        value = _mapping(value, "legacy v1 binding v2")
        _keys(
            value,
            required={
                "source_v1_payload",
                "source_fingerprint",
                "timing_bindings",
                "rule_version",
                "migrated_at",
            },
            context="legacy v1 binding v2",
        )
        bindings = value["timing_bindings"]
        if isinstance(bindings, (str, bytes)) or not isinstance(bindings, Sequence):
            raise TypeError("timing_bindings must be an array")
        return cls(
            source_v1_payload=value["source_v1_payload"],
            source_fingerprint=value["source_fingerprint"],
            timing_bindings=tuple(LegacyTimingBindingV2.from_dict(item) for item in bindings),
            rule_version=value["rule_version"],
            migrated_at=value["migrated_at"],
        )


@dataclass(frozen=True)
class DividendLifecycleMigrationReceiptV2:
    source_schema: str
    source_fingerprint: str
    target_schema: str
    target_fingerprint: str
    rule_version: str
    migrated_at: str
    migration_id: str
    schema: str = DIVIDEND_LIFECYCLE_MIGRATION_SCHEMA_ID_V2

    def __post_init__(self) -> None:
        if self.schema != DIVIDEND_LIFECYCLE_MIGRATION_SCHEMA_ID_V2:
            raise ValueError("unsupported migration receipt schema")
        if self.source_schema != "puresaber.dividend-lifecycle/1":
            raise ValueError("unsupported migration source schema")
        if self.target_schema != DIVIDEND_LIFECYCLE_SCHEMA_ID_V2:
            raise ValueError("unsupported migration target schema")
        for field in ("source_fingerprint", "target_fingerprint"):
            if _SHA256.fullmatch(getattr(self, field)) is None:
                raise ValueError(f"{field} must be a lowercase SHA-256")
        if self.rule_version != "1":
            raise ValueError("unsupported migration receipt rule")
        object.__setattr__(self, "migrated_at", _timestamp(self.migrated_at, "migrated_at"))
        object.__setattr__(self, "migration_id", _text(self.migration_id, "migration_id"))

    def to_dict(self) -> dict[str, str]:
        return {
            "schema": self.schema,
            "source_schema": self.source_schema,
            "source_fingerprint": self.source_fingerprint,
            "target_schema": self.target_schema,
            "target_fingerprint": self.target_fingerprint,
            "rule_version": self.rule_version,
            "migrated_at": self.migrated_at,
            "migration_id": self.migration_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> DividendLifecycleMigrationReceiptV2:
        value = _mapping(value, "dividend lifecycle migration receipt v2")
        _keys(
            value,
            required={
                "schema",
                "source_schema",
                "source_fingerprint",
                "target_schema",
                "target_fingerprint",
                "rule_version",
                "migrated_at",
                "migration_id",
            },
            context="dividend lifecycle migration receipt v2",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class DividendLifecycleMigrationV2:
    lifecycle: DividendLifecycleV2
    receipt: DividendLifecycleMigrationReceiptV2

    def __post_init__(self) -> None:
        if not isinstance(self.lifecycle, DividendLifecycleV2):
            raise TypeError("lifecycle must be DividendLifecycleV2")
        if not isinstance(self.receipt, DividendLifecycleMigrationReceiptV2):
            raise TypeError("receipt must be DividendLifecycleMigrationReceiptV2")
        binding = LegacyV1BindingV2.from_dict(self.lifecycle.legacy_binding)
        if (
            self.receipt.source_fingerprint != binding.source_fingerprint
            or self.receipt.target_fingerprint != self.lifecycle.fingerprint()
            or self.receipt.rule_version != binding.rule_version
            or self.receipt.migrated_at != binding.migrated_at
        ):
            raise ValueError("migration receipt does not bind the lifecycle and source")

    def to_dict(self) -> dict[str, object]:
        return {"lifecycle": self.lifecycle.to_dict(), "receipt": self.receipt.to_dict()}

    @classmethod
    def from_dict(cls, value: object) -> DividendLifecycleMigrationV2:
        value = _mapping(value, "dividend lifecycle migration v2")
        _keys(value, required={"lifecycle", "receipt"}, context="dividend lifecycle migration v2")
        return cls(
            lifecycle=DividendLifecycleV2.from_dict(value["lifecycle"]),
            receipt=DividendLifecycleMigrationReceiptV2.from_dict(value["receipt"]),
        )


def _source_at_pointer(payload: Mapping[str, object], pointer: str) -> object:
    current: object = payload
    for component in pointer.lstrip("/").split("/"):
        if not isinstance(current, Mapping) or component not in current:
            raise ValueError(f"legacy timing pointer does not resolve: {pointer}")
        current = current[component]
    return current


def _phase_evidence_by_event(lifecycle: DividendLifecycleV2) -> dict[str, PhaseEvidenceV2]:
    values = [
        lifecycle.proposal,
        lifecycle.entitlement,
        lifecycle.election,
        lifecycle.conversion,
        lifecycle.payment,
    ]
    evidence = {item.evidence.event_id: item.evidence for item in values if item is not None}
    if lifecycle.payment_policy is not None and lifecycle.payment_policy.evidence is not None:
        evidence[lifecycle.payment_policy.evidence.event_id] = lifecycle.payment_policy.evidence
    return evidence


def _without_timing(payload: Mapping[str, object]) -> dict[str, object]:
    value = json.loads(canonical(payload))
    value.pop("schema", None)
    value.pop("legacy_binding", None)
    for phase_name in (
        "proposal",
        "entitlement",
        "election",
        "conversion",
        "payment_policy",
        "payment",
    ):
        phase = value.get(phase_name)
        if isinstance(phase, dict) and isinstance(phase.get("evidence"), dict):
            phase["evidence"].pop("timing", None)
    return value


def _source_economic_projection(source: DividendLifecycle) -> dict[str, object]:
    value = _without_timing(source.to_dict())
    conversion = value.get("conversion")
    if isinstance(conversion, dict):
        fixing_at = conversion.pop("fixing_at")
        fixing_date = conversion.pop("fixing_date")
        fixing_timezone = conversion.pop("fixing_timezone")
        conversion["fixing_time"] = (
            {
                "kind": "exact_timestamp",
                "exact_at": fixing_at,
                "local_date": None,
                "timezone_name": None,
                "raw_text": None,
                "tolerance_seconds": None,
            }
            if fixing_at is not None
            else {
                "kind": "date",
                "exact_at": None,
                "local_date": fixing_date,
                "timezone_name": fixing_timezone,
                "raw_text": None,
                "tolerance_seconds": None,
            }
        )
    return value


def validate_legacy_v1_binding_v2(lifecycle: DividendLifecycleV2) -> None:
    binding = LegacyV1BindingV2.from_dict(lifecycle.legacy_binding)
    source = DividendLifecycle.from_dict(binding.source_v1_payload)
    if source.fingerprint() != binding.source_fingerprint:
        raise ValueError("legacy source fingerprint does not match the embedded v1 payload")
    if _source_economic_projection(source) != _without_timing(lifecycle.to_dict()):
        raise ValueError("migrated lifecycle economic facts differ from the embedded v1 source")
    targets = _phase_evidence_by_event(lifecycle)
    if {item.target_event_id for item in binding.timing_bindings} != set(targets):
        raise ValueError("legacy timing bindings must cover every migrated phase evidence")
    for item in binding.timing_bindings:
        source_value = EvidenceTiming.from_dict(
            _source_at_pointer(binding.source_v1_payload, item.source_pointer)
        )
        expected_timing, expected_binding = _migrated_timing(
            source_value,
            pointer=item.source_pointer,
            event_id=item.target_event_id,
            migrated_at=binding.migrated_at,
        )
        if item != expected_binding:
            raise ValueError("legacy timing binding values differ from the embedded v1 source")
        target = targets[item.target_event_id].timing
        if target != expected_timing:
            raise ValueError(
                "migrated timing facts differ from the embedded v1 source and migration rule"
            )


def _migrated_timing(
    timing: EvidenceTiming,
    *,
    pointer: str,
    event_id: str,
    migrated_at: str,
) -> tuple[EvidenceTimingV2, LegacyTimingBindingV2]:
    clock = PhysicalClockAccuracyV2("unknown")
    zone = PublicationZoneEvidenceV2("not_applicable")
    display = PublicationDisplayRuleV2("unknown")
    if timing.source_published_at is not None:
        source = SourcePublicationV2(
            raw_text=timing.source_published_at,
            precision="exact_timestamp",
            exact_at=timing.source_published_at,
            local_minute=None,
            local_date=None,
            explicit_interval=None,
            zone=zone,
            display_rule=display,
            clock_accuracy=clock,
            nominal_interval=None,
        )
        availability = AvailabilityDecisionV2(
            boundary_at=timing.available_at,
            admission_relation="at_or_after",
            mode="source_declared_nominal",
            basis="legacy_available_at",
            trust_model="trust_source_declared_time",
            usage_scope="retrospective_only",
            physical_clock_accuracy="unknown",
            risk_note="explicit migration preserves legacy available_at; physical clock accuracy is unknown",
            derivation_version="1",
            material_ids=(),
            downgrade_reasons=(),
            legacy_timing_pointer=pointer,
        )
        usage_scope = "retrospective_only"
    elif timing.source_publication_date is not None:
        source = SourcePublicationV2(
            raw_text=timing.source_publication_date,
            precision="date",
            exact_at=None,
            local_minute=None,
            local_date=timing.source_publication_date,
            explicit_interval=None,
            zone=PublicationZoneEvidenceV2("unverified"),
            display_rule=display,
            clock_accuracy=clock,
            nominal_interval=None,
        )
        reason = DowngradeReasonV2(
            "PUBLICATION_DATE_SEMANTICS_UNVERIFIED",
            "source_publication.local_date",
            (),
            "legacy date-only timing has no evidenced timezone and publication-date semantics",
        )
        availability = AvailabilityDecisionV2(
            timing.captured_at,
            "at_or_after",
            "captured_only",
            "local_capture",
            "capture_receipt_only",
            "forward_and_retrospective",
            "unknown",
            None,
            "1",
            (),
            (reason,),
        )
        usage_scope = "forward_and_retrospective"
    else:
        source = SourcePublicationV2(
            raw_text="",
            precision="unknown",
            exact_at=None,
            local_minute=None,
            local_date=None,
            explicit_interval=None,
            zone=PublicationZoneEvidenceV2("unverified"),
            display_rule=display,
            clock_accuracy=clock,
            nominal_interval=None,
        )
        reason = DowngradeReasonV2(
            "PRECISION_UNKNOWN",
            "source_publication.precision",
            (),
            "legacy v1 supplied no source publication field",
        )
        availability = AvailabilityDecisionV2(
            timing.captured_at,
            "at_or_after",
            "captured_only",
            "local_capture",
            "capture_receipt_only",
            "forward_and_retrospective",
            "unknown",
            None,
            "1",
            (),
            (reason,),
        )
        usage_scope = "forward_and_retrospective"
    migrated = EvidenceTimingV2(
        effective_at=timing.effective_at,
        captured_at=timing.captured_at,
        source_materials=(),
        source_publication=source,
        availability=availability,
        revision=EvidenceRevisionV2(
            revision_id=f"migration:{event_id}",
            supersedes_timing_sha256=None,
            acquired_at=migrated_at,
            usage_scope=usage_scope,
        ),
        _legacy_context=_LEGACY_CONTEXT_V2,
    )
    return migrated, LegacyTimingBindingV2(
        pointer,
        event_id,
        timing.available_at,
        timing.captured_at,
    )


def migrate_dividend_lifecycle_v1_to_v2(
    source_canonical_json: str | bytes,
    *,
    migration_id: str,
    migrated_at: str,
    rule_version: str = "1",
) -> DividendLifecycleMigrationV2:
    if not isinstance(source_canonical_json, (str, bytes)):
        raise TypeError("source_canonical_json must be str or bytes")
    source_bytes = (
        source_canonical_json.encode("utf-8")
        if isinstance(source_canonical_json, str)
        else source_canonical_json
    )
    source = DividendLifecycle.from_json(source_bytes)
    canonical_bytes = source.to_json().encode("utf-8")
    if source_bytes != canonical_bytes:
        raise ValueError("migration source must be exact canonical DividendLifecycle/1 JSON bytes")
    migrated = _timestamp(migrated_at, "migrated_at")
    if rule_version != "1":
        raise ValueError("unsupported dividend lifecycle migration rule")
    bindings: list[LegacyTimingBindingV2] = []

    def evidence(value, pointer: str) -> PhaseEvidenceV2:
        timing, binding = _migrated_timing(
            value.timing,
            pointer=pointer,
            event_id=value.event_id,
            migrated_at=migrated,
        )
        bindings.append(binding)
        return PhaseEvidenceV2(value.event_id, value.source, value.evidence_id, timing)

    proposal = None
    if source.proposal is not None:
        proposal = DividendProposalV2(
            evidence(source.proposal.evidence, "/proposal/evidence/timing"),
            source.proposal.proposed_amount,
            source.proposal.declared_currency,
            source.proposal.proposed_ex_date,
            source.proposal.proposed_payment_date,
        )
    entitlement = DividendEntitlementV2(
        evidence(source.entitlement.evidence, "/entitlement/evidence/timing"),
        source.entitlement.approved_amount,
        source.entitlement.declared_currency,
        source.entitlement.record_date,
        source.entitlement.scheduled_payment_date,
        source.entitlement.payment_currencies,
        source.entitlement.default_payment_currency,
        source.entitlement.approval_status,
    )
    election = None
    if source.election is not None:
        election = DividendPaymentElectionV2(
            evidence(source.election.evidence, "/election/evidence/timing"),
            source.election.account_id,
            source.election.account_policy_id,
            source.election.payment_currency,
            source.election.selection_kind,
            source.election.selection_scope,
        )
    conversion = None
    if source.conversion is not None:
        fixing = (
            IssuerFixingTimeV2(
                "exact_timestamp", source.conversion.fixing_at, None, None, None, None
            )
            if source.conversion.fixing_at is not None
            else IssuerFixingTimeV2(
                "date",
                None,
                source.conversion.fixing_date,
                source.conversion.fixing_timezone,
                None,
                None,
            )
        )
        conversion = IssuerFxConversionV2(
            evidence(source.conversion.evidence, "/conversion/evidence/timing"),
            source.conversion.from_currency,
            source.conversion.to_currency,
            source.conversion.rate_text,
            source.conversion.rate_convention,
            fixing,
            source.conversion.published_payment_amount,
        )
    policy = None
    if source.payment_policy is not None:
        policy_evidence = (
            evidence(source.payment_policy.evidence, "/payment_policy/evidence/timing")
            if source.payment_policy.evidence is not None
            else None
        )
        policy = PaymentPolicyV2(
            source.payment_policy.policy_id,
            source.payment_policy.account_id,
            source.payment_policy.certification_status,
            source.payment_policy.holder_tax_profile_id,
            source.payment_policy.withholding_rule_id,
            source.payment_policy.rounding,
            policy_evidence,
        )
    payment = None
    if source.payment is not None:
        payment = DividendPaymentV2(
            evidence(source.payment.evidence, "/payment/evidence/timing"),
            source.payment.account_id,
            source.payment.payment_currency,
            source.payment.policy_id,
            source.payment.gross_cash_text,
            source.payment.withholding_cash_text,
            source.payment.deductions,
            source.payment.rounding_adjustment_text,
            source.payment.net_cash_text,
            source.payment.receipt_status,
        )
    binding = LegacyV1BindingV2(
        source.to_dict(),
        source.fingerprint(),
        tuple(bindings),
        rule_version,
        migrated,
    )
    lifecycle = DividendLifecycleV2(
        dividend_id=source.dividend_id,
        instrument_id=source.instrument_id,
        proposal=proposal,
        entitlement=entitlement,
        election=election,
        conversion=conversion,
        payment_policy=policy,
        payment=payment,
        legacy_binding=binding.to_dict(),
    )
    receipt = DividendLifecycleMigrationReceiptV2(
        source_schema="puresaber.dividend-lifecycle/1",
        source_fingerprint=source.fingerprint(),
        target_schema=DIVIDEND_LIFECYCLE_SCHEMA_ID_V2,
        target_fingerprint=lifecycle.fingerprint(),
        rule_version=rule_version,
        migrated_at=migrated,
        migration_id=migration_id,
    )
    return DividendLifecycleMigrationV2(lifecycle, receipt)


__all__ = [
    "DIVIDEND_LIFECYCLE_MIGRATION_SCHEMA_ID_V2",
    "DividendLifecycleMigrationReceiptV2",
    "DividendLifecycleMigrationV2",
    "LegacyTimingBindingV2",
    "LegacyV1BindingV2",
    "migrate_dividend_lifecycle_v1_to_v2",
    "validate_legacy_v1_binding_v2",
]
