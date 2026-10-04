"""Independent dividend lifecycle v2 types.

These types intentionally do not inherit the v1 lifecycle or phase classes.
Only closed, timing-free value objects are reused from :mod:`dividends`.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .common import day, utc
from .dividends import CashDeduction, CurrencyReference, PublishedAmount, RoundingPolicy
from .publication_time_v2 import EvidenceTimingV2
from .reconciliation import canonical

DIVIDEND_LIFECYCLE_SCHEMA_ID_V2 = "puresaber.dividend-lifecycle/2"

_DECIMAL_TEXT = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_CURRENCY = re.compile(r"[A-Z]{3}\Z")


class _FrozenMapping(Mapping):
    def __init__(self, value: Mapping[str, object]):
        self._value = {str(key): _freeze(item) for key, item in value.items()}

    def __getitem__(self, key):
        return self._value[key]

    def __iter__(self):
        return iter(self._value)

    def __len__(self):
        return len(self._value)

    def __eq__(self, other):
        return isinstance(other, Mapping) and _thaw(self) == _thaw(other)


def _freeze(value):
    if isinstance(value, (_FrozenMapping, tuple)):
        return value
    if isinstance(value, Mapping):
        return _FrozenMapping(value)
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value):
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


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


def _date(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a plain calendar-date string")
    return day(value).date().isoformat()


def _currency(value: object, field: str) -> str:
    result = _text(value, field)
    if _CURRENCY.fullmatch(result) is None:
        raise ValueError(f"{field} must be a three-letter uppercase calculation currency")
    return result


def _decimal_text(
    value: object,
    field: str,
    *,
    nonnegative: bool = False,
    positive: bool = False,
) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an exact decimal text")
    if _DECIMAL_TEXT.fullmatch(value) is None:
        raise ValueError(f"{field} must be a plain finite decimal text")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:  # pragma: no cover - guarded by the expression
        raise ValueError(f"invalid {field}") from exc
    if not parsed.is_finite() or (nonnegative and parsed < 0) or (positive and parsed <= 0):
        raise ValueError(f"{field} violates its sign or finiteness constraint")
    return value


def _exact(value: str) -> Fraction:
    return Fraction(value)


def _timing_boundary(timing: EvidenceTimingV2):
    return utc(timing.availability.boundary_at)


@dataclass(frozen=True)
class PhaseEvidenceV2:
    event_id: str
    source: str
    evidence_id: str
    timing: EvidenceTimingV2

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _text(self.event_id, "event_id"))
        object.__setattr__(self, "source", _text(self.source, "source"))
        object.__setattr__(self, "evidence_id", _text(self.evidence_id, "evidence_id"))
        if not isinstance(self.timing, EvidenceTimingV2):
            raise TypeError("timing must be EvidenceTimingV2")

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "source": self.source,
            "evidence_id": self.evidence_id,
            "timing": self.timing.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object, *, _legacy_context: object | None = None) -> PhaseEvidenceV2:
        value = _mapping(value, "phase evidence v2")
        _keys(
            value,
            required={"event_id", "source", "evidence_id", "timing"},
            context="phase evidence v2",
        )
        return cls(
            event_id=value["event_id"],
            source=value["source"],
            evidence_id=value["evidence_id"],
            timing=EvidenceTimingV2._from_dict(value["timing"], legacy_context=_legacy_context),
        )


@dataclass(frozen=True)
class DividendProposalV2:
    evidence: PhaseEvidenceV2
    proposed_amount: PublishedAmount
    declared_currency: CurrencyReference
    proposed_ex_date: str
    proposed_payment_date: str

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, PhaseEvidenceV2):
            raise TypeError("proposal evidence must be PhaseEvidenceV2")
        if not isinstance(self.proposed_amount, PublishedAmount):
            raise TypeError("proposed_amount must be PublishedAmount")
        if not isinstance(self.declared_currency, CurrencyReference):
            raise TypeError("declared_currency must be CurrencyReference")
        object.__setattr__(
            self, "proposed_ex_date", _date(self.proposed_ex_date, "proposed_ex_date")
        )
        object.__setattr__(
            self,
            "proposed_payment_date",
            _date(self.proposed_payment_date, "proposed_payment_date"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "phase": "proposal",
            "evidence": self.evidence.to_dict(),
            "proposed_amount": self.proposed_amount.to_dict(),
            "declared_currency": self.declared_currency.to_dict(),
            "proposed_ex_date": self.proposed_ex_date,
            "proposed_payment_date": self.proposed_payment_date,
        }

    @classmethod
    def from_dict(
        cls, value: object, *, _legacy_context: object | None = None
    ) -> DividendProposalV2:
        value = _phase_mapping(
            value,
            "proposal",
            {
                "evidence",
                "proposed_amount",
                "declared_currency",
                "proposed_ex_date",
                "proposed_payment_date",
            },
        )
        return cls(
            evidence=PhaseEvidenceV2.from_dict(value["evidence"], _legacy_context=_legacy_context),
            proposed_amount=PublishedAmount.from_dict(value["proposed_amount"]),
            declared_currency=CurrencyReference.from_dict(value["declared_currency"]),
            proposed_ex_date=value["proposed_ex_date"],
            proposed_payment_date=value["proposed_payment_date"],
        )


def _phase_mapping(value: object, phase: str, fields: set[str]) -> Mapping[str, Any]:
    value = _mapping(value, f"{phase} v2")
    _keys(value, required={"phase", *fields}, context=f"{phase} v2")
    if value["phase"] != phase:
        raise ValueError(f"phase must be {phase!r}")
    return value


@dataclass(frozen=True)
class DividendEntitlementV2:
    evidence: PhaseEvidenceV2
    approved_amount: PublishedAmount
    declared_currency: CurrencyReference
    record_date: str
    scheduled_payment_date: str
    payment_currencies: tuple[CurrencyReference, ...]
    default_payment_currency: str
    approval_status: str = "approved"

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, PhaseEvidenceV2):
            raise TypeError("entitlement evidence must be PhaseEvidenceV2")
        if not isinstance(self.approved_amount, PublishedAmount):
            raise TypeError("approved_amount must be PublishedAmount")
        if not isinstance(self.declared_currency, CurrencyReference):
            raise TypeError("declared_currency must be CurrencyReference")
        if self.approval_status != "approved":
            raise ValueError("entitlement requires approved terms")
        currencies = tuple(self.payment_currencies)
        if not currencies or not all(isinstance(item, CurrencyReference) for item in currencies):
            raise TypeError("payment_currencies must contain CurrencyReference values")
        codes = [item.calculation_currency for item in currencies]
        if len(codes) != len(set(codes)):
            raise ValueError("payment currency options must be unique")
        default = _currency(self.default_payment_currency, "default_payment_currency")
        if default not in codes:
            raise ValueError("default payment currency must be an approved option")
        object.__setattr__(self, "record_date", _date(self.record_date, "record_date"))
        object.__setattr__(
            self,
            "scheduled_payment_date",
            _date(self.scheduled_payment_date, "scheduled_payment_date"),
        )
        object.__setattr__(self, "payment_currencies", currencies)
        object.__setattr__(self, "default_payment_currency", default)

    def to_dict(self) -> dict[str, object]:
        return {
            "phase": "entitlement",
            "evidence": self.evidence.to_dict(),
            "approved_amount": self.approved_amount.to_dict(),
            "declared_currency": self.declared_currency.to_dict(),
            "record_date": self.record_date,
            "scheduled_payment_date": self.scheduled_payment_date,
            "payment_currencies": [item.to_dict() for item in self.payment_currencies],
            "default_payment_currency": self.default_payment_currency,
            "approval_status": self.approval_status,
        }

    @classmethod
    def from_dict(
        cls, value: object, *, _legacy_context: object | None = None
    ) -> DividendEntitlementV2:
        value = _phase_mapping(
            value,
            "entitlement",
            {
                "evidence",
                "approved_amount",
                "declared_currency",
                "record_date",
                "scheduled_payment_date",
                "payment_currencies",
                "default_payment_currency",
                "approval_status",
            },
        )
        currencies = value["payment_currencies"]
        if not isinstance(currencies, list):
            raise TypeError("payment_currencies must be an array")
        return cls(
            evidence=PhaseEvidenceV2.from_dict(value["evidence"], _legacy_context=_legacy_context),
            approved_amount=PublishedAmount.from_dict(value["approved_amount"]),
            declared_currency=CurrencyReference.from_dict(value["declared_currency"]),
            record_date=value["record_date"],
            scheduled_payment_date=value["scheduled_payment_date"],
            payment_currencies=tuple(CurrencyReference.from_dict(item) for item in currencies),
            default_payment_currency=value["default_payment_currency"],
            approval_status=value["approval_status"],
        )


@dataclass(frozen=True)
class DividendPaymentElectionV2:
    evidence: PhaseEvidenceV2
    account_id: str
    account_policy_id: str
    payment_currency: str
    selection_kind: str
    selection_scope: str = "entire_entitlement"

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, PhaseEvidenceV2):
            raise TypeError("payment election evidence must be PhaseEvidenceV2")
        if self.selection_kind not in {"default", "elected"}:
            raise ValueError("selection_kind must be default or elected")
        if self.selection_scope != "entire_entitlement":
            raise ValueError("partial payment election is unsupported")
        object.__setattr__(self, "account_id", _text(self.account_id, "account_id"))
        object.__setattr__(
            self, "account_policy_id", _text(self.account_policy_id, "account_policy_id")
        )
        object.__setattr__(
            self, "payment_currency", _currency(self.payment_currency, "payment_currency")
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "phase": "payment_election",
            "evidence": self.evidence.to_dict(),
            "account_id": self.account_id,
            "account_policy_id": self.account_policy_id,
            "payment_currency": self.payment_currency,
            "selection_kind": self.selection_kind,
            "selection_scope": self.selection_scope,
        }

    @classmethod
    def from_dict(
        cls, value: object, *, _legacy_context: object | None = None
    ) -> DividendPaymentElectionV2:
        value = _phase_mapping(
            value,
            "payment_election",
            {
                "evidence",
                "account_id",
                "account_policy_id",
                "payment_currency",
                "selection_kind",
                "selection_scope",
            },
        )
        return cls(
            evidence=PhaseEvidenceV2.from_dict(value["evidence"], _legacy_context=_legacy_context),
            account_id=value["account_id"],
            account_policy_id=value["account_policy_id"],
            payment_currency=value["payment_currency"],
            selection_kind=value["selection_kind"],
            selection_scope=value["selection_scope"],
        )


@dataclass(frozen=True)
class IssuerFixingTimeV2:
    kind: str
    exact_at: str | None
    local_date: str | None
    timezone_name: str | None
    raw_text: str | None
    tolerance_seconds: int | None

    def __post_init__(self) -> None:
        if self.kind not in {"exact_timestamp", "date", "approximate_unbounded"}:
            raise ValueError("unsupported issuer fixing time kind")
        exact = _timestamp(self.exact_at, "fixing exact_at") if self.exact_at is not None else None
        local_date = (
            _date(self.local_date, "fixing local_date") if self.local_date is not None else None
        )
        zone = self.timezone_name
        if zone is not None:
            zone = _text(zone, "fixing timezone_name")
            try:
                ZoneInfo(zone)
            except (ZoneInfoNotFoundError, ValueError) as exc:
                raise ValueError("fixing timezone_name must be a valid IANA zone") from exc
        raw = self.raw_text.strip() if isinstance(self.raw_text, str) else self.raw_text
        if self.kind == "exact_timestamp":
            if exact is None or any(
                item is not None for item in (local_date, zone, raw, self.tolerance_seconds)
            ):
                raise ValueError("exact fixing requires only exact_at")
        elif self.kind == "date":
            if (
                local_date is None
                or zone is None
                or any(item is not None for item in (exact, raw, self.tolerance_seconds))
            ):
                raise ValueError("date fixing requires only local_date and timezone_name")
        elif (
            exact is not None
            or local_date is not None
            or not isinstance(raw, str)
            or not raw
            or self.tolerance_seconds is not None
        ):
            raise ValueError("unbounded approximate fixing requires only nonempty raw_text")
        object.__setattr__(self, "exact_at", exact)
        object.__setattr__(self, "local_date", local_date)
        object.__setattr__(self, "timezone_name", zone)
        object.__setattr__(self, "raw_text", raw)

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "exact_at": self.exact_at,
            "local_date": self.local_date,
            "timezone_name": self.timezone_name,
            "raw_text": self.raw_text,
            "tolerance_seconds": self.tolerance_seconds,
        }

    @classmethod
    def from_dict(cls, value: object) -> IssuerFixingTimeV2:
        value = _mapping(value, "issuer fixing time v2")
        _keys(
            value,
            required={
                "kind",
                "exact_at",
                "local_date",
                "timezone_name",
                "raw_text",
                "tolerance_seconds",
            },
            context="issuer fixing time v2",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class IssuerFxConversionV2:
    evidence: PhaseEvidenceV2
    from_currency: str
    to_currency: str
    rate_text: str
    rate_convention: str
    fixing_time: IssuerFixingTimeV2
    published_payment_amount: PublishedAmount

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, PhaseEvidenceV2):
            raise TypeError("issuer conversion evidence must be PhaseEvidenceV2")
        source = _currency(self.from_currency, "from_currency")
        target = _currency(self.to_currency, "to_currency")
        if source == target:
            raise ValueError("issuer conversion requires different currencies")
        rate = _decimal_text(self.rate_text, "rate_text", positive=True)
        if self.rate_convention != "quote_per_base":
            raise ValueError("rate_convention must be quote_per_base")
        if not isinstance(self.fixing_time, IssuerFixingTimeV2):
            raise TypeError("fixing_time must be IssuerFixingTimeV2")
        if not isinstance(self.published_payment_amount, PublishedAmount):
            raise TypeError("published_payment_amount must be PublishedAmount")
        boundary = _timing_boundary(self.evidence.timing)
        if self.fixing_time.kind == "exact_timestamp" and utc(self.fixing_time.exact_at) > boundary:
            raise ValueError("issuer conversion cannot be available before its fixing")
        if self.fixing_time.kind == "date":
            zone = ZoneInfo(self.fixing_time.timezone_name)
            if day(self.fixing_time.local_date).date() > boundary.astimezone(zone).date():
                raise ValueError("date-only issuer fixing cannot follow the availability boundary")
        object.__setattr__(self, "from_currency", source)
        object.__setattr__(self, "to_currency", target)
        object.__setattr__(self, "rate_text", rate)

    @property
    def rate(self) -> Decimal:
        return Decimal(self.rate_text)

    def to_dict(self) -> dict[str, object]:
        return {
            "phase": "issuer_fx_conversion",
            "evidence": self.evidence.to_dict(),
            "from_currency": self.from_currency,
            "to_currency": self.to_currency,
            "rate_text": self.rate_text,
            "rate_convention": self.rate_convention,
            "fixing_time": self.fixing_time.to_dict(),
            "published_payment_amount": self.published_payment_amount.to_dict(),
        }

    @classmethod
    def from_dict(
        cls, value: object, *, _legacy_context: object | None = None
    ) -> IssuerFxConversionV2:
        value = _phase_mapping(
            value,
            "issuer_fx_conversion",
            {
                "evidence",
                "from_currency",
                "to_currency",
                "rate_text",
                "rate_convention",
                "fixing_time",
                "published_payment_amount",
            },
        )
        return cls(
            evidence=PhaseEvidenceV2.from_dict(value["evidence"], _legacy_context=_legacy_context),
            from_currency=value["from_currency"],
            to_currency=value["to_currency"],
            rate_text=value["rate_text"],
            rate_convention=value["rate_convention"],
            fixing_time=IssuerFixingTimeV2.from_dict(value["fixing_time"]),
            published_payment_amount=PublishedAmount.from_dict(value["published_payment_amount"]),
        )


@dataclass(frozen=True)
class PaymentPolicyV2:
    policy_id: str
    account_id: str
    certification_status: str
    holder_tax_profile_id: str | None = None
    withholding_rule_id: str | None = None
    rounding: RoundingPolicy | None = None
    evidence: PhaseEvidenceV2 | None = None

    def __post_init__(self) -> None:
        policy_id = _text(self.policy_id, "policy_id")
        account_id = _text(self.account_id, "account_id")
        if self.certification_status not in {"unverified", "certified"}:
            raise ValueError("certification_status must be unverified or certified")
        profile = (
            _text(self.holder_tax_profile_id, "holder_tax_profile_id")
            if self.holder_tax_profile_id is not None
            else None
        )
        rule = (
            _text(self.withholding_rule_id, "withholding_rule_id")
            if self.withholding_rule_id is not None
            else None
        )
        if self.evidence is not None and not isinstance(self.evidence, PhaseEvidenceV2):
            raise TypeError("payment policy evidence must be PhaseEvidenceV2 or None")
        if self.certification_status == "certified" and (
            profile is None
            or rule is None
            or not isinstance(self.rounding, RoundingPolicy)
            or self.evidence is None
        ):
            raise ValueError(
                "certified payment policy requires evidence, profile, rule and rounding"
            )
        if self.certification_status == "unverified" and (
            rule is not None or self.rounding is not None
        ):
            raise ValueError("unverified payment policy cannot certify withholding or rounding")
        object.__setattr__(self, "policy_id", policy_id)
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "holder_tax_profile_id", profile)
        object.__setattr__(self, "withholding_rule_id", rule)

    def to_dict(self) -> dict[str, object]:
        return {
            "phase": "payment_policy",
            "policy_id": self.policy_id,
            "account_id": self.account_id,
            "certification_status": self.certification_status,
            "holder_tax_profile_id": self.holder_tax_profile_id,
            "withholding_rule_id": self.withholding_rule_id,
            "rounding": self.rounding.to_dict() if self.rounding is not None else None,
            "evidence": self.evidence.to_dict() if self.evidence is not None else None,
        }

    @classmethod
    def from_dict(cls, value: object, *, _legacy_context: object | None = None) -> PaymentPolicyV2:
        value = _phase_mapping(
            value,
            "payment_policy",
            {
                "policy_id",
                "account_id",
                "certification_status",
                "holder_tax_profile_id",
                "withholding_rule_id",
                "rounding",
                "evidence",
            },
        )
        rounding = value["rounding"]
        evidence = value["evidence"]
        return cls(
            policy_id=value["policy_id"],
            account_id=value["account_id"],
            certification_status=value["certification_status"],
            holder_tax_profile_id=value["holder_tax_profile_id"],
            withholding_rule_id=value["withholding_rule_id"],
            rounding=RoundingPolicy.from_dict(rounding) if rounding is not None else None,
            evidence=(
                PhaseEvidenceV2.from_dict(evidence, _legacy_context=_legacy_context)
                if evidence is not None
                else None
            ),
        )


@dataclass(frozen=True)
class DividendPaymentV2:
    evidence: PhaseEvidenceV2
    account_id: str
    payment_currency: str
    policy_id: str
    gross_cash_text: str
    withholding_cash_text: str
    deductions: tuple[CashDeduction, ...]
    rounding_adjustment_text: str
    net_cash_text: str
    receipt_status: str = "received"

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, PhaseEvidenceV2):
            raise TypeError("payment evidence must be PhaseEvidenceV2")
        account = _text(self.account_id, "account_id")
        currency = _currency(self.payment_currency, "payment_currency")
        policy = _text(self.policy_id, "policy_id")
        gross = _decimal_text(self.gross_cash_text, "gross_cash_text", nonnegative=True)
        withholding = _decimal_text(
            self.withholding_cash_text, "withholding_cash_text", nonnegative=True
        )
        rounding = _decimal_text(self.rounding_adjustment_text, "rounding_adjustment_text")
        net = _decimal_text(self.net_cash_text, "net_cash_text", nonnegative=True)
        deductions = tuple(self.deductions)
        if not all(isinstance(item, CashDeduction) for item in deductions):
            raise TypeError("deductions must contain CashDeduction values")
        ids = [item.deduction_id for item in deductions]
        if len(ids) != len(set(ids)):
            raise ValueError("cash deduction IDs must be unique")
        if self.receipt_status != "received":
            raise ValueError("payment phase requires actual received cash")
        if _timing_boundary(self.evidence.timing) < utc(self.evidence.timing.effective_at):
            raise ValueError("actual payment cannot be available before cash is received")
        total = sum(
            (
                _exact(item)
                for item in (
                    net,
                    withholding,
                    *(deduction.amount_text for deduction in deductions),
                    rounding,
                )
            ),
            Fraction(0),
        )
        if _exact(gross) != total:
            raise ValueError("cash payment must satisfy gross=net+withholding+deductions+rounding")
        object.__setattr__(self, "account_id", account)
        object.__setattr__(self, "payment_currency", currency)
        object.__setattr__(self, "policy_id", policy)
        object.__setattr__(self, "gross_cash_text", gross)
        object.__setattr__(self, "withholding_cash_text", withholding)
        object.__setattr__(self, "deductions", deductions)
        object.__setattr__(self, "rounding_adjustment_text", rounding)
        object.__setattr__(self, "net_cash_text", net)

    def to_dict(self) -> dict[str, object]:
        return {
            "phase": "payment",
            "evidence": self.evidence.to_dict(),
            "account_id": self.account_id,
            "payment_currency": self.payment_currency,
            "policy_id": self.policy_id,
            "gross_cash_text": self.gross_cash_text,
            "withholding_cash_text": self.withholding_cash_text,
            "deductions": [item.to_dict() for item in self.deductions],
            "rounding_adjustment_text": self.rounding_adjustment_text,
            "net_cash_text": self.net_cash_text,
            "receipt_status": self.receipt_status,
        }

    @classmethod
    def from_dict(
        cls, value: object, *, _legacy_context: object | None = None
    ) -> DividendPaymentV2:
        value = _phase_mapping(
            value,
            "payment",
            {
                "evidence",
                "account_id",
                "payment_currency",
                "policy_id",
                "gross_cash_text",
                "withholding_cash_text",
                "deductions",
                "rounding_adjustment_text",
                "net_cash_text",
                "receipt_status",
            },
        )
        deductions = value["deductions"]
        if not isinstance(deductions, list):
            raise TypeError("deductions must be an array")
        return cls(
            evidence=PhaseEvidenceV2.from_dict(value["evidence"], _legacy_context=_legacy_context),
            account_id=value["account_id"],
            payment_currency=value["payment_currency"],
            policy_id=value["policy_id"],
            gross_cash_text=value["gross_cash_text"],
            withholding_cash_text=value["withholding_cash_text"],
            deductions=tuple(CashDeduction.from_dict(item) for item in deductions),
            rounding_adjustment_text=value["rounding_adjustment_text"],
            net_cash_text=value["net_cash_text"],
            receipt_status=value["receipt_status"],
        )


@dataclass(frozen=True)
class DividendLifecycleV2:
    dividend_id: str
    instrument_id: str
    entitlement: DividendEntitlementV2
    election: DividendPaymentElectionV2 | None = None
    payment_policy: PaymentPolicyV2 | None = None
    proposal: DividendProposalV2 | None = None
    conversion: IssuerFxConversionV2 | None = None
    payment: DividendPaymentV2 | None = None
    legacy_binding: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        dividend_id = _text(self.dividend_id, "dividend_id")
        instrument_id = _text(self.instrument_id, "instrument_id")
        if not isinstance(self.entitlement, DividendEntitlementV2):
            raise TypeError("entitlement must be DividendEntitlementV2")
        expected = (
            (self.election, DividendPaymentElectionV2, "election"),
            (self.payment_policy, PaymentPolicyV2, "payment_policy"),
            (self.proposal, DividendProposalV2, "proposal"),
            (self.conversion, IssuerFxConversionV2, "conversion"),
            (self.payment, DividendPaymentV2, "payment"),
        )
        for value, kind, field in expected:
            if value is not None and not isinstance(value, kind):
                raise TypeError(f"{field} has an invalid v2 type")
        events = [
            phase.evidence
            for phase in (
                self.proposal,
                self.entitlement,
                self.election,
                self.conversion,
                self.payment,
            )
            if phase is not None
        ]
        if self.payment_policy is not None and self.payment_policy.evidence is not None:
            events.append(self.payment_policy.evidence)
        event_ids = [item.event_id for item in events]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("event_id must be unique across dividend lifecycle v2 phases")
        if self.proposal is not None and utc(self.proposal.evidence.timing.effective_at) > utc(
            self.entitlement.evidence.timing.effective_at
        ):
            raise ValueError("proposal cannot become effective after entitlement")
        options = {item.calculation_currency for item in self.entitlement.payment_currencies}
        if self.election is not None:
            if self.election.payment_currency not in options:
                raise ValueError("payment election must select an approved currency")
            if (
                self.election.selection_kind == "default"
                and self.election.payment_currency != self.entitlement.default_payment_currency
            ):
                raise ValueError("default election must use the issuer default currency")
        if self.election is not None and self.payment_policy is not None:
            if self.election.account_policy_id != self.payment_policy.policy_id:
                raise ValueError("payment election and policy IDs differ")
            if self.election.account_id != self.payment_policy.account_id:
                raise ValueError("payment election and policy accounts differ")
        declared = self.entitlement.declared_currency.calculation_currency
        selected = self.election.payment_currency if self.election is not None else None
        if selected is not None and selected == declared and self.conversion is not None:
            raise ValueError("same-currency dividend must not invent an issuer conversion")
        if self.conversion is not None:
            if self.conversion.from_currency != declared:
                raise ValueError("issuer conversion source currency must match entitlement")
            if self.conversion.to_currency not in options:
                raise ValueError("issuer conversion target must be an approved currency")
            if selected is not None and self.conversion.to_currency != selected:
                raise ValueError("issuer conversion does not join entitlement to election")
        if self.payment is not None:
            self._validate_payment(declared, selected)
        legacy = _freeze(self.legacy_binding) if self.legacy_binding is not None else None
        has_legacy = any(item.timing.availability.basis == "legacy_available_at" for item in events)
        if has_legacy and legacy is None:
            raise ValueError("legacy availability requires a lifecycle binding")
        object.__setattr__(self, "dividend_id", dividend_id)
        object.__setattr__(self, "instrument_id", instrument_id)
        object.__setattr__(self, "legacy_binding", legacy)
        if legacy is not None:
            from .dividend_migration_v2 import validate_legacy_v1_binding_v2

            validate_legacy_v1_binding_v2(self)

    def _validate_payment(self, declared: str, selected: str | None) -> None:
        assert self.payment is not None
        if self.election is None or self.payment_policy is None:
            raise ValueError("actual payment requires election and certified policy")
        if selected != declared and self.conversion is None:
            raise ValueError("cross-currency payment requires issuer conversion")
        if self.payment.payment_currency != selected:
            raise ValueError("payment currency differs from election")
        if self.payment.policy_id != self.payment_policy.policy_id:
            raise ValueError("payment and policy IDs differ")
        if self.payment.account_id != self.payment_policy.account_id:
            raise ValueError("payment and policy accounts differ")
        if self.payment_policy.certification_status != "certified":
            raise ValueError("actual payment requires a certified policy")
        payment_effective = utc(self.payment.evidence.timing.effective_at)
        if utc(self.payment_policy.evidence.timing.effective_at) > payment_effective:
            raise ValueError("payment policy cannot become effective after payment")
        if utc(self.entitlement.evidence.timing.effective_at) > payment_effective:
            raise ValueError("payment cannot be effective before entitlement")
        if utc(self.election.evidence.timing.effective_at) > payment_effective:
            raise ValueError("payment election cannot become effective after payment")
        if (
            self.conversion is not None
            and utc(self.conversion.evidence.timing.effective_at) > payment_effective
        ):
            raise ValueError("payment cannot precede issuer conversion")

    def _phase_values(self) -> tuple[object, ...]:
        return (
            self.proposal,
            self.entitlement,
            self.election,
            self.conversion,
            self.payment_policy,
            self.payment,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": DIVIDEND_LIFECYCLE_SCHEMA_ID_V2,
            "dividend_id": self.dividend_id,
            "instrument_id": self.instrument_id,
            "proposal": self.proposal.to_dict() if self.proposal is not None else None,
            "entitlement": self.entitlement.to_dict(),
            "election": self.election.to_dict() if self.election is not None else None,
            "conversion": self.conversion.to_dict() if self.conversion is not None else None,
            "payment_policy": (
                self.payment_policy.to_dict() if self.payment_policy is not None else None
            ),
            "payment": self.payment.to_dict() if self.payment is not None else None,
            "legacy_binding": _thaw(self.legacy_binding),
        }

    def to_json(self) -> str:
        return canonical(self.to_dict()).decode("utf-8")

    def fingerprint(self) -> str:
        return hashlib.sha256(canonical(self.to_dict())).hexdigest()

    @classmethod
    def from_dict(cls, value: object) -> DividendLifecycleV2:
        value = _mapping(value, "dividend lifecycle v2")
        _keys(
            value,
            required={
                "schema",
                "dividend_id",
                "instrument_id",
                "proposal",
                "entitlement",
                "election",
                "conversion",
                "payment_policy",
                "payment",
                "legacy_binding",
            },
            context="dividend lifecycle v2",
        )
        if value["schema"] != DIVIDEND_LIFECYCLE_SCHEMA_ID_V2:
            raise ValueError("unsupported dividend lifecycle v2 schema")
        proposal = value["proposal"]
        election = value["election"]
        conversion = value["conversion"]
        policy = value["payment_policy"]
        payment = value["payment"]
        legacy = value["legacy_binding"]
        if legacy is not None and not isinstance(legacy, Mapping):
            raise TypeError("legacy_binding must be an object or null")
        legacy_context = None
        if legacy is not None:
            from .publication_time_v2 import _LEGACY_CONTEXT_V2

            legacy_context = _LEGACY_CONTEXT_V2
        return cls(
            dividend_id=value["dividend_id"],
            instrument_id=value["instrument_id"],
            proposal=(
                DividendProposalV2.from_dict(proposal, _legacy_context=legacy_context)
                if proposal is not None
                else None
            ),
            entitlement=DividendEntitlementV2.from_dict(
                value["entitlement"], _legacy_context=legacy_context
            ),
            election=(
                DividendPaymentElectionV2.from_dict(election, _legacy_context=legacy_context)
                if election is not None
                else None
            ),
            conversion=(
                IssuerFxConversionV2.from_dict(conversion, _legacy_context=legacy_context)
                if conversion is not None
                else None
            ),
            payment_policy=(
                PaymentPolicyV2.from_dict(policy, _legacy_context=legacy_context)
                if policy is not None
                else None
            ),
            payment=(
                DividendPaymentV2.from_dict(payment, _legacy_context=legacy_context)
                if payment is not None
                else None
            ),
            legacy_binding=legacy,
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> DividendLifecycleV2:
        if not isinstance(value, (str, bytes)):
            raise TypeError("serialized dividend lifecycle v2 must be str or bytes")
        try:
            payload = json.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError("invalid dividend lifecycle v2 JSON") from exc
        return cls.from_dict(payload)


__all__ = [
    "DIVIDEND_LIFECYCLE_SCHEMA_ID_V2",
    "DividendEntitlementV2",
    "DividendLifecycleV2",
    "DividendPaymentElectionV2",
    "DividendPaymentV2",
    "DividendProposalV2",
    "IssuerFixingTimeV2",
    "IssuerFxConversionV2",
    "PaymentPolicyV2",
    "PhaseEvidenceV2",
]
