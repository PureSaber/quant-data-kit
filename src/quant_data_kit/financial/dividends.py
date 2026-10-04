"""Versioned dividend facts without account-entitlement inference.

The lifecycle separates issuer proposals, approved entitlements, account payment
elections, issuer FX conversions, and evidenced cash receipt.  It deliberately
does not determine the entitled quantity; execution must obtain that quantity
from a point-in-time position ledger.
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
from .reconciliation import canonical

DIVIDEND_LIFECYCLE_SCHEMA_ID = "puresaber.dividend-lifecycle/1"
UNSUPPORTED_DIVIDEND_FEATURES = (
    "partial_payment_election",
    "split_payment",
    "correction_or_revocation",
    "multiple_issuer_conversions",
)

_DECIMAL_TEXT = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_CURRENCY = re.compile(r"[A-Z]{3}\Z")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{field} must be a nonempty string")
    return value.strip()


def _decimal_text(
    value: object,
    field: str,
    *,
    nonnegative: bool = False,
    positive: bool = False,
) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an exact decimal text")
    if not _DECIMAL_TEXT.fullmatch(value):
        raise ValueError(f"{field} must be a plain finite decimal text")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:  # pragma: no cover - guarded by the expression
        raise ValueError(f"invalid {field}") from exc
    if not parsed.is_finite() or (nonnegative and parsed < 0) or (positive and parsed <= 0):
        raise ValueError(f"{field} violates its sign or finiteness constraint")
    return value


def _exact_fraction(value: str) -> Fraction:
    """Convert validated plain decimal text without consulting Decimal context."""

    return Fraction(value)


def _finite_decimal(value: Fraction, field: str) -> Decimal:
    """Return an exact finite Decimal or reject a repeating decimal quotient."""

    denominator = value.denominator
    powers_of_two = 0
    powers_of_five = 0
    while denominator % 2 == 0:
        powers_of_two += 1
        denominator //= 2
    while denominator % 5 == 0:
        powers_of_five += 1
        denominator //= 5
    if denominator != 1:
        raise ValueError(f"{field} is not an exactly representable finite decimal")
    scale = max(powers_of_two, powers_of_five)
    coefficient = abs(value.numerator)
    coefficient *= 2 ** (scale - powers_of_two)
    coefficient *= 5 ** (scale - powers_of_five)
    digits = tuple(int(character) for character in str(coefficient))
    return Decimal((value.numerator < 0, digits, -scale))


def _timestamp(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a timezone-aware timestamp string")
    return utc(value, field).isoformat().replace("+00:00", "Z")


def _date(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a plain calendar-date string")
    return day(value).date().isoformat()


def _currency(value: object, field: str) -> str:
    value = _text(value, field)
    if not _CURRENCY.fullmatch(value):
        raise ValueError(f"{field} must be a three-letter uppercase calculation currency")
    return value


def _iana_timezone(value: object, field: str) -> tuple[str, ZoneInfo]:
    name = _text(value, field)
    try:
        zone = ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"{field} must be a valid IANA time zone") from exc
    return name, zone


def _mapping(value: object, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{context} must be an object")
    return value


def _keys(
    value: Mapping[str, Any],
    *,
    required: set[str],
    optional: set[str] | None = None,
    context: str,
) -> None:
    optional = optional or set()
    missing = required - set(value)
    unknown = set(value) - required - optional
    if missing:
        raise ValueError(f"{context} is missing fields: {sorted(missing)}")
    if unknown:
        raise ValueError(f"{context} has unsupported fields: {sorted(unknown)}")


@dataclass(frozen=True)
class CurrencyReference:
    """Preserve the issuer label while naming the calculation currency."""

    source_label: str
    calculation_currency: str
    normalization_rule: str

    def __post_init__(self) -> None:
        source = _text(self.source_label, "source_label")
        calculation = _currency(self.calculation_currency, "calculation_currency")
        rule = _text(self.normalization_rule, "normalization_rule")
        if source == calculation and rule != "identity":
            raise ValueError("identical currency labels require normalization_rule='identity'")
        if source != calculation and rule == "identity":
            raise ValueError("different currency labels require an explicit non-identity mapping")
        object.__setattr__(self, "source_label", source)
        object.__setattr__(self, "calculation_currency", calculation)
        object.__setattr__(self, "normalization_rule", rule)

    def to_dict(self) -> dict[str, str]:
        return {
            "source_label": self.source_label,
            "calculation_currency": self.calculation_currency,
            "normalization_rule": self.normalization_rule,
        }

    @classmethod
    def from_dict(cls, value: object) -> CurrencyReference:
        value = _mapping(value, "currency reference")
        _keys(
            value,
            required={"source_label", "calculation_currency", "normalization_rule"},
            context="currency reference",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class PublishedAmount:
    """Issuer amount as printed, including its source denominator and precision."""

    amount_text: str
    source_unit_text: str
    source_unit_name: str
    published_decimal_places: int
    approximate: bool

    def __post_init__(self) -> None:
        amount = _decimal_text(self.amount_text, "amount_text", nonnegative=True)
        units = _decimal_text(self.source_unit_text, "source_unit_text", positive=True)
        unit_name = _text(self.source_unit_name, "source_unit_name")
        if type(self.published_decimal_places) is not int or self.published_decimal_places < 0:
            raise TypeError("published_decimal_places must be a nonnegative integer")
        actual_places = len(amount.partition(".")[2])
        if self.published_decimal_places != actual_places:
            raise ValueError("published_decimal_places must preserve the printed amount precision")
        if type(self.approximate) is not bool:
            raise TypeError("approximate must be a boolean")
        object.__setattr__(self, "amount_text", amount)
        object.__setattr__(self, "source_unit_text", units)
        object.__setattr__(self, "source_unit_name", unit_name)

    @property
    def amount(self) -> Decimal:
        return Decimal(self.amount_text)

    @property
    def source_units(self) -> Decimal:
        return Decimal(self.source_unit_text)

    @property
    def per_source_unit(self) -> Decimal:
        quotient = _exact_fraction(self.amount_text) / _exact_fraction(self.source_unit_text)
        return _finite_decimal(quotient, "published amount per source unit")

    def to_dict(self) -> dict[str, object]:
        return {
            "amount_text": self.amount_text,
            "source_unit_text": self.source_unit_text,
            "source_unit_name": self.source_unit_name,
            "published_decimal_places": self.published_decimal_places,
            "approximate": self.approximate,
        }

    @classmethod
    def from_dict(cls, value: object) -> PublishedAmount:
        value = _mapping(value, "published amount")
        _keys(
            value,
            required={
                "amount_text",
                "source_unit_text",
                "source_unit_name",
                "published_decimal_places",
                "approximate",
            },
            context="published amount",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class EvidenceTiming:
    """Separate economic, public-availability, and local-capture times."""

    effective_at: str
    available_at: str
    captured_at: str
    source_published_at: str | None = None
    source_publication_date: str | None = None

    def __post_init__(self) -> None:
        effective = _timestamp(self.effective_at, "effective_at")
        available = _timestamp(self.available_at, "available_at")
        captured = _timestamp(self.captured_at, "captured_at")
        published = (
            _timestamp(self.source_published_at, "source_published_at")
            if self.source_published_at is not None
            else None
        )
        publication_date = (
            _date(self.source_publication_date, "source_publication_date")
            if self.source_publication_date is not None
            else None
        )
        if published is not None and publication_date is not None:
            raise ValueError("use an exact source timestamp or a date-only source fact, not both")
        if utc(available) > utc(captured):
            raise ValueError("available_at cannot follow local captured_at")
        if published is not None and utc(published) > utc(available):
            raise ValueError("available_at cannot precede the evidenced source publication time")
        if publication_date is not None and day(publication_date).date() > utc(captured).date():
            raise ValueError("source_publication_date cannot follow the local captured_at date")
        if published is None and utc(available) != utc(captured):
            raise ValueError(
                "without an exact source publication timestamp, available_at must equal captured_at"
            )
        object.__setattr__(self, "effective_at", effective)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "captured_at", captured)
        object.__setattr__(self, "source_published_at", published)
        object.__setattr__(self, "source_publication_date", publication_date)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "effective_at": self.effective_at,
            "available_at": self.available_at,
            "captured_at": self.captured_at,
            "source_published_at": self.source_published_at,
            "source_publication_date": self.source_publication_date,
        }

    @classmethod
    def from_dict(cls, value: object) -> EvidenceTiming:
        value = _mapping(value, "evidence timing")
        _keys(
            value,
            required={
                "effective_at",
                "available_at",
                "captured_at",
                "source_published_at",
                "source_publication_date",
            },
            context="evidence timing",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class PhaseEvidence:
    event_id: str
    source: str
    evidence_id: str
    timing: EvidenceTiming

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _text(self.event_id, "event_id"))
        object.__setattr__(self, "source", _text(self.source, "source"))
        object.__setattr__(self, "evidence_id", _text(self.evidence_id, "evidence_id"))
        if not isinstance(self.timing, EvidenceTiming):
            raise TypeError("timing must be EvidenceTiming")

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "source": self.source,
            "evidence_id": self.evidence_id,
            "timing": self.timing.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> PhaseEvidence:
        value = _mapping(value, "phase evidence")
        _keys(
            value,
            required={"event_id", "source", "evidence_id", "timing"},
            context="phase evidence",
        )
        return cls(
            event_id=value["event_id"],
            source=value["source"],
            evidence_id=value["evidence_id"],
            timing=EvidenceTiming.from_dict(value["timing"]),
        )


@dataclass(frozen=True)
class DividendProposal:
    evidence: PhaseEvidence
    proposed_amount: PublishedAmount
    declared_currency: CurrencyReference
    proposed_ex_date: str
    proposed_payment_date: str

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, PhaseEvidence):
            raise TypeError("proposal evidence must be PhaseEvidence")
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
    def from_dict(cls, value: object) -> DividendProposal:
        value = _mapping(value, "dividend proposal")
        _keys(
            value,
            required={
                "phase",
                "evidence",
                "proposed_amount",
                "declared_currency",
                "proposed_ex_date",
                "proposed_payment_date",
            },
            context="dividend proposal",
        )
        if value["phase"] != "proposal":
            raise ValueError("dividend proposal phase must be 'proposal'")
        return cls(
            evidence=PhaseEvidence.from_dict(value["evidence"]),
            proposed_amount=PublishedAmount.from_dict(value["proposed_amount"]),
            declared_currency=CurrencyReference.from_dict(value["declared_currency"]),
            proposed_ex_date=value["proposed_ex_date"],
            proposed_payment_date=value["proposed_payment_date"],
        )


@dataclass(frozen=True)
class DividendEntitlement:
    evidence: PhaseEvidence
    approved_amount: PublishedAmount
    declared_currency: CurrencyReference
    record_date: str
    scheduled_payment_date: str
    payment_currencies: tuple[CurrencyReference, ...]
    default_payment_currency: str
    approval_status: str = "approved"

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, PhaseEvidence):
            raise TypeError("entitlement evidence must be PhaseEvidence")
        if not isinstance(self.approved_amount, PublishedAmount):
            raise TypeError("approved_amount must be PublishedAmount")
        if not isinstance(self.declared_currency, CurrencyReference):
            raise TypeError("declared_currency must be CurrencyReference")
        if self.approval_status != "approved":
            raise ValueError("entitlement requires actual approved terms; proposals are separate")
        record = _date(self.record_date, "record_date")
        payment_date = _date(self.scheduled_payment_date, "scheduled_payment_date")
        currencies = tuple(self.payment_currencies)
        if not currencies or not all(isinstance(item, CurrencyReference) for item in currencies):
            raise TypeError("payment_currencies must contain CurrencyReference values")
        codes = [item.calculation_currency for item in currencies]
        if len(codes) != len(set(codes)):
            raise ValueError("payment currency options must be unique")
        default = _currency(self.default_payment_currency, "default_payment_currency")
        if default not in codes:
            raise ValueError("default payment currency must be one of the approved options")
        object.__setattr__(self, "record_date", record)
        object.__setattr__(self, "scheduled_payment_date", payment_date)
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
    def from_dict(cls, value: object) -> DividendEntitlement:
        value = _mapping(value, "dividend entitlement")
        _keys(
            value,
            required={
                "phase",
                "evidence",
                "approved_amount",
                "declared_currency",
                "record_date",
                "scheduled_payment_date",
                "payment_currencies",
                "default_payment_currency",
                "approval_status",
            },
            context="dividend entitlement",
        )
        if value["phase"] != "entitlement":
            raise ValueError("dividend entitlement phase must be 'entitlement'")
        if not isinstance(value["payment_currencies"], list):
            raise TypeError("payment_currencies must be an array")
        return cls(
            evidence=PhaseEvidence.from_dict(value["evidence"]),
            approved_amount=PublishedAmount.from_dict(value["approved_amount"]),
            declared_currency=CurrencyReference.from_dict(value["declared_currency"]),
            record_date=value["record_date"],
            scheduled_payment_date=value["scheduled_payment_date"],
            payment_currencies=tuple(
                CurrencyReference.from_dict(item) for item in value["payment_currencies"]
            ),
            default_payment_currency=value["default_payment_currency"],
            approval_status=value["approval_status"],
        )


@dataclass(frozen=True)
class DividendPaymentElection:
    evidence: PhaseEvidence
    account_id: str
    account_policy_id: str
    payment_currency: str
    selection_kind: str
    selection_scope: str = "entire_entitlement"

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, PhaseEvidence):
            raise TypeError("payment election evidence must be PhaseEvidence")
        object.__setattr__(self, "account_id", _text(self.account_id, "account_id"))
        object.__setattr__(
            self, "account_policy_id", _text(self.account_policy_id, "account_policy_id")
        )
        object.__setattr__(
            self, "payment_currency", _currency(self.payment_currency, "payment_currency")
        )
        if self.selection_kind not in {"default", "explicit"}:
            raise ValueError("selection_kind must be 'default' or 'explicit'")
        if self.selection_scope != "entire_entitlement":
            raise ValueError("partial payment election is unsupported")

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
    def from_dict(cls, value: object) -> DividendPaymentElection:
        value = _mapping(value, "payment election")
        _keys(
            value,
            required={
                "phase",
                "evidence",
                "account_id",
                "account_policy_id",
                "payment_currency",
                "selection_kind",
                "selection_scope",
            },
            context="payment election",
        )
        if value["phase"] != "payment_election":
            raise ValueError("payment election phase must be 'payment_election'")
        return cls(
            evidence=PhaseEvidence.from_dict(value["evidence"]),
            account_id=value["account_id"],
            account_policy_id=value["account_policy_id"],
            payment_currency=value["payment_currency"],
            selection_kind=value["selection_kind"],
            selection_scope=value["selection_scope"],
        )


@dataclass(frozen=True)
class IssuerFxConversion:
    evidence: PhaseEvidence
    from_currency: str
    to_currency: str
    rate_text: str
    rate_convention: str
    fixing_at: str | None
    fixing_date: str | None
    published_payment_amount: PublishedAmount
    fixing_timezone: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, PhaseEvidence):
            raise TypeError("issuer conversion evidence must be PhaseEvidence")
        source = _currency(self.from_currency, "from_currency")
        target = _currency(self.to_currency, "to_currency")
        if source == target:
            raise ValueError("issuer conversion requires different currencies")
        rate = _decimal_text(self.rate_text, "rate_text", positive=True)
        if self.rate_convention != "quote_per_base":
            raise ValueError("rate_convention must be 'quote_per_base'")
        fixing = _timestamp(self.fixing_at, "fixing_at") if self.fixing_at is not None else None
        fixing_date = (
            _date(self.fixing_date, "fixing_date") if self.fixing_date is not None else None
        )
        if (fixing is None) == (fixing_date is None):
            raise ValueError("issuer conversion requires exactly one of fixing_at or fixing_date")
        fixing_timezone = None
        fixing_zone = None
        if self.fixing_timezone is not None:
            fixing_timezone, fixing_zone = _iana_timezone(self.fixing_timezone, "fixing_timezone")
        if fixing_date is not None and fixing_zone is None:
            raise ValueError("date-only issuer fixing requires fixing_timezone")
        if fixing is not None and utc(fixing) > utc(self.evidence.timing.available_at):
            raise ValueError("issuer conversion cannot be available before its fixing")
        if (
            fixing_date is not None
            and day(fixing_date).date()
            > utc(self.evidence.timing.available_at).astimezone(fixing_zone).date()
        ):
            raise ValueError(
                "date-only issuer fixing cannot follow available_at in fixing_timezone"
            )
        if not isinstance(self.published_payment_amount, PublishedAmount):
            raise TypeError("published_payment_amount must be PublishedAmount")
        object.__setattr__(self, "from_currency", source)
        object.__setattr__(self, "to_currency", target)
        object.__setattr__(self, "rate_text", rate)
        object.__setattr__(self, "fixing_at", fixing)
        object.__setattr__(self, "fixing_date", fixing_date)
        object.__setattr__(self, "fixing_timezone", fixing_timezone)

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
            "fixing_at": self.fixing_at,
            "fixing_date": self.fixing_date,
            "fixing_timezone": self.fixing_timezone,
            "published_payment_amount": self.published_payment_amount.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> IssuerFxConversion:
        value = _mapping(value, "issuer FX conversion")
        _keys(
            value,
            required={
                "phase",
                "evidence",
                "from_currency",
                "to_currency",
                "rate_text",
                "rate_convention",
                "fixing_at",
                "fixing_date",
                "fixing_timezone",
                "published_payment_amount",
            },
            context="issuer FX conversion",
        )
        if value["phase"] != "issuer_fx_conversion":
            raise ValueError("issuer FX conversion phase must be 'issuer_fx_conversion'")
        return cls(
            evidence=PhaseEvidence.from_dict(value["evidence"]),
            from_currency=value["from_currency"],
            to_currency=value["to_currency"],
            rate_text=value["rate_text"],
            rate_convention=value["rate_convention"],
            fixing_at=value["fixing_at"],
            fixing_date=value["fixing_date"],
            fixing_timezone=value["fixing_timezone"],
            published_payment_amount=PublishedAmount.from_dict(value["published_payment_amount"]),
        )


@dataclass(frozen=True)
class RoundingPolicy:
    scope: str
    decimal_places: int
    mode: str
    evidence_id: str

    def __post_init__(self) -> None:
        if self.scope not in {"per_source_unit", "aggregate_account"}:
            raise ValueError("unsupported rounding scope")
        if type(self.decimal_places) is not int or self.decimal_places < 0:
            raise TypeError("rounding decimal_places must be a nonnegative integer")
        if self.mode not in {"ROUND_HALF_UP", "ROUND_HALF_EVEN", "ROUND_DOWN", "ROUND_UP"}:
            raise ValueError("unsupported rounding mode")
        object.__setattr__(self, "evidence_id", _text(self.evidence_id, "rounding evidence_id"))

    def to_dict(self) -> dict[str, object]:
        return {
            "scope": self.scope,
            "decimal_places": self.decimal_places,
            "mode": self.mode,
            "evidence_id": self.evidence_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> RoundingPolicy:
        value = _mapping(value, "rounding policy")
        _keys(
            value,
            required={"scope", "decimal_places", "mode", "evidence_id"},
            context="rounding policy",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class PaymentPolicy:
    policy_id: str
    account_id: str
    certification_status: str
    holder_tax_profile_id: str | None = None
    withholding_rule_id: str | None = None
    rounding: RoundingPolicy | None = None
    evidence: PhaseEvidence | None = None

    def __post_init__(self) -> None:
        policy_id = _text(self.policy_id, "policy_id")
        account_id = _text(self.account_id, "account_id")
        if self.certification_status not in {"unverified", "certified"}:
            raise ValueError("certification_status must be 'unverified' or 'certified'")
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
        if self.evidence is not None and not isinstance(self.evidence, PhaseEvidence):
            raise TypeError("payment policy evidence must be PhaseEvidence or None")
        if self.certification_status == "certified" and (
            profile is None
            or rule is None
            or not isinstance(self.rounding, RoundingPolicy)
            or self.evidence is None
        ):
            raise ValueError(
                "certified payment policy requires evidence, holder profile, "
                "withholding rule and rounding"
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
    def from_dict(cls, value: object) -> PaymentPolicy:
        value = _mapping(value, "payment policy")
        _keys(
            value,
            required={
                "phase",
                "policy_id",
                "account_id",
                "certification_status",
                "holder_tax_profile_id",
                "withholding_rule_id",
                "rounding",
                "evidence",
            },
            context="payment policy",
        )
        if value["phase"] != "payment_policy":
            raise ValueError("payment policy phase must be 'payment_policy'")
        rounding = value["rounding"]
        evidence = value["evidence"]
        return cls(
            policy_id=value["policy_id"],
            account_id=value["account_id"],
            certification_status=value["certification_status"],
            holder_tax_profile_id=value["holder_tax_profile_id"],
            withholding_rule_id=value["withholding_rule_id"],
            rounding=RoundingPolicy.from_dict(rounding) if rounding is not None else None,
            evidence=PhaseEvidence.from_dict(evidence) if evidence is not None else None,
        )


@dataclass(frozen=True)
class CashDeduction:
    deduction_id: str
    amount_text: str
    evidence_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "deduction_id", _text(self.deduction_id, "deduction_id"))
        object.__setattr__(
            self, "amount_text", _decimal_text(self.amount_text, "amount_text", nonnegative=True)
        )
        object.__setattr__(self, "evidence_id", _text(self.evidence_id, "deduction evidence_id"))

    @property
    def amount(self) -> Decimal:
        return Decimal(self.amount_text)

    def to_dict(self) -> dict[str, str]:
        return {
            "deduction_id": self.deduction_id,
            "amount_text": self.amount_text,
            "evidence_id": self.evidence_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> CashDeduction:
        value = _mapping(value, "cash deduction")
        _keys(
            value,
            required={"deduction_id", "amount_text", "evidence_id"},
            context="cash deduction",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class DividendPayment:
    evidence: PhaseEvidence
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
        if not isinstance(self.evidence, PhaseEvidence):
            raise TypeError("payment evidence must be PhaseEvidence")
        account_id = _text(self.account_id, "account_id")
        currency = _currency(self.payment_currency, "payment_currency")
        policy = _text(self.policy_id, "policy_id")
        gross_text = _decimal_text(self.gross_cash_text, "gross_cash_text", nonnegative=True)
        withholding_text = _decimal_text(
            self.withholding_cash_text, "withholding_cash_text", nonnegative=True
        )
        rounding_text = _decimal_text(self.rounding_adjustment_text, "rounding_adjustment_text")
        net_text = _decimal_text(self.net_cash_text, "net_cash_text", nonnegative=True)
        deductions = tuple(self.deductions)
        if not all(isinstance(item, CashDeduction) for item in deductions):
            raise TypeError("deductions must contain CashDeduction values")
        ids = [item.deduction_id for item in deductions]
        if len(ids) != len(set(ids)):
            raise ValueError("cash deduction IDs must be unique")
        if self.receipt_status != "received":
            raise ValueError(
                "payment phase is actual receipt; planned or pending cash is not payment"
            )
        if utc(self.evidence.timing.available_at) < utc(self.evidence.timing.effective_at):
            raise ValueError("actual payment cannot be available before cash is received")
        gross = _exact_fraction(gross_text)
        components = sum(
            (
                _exact_fraction(value)
                for value in (
                    net_text,
                    withholding_text,
                    *(item.amount_text for item in deductions),
                    rounding_text,
                )
            ),
            Fraction(0),
        )
        if gross != components:
            raise ValueError(
                "cash payment must satisfy gross=net+withholding+deductions+rounding_adjustment"
            )
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "payment_currency", currency)
        object.__setattr__(self, "policy_id", policy)
        object.__setattr__(self, "gross_cash_text", gross_text)
        object.__setattr__(self, "withholding_cash_text", withholding_text)
        object.__setattr__(self, "deductions", deductions)
        object.__setattr__(self, "rounding_adjustment_text", rounding_text)
        object.__setattr__(self, "net_cash_text", net_text)

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
    def from_dict(cls, value: object) -> DividendPayment:
        value = _mapping(value, "dividend payment")
        _keys(
            value,
            required={
                "phase",
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
            context="dividend payment",
        )
        if value["phase"] != "payment":
            raise ValueError("dividend payment phase must be 'payment'")
        if not isinstance(value["deductions"], list):
            raise TypeError("deductions must be an array")
        return cls(
            evidence=PhaseEvidence.from_dict(value["evidence"]),
            account_id=value["account_id"],
            payment_currency=value["payment_currency"],
            policy_id=value["policy_id"],
            gross_cash_text=value["gross_cash_text"],
            withholding_cash_text=value["withholding_cash_text"],
            deductions=tuple(CashDeduction.from_dict(item) for item in value["deductions"]),
            rounding_adjustment_text=value["rounding_adjustment_text"],
            net_cash_text=value["net_cash_text"],
            receipt_status=value["receipt_status"],
        )


@dataclass(frozen=True)
class DividendLifecycle:
    dividend_id: str
    instrument_id: str
    entitlement: DividendEntitlement
    election: DividendPaymentElection | None = None
    payment_policy: PaymentPolicy | None = None
    proposal: DividendProposal | None = None
    conversion: IssuerFxConversion | None = None
    payment: DividendPayment | None = None

    def __post_init__(self) -> None:
        dividend_id = _text(self.dividend_id, "dividend_id")
        instrument_id = _text(self.instrument_id, "instrument_id")
        if not isinstance(self.entitlement, DividendEntitlement):
            raise TypeError("entitlement must be DividendEntitlement")
        if self.election is not None and not isinstance(self.election, DividendPaymentElection):
            raise TypeError("election must be DividendPaymentElection or None")
        if self.payment_policy is not None and not isinstance(self.payment_policy, PaymentPolicy):
            raise TypeError("payment_policy must be PaymentPolicy or None")
        if self.proposal is not None and not isinstance(self.proposal, DividendProposal):
            raise TypeError("proposal must be DividendProposal or None")
        if self.conversion is not None and not isinstance(self.conversion, IssuerFxConversion):
            raise TypeError("conversion must be IssuerFxConversion or None")
        if self.payment is not None and not isinstance(self.payment, DividendPayment):
            raise TypeError("payment must be DividendPayment or None")

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
            raise ValueError("event_id must be unique across dividend lifecycle phases")
        if self.proposal is not None and utc(self.proposal.evidence.timing.effective_at) > utc(
            self.entitlement.evidence.timing.effective_at
        ):
            raise ValueError("proposal cannot become effective after approved entitlement terms")

        options = {item.calculation_currency for item in self.entitlement.payment_currencies}
        if self.election is not None:
            if self.election.payment_currency not in options:
                raise ValueError("payment election must select an approved payment currency")
            if (
                self.election.selection_kind == "default"
                and self.election.payment_currency != self.entitlement.default_payment_currency
            ):
                raise ValueError("default election must use the issuer default payment currency")
        if self.election is not None and self.payment_policy is not None:
            if self.election.account_policy_id != self.payment_policy.policy_id:
                raise ValueError("payment election and payment policy must use the same policy ID")
            if self.election.account_id != self.payment_policy.account_id:
                raise ValueError("payment election and payment policy must use the same account ID")

        declared = self.entitlement.declared_currency.calculation_currency
        selected = self.election.payment_currency if self.election is not None else None
        if selected is not None and declared == selected and self.conversion is not None:
            raise ValueError("same-currency dividend must not invent an issuer FX conversion")
        if self.conversion is not None:
            if self.conversion.from_currency != declared:
                raise ValueError("issuer conversion source currency must match entitlement")
            if self.conversion.to_currency not in options:
                raise ValueError("issuer conversion target must be an approved payment currency")
            if selected is not None and self.conversion.to_currency != selected:
                raise ValueError("issuer conversion currencies do not join entitlement to election")
        if self.payment is not None:
            if self.election is None or self.payment_policy is None:
                raise ValueError("actual payment requires election and certified payment policy")
            if selected != declared and self.conversion is None:
                raise ValueError("cross-currency payment requires the evidenced issuer conversion")
            if self.payment.payment_currency != selected:
                raise ValueError("payment currency differs from the account election")
            if self.payment.policy_id != self.payment_policy.policy_id:
                raise ValueError("payment and payment policy must use the same policy ID")
            if self.payment.account_id != self.payment_policy.account_id:
                raise ValueError("payment and payment policy must use the same account ID")
            if self.payment_policy.certification_status != "certified":
                raise ValueError("actual payment requires a certified account payment policy")
            if utc(self.payment_policy.evidence.timing.effective_at) > utc(
                self.payment.evidence.timing.effective_at
            ):
                raise ValueError("payment policy cannot become effective after payment")
            if utc(self.payment.evidence.timing.effective_at) < utc(
                self.entitlement.evidence.timing.effective_at
            ):
                raise ValueError("payment cannot be effective before entitlement")
            if utc(self.election.evidence.timing.effective_at) > utc(
                self.payment.evidence.timing.effective_at
            ):
                raise ValueError("payment election cannot become effective after payment")
            if self.conversion is not None and utc(self.payment.evidence.timing.effective_at) < utc(
                self.conversion.evidence.timing.effective_at
            ):
                raise ValueError("payment cannot precede issuer FX conversion")
        object.__setattr__(self, "dividend_id", dividend_id)
        object.__setattr__(self, "instrument_id", instrument_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": DIVIDEND_LIFECYCLE_SCHEMA_ID,
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
        }

    def to_json(self) -> str:
        return canonical(self.to_dict()).decode("utf-8")

    def fingerprint(self) -> str:
        return hashlib.sha256(canonical(self.to_dict())).hexdigest()

    @classmethod
    def from_dict(cls, value: object) -> DividendLifecycle:
        value = _mapping(value, "dividend lifecycle")
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
            },
            context="dividend lifecycle",
        )
        if value["schema"] != DIVIDEND_LIFECYCLE_SCHEMA_ID:
            raise ValueError("unsupported dividend lifecycle schema")
        return cls(
            dividend_id=value["dividend_id"],
            instrument_id=value["instrument_id"],
            proposal=(
                DividendProposal.from_dict(value["proposal"])
                if value["proposal"] is not None
                else None
            ),
            entitlement=DividendEntitlement.from_dict(value["entitlement"]),
            election=(
                DividendPaymentElection.from_dict(value["election"])
                if value["election"] is not None
                else None
            ),
            conversion=(
                IssuerFxConversion.from_dict(value["conversion"])
                if value["conversion"] is not None
                else None
            ),
            payment_policy=(
                PaymentPolicy.from_dict(value["payment_policy"])
                if value["payment_policy"] is not None
                else None
            ),
            payment=(
                DividendPayment.from_dict(value["payment"])
                if value["payment"] is not None
                else None
            ),
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> DividendLifecycle:
        if not isinstance(value, (str, bytes)):
            raise TypeError("serialized dividend lifecycle must be str or bytes")
        try:
            payload = json.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError("invalid dividend lifecycle JSON") from exc
        return cls.from_dict(payload)
