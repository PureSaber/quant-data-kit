"""Account-specific share entitlements; ratios are not registry allocation facts."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint
from quant_data_kit.temporal_v2 import ensure_utc_datetime


class EntitlementUnresolved(ValidationError):
    def __init__(self, payload):
        super().__init__("share entitlement requires an evidenced integer quantity")
        self.diagnostics = {
            "stage": "corporate_action",
            "reason": "entitlement_unresolved",
            "terms": payload,
            "publishable": False,
        }


@dataclass(frozen=True, kw_only=True)
class ShareEntitlement:
    entitlement_id: str
    account_id: str
    record_date: date
    eligible_quantity: FixedPoint
    ordinary_additional_ratio: FixedPoint
    known_at: datetime
    credit_at: datetime
    sellable_date: date
    evidence_source: str
    evidence_sha256: str
    evidence_status: str = "unresolved"
    confirmed_quantity: FixedPoint | None = None
    fractional_rule: str = "unknown"
    reference_price: FixedPoint | None = None

    def __post_init__(self):
        for name in ("entitlement_id", "account_id", "evidence_source"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValidationError(f"share entitlement requires {name}")
        if len(self.evidence_sha256) != 64 or any(
            c not in "0123456789abcdef" for c in self.evidence_sha256
        ):
            raise ValidationError("share entitlement requires evidence SHA-256")
        for name in ("record_date", "sellable_date"):
            value = getattr(self, name)
            if not isinstance(value, date) or isinstance(value, datetime):
                raise ValidationError(f"{name} must be a date")
        for name in ("known_at", "credit_at"):
            object.__setattr__(self, name, ensure_utc_datetime(getattr(self, name), field=name))
        if self.credit_at < self.known_at or self.credit_at.date() < self.record_date:
            raise ValidationError("credit cannot precede evidence or record date")
        if self.sellable_date < self.credit_at.date():
            raise ValidationError("sellable date cannot precede credited shares")
        for name in ("eligible_quantity", "ordinary_additional_ratio", "confirmed_quantity"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, FixedPoint) or value.units < 0):
                raise ValidationError(f"{name} must be nonnegative FixedPoint")
        if self.eligible_quantity is None or self.ordinary_additional_ratio is None:
            raise ValidationError("eligible quantity and ordinary-holder ratio required")
        if (
            self.eligible_quantity.to_decimal()
            != self.eligible_quantity.to_decimal().to_integral_value()
        ):
            raise ValidationError("registered eligible shares must be integers")
        if self.reference_price is not None and (
            not isinstance(self.reference_price, FixedPoint) or self.reference_price.units <= 0
        ):
            raise ValidationError("reference price must be positive FixedPoint")
        if self.evidence_status not in {
            "unresolved",
            "confirmed",
            "sensitivity_floor",
            "sensitivity_ceil",
        }:
            raise ValidationError("unknown share evidence status")
        if self.fractional_rule not in {"unknown", "exact_pro_rata", "floor", "largest_fraction"}:
            raise ValidationError("unknown fractional allocation rule")
        if (self.confirmed_quantity is not None) != (self.evidence_status == "confirmed"):
            raise ValidationError("only confirmed evidence may supply an exact credited quantity")
        if self.confirmed_quantity is not None and (
            self.confirmed_quantity.to_decimal()
            != self.confirmed_quantity.to_decimal().to_integral_value()
        ):
            raise ValidationError("credited shares must be integers")
        if self.evidence_status.startswith("sensitivity") and self.fractional_rule not in {
            "floor",
            "largest_fraction",
        }:
            raise ValidationError("sensitivity requires an evidenced fractional allocation rule")
        if self.evidence_status == "sensitivity_ceil" and self.fractional_rule == "floor":
            raise ValidationError("ceiling is not permitted by a floor-only allocation rule")

    def resolved_quantity(self):
        if self.confirmed_quantity is not None:
            return self.confirmed_quantity.to_decimal()
        raw = self.eligible_quantity.to_decimal() * self.ordinary_additional_ratio.to_decimal()
        if raw == raw.to_integral_value():
            return raw
        if self.evidence_status == "sensitivity_floor":
            return raw.to_integral_value(rounding=ROUND_FLOOR)
        if self.evidence_status == "sensitivity_ceil":
            return raw.to_integral_value(rounding=ROUND_CEILING)
        raise EntitlementUnresolved(self.payload())

    def payload(self):
        result = {}
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if isinstance(value, FixedPoint):
                value = {"units": value.units, "scale": value.scale}
            elif isinstance(value, (datetime, date)):
                value = value.isoformat()
            result[name] = value
        return result

    @classmethod
    def from_payload(cls, payload):
        values = dict(payload)
        for name in (
            "eligible_quantity",
            "ordinary_additional_ratio",
            "confirmed_quantity",
            "reference_price",
        ):
            if values.get(name) is not None:
                values[name] = FixedPoint(**values[name])
        for name in ("known_at", "credit_at"):
            values[name] = datetime.fromisoformat(values[name].replace("Z", "+00:00"))
        for name in ("record_date", "sellable_date"):
            values[name] = date.fromisoformat(values[name])
        return cls(**values)
