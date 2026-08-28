"""Exact fixed-point values used by cross-asset public contracts."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from quant_data_kit.exceptions import ValidationError

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_MAX_SCALE = 18


@dataclass(frozen=True)
class FixedPoint:
    """A signed integer scaled by a power of ten."""

    units: int
    scale: int

    def __post_init__(self) -> None:
        if isinstance(self.units, bool) or not isinstance(self.units, int):
            raise ValidationError("fixed-point units must be an integer")
        if not _INT64_MIN <= self.units <= _INT64_MAX:
            raise ValidationError("fixed-point units exceed signed int64")
        if isinstance(self.scale, bool) or not isinstance(self.scale, int):
            raise ValidationError("fixed-point scale must be an integer")
        if not 0 <= self.scale <= _MAX_SCALE:
            raise ValidationError(f"fixed-point scale must be in [0, {_MAX_SCALE}]")

    @classmethod
    def from_decimal(
        cls,
        value: Decimal | int | str,
        scale: int,
        *,
        rounding: str | None = None,
    ) -> "FixedPoint":
        """Create a value without implicit rounding."""
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        if not decimal_value.is_finite():
            raise ValidationError("fixed-point value must be finite")
        scaled = decimal_value.scaleb(scale)
        integral = scaled.to_integral_value(rounding=rounding) if rounding else scaled
        if rounding is None and scaled != scaled.to_integral_value():
            raise ValidationError(f"value {value!r} is not exact at scale {scale}")
        return cls(units=int(integral), scale=scale)

    def to_decimal(self) -> Decimal:
        return Decimal(self.units).scaleb(-self.scale)

    def is_positive(self) -> bool:
        return self.units > 0

    def is_non_negative(self) -> bool:
        return self.units >= 0

