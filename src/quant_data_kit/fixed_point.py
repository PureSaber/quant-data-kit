"""Exact fixed-point values used by cross-asset public contracts."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import (
    ROUND_05UP,
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_DOWN,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    ROUND_UP,
    Decimal,
)

from quant_data_kit.exceptions import ValidationError

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_MAX_SCALE = 18
_MAX_INT64_DIGITS = 19
_ROUNDING_MODES = frozenset(
    {
        ROUND_05UP,
        ROUND_CEILING,
        ROUND_DOWN,
        ROUND_FLOOR,
        ROUND_HALF_DOWN,
        ROUND_HALF_EVEN,
        ROUND_HALF_UP,
        ROUND_UP,
    }
)
_ROUNDING_ERROR = """valid values for rounding are:
  [ROUND_CEILING, ROUND_FLOOR, ROUND_UP, ROUND_DOWN,
   ROUND_HALF_UP, ROUND_HALF_DOWN, ROUND_HALF_EVEN,
   ROUND_05UP]"""


def _validate_scale(scale: int) -> None:
    if isinstance(scale, bool) or not isinstance(scale, int):
        raise ValidationError("fixed-point scale must be an integer")
    if not 0 <= scale <= _MAX_SCALE:
        raise ValidationError(f"fixed-point scale must be in [0, {_MAX_SCALE}]")


def _digits_to_int(digits: tuple[int, ...]) -> int:
    value = 0
    for digit in digits:
        value = value * 10 + digit
    return value


def _has_nonzero(digits: tuple[int, ...], start: int) -> bool:
    return any(digits[index] != 0 for index in range(start, len(digits)))


def _round_magnitude(
    quotient: int,
    *,
    negative: bool,
    rounding: str,
    half_comparison: int,
) -> int:
    increment = False
    if rounding == ROUND_UP:
        increment = True
    elif rounding == ROUND_CEILING:
        increment = not negative
    elif rounding == ROUND_FLOOR:
        increment = negative
    elif rounding == ROUND_HALF_UP:
        increment = half_comparison >= 0
    elif rounding == ROUND_HALF_DOWN:
        increment = half_comparison > 0
    elif rounding == ROUND_HALF_EVEN:
        increment = half_comparison > 0 or (half_comparison == 0 and quotient % 2 == 1)
    elif rounding == ROUND_05UP:
        increment = quotient % 10 in {0, 5}
    return quotient + int(increment)


def _scaled_magnitude(
    decimal_value: Decimal,
    scale: int,
    rounding: str | None,
    source_value: Decimal | int | str,
) -> int:
    if rounding is not None and (not isinstance(rounding, str) or rounding not in _ROUNDING_MODES):
        raise TypeError(_ROUNDING_ERROR)

    parts = decimal_value.as_tuple()
    digits = parts.digits
    if not any(digits):
        return 0

    shift = int(parts.exponent) + scale
    if shift >= 0:
        if len(digits) + shift > _MAX_INT64_DIGITS:
            raise ValidationError("fixed-point units exceed signed int64")
        return _digits_to_int(digits) * 10**shift

    discarded_places = -shift
    cut = len(digits) - discarded_places
    if cut > _MAX_INT64_DIGITS:
        raise ValidationError("fixed-point units exceed signed int64")
    quotient = _digits_to_int(digits[:cut]) if cut > 0 else 0

    discarded_start = max(cut, 0)
    has_remainder = cut < 0 or _has_nonzero(digits, discarded_start)
    if not has_remainder:
        return quotient
    if rounding is None:
        raise ValidationError(f"value {source_value!r} is not exact at scale {scale}")

    if cut < 0 or digits[cut] < 5:
        half_comparison = -1
    elif digits[cut] > 5 or _has_nonzero(digits, cut + 1):
        half_comparison = 1
    else:
        half_comparison = 0
    return _round_magnitude(
        quotient,
        negative=bool(parts.sign),
        rounding=rounding,
        half_comparison=half_comparison,
    )


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
        _validate_scale(self.scale)

    @classmethod
    def from_decimal(
        cls,
        value: Decimal | int | str,
        scale: int,
        *,
        rounding: str | None = None,
    ) -> FixedPoint:
        """Create a value without implicit rounding."""
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        if not decimal_value.is_finite():
            raise ValidationError("fixed-point value must be finite")
        _validate_scale(scale)
        magnitude = _scaled_magnitude(decimal_value, scale, rounding, value)
        units = -magnitude if decimal_value.is_signed() and magnitude else magnitude
        return cls(units=units, scale=scale)

    def to_decimal(self) -> Decimal:
        magnitude = abs(self.units)
        digits = tuple(int(digit) for digit in str(magnitude)) if magnitude else (0,)
        return Decimal((int(self.units < 0), digits, -self.scale))

    def is_positive(self) -> bool:
        return self.units > 0

    def is_non_negative(self) -> bool:
        return self.units >= 0
