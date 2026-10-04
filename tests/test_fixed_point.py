from __future__ import annotations

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
    Inexact,
    Overflow,
    Rounded,
    Underflow,
    localcontext,
)

import pytest

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint

ROUNDING_MODES = (
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_UP,
    ROUND_DOWN,
    ROUND_HALF_UP,
    ROUND_HALF_DOWN,
    ROUND_HALF_EVEN,
    ROUND_05UP,
)


def test_conversion_is_exact_under_low_precision_and_call_order() -> None:
    with localcontext() as context:
        context.prec = 2
        low_precision = FixedPoint.from_decimal(Decimal("100.01"), 2)
    with localcontext() as context:
        context.prec = 50
        high_precision_after_low = FixedPoint.from_decimal(Decimal("100.01"), 2)

    assert low_precision == FixedPoint(units=10001, scale=2)
    assert high_precision_after_low == low_precision
    assert low_precision.to_decimal() == Decimal("100.01")
    assert FixedPoint(units=10001, scale=2).to_decimal() == Decimal("100.01")


def test_inexact_conversion_without_rounding_is_rejected_under_low_precision() -> None:
    with localcontext() as context:
        context.prec = 1
        with pytest.raises(ValidationError, match="not exact"):
            FixedPoint.from_decimal(Decimal("0.001"), 2)


@pytest.mark.parametrize(
    ("rounding", "positive", "negative"),
    [
        (ROUND_CEILING, 3, -2),
        (ROUND_FLOOR, 2, -3),
        (ROUND_UP, 3, -3),
        (ROUND_DOWN, 2, -2),
        (ROUND_HALF_UP, 3, -3),
        (ROUND_HALF_DOWN, 2, -2),
        (ROUND_HALF_EVEN, 2, -2),
        (ROUND_05UP, 2, -2),
    ],
)
def test_all_decimal_rounding_modes_handle_positive_and_negative_ties(
    rounding: str, positive: int, negative: int
) -> None:
    assert FixedPoint.from_decimal("2.5", 0, rounding=rounding).units == positive
    assert FixedPoint.from_decimal("-2.5", 0, rounding=rounding).units == negative


@pytest.mark.parametrize("rounding", ROUNDING_MODES)
def test_rounding_matches_decimal_reference_for_signed_values(rounding: str) -> None:
    quantum = Decimal("0.01")
    for text in ("12.344", "12.345", "12.355", "5.001", "0.001", "-0.001", "-5.001"):
        value = Decimal(text)
        expected = int(value.quantize(quantum, rounding=rounding) * 100)
        assert FixedPoint.from_decimal(value, 2, rounding=rounding).units == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [("5.01", 6), ("-5.01", -6), ("1.99", 1), ("-1.99", -1), ("0.001", 1)],
)
def test_round_05up_uses_the_last_retained_digit(value: str, expected: int) -> None:
    assert FixedPoint.from_decimal(value, 0, rounding=ROUND_05UP).units == expected


def test_context_traps_and_exponent_limits_do_not_change_conversion() -> None:
    with localcontext() as context:
        context.prec = 2
        context.Emin = -2
        context.Emax = 2
        context.traps[Inexact] = True
        context.traps[Rounded] = True
        context.traps[Underflow] = True
        context.traps[Overflow] = True

        value = FixedPoint.from_decimal(Decimal("100.01"), 2)
        rounded = FixedPoint.from_decimal(Decimal("123.455"), 2, rounding=ROUND_HALF_EVEN)
        restored = FixedPoint(units=1, scale=18).to_decimal()

    assert value.units == 10001
    assert rounded.units == 12346
    assert restored == Decimal("0.000000000000000001")


def test_large_exponents_are_bounded_before_power_allocation() -> None:
    with pytest.raises(ValidationError, match="signed int64"):
        FixedPoint.from_decimal(Decimal("1E+999999999"), 18)
    with pytest.raises(ValidationError, match="not exact"):
        FixedPoint.from_decimal(Decimal("1E-999999999"), 18)

    assert FixedPoint.from_decimal(Decimal("0E+999999999"), 18).units == 0
    assert FixedPoint.from_decimal(Decimal("1E-999999999"), 18, rounding=ROUND_DOWN).units == 0
    assert FixedPoint.from_decimal(Decimal("1E-999999999"), 18, rounding=ROUND_UP).units == 1
    assert FixedPoint.from_decimal(Decimal("-1E-999999999"), 18, rounding=ROUND_FLOOR).units == -1


@pytest.mark.parametrize(
    "value",
    ["9223372036854775808", "-9223372036854775809", "1E+999999999"],
)
def test_signed_int64_overflow_is_rejected(value: str) -> None:
    with pytest.raises(ValidationError, match="signed int64"):
        FixedPoint.from_decimal(value, 0)


def test_signed_int64_boundaries_and_rounding_are_preserved() -> None:
    assert FixedPoint.from_decimal("9223372036854775807", 0).units == 2**63 - 1
    assert FixedPoint.from_decimal("-9223372036854775808", 0).units == -(2**63)
    assert (
        FixedPoint.from_decimal("9223372036854775807.4", 0, rounding=ROUND_DOWN).units == 2**63 - 1
    )
    with pytest.raises(ValidationError, match="signed int64"):
        FixedPoint.from_decimal("9223372036854775807.4", 0, rounding=ROUND_UP)


@pytest.mark.parametrize("scale", [-1, 19, True, 1.5])
def test_invalid_scale_is_rejected(scale: object) -> None:
    with pytest.raises(ValidationError, match="fixed-point scale"):
        FixedPoint.from_decimal("1", scale)  # type: ignore[arg-type]


def test_invalid_rounding_mode_keeps_decimal_api_failure() -> None:
    with pytest.raises(TypeError, match="valid values for rounding"):
        FixedPoint.from_decimal("1.1", 0, rounding="INVALID")
