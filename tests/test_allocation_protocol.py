import pytest

from quant_data_kit.allocation_protocol import METHODS, validate_research_allocation, validate_v2


def test_versions_never_silently_change_score_semantics():
    assert validate_research_allocation({"mode": "cost_aware"})["expected_return_model"] == "score"
    with pytest.raises(ValueError, match="requires"):
        validate_research_allocation({"protocol_version": 2, "mode": "cost_aware"})
    with pytest.raises(ValueError, match="protocol_version"):
        validate_research_allocation({"protocol_version": 3, "mode": "cost_aware"})


@pytest.mark.parametrize("mode", METHODS)
def test_all_methods_have_explicit_v2_capabilities(mode):
    expected = "ic_vol" if mode in {"cost_aware", "max_sharpe", "cvar", "evar", "cdar"} else "none"
    cfg = {
        "protocol_version": 2,
        "mode": mode,
        "risk_objective": "absolute",
        "forecast_horizon_sessions": 5,
        "expected_return_model": expected,
    }
    assert validate_v2(validate_v2(cfg)) == validate_v2(cfg)
    if mode != "cost_aware":
        with pytest.raises(ValueError, match="active risk"):
            validate_v2({**cfg, "risk_objective": "active"})


@pytest.mark.parametrize(
    "key,value",
    [
        ("forecast_horizon_sessions", 0),
        ("timeout_seconds", -1),
        ("max_tracking_error", float("nan")),
        ("expected_return_model", "score"),
        ("max_turnover", 3),
        ("risk_free_rate", 0.1),
        ("unknown", 1),
    ],
)
def test_invalid_v2_economics_rejected(key, value):
    with pytest.raises(ValueError):
        validate_v2(
            {
                "protocol_version": 2,
                "mode": "cost_aware",
                "risk_objective": "active",
                "forecast_horizon_sessions": 1,
                "expected_return_model": "ic_vol",
                key: value,
            }
        )
