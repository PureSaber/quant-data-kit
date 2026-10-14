"""Versioned allocation capabilities shared by research, execution and UI.

No optimizer or optional numerical dependency is imported by this contract.
"""

from __future__ import annotations

import math
from collections.abc import Mapping

METHODS = {
    "equal": {"objective": "target", "risk_usage": "constraints"},
    "inverse_vol": {"objective": "target", "risk_usage": "target_and_constraints"},
    "risk_parity": {"objective": "target", "risk_usage": "target_and_constraints"},
    "hrp": {"objective": "target", "risk_usage": "target_and_constraints"},
    "cost_aware": {"objective": "mean_variance", "risk_usage": "objective_and_constraints"},
    "max_sharpe": {"objective": "max_sharpe", "risk_usage": "objective_and_constraints"},
    "max_diversification": {
        "objective": "max_diversification",
        "risk_usage": "objective_and_constraints",
    },
    "cvar": {"objective": "cvar", "risk_usage": "scenarios_and_constraints"},
    "evar": {"objective": "evar", "risk_usage": "scenarios_and_constraints"},
    "cdar": {"objective": "cdar", "risk_usage": "scenarios_and_constraints"},
}


def validate_v2(value: Mapping) -> dict:
    """Require explicit economics; absence never selects a replacement strategy."""
    fields = {
        "protocol_version",
        "mode",
        "risk_objective",
        "forecast_horizon_sessions",
        "expected_return_model",
        "lookback",
        "min_observations",
        "covariance_shrinkage",
        "risk_aversion",
        "max_turnover",
        "turnover_penalty",
        "max_tracking_error",
        "factor_bound_reference",
        "timeout_seconds",
        "ic_lookback",
        "ic_min_periods",
        "risk_free_rate",
        "cvar_beta",
        "evar_beta",
        "cdar_beta",
    }
    if not isinstance(value, Mapping) or set(value) - fields:
        raise ValueError("unknown allocation v2 fields")
    if value.get("protocol_version") != 2 or type(value["protocol_version"]) is not int:
        raise ValueError("allocation.protocol_version must be 2")
    for key in ("mode", "risk_objective", "forecast_horizon_sessions", "expected_return_model"):
        if key not in value:
            raise ValueError(f"allocation v2 requires {key}")
    result = {
        "lookback": 60,
        "min_observations": 20,
        "covariance_shrinkage": 0.2,
        "risk_aversion": 5.0,
        "max_turnover": 1.0,
        "turnover_penalty": 0.0,
        "max_tracking_error": None,
        "factor_bound_reference": "absolute",
        "timeout_seconds": 30.0,
        "ic_lookback": 120,
        "ic_min_periods": 20,
        **value,
    }
    mode = result["mode"]
    if mode not in METHODS:
        raise ValueError("unsupported allocation method")
    if result["risk_objective"] not in {"absolute", "active"}:
        raise ValueError("risk_objective must be absolute or active")
    if result["risk_objective"] == "active" and mode != "cost_aware":
        raise ValueError("active risk objective is supported only by cost_aware")
    model = result["expected_return_model"]
    if model not in {"ic_vol", "provided", "none"}:
        raise ValueError("v2 requires mature ic_vol or explicit provided returns")
    uses_return = mode in {"cost_aware", "max_sharpe", "cvar", "evar", "cdar"}
    if uses_return == (model == "none"):
        raise ValueError("expected-return model is incompatible with allocation method")
    if result["factor_bound_reference"] not in {"absolute", "active"}:
        raise ValueError("factor_bound_reference must be absolute or active")
    for key in (
        "forecast_horizon_sessions",
        "lookback",
        "min_observations",
        "ic_lookback",
        "ic_min_periods",
    ):
        if type(result[key]) is not int or result[key] < (
            1 if key == "forecast_horizon_sessions" else 2
        ):
            raise ValueError(f"invalid allocation.{key}")
    if (
        result["min_observations"] > result["lookback"]
        or result["ic_min_periods"] > result["ic_lookback"]
    ):
        raise ValueError("minimum observations exceed rolling window")
    for key in (
        "risk_aversion",
        "max_turnover",
        "turnover_penalty",
        "covariance_shrinkage",
        "timeout_seconds",
    ):
        x = result[key]
        if isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x):
            raise ValueError(f"invalid allocation.{key}")
        if x < 0 or (key in {"risk_aversion", "timeout_seconds"} and x == 0):
            raise ValueError(f"invalid allocation.{key}")
    if result["max_turnover"] > 2 or result["covariance_shrinkage"] > 1:
        raise ValueError("invalid turnover or shrinkage")
    if result["max_tracking_error"] is not None:
        x = result["max_tracking_error"]
        if isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) or x < 0:
            raise ValueError("invalid max_tracking_error")
    for key in ("cvar_beta", "evar_beta", "cdar_beta", "risk_free_rate"):
        if key in value:
            expected = "max_sharpe" if key == "risk_free_rate" else key.removesuffix("_beta")
            x = value[key]
            if (
                mode != expected
                or isinstance(x, bool)
                or not isinstance(x, (int, float))
                or not math.isfinite(x)
            ):
                raise ValueError(f"invalid allocation.{key}")
            if key.endswith("_beta") and not 0 < x < 1:
                raise ValueError(f"invalid allocation.{key}")
    return result


_RESEARCH_ALLOCATION_FIELDS = {
    "mode",
    "lookback",
    "min_observations",
    "covariance_shrinkage",
    "covariance_estimator",
    "risk_aversion",
    "turnover_penalty",
    "max_turnover",
    "expected_return_model",
    "information_coefficient",
    "black_litterman_tau",
    "cvar_beta",
    "evar_beta",
    "cdar_beta",
    "ewma_decay",
    "risk_free_rate",
    "max_tracking_error",
    "factor_bound_reference",
}
_ALLOCATION_MODES = {
    "equal",
    "inverse_vol",
    "cost_aware",
    "risk_parity",
    "hrp",
    "cvar",
    "max_sharpe",
    "max_diversification",
    "evar",
    "cdar",
}
_COVARIANCE_MODES = {
    "cost_aware",
    "risk_parity",
    "hrp",
    "max_sharpe",
    "max_diversification",
}
_PATH_MODES = {"cvar", "evar", "cdar"}
_COVARIANCE_ESTIMATORS = {
    "diagonal",
    "ledoit_wolf",
    "factor",
    "ewma",
    "constant_correlation",
    "oas",
    "random_matrix",
}


def validate_research_allocation(value: Mapping[str, object]) -> dict[str, object]:
    """Validate and normalize the closed research-allocation recipe fragment."""

    if not isinstance(value, Mapping):
        raise TypeError("allocation must be a mapping")
    if "protocol_version" in value:
        return validate_v2(value)
    unknown = set(value) - _RESEARCH_ALLOCATION_FIELDS
    if unknown:
        raise ValueError(f"allocation contains unknown fields: {sorted(unknown)}")
    mode = value.get("mode")
    if mode not in _ALLOCATION_MODES:
        raise ValueError(
            "allocation.mode must be equal, inverse_vol, cost_aware, risk_parity, hrp or cvar, "
            "max_sharpe, max_diversification, evar or cdar"
        )

    def integer(name: str, default: int) -> int:
        result = value.get(name, default)
        if isinstance(result, bool) or not isinstance(result, int):
            raise TypeError(f"allocation.{name} must be an integer")
        return result

    def number(name: str, default: float) -> float:
        result = value.get(name, default)
        if isinstance(result, bool) or not isinstance(result, (int, float)):
            raise TypeError(f"allocation.{name} must be numeric")
        result = float(result)
        if not math.isfinite(result):
            raise ValueError(f"allocation.{name} must be finite")
        return result

    lookback = integer("lookback", 20)
    min_observations = integer("min_observations", 10)
    shrinkage = number("covariance_shrinkage", 0.2)
    risk_aversion = number("risk_aversion", 5.0)
    turnover_penalty = number("turnover_penalty", 0.0)
    max_turnover = number("max_turnover", 1.0)
    if lookback < 2:
        raise ValueError("allocation.lookback must be at least 2")
    if not 2 <= min_observations <= lookback:
        raise ValueError("allocation.min_observations must be between 2 and lookback")
    if not 0 <= shrinkage <= 1:
        raise ValueError("allocation.covariance_shrinkage must be in [0, 1]")
    if risk_aversion <= 0:
        raise ValueError("allocation.risk_aversion must be positive")
    if turnover_penalty < 0:
        raise ValueError("allocation.turnover_penalty must be non-negative")
    if not 0 <= max_turnover <= 2:
        raise ValueError("allocation.max_turnover must be in [0, 2]")
    estimator = value.get("covariance_estimator", "diagonal")
    if estimator not in _COVARIANCE_ESTIMATORS:
        raise ValueError(
            "allocation.covariance_estimator must be diagonal, ledoit_wolf, factor, ewma, "
            "constant_correlation, oas or random_matrix"
        )
    if mode not in _COVARIANCE_MODES and estimator != "diagonal":
        raise ValueError(
            "allocation.covariance_estimator is only used by cost_aware, risk_parity, hrp, "
            "max_sharpe and max_diversification"
        )
    if "ewma_decay" in value and estimator != "ewma":
        raise ValueError("allocation.ewma_decay is only supported for the ewma estimator")
    decay = number("ewma_decay", 0.94)
    if estimator == "ewma" and not 0 < decay < 1:
        raise ValueError("allocation.ewma_decay must be in (0, 1)")
    if "risk_free_rate" in value and mode != "max_sharpe":
        raise ValueError("allocation.risk_free_rate is only supported for max_sharpe")
    risk_free_rate = number("risk_free_rate", 0.0)
    if "max_tracking_error" in value and mode != "cost_aware":
        raise ValueError("allocation.max_tracking_error is only supported for cost_aware")
    raw_tracking_error = value.get("max_tracking_error")
    if raw_tracking_error is None:
        max_tracking_error = None
    else:
        max_tracking_error = number("max_tracking_error", 0.0)
        if max_tracking_error < 0:
            raise ValueError("allocation.max_tracking_error must be non-negative")
    if "evar_beta" in value and mode != "evar":
        raise ValueError("allocation.evar_beta is only supported for evar")
    if "cdar_beta" in value and mode != "cdar":
        raise ValueError("allocation.cdar_beta is only supported for cdar")
    expected_return_model = value.get("expected_return_model", "score")
    if expected_return_model not in {"score", "ic_vol", "black_litterman"}:
        raise ValueError(
            "allocation.expected_return_model must be score, ic_vol or black_litterman"
        )
    if mode != "cost_aware" and (
        expected_return_model != "score"
        or "information_coefficient" in value
        or "black_litterman_tau" in value
    ):
        raise ValueError("expected-return fields are only supported for cost_aware")
    information_coefficient = number("information_coefficient", 0.05)
    tau = number("black_litterman_tau", 0.05)
    if (
        expected_return_model in {"ic_vol", "black_litterman"}
        and not 0 < information_coefficient <= 1
    ):
        raise ValueError("allocation.information_coefficient must be in (0, 1]")
    if expected_return_model == "black_litterman" and not 0 < tau <= 1:
        raise ValueError("allocation.black_litterman_tau must be in (0, 1]")
    if mode != "cvar" and "cvar_beta" in value:
        raise ValueError("allocation.cvar_beta is only supported for cvar")
    cvar_beta = number("cvar_beta", 0.95)
    if mode == "cvar" and not 0 < cvar_beta < 1:
        raise ValueError("allocation.cvar_beta must be in (0, 1)")
    evar_beta = number("evar_beta", 0.95)
    cdar_beta = number("cdar_beta", 0.95)
    if mode == "evar" and not 0 < evar_beta < 1:
        raise ValueError("allocation.evar_beta must be in (0, 1)")
    if mode == "cdar" and not 0 < cdar_beta < 1:
        raise ValueError("allocation.cdar_beta must be in (0, 1)")
    factor_bound_reference = value.get("factor_bound_reference", "absolute")
    if factor_bound_reference not in {"absolute", "active"}:
        raise ValueError("allocation.factor_bound_reference must be absolute or active")
    if mode != "cost_aware" and factor_bound_reference != "absolute":
        raise ValueError("allocation.factor_bound_reference is only supported for cost_aware")
    normalized = {
        "mode": mode,
        "lookback": lookback,
        "min_observations": min_observations,
        "covariance_shrinkage": shrinkage,
        "covariance_estimator": estimator,
        "risk_aversion": risk_aversion,
        "turnover_penalty": turnover_penalty,
        "max_turnover": max_turnover,
        "expected_return_model": expected_return_model,
        "factor_bound_reference": factor_bound_reference,
    }
    # Persist only applicable optional fields. The execution adapter validates this
    # normalized fragment again after serializing its plan.
    if estimator == "ewma":
        normalized["ewma_decay"] = decay
    if mode == "cost_aware":
        normalized.update(
            information_coefficient=information_coefficient,
            black_litterman_tau=tau,
            max_tracking_error=max_tracking_error,
        )
    if mode == "max_sharpe":
        normalized["risk_free_rate"] = risk_free_rate
    if mode in _PATH_MODES:
        normalized[f"{mode}_beta"] = {"cvar": cvar_beta, "evar": evar_beta, "cdar": cdar_beta}[mode]
    return normalized
