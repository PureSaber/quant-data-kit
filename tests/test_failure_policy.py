import pytest

from quant_data_kit.failure_policy import HARD_STOPS, assess_fault, provisional_valuation


@pytest.mark.parametrize("kind", sorted(HARD_STOPS))
def test_hard_stops_cannot_be_downgraded(kind):
    result = assess_fault(kind, template_reviewed=True, candidate_policy="exclude")
    assert result["tier"] == "hard_stop"
    assert result["trade_permission"] == "none"
    assert not result["acceptance_eligible"]


def test_each_recoverable_fault_retains_distinct_permissions():
    for kind in (
        "held_suspended_price",
        "held_price_unknown",
        "entitlement_unresolved",
        "candidate_price_missing",
        "optimizer_search_failed",
        "optimizer_proven_infeasible",
        "rules_unknown",
        "rule_changed",
        "unparsed",
        "template_changed",
    ):
        result = assess_fault(kind)
        assert not result["publish_target"]
        assert not any(result[k] for k in ("gate_a_credit", "gate_b_credit", "gate_c_credit"))
    assert (
        assess_fault("candidate_price_missing", candidate_policy="exclude")["tier"]
        == "candidate_exclusion"
    )
    assert assess_fault("rules_unknown", assets=["A"], scope_known=True)["tier"] == "asset_block"
    assert assess_fault("template_changed", template_reviewed=True)["tier"] == "warning"


def valuation(item):
    return provisional_valuation(
        cash=500,
        quantities={"A": 100},
        marks={"A": 5},
        uncertain={"A": item},
        max_stale_sessions=2,
        constraint_check=lambda w, c: {"cap": w["A"] - 0.55},
    )


def test_suspension_requires_real_bounds_age_and_all_corner_compliance():
    item = {
        "status": "suspended",
        "source": "issuer",
        "evidence_sha256": "a" * 64,
        "stale_sessions": 1,
        "bounds": [4, 5],
        "bounds_basis": "declared external valuation",
    }
    result = valuation(item)
    assert result["nav_interval"] == [900, 1000]
    assert result["analysis_permitted"] and not result["publish_target"]
    assert not result["acceptance_eligible"]
    assert not valuation({**item, "bounds": [4, 7]})["analysis_permitted"]
    for changed in ({"stale_sessions": 3}, {"bounds": None}, {"evidence_sha256": ""}):
        assert valuation({**item, **changed})["nav_interval"] is None
