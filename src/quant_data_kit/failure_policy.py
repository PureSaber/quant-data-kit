"""Evidence labels and execution permissions are separate, fail-closed decisions."""

from itertools import product
from math import isfinite

HARD_STOPS = {
    "identity_changed",
    "history_revised",
    "future_information",
    "ledger_mismatch",
    "runtime_changed",
    "second_writer",
}


class EvidenceFault(ValueError):
    """Carry a typed stop through native workflows and research failure receipts."""

    def __init__(self, kind, message, **context):
        self.failure_assessment = assess_fault(kind, **context)
        self.diagnostics = {"failure_assessment": self.failure_assessment}
        super().__init__(message)


def assess_fault(
    kind,
    *,
    assets=(),
    candidate_policy=None,
    holding_review=None,
    template_reviewed=False,
    scope_known=False,
):
    report = {
        "schema": "qdk.failure-assessment/v1",
        "kind": kind,
        "assets": list(assets),
        "tier": "account_block",
        "evidence_status": "unknown",
        "nav_status": "unknown",
        "weights_status": "unknown",
        "risk_status": "unknown",
        "publish_target": False,
        "trade_permission": "none",
        "acceptance_eligible": False,
        "gate_a_credit": False,
        "gate_b_credit": False,
        "gate_c_credit": False,
    }
    if kind in HARD_STOPS:
        report["tier"] = "hard_stop"
    elif kind in {"held_suspended_price", "entitlement_unresolved"}:
        report.update(
            evidence_status="provisional",
            nav_status="provisional",
            weights_status="provisional",
            risk_status="provisional",
        )
    elif kind == "candidate_price_missing":
        if candidate_policy not in {None, "exclude", "block"}:
            raise ValueError("candidate missing-price policy must be preregistered")
        if candidate_policy == "exclude":
            report.update(tier="candidate_exclusion", trade_permission="unaffected_assets_only")
        report.update(nav_status="validated", weights_status="validated", risk_status="unknown")
    elif kind in {
        "optimizer_search_failed",
        "optimizer_infeasible_reported",
        "optimizer_proven_infeasible",
    }:
        report.update(nav_status="validated", weights_status="validated")
        if holding_review and holding_review.get("hold_permitted") is True:
            report.update(tier="hold_only", risk_status="validated", evidence_status="validated")
        report["holding_review"] = holding_review
    elif kind == "template_changed" and template_reviewed is True:
        report.update(
            tier="warning",
            evidence_status="validated",
            nav_status="validated",
            weights_status="validated",
            risk_status="validated",
            trade_permission="subject_to_normal_checks",
            acceptance_eligible=True,
        )
        # A warning never manufactures native acceptance credit.
    elif kind in {"rules_unknown", "rule_changed", "unparsed"}:
        report["tier"] = "asset_block" if scope_known and assets else "account_block"
        report["trade_permission"] = (
            "unaffected_assets_only" if report["tier"] == "asset_block" else "none"
        )
    elif kind not in {"held_price_unknown", "template_changed"}:
        raise ValueError(f"unknown failure kind: {kind}")
    return report


def provisional_valuation(
    *, cash, quantities, marks, uncertain, max_stale_sessions, constraint_check
):
    """Bounded analysis, never a writable ledger or an accepted return observation.

    Each uncertain mark requires source evidence, positive bounds and a known
    suspension. All corner portfolios must pass the consumer's convex constraint
    checker. Unknown constraint families must return None. No synthetic bars.
    """
    report = assess_fault("held_suspended_price", assets=uncertain)
    report.update(nav=None, nav_interval=None, weight_intervals=None, analysis_permitted=False)
    if not isfinite(cash) or cash < 0 or not uncertain or len(uncertain) > 8:
        return report
    if type(max_stale_sessions) is not int or max_stale_sessions < 0:
        raise ValueError("declare a nonnegative maximum stale age")
    if any(not isfinite(q) or q < 0 for q in quantities.values()):
        raise ValueError("analysis supports nonnegative finite holdings only")
    for asset, item in uncertain.items():
        if asset not in quantities or not quantities[asset]:
            raise ValueError("uncertainty must identify a held asset")
        if (
            not item.get("evidence_sha256")
            or len(item["evidence_sha256"]) != 64
            or any(c not in "0123456789abcdef" for c in item["evidence_sha256"])
            or not item.get("source")
            or item.get("status") != "suspended"
            or type(item.get("stale_sessions")) is not int
            or not 0 <= item["stale_sessions"] <= max_stale_sessions
        ):
            return report
        bounds = item.get("bounds")
        if (
            not isinstance(bounds, (list, tuple))
            or len(bounds) != 2
            or any(type(v) not in {float, int} or not isfinite(v) for v in bounds)
            or not 0 < bounds[0] <= bounds[1]
            or not item.get("bounds_basis")
        ):
            return report
    if any(
        a not in marks or not isfinite(marks[a]) or marks[a] <= 0
        for a, q in quantities.items()
        if q and a not in uncertain
    ):
        return assess_fault("held_price_unknown", assets=set(quantities) - set(marks))
    scenarios = []
    for corner in product(*(item["bounds"] for item in uncertain.values())):
        prices = {**marks, **dict(zip(uncertain, corner))}
        nav = cash + sum(q * prices[a] for a, q in quantities.items() if q)
        if nav <= 0:
            return report
        weights = {a: q * prices[a] / nav for a, q in quantities.items() if q}
        residuals = constraint_check(weights, cash / nav)
        passed = (
            isinstance(residuals, dict)
            and bool(residuals)
            and all(
                type(v) in {float, int} and isfinite(v) and v <= 1e-7 for v in residuals.values()
            )
        )
        scenarios.append({"nav": nav, "weights": weights, "passed": passed, "residuals": residuals})
    report.update(
        nav_interval=[min(s["nav"] for s in scenarios), max(s["nav"] for s in scenarios)],
        weight_intervals={
            a: [min(s["weights"][a] for s in scenarios), max(s["weights"][a] for s in scenarios)]
            for a, q in quantities.items()
            if q
        },
        scenarios=scenarios,
        analysis_permitted=all(s["passed"] for s in scenarios),
    )
    return report
