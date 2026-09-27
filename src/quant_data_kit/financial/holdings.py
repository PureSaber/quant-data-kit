"""Disclosure-aware fund/ETF look-through, retaining every unknown weight."""

from decimal import Decimal

import pandas as pd

from .common import number, table, utc

COLUMNS = [
    "disclosure_id",
    "fund_id",
    "holding_date",
    "available_at",
    "instrument_id",
    "asset_type",
    "currency",
    "weight",
    "source",
    "evidence_id",
]
ONE = Decimal(1)


def validate_holdings(frame):
    out = table(
        frame,
        COLUMNS,
        text=(
            "disclosure_id",
            "fund_id",
            "instrument_id",
            "asset_type",
            "currency",
            "source",
            "evidence_id",
        ),
        dates=("holding_date",),
        timestamps=("available_at",),
        unique=("disclosure_id", "instrument_id"),
    )
    if not out.asset_type.isin(["fund", "security", "cash"]).all():
        raise ValueError("unsupported holding asset type")
    if not out.currency.str.fullmatch(r"[A-Z]{3}").all():
        raise ValueError("holding currency must be explicit")
    for row in out.itertuples():
        if row.asset_type == "cash" and row.instrument_id != f"CASH:{row.currency}":
            raise ValueError("cash identity must be CASH:<currency>")
        if row.asset_type != "cash" and row.instrument_id.startswith("CASH:"):
            raise ValueError("cash identity cannot label a security or fund")
    if out.groupby("instrument_id")[["currency", "asset_type"]].nunique().gt(1).any().any():
        raise ValueError("stable holding identity has conflicting currency or asset type")
    out["weight"] = out.weight.map(lambda x: number(x, nonnegative=True))
    if (out.holding_date > out.available_at.dt.tz_localize(None).dt.normalize()).any():
        raise ValueError("holdings cannot be disclosed before their holding date")
    for _, rows in out.groupby("disclosure_id"):
        if any(rows[x].nunique() != 1 for x in ("fund_id", "holding_date", "available_at")):
            raise ValueError("one disclosure must be one complete dated snapshot")
        if sum(rows.weight, Decimal(0)) > ONE:
            raise ValueError("long-only disclosed weights exceed 100%; leverage is unsupported")
    snapshots = out[["disclosure_id", "fund_id", "holding_date", "available_at"]].drop_duplicates()
    if snapshots.duplicated(["fund_id", "holding_date", "available_at"]).any():
        raise ValueError("ambiguous fund disclosure; reconcile sources first")
    return out


def holdings_asof(frame, at):
    out = validate_holdings(frame)
    visible = out.loc[out.available_at <= utc(at)]
    snapshots = visible[["disclosure_id", "fund_id", "holding_date", "available_at"]]
    ids = (
        snapshots.drop_duplicates()
        .sort_values(["holding_date", "available_at"])
        .groupby("fund_id")
        .tail(1)
        .disclosure_id
    )
    return visible.loc[visible.disclosure_id.isin(ids)].reset_index(drop=True)


def look_through(frame, allocations, at, *, max_depth=8, max_age_days=180):
    """Return weighted leaves with paths; sum(weights) equals supplied allocations.

    Allocations are fund -> portfolio WEIGHT, not amounts in different currencies.
    Missing, stale, cyclic, depth-limited and undisclosed portions stay UNKNOWN.
    Shares may be retained as optional source columns, never silently treated as weights.
    """
    if (
        type(max_depth) is not int
        or max_depth < 1
        or type(max_age_days) is not int
        or max_age_days < 0
    ):
        raise ValueError("positive max_depth and nonnegative max_age_days required")
    allocations = {key: number(value, nonnegative=True) for key, value in allocations.items()}
    if sum(allocations.values(), Decimal(0)) > ONE:
        raise ValueError("portfolio allocations exceed 100%")
    visible = holdings_asof(frame, at)
    groups = {key: value for key, value in visible.groupby("fund_id")}
    leaves = []

    def leaf(instrument, weight, currency, path, reason, holding_date=None, evidence=None):
        if weight:
            leaves.append(
                {
                    "instrument_id": instrument,
                    "weight": weight,
                    "currency": currency,
                    "path": tuple(path),
                    "reason": reason,
                    "holding_date": holding_date,
                    "evidence_id": evidence,
                    "known": reason == "disclosed",
                }
            )

    def visit(fund, weight, path):
        reason = None
        if fund in path:
            reason = "cycle"
        elif len(path) >= max_depth:
            reason = "depth_limit"
        elif fund not in groups:
            reason = "missing_disclosure"
        if reason:
            leaf(f"UNKNOWN:{fund}", weight, None, [*path, fund], reason)
            return
        rows = groups[fund]
        held = rows.iloc[0].holding_date
        if (utc(at).tz_localize(None).normalize() - held).days > max_age_days:
            leaf(f"UNKNOWN:{fund}", weight, None, [*path, fund], "stale", held)
            return
        for row in rows.itertuples():
            child_weight = weight * row.weight
            if row.asset_type == "fund":
                visit(row.instrument_id, child_weight, [*path, fund])
            else:
                leaf(
                    row.instrument_id,
                    child_weight,
                    row.currency,
                    [*path, fund],
                    "disclosed",
                    held,
                    row.evidence_id,
                )
        uncovered = weight * (ONE - sum(rows.weight, Decimal(0)))
        leaf(f"UNKNOWN:{fund}", uncovered, None, [*path, fund], "undisclosed", held)

    for fund, weight in sorted(allocations.items()):
        visit(fund, weight, [])
    result = pd.DataFrame(
        leaves,
        columns=[
            "instrument_id",
            "weight",
            "currency",
            "path",
            "reason",
            "holding_date",
            "evidence_id",
            "known",
        ],
    )
    if sum(result.weight, Decimal(0)) != sum(allocations.values(), Decimal(0)):
        raise ArithmeticError("look-through failed portfolio weight conservation")
    return result


def exposure_summary(leaves):
    """Aggregate stable security identity without conflating currencies or unknowns."""
    if leaves.empty:
        return {"known_weight": Decimal(0), "unknown_weight": Decimal(0), "exposures": []}
    exposures = (
        leaves.loc[leaves.known].groupby(["instrument_id", "currency"], sort=True).weight.sum()
    )
    return {
        "known_weight": sum(leaves.loc[leaves.known, "weight"], Decimal(0)),
        "unknown_weight": sum(leaves.loc[~leaves.known, "weight"], Decimal(0)),
        "exposures": [
            {"instrument_id": key[0], "currency": key[1], "weight": value}
            for key, value in exposures.items()
        ],
    }
