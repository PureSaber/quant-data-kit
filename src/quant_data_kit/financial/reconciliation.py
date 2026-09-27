"""Immutable source discrepancy decisions and explicit downstream invalidation."""

import hashlib
import json
from pathlib import Path

from .common import number, table, utc

COLUMNS = [
    "observation_id",
    "instrument_id",
    "field",
    "effective_at",
    "available_at",
    "value",
    "unit",
    "currency",
    "basis",
    "source",
    "evidence_id",
]
UNITS = {
    "currency": ("currency", 1),
    "thousand_currency": ("currency", 1000),
    "shares": ("shares", 1),
    "ratio": ("ratio", 1),
    "percent": ("ratio", "0.01"),
}


def canonical(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str
    ).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def validate_observations(frame):
    out = table(
        frame,
        COLUMNS,
        text=tuple(c for c in COLUMNS if c not in {"value", "effective_at", "available_at"}),
        timestamps=("effective_at", "available_at"),
        unique=("observation_id",),
    )
    if not out.unit.isin(UNITS).all():
        raise ValueError("unsupported unit: normalize with explicit evidence before comparison")
    out["value"] = out.value.map(number)
    out["normalized_value"] = [row.value * number(UNITS[row.unit][1]) for row in out.itertuples()]
    out["normalized_unit"] = out.unit.map(lambda x: UNITS[x][0])
    return out


def discrepancies(frame, *, at, tolerance="0"):
    out = validate_observations(frame)
    out = out.loc[out.available_at <= utc(at)]
    tolerance = number(tolerance, nonnegative=True)
    cases = []
    for key, rows in out.groupby(["instrument_id", "field", "effective_at"], sort=True):
        rows = rows.sort_values(["available_at", "observation_id"]).groupby("source").tail(1)
        if len(rows) < 2:
            continue
        semantics = rows[["normalized_unit", "currency", "basis"]].drop_duplicates()
        reason = "incomparable_semantics" if len(semantics) > 1 else "value_disagreement"
        if (
            len(semantics) == 1
            and max(rows.normalized_value) - min(rows.normalized_value) <= tolerance
        ):
            continue
        observations = rows.sort_values("observation_id")[COLUMNS].to_dict("records")
        case = {
            "instrument_id": key[0],
            "field": key[1],
            "effective_at": str(key[2]),
            "reason": reason,
            "observations": observations,
        }
        case["case_id"] = digest(case)
        cases.append(case)
    return cases


def impacted_nodes(changed_observation_ids, dependencies):
    """dependencies: artifact/node -> direct input ids. Cycles terminate safely."""
    affected = set(changed_observation_ids)
    while True:
        extra = {node for node, inputs in dependencies.items() if set(inputs) & affected}
        if extra <= affected:
            break
        affected |= extra
    return sorted(affected - set(changed_observation_ids))


def adjudicate(
    case,
    *,
    selected_observation_id,
    resolved_at,
    reviewer,
    rationale,
    evidence_uri,
    prior_snapshot,
    dependencies,
):
    original = {k: v for k, v in case.items() if k != "case_id"}
    if digest(original) != case.get("case_id"):
        raise ValueError("case content hash mismatch")
    if not all(
        isinstance(x, str) and x.strip()
        for x in (reviewer, rationale, evidence_uri, prior_snapshot)
    ):
        raise ValueError("adjudication requires reviewer, rationale, evidence and parent snapshot")
    at = utc(resolved_at)
    ids = [x["observation_id"] for x in case["observations"]]
    if selected_observation_id not in ids:
        raise ValueError("decision must select an evidenced observation, not invent a replacement")
    if any(utc(x["available_at"]) > at for x in case["observations"]):
        raise ValueError("cannot adjudicate observations before they were known")
    decision = {
        "schema": "qdk.source-adjudication/1",
        "case": case,
        "selected_observation_id": selected_observation_id,
        "resolved_at": str(at),
        "reviewer": reviewer,
        "rationale": rationale,
        "evidence_uri": evidence_uri,
        "prior_snapshot": prior_snapshot,
        "impacted": impacted_nodes(ids, dependencies),
    }
    decision["snapshot_id"] = digest(decision)
    return decision


def publish_decision(decision, destination):
    """Exclusive create; never mutate the parent snapshot or existing resolution."""
    expected = digest({k: v for k, v in decision.items() if k != "snapshot_id"})
    if decision.get("snapshot_id") != expected:
        raise ValueError("decision hash mismatch")
    destination = Path(destination)
    with destination.open("xb") as stream:
        stream.write(canonical(decision))
    return destination


def freeze_snapshot(records, *, parent_snapshot=None, decision_id=None):
    """A governed observation snapshot: exactly one chosen fact per economic key."""
    import pandas as pd

    frame = validate_observations(pd.DataFrame(records))
    if frame.duplicated(["instrument_id", "field", "effective_at"]).any():
        raise ValueError("snapshot must contain one selected observation per economic key")
    snapshot = {
        "schema": "qdk.resolved-observations/1",
        "parent_snapshot": parent_snapshot,
        "decision_id": decision_id,
        "records": frame[COLUMNS].sort_values("observation_id").to_dict("records"),
    }
    snapshot["snapshot_id"] = digest(snapshot)
    return snapshot


def apply_decision_snapshot(parent, decision, destination):
    """Create a new data snapshot, retaining the immutable parent and knowledge boundary.

    Rerunning downstream artifacts is a separate explicit operation, never implied
    by publishing a source adjudication. Revised facts become known at resolution.
    """
    for artifact in (parent, decision):
        if digest({k: v for k, v in artifact.items() if k != "snapshot_id"}) != artifact.get(
            "snapshot_id"
        ):
            raise ValueError("snapshot/decision hash mismatch")
    if parent.get("schema") != "qdk.resolved-observations/1":
        raise ValueError("unsupported parent snapshot")
    if decision["prior_snapshot"] != parent["snapshot_id"]:
        raise ValueError("decision targets a different parent snapshot")
    case = decision["case"]
    if digest({k: v for k, v in case.items() if k != "case_id"}) != case["case_id"]:
        raise ValueError("case hash mismatch")
    selected = dict(
        next(
            x
            for x in case["observations"]
            if x["observation_id"] == decision["selected_observation_id"]
        )
    )
    key = lambda row: (row["instrument_id"], row["field"], utc(row["effective_at"]))
    matches = [row for row in parent["records"] if key(row) == key(selected)]
    if len(matches) != 1 or matches[0]["observation_id"] not in {
        x["observation_id"] for x in case["observations"]
    }:
        raise ValueError("parent observation does not match the adjudicated case")
    resolved = utc(decision["resolved_at"])
    if resolved < max(utc(x["available_at"]) for x in case["observations"]):
        raise ValueError("resolution precedes source knowledge")
    selected["available_at"] = str(resolved)
    selected["observation_id"] += ":resolution:" + decision["snapshot_id"]
    selected["evidence_id"] += ":decision:" + decision["snapshot_id"]
    records = [row for row in parent["records"] if key(row) != key(selected)] + [selected]
    result = freeze_snapshot(
        records, parent_snapshot=parent["snapshot_id"], decision_id=decision["snapshot_id"]
    )
    publish_decision(result, destination)
    return result
