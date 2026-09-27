"""Stable security identities and knowledge-time lifecycle/universe selection.

Companion to instrument_master's evidence-bound rule catalog, not a replacement
for that catalog. Ticker changes never change instrument_id. Universe exits do
not imply delisting and holdings remain valuation/exit obligations.
"""

from dataclasses import dataclass

import pandas as pd

from .common import table, utc

KINDS = {"listing", "delisting", "symbol_change", "venue_change", "merger", "entry", "exit"}
COLUMNS = [
    "event_id",
    "instrument_id",
    "kind",
    "effective_at",
    "available_at",
    "source",
    "evidence_id",
    "symbol",
    "venue",
    "universe_id",
    "successor_id",
]


def validate_lifecycle(events: pd.DataFrame) -> pd.DataFrame:
    out = table(
        events,
        COLUMNS,
        text=("event_id", "instrument_id", "kind", "source", "evidence_id"),
        timestamps=("effective_at", "available_at"),
        unique=("event_id",),
    )
    if not out.kind.isin(KINDS).all():
        raise ValueError("unsupported lifecycle event")
    for row in out.itertuples():
        required = {
            "listing": ("symbol", "venue"),
            "symbol_change": ("symbol",),
            "venue_change": ("venue",),
            "entry": ("universe_id",),
            "exit": ("universe_id",),
            "merger": ("successor_id",),
        }.get(row.kind, ())
        if any(
            not isinstance(getattr(row, k), str) or not getattr(row, k).strip() for k in required
        ):
            raise ValueError(f"{row.kind} requires {required}")
        if row.kind == "merger" and row.successor_id == row.instrument_id:
            raise ValueError("merger successor must be a different stable identity")
    keys = ["instrument_id", "kind", "effective_at", "available_at", "universe_id"]
    if out.duplicated(keys).any():
        raise ValueError("ambiguous lifecycle version")
    return out


def lifecycle_asof(events, effective_at, known_at) -> pd.DataFrame:
    out = validate_lifecycle(events)
    visible = out.loc[(out.effective_at <= utc(effective_at)) & (out.available_at <= utc(known_at))]
    # A later correction supersedes the same economic event only after publication.
    visible = visible.sort_values(["effective_at", "available_at", "event_id"])
    visible = visible.drop_duplicates(
        ["instrument_id", "kind", "effective_at", "universe_id"], keep="last"
    )
    for _, simultaneous in visible.groupby(["instrument_id", "effective_at"]):
        kinds = set(simultaneous.kind)
        if "listing" in kinds and kinds & {"delisting", "merger"}:
            raise ValueError("conflicting simultaneous lifecycle states")
        for _, membership in simultaneous.groupby("universe_id"):
            if {"entry", "exit"} <= set(membership.kind):
                raise ValueError("conflicting simultaneous membership states")
    records = []
    for instrument, rows in visible.groupby("instrument_id", sort=True):
        state = {
            "instrument_id": instrument,
            "listed": None,
            "symbol": None,
            "venue": None,
            "successor_id": None,
            "memberships": set(),
            "evidence_ids": [],
        }
        for row in rows.itertuples():
            state["evidence_ids"].append(row.evidence_id)
            if row.kind == "listing":
                state.update(listed=True, symbol=row.symbol, venue=row.venue)
            elif row.kind in {"delisting", "merger"}:
                state["listed"] = False
                if row.kind == "merger":
                    state["successor_id"] = row.successor_id
            elif row.kind == "symbol_change":
                state["symbol"] = row.symbol
            elif row.kind == "venue_change":
                state["venue"] = row.venue
            elif row.kind == "entry":
                state["memberships"].add(row.universe_id)
            elif row.kind == "exit":
                state["memberships"].discard(row.universe_id)
        state["memberships"] = tuple(sorted(state["memberships"]))
        state["evidence_ids"] = tuple(state["evidence_ids"])
        records.append(state)
    result = pd.DataFrame(
        records,
        columns=[
            "instrument_id",
            "listed",
            "symbol",
            "venue",
            "successor_id",
            "memberships",
            "evidence_ids",
        ],
    )
    if result.loc[result.listed.eq(True)].duplicated(["symbol", "venue"]).any():
        raise ValueError("active ticker/venue maps to multiple stable identities")
    return result


@dataclass(frozen=True)
class UniverseSelection:
    eligible: frozenset[str]
    retained_holdings: frozenset[str]
    unknown_holdings: frozenset[str]


def select_universe(events, effective_at, known_at, *, universe_id=None, holdings=()):
    states = lifecycle_asof(events, effective_at, known_at)
    eligible = frozenset(
        row.instrument_id
        for row in states.itertuples()
        if row.listed is True and (universe_id is None or universe_id in row.memberships)
    )
    held = frozenset(holdings)
    known = set(states.loc[states.listed.notna(), "instrument_id"])
    return UniverseSelection(eligible, held - eligible, held - known)
