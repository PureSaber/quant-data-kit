"""Side-specific, expiring status evidence; absence is UNKNOWN, not permission."""

from dataclasses import dataclass

from .common import table, utc

COLUMNS = [
    "instrument_id",
    "effective_from",
    "effective_to",
    "available_at",
    "buy_status",
    "sell_status",
    "reason",
    "source",
    "evidence_id",
]
STATES = {"tradable", "blocked", "unknown"}


def validate_status(frame):
    out = table(
        frame,
        COLUMNS,
        text=("instrument_id", "reason", "source", "evidence_id"),
        timestamps=("effective_from", "effective_to", "available_at"),
        unique=("instrument_id", "effective_from", "available_at", "source"),
    )
    if (out.effective_to <= out.effective_from).any():
        raise ValueError("status evidence requires a bounded positive validity interval")
    if not out.buy_status.isin(STATES).all() or not out.sell_status.isin(STATES).all():
        raise ValueError("status must be tradable, blocked or unknown")
    # A statement that no restriction was found is not positive trading evidence.
    if (
        out.reason.eq("no_restriction")
        & (out.buy_status.eq("tradable") | out.sell_status.eq("tradable"))
    ).any():
        raise ValueError("no_restriction is not affirmative tradability evidence")
    return out


@dataclass(frozen=True)
class TradingPermission:
    buy: str
    sell: str
    reason: str
    evidence_ids: tuple[str, ...] = ()


def permission_asof(frame, instrument_id, effective_at, known_at):
    out = validate_status(frame)
    at, known = utc(effective_at), utc(known_at)
    rows = out.loc[
        out.instrument_id.eq(instrument_id)
        & (out.effective_from <= at)
        & (at < out.effective_to)
        & (out.available_at <= known)
    ]
    if rows.empty:
        return TradingPermission("unknown", "unknown", "missing_or_expired_evidence")
    # Each source can correct itself; disagreement across sources remains unknown.
    rows = rows.sort_values(["available_at", "effective_from"]).groupby("source").tail(1)
    evidence = tuple(sorted(rows.evidence_id))
    if rows[["buy_status", "sell_status"]].drop_duplicates().shape[0] != 1:
        return TradingPermission("unknown", "unknown", "conflicting_sources", evidence)
    row = rows.iloc[0]
    return TradingPermission(
        row.buy_status, row.sell_status, "|".join(sorted(set(rows.reason))), evidence
    )
