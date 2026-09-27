"""Membership events selected by knowledge time and effective session."""

import pandas as pd

from .prices import utc


def validate_membership(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"instrument_id", "effective_session", "known_at", "member", "source", "event_id"}
    if frame.empty or not required.issubset(frame):
        raise ValueError("nonempty historical membership events required")
    result = frame.copy()
    for field in ("instrument_id", "source", "event_id"):
        if result[field].isna().any() or result[field].astype(str).str.strip().eq("").any():
            raise ValueError(f"membership has empty {field}")
    if result.event_id.duplicated().any():
        raise ValueError("duplicate membership event")
    result["effective_session"] = pd.to_datetime(result.effective_session)
    result["known_at"] = result.known_at.map(lambda value: utc(value, "known_at"))
    if not result.member.isin([True, False, 0, 1]).all():
        raise ValueError("membership must be boolean")
    result["member"] = result.member.astype(bool)
    if result.duplicated(["instrument_id", "effective_session", "known_at"]).any():
        raise ValueError("ambiguous membership events")
    return result


def members_asof(events: pd.DataFrame, session: str, at) -> set[str]:
    visible = events.loc[
        (events.known_at <= utc(at)) & (events.effective_session <= pd.Timestamp(session))
    ]
    latest = (
        visible.sort_values(["effective_session", "known_at", "event_id"])
        .groupby("instrument_id", sort=False)
        .tail(1)
    )
    return set(latest.loc[latest.member, "instrument_id"])
