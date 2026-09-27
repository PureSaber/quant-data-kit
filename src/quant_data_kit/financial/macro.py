"""Macro observations with vintage dates AND intraday release evidence."""

import pandas as pd

from .common import number, table, utc

COLUMNS = [
    "series_id",
    "observation_date",
    "vintage_date",
    "released_at",
    "available_at",
    "value",
    "unit",
    "source",
    "evidence_id",
]


def validate_macro(frame):
    out = table(
        frame,
        COLUMNS,
        text=("series_id", "unit", "source", "evidence_id"),
        timestamps=("released_at", "available_at"),
        dates=("observation_date", "vintage_date"),
        unique=("series_id", "observation_date", "available_at"),
    )
    if (out.released_at > out.available_at).any():
        raise ValueError("macro value cannot precede release")
    release_dates = (
        out.released_at.dt.tz_convert("America/New_York").dt.tz_localize(None).dt.normalize()
    )
    if (out.vintage_date > release_dates).any():
        raise ValueError("macro vintage cannot be visible before its publication date")
    if (out.observation_date > out.released_at.dt.tz_localize(None).dt.normalize()).any():
        raise ValueError("macro observation cannot be in the future")
    # One series keeps one unit; transformations must get new series identities.
    if out.groupby("series_id").unit.nunique().gt(1).any():
        raise ValueError("macro unit drift requires a new series identity")
    out["value"] = out.value.map(lambda x: None if pd.isna(x) or x == "." else number(x))
    return out


def macro_asof(frame, at, *, latest_observation=False):
    out = validate_macro(frame)
    out = out.loc[out.available_at <= utc(at)].sort_values(
        ["series_id", "observation_date", "available_at"]
    )
    out = out.drop_duplicates(["series_id", "observation_date"], keep="last")
    if latest_observation:
        out = out.groupby("series_id", sort=True).tail(1)
    return out.reset_index(drop=True)


def from_alfred(observations, *, series_id, unit, release_times, source):
    """Parse archived FRED JSON; release_times maps vintage DATE to evidenced UTC time.

    FRED realtime_start is date precision, not an 08:30 release timestamp. Missing
    release evidence fails closed instead of making a revision visible at midnight.
    API fetching/keys stay with the caller; missing '.' values remain missing.
    """
    rows = []
    for i, obs in enumerate(observations):
        vintage = obs["realtime_start"]
        if vintage not in release_times:
            raise ValueError(f"missing evidenced release timestamp for vintage {vintage}")
        released = utc(release_times[vintage])
        if released.tz_convert("America/New_York").date().isoformat() != vintage:
            raise ValueError("release timestamp and ALFRED vintage date disagree")
        rows.append(
            {
                "series_id": series_id,
                "observation_date": obs["date"],
                "vintage_date": vintage,
                "released_at": released,
                "available_at": released,
                "value": obs["value"],
                "unit": unit,
                "source": source,
                "evidence_id": f"{series_id}:{vintage}:{i}",
            }
        )
    return validate_macro(pd.DataFrame(rows, columns=COLUMNS))
