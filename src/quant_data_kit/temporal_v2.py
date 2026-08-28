"""Strict UTC and bitemporal validation for schema v2."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from quant_data_kit.exceptions import ValidationError

_UTC_ZONE_NAMES = {"UTC", "Etc/UTC", "GMT", "Etc/GMT", "Z"}


def ensure_utc_datetime(value: datetime | pd.Timestamp, *, field: str) -> datetime:
    """Return a normalized UTC datetime and reject naive or non-UTC zones."""
    timestamp = value.to_pydatetime() if isinstance(value, pd.Timestamp) else value
    if not isinstance(timestamp, datetime):
        raise ValidationError(f"{field} must be a datetime")
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValidationError(f"{field} must be timezone-aware UTC")
    zone_name = getattr(timestamp.tzinfo, "key", None) or getattr(
        timestamp.tzinfo, "zone", None
    )
    if zone_name is not None:
        if str(zone_name) not in _UTC_ZONE_NAMES:
            raise ValidationError(f"{field} must use UTC, got {zone_name}")
    elif timestamp.utcoffset().total_seconds() != 0:
        raise ValidationError(f"{field} must use UTC")
    return timestamp.astimezone(timezone.utc)


def _utc_series(frame: pd.DataFrame, column: str, *, nullable: bool = False) -> pd.Series:
    if column not in frame.columns:
        raise ValidationError(f"Missing temporal column: {column}")
    values: list[pd.Timestamp | pd.NaT] = []
    for index, value in frame[column].items():
        if pd.isna(value):
            if nullable:
                values.append(pd.NaT)
                continue
            raise ValidationError(f"{column} contains null at index {index}")
        values.append(pd.Timestamp(ensure_utc_datetime(value, field=f"{column}[{index}]")))
    return pd.Series(values, index=frame.index, dtype="datetime64[ns, UTC]")


@dataclass(frozen=True)
class BitemporalAudit:
    rows: int
    ambiguous_pairs: int


def validate_bitemporal_frame(
    frame: pd.DataFrame,
    *,
    key_columns: Sequence[str],
    effective_from: str = "effective_from",
    effective_to: str = "effective_to",
    available_at: str = "available_at",
    superseded_at: str = "superseded_at",
) -> BitemporalAudit:
    """Validate business-time and knowledge-time intervals without guessing."""
    missing = [column for column in key_columns if column not in frame.columns]
    if missing:
        raise ValidationError(f"Missing bitemporal keys: {missing}")
    work = frame.copy()
    work[effective_from] = _utc_series(work, effective_from)
    work[effective_to] = _utc_series(work, effective_to, nullable=True)
    work[available_at] = _utc_series(work, available_at)
    work[superseded_at] = _utc_series(work, superseded_at, nullable=True)
    invalid_effective = work[effective_to].notna() & (
        work[effective_to] <= work[effective_from]
    )
    if invalid_effective.any():
        raise ValidationError("effective_to must be later than effective_from")
    invalid_knowledge = work[superseded_at].notna() & (
        work[superseded_at] <= work[available_at]
    )
    if invalid_knowledge.any():
        raise ValidationError("superseded_at must be later than available_at")

    far_future = pd.Timestamp.max.tz_localize("UTC")
    ambiguous = 0
    group_key: str | list[str] = (
        key_columns[0] if len(key_columns) == 1 else list(key_columns)
    )
    for _, group in work.groupby(group_key, dropna=False, sort=False):
        rows = list(group.itertuples(index=False))
        names = {name: position for position, name in enumerate(work.columns)}
        for left_index, left in enumerate(rows):
            for right in rows[left_index + 1 :]:
                left_effective_end = left[names[effective_to]]
                right_effective_end = right[names[effective_to]]
                left_knowledge_end = left[names[superseded_at]]
                right_knowledge_end = right[names[superseded_at]]
                business_overlap = max(
                    left[names[effective_from]], right[names[effective_from]]
                ) < min(
                    left_effective_end if pd.notna(left_effective_end) else far_future,
                    right_effective_end if pd.notna(right_effective_end) else far_future,
                )
                knowledge_overlap = max(
                    left[names[available_at]], right[names[available_at]]
                ) < min(
                    left_knowledge_end if pd.notna(left_knowledge_end) else far_future,
                    right_knowledge_end if pd.notna(right_knowledge_end) else far_future,
                )
                if business_overlap and knowledge_overlap:
                    ambiguous += 1
    if ambiguous:
        raise ValidationError(f"Found {ambiguous} ambiguous bitemporal record pairs")
    return BitemporalAudit(rows=len(work), ambiguous_pairs=0)


def point_in_time_join_bitemporal(
    observations: pd.DataFrame,
    facts: pd.DataFrame,
    *,
    observation_time: str = "observation_time",
    as_of_time: str = "as_of",
    by: Sequence[str] = ("instrument_id",),
    effective_from: str = "effective_from",
    effective_to: str = "effective_to",
    available_at: str = "available_at",
    superseded_at: str = "superseded_at",
    fact_columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Join the unique fact valid in both business and knowledge time."""
    if observations.empty:
        return observations.copy()
    missing = [column for column in by if column not in observations.columns]
    missing += [column for column in by if column not in facts.columns]
    if missing:
        raise ValidationError(f"Missing point-in-time keys: {sorted(set(missing))}")
    validate_bitemporal_frame(
        facts,
        key_columns=by,
        effective_from=effective_from,
        effective_to=effective_to,
        available_at=available_at,
        superseded_at=superseded_at,
    )
    left = observations.copy()
    right = facts.copy()
    left[observation_time] = _utc_series(left, observation_time)
    left[as_of_time] = _utc_series(left, as_of_time)
    right[effective_from] = _utc_series(right, effective_from)
    right[effective_to] = _utc_series(right, effective_to, nullable=True)
    right[available_at] = _utc_series(right, available_at)
    right[superseded_at] = _utc_series(right, superseded_at, nullable=True)
    selected = list(fact_columns) if fact_columns is not None else [
        column
        for column in right.columns
        if column not in {*by, effective_from, effective_to, available_at, superseded_at}
    ]
    missing_facts = [column for column in selected if column not in right.columns]
    if missing_facts:
        raise ValidationError(f"Missing fact columns: {missing_facts}")

    output_rows: list[dict] = []
    for _, observation in left.iterrows():
        mask = pd.Series(True, index=right.index)
        for column in by:
            mask &= right[column].eq(observation[column])
        mask &= right[effective_from].le(observation[observation_time])
        mask &= right[effective_to].isna() | right[effective_to].gt(
            observation[observation_time]
        )
        mask &= right[available_at].le(observation[as_of_time])
        mask &= right[superseded_at].isna() | right[superseded_at].gt(
            observation[as_of_time]
        )
        matches = right.loc[mask]
        if len(matches) > 1:
            raise ValidationError("Point-in-time join found ambiguous facts")
        row = observation.to_dict()
        if matches.empty:
            row.update({column: pd.NA for column in selected})
            row["source_effective_from"] = pd.NaT
            row["source_available_at"] = pd.NaT
        else:
            match = matches.iloc[0]
            row.update({column: match[column] for column in selected})
            row["source_effective_from"] = match[effective_from]
            row["source_available_at"] = match[available_at]
        output_rows.append(row)
    return pd.DataFrame(output_rows)
