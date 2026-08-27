"""Point-in-time joins and symbol lifecycle controls.

The helpers in this module make data availability explicit. Event dates describe
when something happened; ``available_at`` describes when a researcher could
first have observed it. Only the latter may be used for a causal join.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd

from quant_data_kit.exceptions import ValidationError


@dataclass(frozen=True)
class TemporalAudit:
    rows: int
    matched_rows: int
    unavailable_rows: int
    stale_rows: int
    max_age_days: float | None


def _normalize_times(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            raise ValidationError(f"Missing temporal column: {column}")
        result[column] = pd.to_datetime(result[column], errors="coerce")
        if result[column].isna().any():
            raise ValidationError(f"Invalid timestamps in column: {column}")
    return result


def point_in_time_join(
    observations: pd.DataFrame,
    facts: pd.DataFrame,
    *,
    observation_time: str = "date",
    available_time: str = "available_at",
    by: Sequence[str] = ("symbol",),
    fact_columns: Sequence[str] | None = None,
    max_age: str | pd.Timedelta | None = None,
    availability_output: str = "source_available_at",
) -> pd.DataFrame:
    """Backward-asof join facts that were available at each observation.

    ``max_age`` is optional and prevents indefinitely carrying stale facts. The
    source availability timestamp is retained so every joined row can be
    audited after feature construction.
    """
    if observations.empty:
        return observations.copy()
    missing_keys = [column for column in by if column not in observations.columns]
    missing_keys += [column for column in by if column not in facts.columns]
    if missing_keys:
        raise ValidationError(f"Missing point-in-time keys: {sorted(set(missing_keys))}")

    left = _normalize_times(observations, [observation_time])
    right = _normalize_times(facts, [available_time])
    right = right.rename(columns={available_time: availability_output})
    selected = (
        list(fact_columns)
        if fact_columns is not None
        else [column for column in right.columns if column not in {*by, availability_output}]
    )
    missing_facts = [column for column in selected if column not in right.columns]
    if missing_facts:
        raise ValidationError(f"Missing fact columns: {missing_facts}")

    tolerance = pd.Timedelta(max_age) if max_age is not None else None
    parts: list[pd.DataFrame] = []
    group_key: str | list[str] = by[0] if len(by) == 1 else list(by)
    for key, left_group in left.groupby(group_key, sort=False, dropna=False):
        key_tuple = (key,) if len(by) == 1 else tuple(key)
        mask = pd.Series(True, index=right.index)
        for column, value in zip(by, key_tuple, strict=True):
            mask &= right[column].eq(value)
        right_group = right.loc[mask, [availability_output, *selected]].sort_values(
            availability_output
        )
        if right_group.empty:
            part = left_group.copy()
            part[availability_output] = pd.NaT
            for column in selected:
                part[column] = pd.NA
        else:
            part = pd.merge_asof(
                left_group.sort_values(observation_time),
                right_group,
                left_on=observation_time,
                right_on=availability_output,
                direction="backward",
                tolerance=tolerance,
                allow_exact_matches=True,
            )
        parts.append(part)

    merged = pd.concat(parts, ignore_index=True)
    if (merged[availability_output] > merged[observation_time]).fillna(False).any():
        raise ValidationError("Point-in-time join produced unavailable future facts")
    return merged.sort_values([observation_time, *by]).reset_index(drop=True)


def audit_point_in_time(
    frame: pd.DataFrame,
    *,
    observation_time: str = "date",
    available_time: str = "source_available_at",
    max_age: str | pd.Timedelta | None = None,
) -> TemporalAudit:
    checked = _normalize_times(frame, [observation_time])
    if available_time not in checked.columns:
        raise ValidationError(f"Missing availability evidence: {available_time}")
    checked[available_time] = pd.to_datetime(checked[available_time], errors="coerce")
    matched = checked[available_time].notna()
    unavailable = matched & (checked[available_time] > checked[observation_time])
    if unavailable.any():
        raise ValidationError(f"Found {int(unavailable.sum())} future-data rows")
    ages = checked[observation_time] - checked[available_time]
    stale = pd.Series(False, index=checked.index)
    max_age_days: float | None = None
    if max_age is not None:
        limit = pd.Timedelta(max_age)
        stale = matched & (ages > limit)
        max_age_days = limit.total_seconds() / 86400
    return TemporalAudit(
        rows=len(checked),
        matched_rows=int(matched.sum()),
        unavailable_rows=int(unavailable.sum()),
        stale_rows=int(stale.sum()),
        max_age_days=max_age_days,
    )


def apply_symbol_lifecycle(
    panel: pd.DataFrame,
    lifecycle: pd.DataFrame,
    *,
    date_col: str = "date",
    symbol_col: str = "symbol",
    listed_col: str = "listed_at",
    delisted_col: str = "delisted_at",
) -> pd.DataFrame:
    """Keep observations inside each symbol's historical listing interval."""
    if lifecycle.empty:
        raise ValidationError("Symbol lifecycle table is empty")
    required = {symbol_col, listed_col, delisted_col}
    missing = sorted(required.difference(lifecycle.columns))
    if missing:
        raise ValidationError(f"Missing lifecycle columns: {missing}")
    result = panel.copy()
    result[date_col] = pd.to_datetime(result[date_col])
    life = lifecycle[[symbol_col, listed_col, delisted_col]].drop_duplicates(symbol_col).copy()
    life[listed_col] = pd.to_datetime(life[listed_col], errors="coerce")
    life[delisted_col] = pd.to_datetime(life[delisted_col], errors="coerce")
    result = result.merge(life, on=symbol_col, how="left", validate="many_to_one")
    active = result[listed_col].notna() & (result[date_col] >= result[listed_col])
    active &= result[delisted_col].isna() | (result[date_col] <= result[delisted_col])
    return result.loc[active].sort_values([date_col, symbol_col]).reset_index(drop=True)
