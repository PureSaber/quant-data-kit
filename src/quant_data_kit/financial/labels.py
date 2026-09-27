"""Selection-preserving labels: missing outcomes are evidence, never vanished rows.

Levels must be explicitly declared economic-return levels. Terminal observations
are per-selection holding-period returns with supplied lineage, NOT assumed zeros.
Market adapters must convert terminal cash with the same share/action basis.
"""

import numpy as np
import pandas as pd

from .common import utc


def forward_labels(
    levels,
    selections,
    sessions,
    *,
    horizon,
    as_of,
    terminals=None,
    lower_return=None,
    upper_return=None,
):
    required = {"date", "instrument_id", "value", "available_at"}
    if not required <= set(levels) or not {"sample_id", "date", "instrument_id"} <= set(selections):
        raise ValueError("dated economic levels and explicit selected sample IDs required")
    grid = pd.DatetimeIndex(sessions)
    if (
        type(horizon) is not int
        or horizon < 1
        or grid.hasnans
        or not grid.is_unique
        or not grid.is_monotonic_increasing
        or grid.tz is not None
        or not grid.equals(grid.normalize())
    ):
        raise ValueError(
            "positive horizon and explicit ordered plain-date session calendar required"
        )
    at = utc(as_of)
    prices, selected = levels.copy(), selections.copy()
    for frame in (prices, selected):
        frame["date"] = pd.to_datetime(frame.date)
        if not frame.date.isin(grid).all():
            raise ValueError("observations outside explicit session calendar")
    if prices.duplicated(["instrument_id", "date"]).any() or selected.sample_id.duplicated().any():
        raise ValueError("duplicate prices or selected sample IDs")
    if (
        any(not isinstance(v, str) or not v.strip() for v in selected.sample_id)
        or any(not isinstance(v, str) or not v.strip() for v in selected.instrument_id)
        or any(not isinstance(v, str) or not v.strip() for v in prices.instrument_id)
    ):
        raise ValueError("selected identities cannot be missing")
    prices["available_at"] = prices.available_at.map(utc)
    prices["value"] = pd.to_numeric(prices.value, errors="raise")
    if not np.isfinite(prices.value).all() or (prices.value <= 0).any():
        raise ValueError("observed levels must be positive; evidenced terminal zero is separate")
    visible = prices.loc[prices.available_at <= at].set_index(["instrument_id", "date"])
    terminal = {} if terminals is None else terminals.copy()
    if terminals is not None:
        if not {"sample_id", "date", "realized_return", "available_at", "source"} <= set(terminal):
            raise ValueError(
                "terminal labels require per-sample economic return and source evidence"
            )
        if (
            terminal.sample_id.duplicated().any()
            or not terminal.sample_id.isin(selected.sample_id).all()
        ):
            raise ValueError("ambiguous or unselected terminal sample")
        terminal["date"] = pd.to_datetime(terminal.date)
        if (
            terminal.date.isna().any()
            or terminal.date.dt.tz is not None
            or not terminal.date.equals(terminal.date.dt.normalize())
        ):
            raise ValueError("terminal effective dates must be finite plain dates")
        terminal["available_at"] = terminal.available_at.map(utc)
        if (
            not np.isfinite(terminal.realized_return).all()
            or (terminal.realized_return < -1).any()
            or terminal.source.isna().any()
            or terminal.source.astype(str).str.strip().eq("").any()
        ):
            raise ValueError("terminal return must be evidenced and no less than -1")
        terminal = {
            r.sample_id: r
            for r in terminal.itertuples()
            if r.available_at <= at and r.date <= at.tz_localize(None).normalize()
        }
    bounds = None
    if lower_return is not None or upper_return is not None:
        if (
            lower_return is None
            or upper_return is None
            or not np.isfinite([lower_return, upper_return]).all()
            or not -1 <= lower_return <= upper_return
        ):
            raise ValueError("explicit finite ordered lower/upper return scenarios required")
        bounds = (lower_return, upper_return)
    records = []
    for row in selected.itertuples():
        location = grid.get_loc(row.date)
        endpoint = grid[location + horizon] if location + horizon < len(grid) else None
        value, status, source = None, "immature", None
        event = terminal.get(row.sample_id)
        if event is not None and (
            event.date <= row.date or endpoint is not None and event.date > endpoint
        ):
            raise ValueError("terminal observation is outside the selected label interval")
        if event is not None:
            # Cash is retained through the label endpoint; no implicit reinvestment/interest.
            value, status, source = float(event.realized_return), "terminal", event.source
        elif endpoint is not None and endpoint <= at.tz_localize(None).normalize():
            keys = [(row.instrument_id, day) for day in grid[location : location + horizon + 1]]
            if all(key in visible.index for key in keys):
                value = float(visible.loc[keys[-1], "value"] / visible.loc[keys[0], "value"] - 1)
                status = "observed"
            else:
                status = getattr(row, "missing_reason", "missing_price")
                if status not in {"suspended", "delisted_unknown_terminal", "missing_price"}:
                    raise ValueError("explicit supported missing-label reason required")
        records.append(
            {
                "sample_id": row.sample_id,
                "instrument_id": row.instrument_id,
                "date": row.date,
                "label_end": endpoint,
                "return": value,
                "status": status,
                "source": source,
                "lower_return": value
                if value is not None
                else bounds[0]
                if bounds and status != "immature"
                else None,
                "upper_return": value
                if value is not None
                else bounds[1]
                if bounds and status != "immature"
                else None,
            }
        )
    return pd.DataFrame(
        records,
        columns=[
            "sample_id",
            "instrument_id",
            "date",
            "label_end",
            "return",
            "status",
            "source",
            "lower_return",
            "upper_return",
        ],
    )


def label_coverage(labels):
    counts = labels.status.value_counts().to_dict()
    return {
        "selected_samples": len(labels),
        "observed_or_terminal": int(labels["return"].notna().sum()),
        "status_counts": {str(k): int(v) for k, v in counts.items()},
        "lower_mean_scenario": float(labels.lower_return.mean())
        if labels.lower_return.notna().all() and len(labels)
        else None,
        "upper_mean_scenario": float(labels.upper_return.mean())
        if labels.upper_return.notna().all() and len(labels)
        else None,
        "policy": "unobserved outcomes retained; scenario bounds are assumptions, not point estimates",
    }
