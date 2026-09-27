"""Simple shareholder-return signal index, separate from spendable cash accounting."""

import numpy as np
import pandas as pd

from .actions import ActionTerms
from .common import utc


def total_return_panel(prices, actions, *, timezone, price_basis="raw"):
    """Raw date/symbol/close panel plus explicit split and dividend entitlements.

    Index assumes theoretical reinvestment at ex-day close, NOT cash availability.
    Execution must separately carry dividend receivables until payment. Complex
    conversions or simultaneous share/cash terms require an account-based return,
    and are rejected rather than silently omitted.
    """
    if price_basis != "raw":
        raise ValueError("explicit actions require raw quotes; do not double-adjust returns")
    for field in ("adjustment", "price_basis"):
        if field in prices and not prices[field].isin(["none", "raw"]).all():
            raise ValueError("explicit actions require raw quotes; adjusted input detected")
    if not np.isfinite(pd.to_numeric(prices.close, errors="raise")).all():
        raise ValueError("finite raw prices required")
    terms = [x if isinstance(x, ActionTerms) else ActionTerms(**x) for x in actions]
    economic = [x for x in terms if x.kind != "dividend_payment"]
    if any(x.kind not in {"split", "dividend_entitlement"} for x in economic):
        raise ValueError("complex actions need account-based total return")
    indexed = {}
    for action in economic:
        effective = utc(action.effective_at)
        if utc(action.available_at) > effective:
            raise ValueError("late-known corporate action cannot backfill historical signal")
        key = (action.instrument_id, effective.tz_convert(timezone).date())
        if key in indexed:
            raise ValueError("simultaneous actions need an explicit combined entitlement basis")
        indexed[key] = action
    pieces = []
    for symbol, group in prices.groupby("symbol", sort=False):
        group = group.sort_values("date").copy()
        if group.date.duplicated().any() or (group.close <= 0).any():
            raise ValueError("unique dates and positive raw prices required")
        dates = set(pd.to_datetime(group.date).dt.date)
        if any(
            sym == symbol and min(dates) <= date <= max(dates) and date not in dates
            for sym, date in indexed
        ):
            raise ValueError("corporate action date has no raw quote")
        values = []
        previous, index = None, 100.0
        for row in group.itertuples():
            action = indexed.get((symbol, pd.Timestamp(row.date).date()))
            ratio, cash = 1.0, 0.0
            if action:
                if action.kind == "split":
                    ratio = float(action.ratio)
                else:
                    cash = float(action.cash_per_unit)
            if previous is not None:
                index *= (row.close * ratio + cash) / previous
            values.append(index)
            previous = row.close
        group["return_close"] = values
        pieces.append(group)
    return pd.concat(pieces, ignore_index=True) if pieces else prices.assign(return_close=[])
