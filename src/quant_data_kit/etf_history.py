"""Admission audit for a fixed ETF pool, without filling missing market observations."""

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from quant_data_kit.etf_events import adjust_etf_prices


def audit_etf_history(
    raw,
    comparison,
    calendar,
    actions,
    *,
    symbols,
    start,
    end,
    master_root=None,
    nontrading=(),
    evidence_root=None,
    volume_tolerance=100,
    amount_tolerance=100,
):
    """Return an explicit blocking inventory; a clean price comparison is not admission.

    Tolerances are absolute shares/CNY rounding allowances, frozen by the caller.
    Nontrading declarations must identify an original attachment, full-session
    suspension, and a knowledge timestamp before that session opens. They explain
    missing bars but never synthesize a return or authorize marking held positions.
    """
    issues = []
    checks = {}
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    expected = pd.DatetimeIndex(pd.to_datetime(calendar)).sort_values()
    if expected.has_duplicates or expected.min() > start or expected.max() <= end:
        raise ValueError("calendar must be unique and cover the interval plus the next session")
    expected = expected[(expected >= start) & (expected <= end)]
    if expected.empty or start > end or len(symbols) != len(set(symbols)):
        raise ValueError("explicit nonempty sessions and unique ETF pool required")
    if any(
        type(v) not in {float, int} or not np.isfinite(v) or v < 0
        for v in (volume_tolerance, amount_tolerance)
    ):
        raise ValueError("comparison tolerances must be finite and nonnegative")
    allowed = {}
    for row in nontrading:
        required = {"symbol", "date", "status", "known_at", "source", "file", "sha256"}
        if set(row) != required or row["status"] != "full_session_suspension" or not row["source"]:
            raise ValueError("nontrading declaration requires full-session source evidence")
        root = Path(evidence_root).resolve()
        path = (root / row["file"]).resolve()
        if (
            root not in path.parents
            or hashlib.sha256(path.read_bytes()).hexdigest() != row["sha256"]
        ):
            raise ValueError("nontrading evidence identity mismatch")
        day, known = pd.Timestamp(row["date"]), pd.Timestamp(row["known_at"])
        opening = day.tz_localize("Asia/Shanghai") + pd.Timedelta(hours=9, minutes=30)
        if (
            known.tzinfo is None
            or known > opening
            or day not in expected
            or row["symbol"] not in symbols
        ):
            raise ValueError("nontrading evidence is late or outside the requested pool/window")
        key = (row["symbol"], day)
        if key in allowed:
            raise ValueError("duplicate nontrading declaration")
        allowed[key] = row
    keys = ["symbol", "date"]
    fields = ["open", "high", "low", "close", "volume", "amount"]
    prepared = []
    for name, frame in (("primary", raw), ("comparison", comparison)):
        frame = frame.copy()
        if set(keys + fields + ["adjustment", "source"]) - set(frame):
            raise ValueError("bar source lacks required fields")
        frame["date"] = pd.to_datetime(frame.date)
        frame = frame[frame.date.between(start, end)]
        if frame.duplicated(keys).any() or set(frame.symbol) != set(symbols):
            raise ValueError("bar source must contain the exact pool with unique symbol/date keys")
        if not frame.adjustment.eq("none").all():
            raise ValueError("cross-provider audit requires unadjusted observations")
        if not np.isfinite(frame[fields].to_numpy(dtype=float)).all():
            raise ValueError("bar source has non-finite observations")
        invalid = frame[
            (frame[fields] <= 0).any(axis=1)
            | (frame.low > frame[["open", "close"]].min(axis=1))
            | (frame.high < frame[["open", "close"]].max(axis=1))
        ]
        for row in invalid.itertuples():
            issues.append(
                {
                    "kind": "invalid_traded_bar",
                    "provider": name,
                    "symbol": row.symbol,
                    "date": str(row.date.date()),
                }
            )
        observed = set(map(tuple, frame[keys].itertuples(index=False, name=None)))
        for symbol in symbols:
            for day in expected:
                if (symbol, day) not in observed and (symbol, day) not in allowed:
                    issues.append(
                        {
                            "kind": "unexplained_missing_session",
                            "provider": name,
                            "symbol": symbol,
                            "date": str(day.date()),
                        }
                    )
        if observed & set(allowed):
            raise ValueError("full-session suspension contradicts an observed trading bar")
        if set(frame.date) - set(expected):
            raise ValueError("bar appears outside the official session calendar")
        prepared.append(frame)
    a, b = prepared
    joined = a.merge(b, on=keys, suffixes=("_primary", "_comparison"), validate="one_to_one")
    if any(
        str(x.source_primary).split(":")[0] == str(x.source_comparison).split(":")[0]
        for x in joined.itertuples()
    ):
        issues.append({"kind": "independent_provider_required"})
    for field in fields:
        delta = (joined[field + "_primary"] - joined[field + "_comparison"]).abs()
        tolerance = (
            volume_tolerance
            if field == "volume"
            else amount_tolerance
            if field == "amount"
            else 1e-10
        )
        checks[field] = {
            "max_absolute_delta": float(delta.max()) if len(delta) else None,
            "tolerance": tolerance,
            "conflicts": int((delta > tolerance).sum()),
        }
        for _, row in joined[delta > tolerance].iterrows():
            issues.append(
                {
                    "kind": "provider_conflict",
                    "field": field,
                    "symbol": row.symbol,
                    "date": str(row.date.date()),
                    "primary": float(row[field + "_primary"]),
                    "comparison": float(row[field + "_comparison"]),
                }
            )
    try:
        adjusted = adjust_etf_prices(a, actions)
        checks["cash_and_share_adjustment"] = {
            "passed": True,
            "events": len(actions),
            "rows": len(adjusted),
        }
    except (ValueError, TypeError, KeyError) as exc:
        issues.append({"kind": "corporate_actions_invalid", "reason": str(exc)})
    if master_root is None:
        issues.append({"kind": "historical_master_missing"})
    else:
        from quant_data_kit.instrument_master import (
            load_instrument_master,
            resolve_instrument_catalog,
        )

        try:
            manifest, catalog = load_instrument_master(Path(master_root))
            resolve_instrument_catalog(catalog, symbols=symbols, start=start, end=end)
            checks["master"] = {"passed": True, "bundle_sha256": manifest["bundle_sha256"]}
        except (ValueError, OSError) as exc:
            issues.append({"kind": "historical_master_invalid", "reason": str(exc)})
    return {
        "schema": "qdk.etf-history-admission/v1",
        "admitted": not issues,
        "start": str(start.date()),
        "end": str(end.date()),
        "sessions": len(expected),
        "symbols": list(symbols),
        "checks": checks,
        "blocking_issues": issues,
        "evidenced_nontrading_sessions": len(allowed),
        "synthetic_bars": 0,
        "scope": "retrospective fixed pool selected later; no capacity, daily tradability or future-research claim",
    }
