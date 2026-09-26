"""Frozen supplemental history and point-in-time research data preflight.

Imported histories are source declarations, not independent market certification.
There is no current-universe, timestamp or price forward-fill fallback.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd

SCHEMA = "qdk.research-history/v1"
KEYS = ["domain", "symbol", "field", "effective_at", "available_at"]
DOMAINS = {"fundamentals", "universe", "status", "classification", "corporate_actions"}


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _timestamp(value: object) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("History timestamps require explicit time zones")
    return stamp.tz_convert("UTC")


def validate_history(frame: pd.DataFrame) -> pd.DataFrame:
    missing = set(KEYS + ["value"]) - set(frame)
    if missing or frame.empty:
        raise ValueError(f"History is empty or missing fields: {sorted(missing)}")
    out = frame[KEYS + ["value"]].copy()
    if out.isna().any().any():
        raise ValueError("Unknown history values/timestamps cannot be admitted")
    if not set(out.domain).issubset(DOMAINS):
        raise ValueError("Unknown history domain")
    for col in ("symbol", "field"):
        if not out[col].map(lambda v: isinstance(v, str) and bool(v.strip())).all():
            raise ValueError(f"History {col} must be a nonempty string")
    for col in ("effective_at", "available_at"):
        out[col] = pd.to_datetime(out[col].map(_timestamp), utc=True)
    if out.duplicated(KEYS).any():
        raise ValueError("Duplicate history revision")
    out["value"] = out.value.astype(str)
    numeric = out.domain.eq("fundamentals")
    if not np.isfinite(pd.to_numeric(out.loc[numeric, "value"], errors="coerce")).all():
        raise ValueError("Fundamental values must be finite numbers")
    logical = out.domain.isin(["universe", "status"])
    if not out.loc[logical, "value"].isin(["true", "false"]).all():
        raise ValueError("Universe/status values must be explicit true or false")
    return out.sort_values(KEYS).reset_index(drop=True)


def import_history(
    source: Path, output: Path, *, provider: str, source_uri: str, license_note: str
) -> dict:
    """Publish one immutable CSV/Parquet import including original-source bytes."""
    if not all(v.strip() for v in (provider, source_uri, license_note)):
        raise ValueError("Provider, source URI and data-rights declaration are required")
    if output.exists():
        raise FileExistsError(output)
    data = source.read_bytes()
    if source.suffix.lower() == ".csv":
        frame = pd.read_csv(io.BytesIO(data), dtype={"symbol": str, "value": str})
    elif source.suffix.lower() == ".parquet":
        frame = pd.read_parquet(io.BytesIO(data))
    else:
        raise ValueError("Only CSV or Parquet history imports are supported")
    frame = validate_history(frame)
    output.mkdir(parents=True, exist_ok=False)
    original = output / ("source" + source.suffix.lower())
    original.write_bytes(data)
    target = output / "history.parquet"
    frame.to_parquet(target, index=False)
    manifest = {
        "schema_version": SCHEMA,
        "provider": provider,
        "source_uri": source_uri,
        "license_note": license_note,
        "scope": "imported-source-declaration",
        "original": {"file": original.name, "sha256": _digest(data)},
        "history": {"file": target.name, "sha256": _digest(target.read_bytes())},
        "rows": len(frame),
        "domains": sorted(frame.domain.unique()),
        "symbols": sorted(frame.symbol.unique()),
    }
    # Manifest is the commit marker. A failed/incomplete import is not loadable.
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def load_history(root: Path) -> tuple[dict, pd.DataFrame]:
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != SCHEMA:
        raise ValueError("Unsupported history schema")
    payloads = {}
    for name in ("original", "history"):
        entry = manifest[name]
        path = (root / entry["file"]).resolve()
        if path.parent != root.resolve():
            raise ValueError("History path escapes snapshot")
        payload = path.read_bytes()
        if _digest(payload) != entry["sha256"]:
            raise ValueError("History hash mismatch")
        payloads[name] = payload
    frame = validate_history(pd.read_parquet(io.BytesIO(payloads["history"])))
    if (
        len(frame) != manifest["rows"]
        or sorted(frame.symbol.unique()) != manifest["symbols"]
        or sorted(frame.domain.unique()) != manifest["domains"]
    ):
        raise ValueError("History manifest differs from content")
    return manifest, frame


def asof_history(history: pd.DataFrame, *, as_of: object, domain: str, field: str) -> pd.DataFrame:
    """Latest effective record known by the decision time; retain revision lineage."""
    cutoff = _timestamp(as_of)
    eligible = history[
        (history.domain == domain)
        & (history.field == field)
        & (history.effective_at <= cutoff)
        & (history.available_at <= cutoff)
    ]
    return eligible.sort_values(["effective_at", "available_at"]).drop_duplicates(
        "symbol", keep="last"
    )


def attach_history(
    prices: pd.DataFrame, history: pd.DataFrame, fields: dict[str, str], *, max_age_days: int = 550
) -> pd.DataFrame:
    """Attach known fundamentals/classifications to each historical close."""
    if type(max_age_days) is not int or max_age_days < 1:
        raise ValueError("max_age_days must be positive")
    output = prices.copy()
    output["date"] = pd.to_datetime(output.date)
    for field, domain in fields.items():
        if field in output or domain not in {"fundamentals", "classification"}:
            raise ValueError(f"Invalid or conflicting PIT field: {field}")
        values, times = {}, {}
        for date, day in output.groupby("date"):
            close_time = date.tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15)
            rows = asof_history(history, as_of=close_time, domain=domain, field=field)
            rows = rows[
                (close_time.tz_convert("UTC") - rows.effective_at)
                <= pd.Timedelta(days=max_age_days)
            ].set_index("symbol")
            for index, symbol in day.symbol.items():
                if symbol in rows.index:
                    value = rows.loc[symbol, "value"]
                    values[index] = float(value) if domain == "fundamentals" else value
                    times[index] = rows.loc[symbol, "available_at"]
        output[field] = pd.Series(values).reindex(output.index)
        output[field + "_available_at"] = pd.to_datetime(
            pd.Series(times).reindex(output.index), utc=True
        )
    return output


def preflight(
    prices: pd.DataFrame,
    calendar: pd.DataFrame,
    *,
    symbols: list[str],
    start: str,
    end: str,
    requirements: dict[str, dict],
    history: pd.DataFrame | None = None,
    required_history: dict[str, str] | None = None,
) -> dict:
    """Report all gaps before expensive computation, using an explicit session calendar."""
    start_date, end_date = pd.Timestamp(start), pd.Timestamp(end)
    if start_date > end_date or not symbols or len(symbols) != len(set(symbols)):
        raise ValueError("Invalid date range or symbol set")
    issues, coverage = [], []
    dates = pd.to_datetime(calendar.date)
    sessions = pd.DatetimeIndex(dates[(dates >= start_date) & (dates <= end_date)])
    if (
        len(sessions) == 0
        or sessions.has_duplicates
        or dates.min() > start_date
        or dates.max() < end_date
    ):
        issues.append(
            {"code": "CALENDAR_COVERAGE", "detail": "Calendar missing or duplicated sessions"}
        )
    p = prices.copy()
    p["date"] = pd.to_datetime(p.date)
    if p.duplicated(["symbol", "date"]).any():
        issues.append({"code": "DUPLICATE_PRICE", "detail": "Duplicate symbol/date rows"})
    required_columns = sorted({c for req in requirements.values() for c in req["columns"]})
    warmup = max((r["warmup_bars"] for r in requirements.values()), default=1)
    for symbol in symbols:
        group = p[p.symbol == symbol]
        missing_sessions = sessions.difference(pd.DatetimeIndex(group.date))
        prior = int((group.date < start_date).sum())
        warmup_sessions = pd.DatetimeIndex(dates[dates < start_date].sort_values().tail(warmup))
        warmup_gaps = warmup_sessions.difference(pd.DatetimeIndex(group.date))
        missing_columns = [c for c in required_columns if c not in group]
        available = [c for c in required_columns if c in group]
        needed = group[group.date <= end_date].tail(len(sessions) + warmup)
        missing_values = {
            c: int((~np.isfinite(pd.to_numeric(needed[c], errors="coerce"))).sum())
            for c in available
        }
        row = {
            "symbol": symbol,
            "missing_sessions": [str(d.date()) for d in missing_sessions],
            "warmup_available": prior,
            "warmup_required": warmup,
            "missing_warmup_sessions": [str(d.date()) for d in warmup_gaps],
            "missing_columns": missing_columns,
            "missing_values": missing_values,
        }
        coverage.append(row)
        if (
            len(missing_sessions)
            or len(warmup_gaps)
            or len(warmup_sessions) < warmup
            or prior < warmup
            or missing_columns
            or any(missing_values.values())
        ):
            issues.append({"code": "PRICE_OR_FEATURE_COVERAGE", "detail": row})
    for field, domain in (required_history or {}).items():
        missing = 0
        for day in sessions:
            rows = (
                pd.DataFrame()
                if history is None
                else asof_history(
                    history,
                    as_of=day.tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15),
                    domain=domain,
                    field=field,
                )
            )
            missing += len(set(symbols) - set(rows.symbol if "symbol" in rows else []))
        if missing:
            issues.append(
                {
                    "code": "HISTORY_COVERAGE",
                    "detail": {
                        "domain": domain,
                        "field": field,
                        "missing_symbol_sessions": missing,
                    },
                }
            )
    return {
        "schema_version": "qdk.research-preflight/v1",
        "passed": not issues,
        "start": start,
        "end": end,
        "sessions": len(sessions),
        "coverage": coverage,
        "issues": issues,
        "history_scope": "source-declared" if history is not None else "unavailable",
        "market_data_certified": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--provider", required=True)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--license-note", required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            import_history(
                args.source,
                args.output,
                provider=args.provider,
                source_uri=args.source_uri,
                license_note=args.license_note,
            )
        )
    )


if __name__ == "__main__":
    main()
