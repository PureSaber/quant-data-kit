"""Immutable USD daily research panels with explicit adjustment semantics."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

from .calendar import schedule

REQUIRED = {
    "instrument_id",
    "symbol",
    "session",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "dividend",
    "split_ratio",
    "currency",
    "price_basis",
    "available_at",
    "source",
}


def utc(value, field: str = "timestamp") -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tz is None:
        raise ValueError(f"{field} requires a timezone-aware timestamp")
    return stamp.tz_convert("UTC")


def us_symbol(value: str) -> str:
    symbol = value.strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9]*(?:[.-][A-Z0-9]+)*", symbol):
        raise ValueError(f"invalid explicit US equity symbol: {value!r}")
    return symbol


def validate_prices(frame: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED - set(frame.columns)
    if missing or frame.empty:
        raise ValueError(f"nonempty US price panel required; missing {sorted(missing)}")
    out = frame.copy()
    for name in ("instrument_id", "symbol", "source"):
        if out[name].isna().any() or out[name].astype(str).str.strip().eq("").any():
            raise ValueError(f"empty {name}")
    out["symbol"] = out.symbol.map(us_symbol)
    out["session"] = pd.to_datetime(out.session, errors="raise")
    if out.session.dt.tz is not None or not out.session.eq(out.session.dt.normalize()).all():
        raise ValueError("session must contain plain dates")
    if out.duplicated(["instrument_id", "session"]).any():
        raise ValueError("duplicate instrument/session")
    if not out.currency.eq("USD").all():
        raise ValueError("US research requires explicit USD prices")
    if not out.price_basis.isin(["raw", "split_normalized"]).all():
        raise ValueError("prices must be raw or split_normalized, never total-return prices")
    if out.groupby("instrument_id").price_basis.nunique().gt(1).any():
        raise ValueError("price basis changed within an instrument")
    numeric = ["open", "high", "low", "close", "volume", "dividend", "split_ratio"]
    for name in numeric:
        out[name] = pd.to_numeric(out[name], errors="raise")
    if not np.isfinite(out[numeric].to_numpy(dtype=float)).all():
        raise ValueError("nonfinite price or action")
    if (out[["open", "high", "low", "close", "split_ratio"]] <= 0).any().any():
        raise ValueError("prices and split ratios must be positive")
    if (out[["volume", "dividend"]] < 0).any().any():
        raise ValueError("volume and dividends must be nonnegative")
    if (
        (out.high < out[["open", "close", "low"]].max(axis=1))
        | (out.low > out[["open", "close", "high"]].min(axis=1))
    ).any():
        raise ValueError("inconsistent OHLC")
    sessions = schedule(str(out.session.min().date()), str(out.session.max().date()))
    if not out.session.isin(sessions.index).all():
        raise ValueError("price row outside regular US equity sessions")
    for _, group in out.groupby("instrument_id"):
        expected = sessions.loc[group.session.min() : group.session.max()].index
        if len(group) != len(expected):
            raise ValueError("missing interior daily bars; explicit status handling is required")
    out["available_at"] = out.available_at.map(lambda value: utc(value, "available_at"))
    closes = out.session.map(sessions.close)
    if (out.available_at < closes).any():
        raise ValueError("complete daily bars cannot be available before session close")
    if "pay_date" not in out:
        out["pay_date"] = None
    for row in out.loc[out.pay_date.notna() & out.pay_date.ne("")].itertuples():
        pay = pd.Timestamp(row.pay_date)
        if pay.tz is not None or pay < row.session or pay != pay.normalize():
            raise ValueError("invalid dividend payment date")
    return out.sort_values(["session", "instrument_id"]).reset_index(drop=True)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_bundle(
    path: str | Path,
    prices: pd.DataFrame,
    provenance: dict,
    *,
    tables: dict[str, pd.DataFrame] | None = None,
) -> dict:
    prices = validate_prices(prices)
    required = {"source", "observed_at", "evidence_kind", "universe_kind"}
    if not required.issubset(provenance):
        raise ValueError(f"provenance needs {sorted(required)}")
    utc(provenance["observed_at"], "observed_at")
    if provenance["evidence_kind"] not in {"synthetic", "retrospective", "historical_pit"}:
        raise ValueError("unknown evidence kind")
    path = Path(path)
    path.mkdir(parents=True, exist_ok=False)
    payload = {"prices": prices, **(tables or {})}
    if payload["prices"] is not prices:
        raise ValueError("cannot replace validated prices")
    files = {}
    for name, table in payload.items():
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("invalid table name")
        target = path / f"{name}.csv"
        table.to_csv(target, index=False, lineterminator="\n")
        files[target.name] = {"sha256": sha256(target), "rows": len(table)}
    manifest = {"schema": "puresaber.us-research/1", "provenance": provenance, "files": files}
    (path / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8"
    )
    return manifest


def load_bundle(path: str | Path) -> tuple[dict, dict[str, pd.DataFrame]]:
    path = Path(path)
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema") != "puresaber.us-research/1":
        raise ValueError("unsupported bundle schema")
    tables = {}
    for name, entry in manifest["files"].items():
        if not re.fullmatch(r"[a-z][a-z0-9_]*\.csv", name):
            raise ValueError("invalid manifest file name")
        target = path / name
        if sha256(target) != entry["sha256"]:
            raise ValueError(f"bundle hash mismatch: {name}")
        table = pd.read_csv(target, dtype={"instrument_id": str, "symbol": str})
        if len(table) != entry["rows"]:
            raise ValueError(f"bundle row count mismatch: {name}")
        tables[name[:-4]] = table
    tables["prices"] = validate_prices(tables["prices"])
    return manifest, tables


def fetch_yahoo(symbols: list[str], start: str, end: str) -> tuple[pd.DataFrame, dict]:
    """Retrospective split-normalized quotes. No historical-universe/PIT claim.

    Yahoo OHLC with auto_adjust=False is split-normalized, not as-traded raw.
    Reported splits remain metadata; consumers must not multiply positions again.
    Payment dates are not supplied, so dividends stay receivable in cash replay.
    """
    import yfinance as yf

    if not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("unique symbols required")
    sessions = schedule(start, end)
    observed = pd.Timestamp.now(tz="UTC")
    if sessions.empty or sessions.close.max() >= observed:
        raise ValueError("request completed historical sessions only")
    pieces = []
    for symbol in symbols:
        symbol = us_symbol(symbol)
        ticker = yf.Ticker(symbol)
        history = ticker.history(
            start=start,
            end=str((pd.Timestamp(end) + pd.Timedelta(days=1)).date()),
            auto_adjust=False,
            back_adjust=False,
            actions=True,
            repair=False,
            raise_errors=True,
        )
        metadata = ticker.get_history_metadata()
        if metadata.get("currency") != "USD":
            raise ValueError(f"{symbol}: source did not confirm USD")
        if history.empty or not {"Dividends", "Stock Splits"}.issubset(history):
            raise ValueError(f"{symbol}: missing prices/actions")
        local_dates = history.index.tz_convert("America/New_York").tz_localize(None).normalize()
        for day, (_, row) in zip(local_dates, history.iterrows(), strict=True):
            if day not in sessions.index:
                raise ValueError(f"{symbol}: unexpected session {day}")
            pieces.append(
                {
                    "instrument_id": f"US:YAHOO:{symbol}",
                    "symbol": symbol,
                    "session": day,
                    "open": row.Open,
                    "high": row.High,
                    "low": row.Low,
                    "close": row.Close,
                    "volume": row.Volume,
                    "dividend": row.Dividends,
                    "split_ratio": row["Stock Splits"] or 1.0,
                    "currency": "USD",
                    "price_basis": "split_normalized",
                    "available_at": sessions.loc[day, "close"],
                    "source": f"yfinance:{yf.__version__}",
                    "pay_date": None,
                }
            )
    provenance = {
        "source": "Yahoo Finance via yfinance",
        "observed_at": observed.isoformat(),
        "evidence_kind": "retrospective",
        "universe_kind": "fixed_cohort",
        "symbols": symbols,
        "start": start,
        "end": end,
        "limitations": [
            "Current symbol identities; no historical constituent certification",
            "Close timestamps are modeled availability, not archived PIT evidence",
            "Split-normalized research share units, not as-traded shares",
            "Dividend payment dates absent; cash replay retains receivables",
            "Yahoo revisions and licensing apply; snapshot is not a redistribution grant",
        ],
    }
    return validate_prices(pd.DataFrame(pieces)), provenance
