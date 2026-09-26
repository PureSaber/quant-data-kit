"""Explicit Hong Kong daily-data boundary; never applies mainland symbol/unit rules.

Providers are selected by the caller. A failed provider never silently changes
the provenance or creates synthetic prices. Snapshots retain returned source rows.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


def hk_symbol(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("HK symbols must be strings, preserving leading zeros")
    text = value.strip().upper()
    if text.startswith("HK."):
        text = text[3:]
    elif text.endswith(".HK"):
        text = text[:-3]
    if not re.fullmatch(r"[0-9]{1,5}", text) or int(text) == 0:
        raise ValueError(f"Invalid Hong Kong symbol: {value!r}")
    return text.zfill(5)


def hk_yahoo_symbol(value: str) -> str:
    return f"{int(hk_symbol(value)):04d}.HK"


def hk_calendar(start: str, end: str) -> pd.DataFrame:
    """XHKG sessions, including half-day close times; no weekday/SSE fallback."""
    import exchange_calendars as xcals

    if pd.Timestamp(start) > pd.Timestamp(end):
        raise ValueError("start must not follow end")
    cal = xcals.get_calendar("XHKG", start=start, end=end)
    result = cal.schedule.loc[start:end, ["open", "close"]].copy()
    result.index = result.index.tz_localize(None) if result.index.tz else result.index
    result.index.name = "date"
    return result.reset_index()


def normalize_hk_bars(frame: pd.DataFrame, symbol: str, provider: str) -> pd.DataFrame:
    required = ["date", "open", "high", "low", "close", "volume", "amount"]
    if frame.empty or not set(required).issubset(frame):
        raise ValueError("HK daily response is empty or missing required columns")
    out = frame[required].copy()
    out["date"] = pd.to_datetime(out["date"], errors="raise")
    if out.date.dt.tz is not None:
        out["date"] = out.date.dt.tz_convert("Asia/Hong_Kong").dt.tz_localize(None)
    out["date"] = out.date.dt.normalize()
    if out.date.isna().any() or out.date.duplicated().any():
        raise ValueError("HK daily dates must be valid and unique")
    for column in required[1:]:
        out[column] = pd.to_numeric(out[column], errors="raise")
    if not np.isfinite(out[required[1:]].to_numpy()).all():
        raise ValueError("HK daily values must be finite")
    if (out[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("HK prices must be positive")
    if (out[["volume", "amount"]] < 0).any().any():
        raise ValueError("HK volume/turnover cannot be negative")
    # Small tolerances accommodate floating point source serialization, not bad bars.
    if (out.high + 0.0001 < out[["open", "close", "low"]].max(axis=1)).any():
        raise ValueError("HK high is below another OHLC value")
    if (out.low - 0.0001 > out[["open", "close", "high"]].min(axis=1)).any():
        raise ValueError("HK low is above another OHLC value")
    if not np.equal(out.volume, np.floor(out.volume)).all():
        raise ValueError("HK daily volume must be whole shares")
    out["symbol"] = hk_symbol(symbol)
    out["market"] = "XHKG"
    out["currency"] = "HKD"
    out["volume_unit"] = "share"
    out["amount_unit"] = "HKD"
    out["adjustment"] = "none"
    out["provider"] = provider
    return out.sort_values("date").reset_index(drop=True)


def fetch_hk_bars(
    symbol: str, start: str, end: str, *, provider: str, raw_path: Path | None = None
) -> tuple:
    """Return normalized and original response tables. Prices are unadjusted.

    Neither provider supplies complete corporate-action/payment history. The
    caller must not label these bars as total-return or PIT-certified data.
    """
    import akshare as ak

    code = hk_symbol(symbol)
    if pd.Timestamp(start) > pd.Timestamp(end):
        raise ValueError("start must not follow end")
    if provider == "akshare_sina_hk":
        raw = ak.stock_hk_daily(symbol=code, adjust="")
        renamed = raw
    elif provider == "akshare_eastmoney_hk":
        raw = ak.stock_hk_hist(
            symbol=code,
            period="daily",
            start_date=pd.Timestamp(start).strftime("%Y%m%d"),
            end_date=pd.Timestamp(end).strftime("%Y%m%d"),
            adjust="",
        )
        renamed = raw.rename(
            columns={
                "日期": "date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "volume",
                "成交额": "amount",
            }
        )
    else:
        raise ValueError(f"Unsupported explicit HK provider: {provider}")
    if raw_path is not None:
        with Path(raw_path).open("x", encoding="utf-8", newline="") as stream:
            raw.to_csv(stream, index=False)
    dates = pd.to_datetime(renamed["date"], errors="raise")
    requested = renamed[dates.between(pd.Timestamp(start), pd.Timestamp(end))]
    bars = normalize_hk_bars(requested, code, provider)
    if bars.empty:
        raise ValueError(f"No HK data within requested range for {code}")
    return bars, raw


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def capture_hk_snapshot(root: Path, symbols: list[str], start: str, end: str, *, provider: str):
    """Create once, retaining failures and refusing an incomplete success manifest."""
    import importlib.metadata

    root = Path(root)
    codes = [hk_symbol(s) for s in symbols]
    if not codes or len(set(codes)) != len(codes):
        raise ValueError("Snapshot needs nonempty unique HK symbols")
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema": "quant-hk-snapshot/v1",
        "status": "capturing",
        "provider": provider,
        "symbols": codes,
        "start": start,
        "end": end,
        "market": "XHKG",
        "currency": "HKD",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "files": {},
        "corporate_actions_complete": False,
        "pit_universe": False,
        "price_basis": "unadjusted_price_return_only",
        "versions": {},
    }
    try:
        manifest["versions"] = {
            p: importlib.metadata.version(p) for p in ("akshare", "exchange-calendars")
        }
        frames = []
        for code in codes:
            bars, _ = fetch_hk_bars(
                code, start, end, provider=provider, raw_path=root / f"raw-{code}.csv"
            )
            frames.append(bars)
        pd.concat(frames, ignore_index=True).to_parquet(root / "bars.parquet", index=False)
        # Extra sessions are needed to determine settlement of the final trades.
        calendar_end = (pd.Timestamp(end) + pd.Timedelta(days=20)).date().isoformat()
        hk_calendar(start, calendar_end).to_csv(root / "calendar.csv", index=False)
        manifest["status"] = "complete"
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        manifest["files"] = {p.name: sha256(p) for p in sorted(root.iterdir()) if p.is_file()}
        (root / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    return manifest


def load_hk_snapshot(root: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema") != "quant-hk-snapshot/v1" or manifest.get("status") != "complete":
        raise ValueError("HK snapshot is not complete")
    if not {"bars.parquet", "calendar.csv"}.issubset(manifest.get("files", {})):
        raise ValueError("HK snapshot lacks required evidence files")
    for name, digest in manifest["files"].items():
        if Path(name).name != name or sha256(root / name) != digest:
            raise ValueError(f"HK snapshot hash mismatch or unsafe path: {name}")
    bars = pd.read_parquet(root / "bars.parquet")
    for symbol, group in bars.groupby("symbol"):
        normalize_hk_bars(group, symbol, manifest["provider"])
    if set(bars.symbol) != set(manifest["symbols"]):
        raise ValueError("HK snapshot symbols differ from its manifest")
    for column, value in {
        "market": "XHKG",
        "currency": "HKD",
        "volume_unit": "share",
        "amount_unit": "HKD",
        "adjustment": "none",
    }.items():
        if column not in bars or not bars[column].eq(value).all():
            raise ValueError(f"HK snapshot has incompatible {column}")
    calendar = pd.read_csv(root / "calendar.csv", parse_dates=["date", "open", "close"])
    if calendar.date.duplicated().any() or not calendar.date.is_monotonic_increasing:
        raise ValueError("HK calendar must have sorted unique dates")
    if calendar.empty or calendar[["date", "open", "close"]].isna().any().any():
        raise ValueError("HK calendar contains missing dates or times")
    for column in ("open", "close"):
        if calendar[column].dt.tz is None:
            raise ValueError("HK calendar event times must have an explicit timezone")
        if (
            not calendar[column]
            .dt.tz_convert("Asia/Hong_Kong")
            .dt.date.eq(calendar.date.dt.date)
            .all()
        ):
            raise ValueError("HK calendar times must belong to their local trading date")
    if (calendar["close"] <= calendar["open"]).any():
        raise ValueError("HK calendar close must follow open")
    return bars, calendar, manifest
