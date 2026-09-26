"""Immutable, incrementally refreshable ETF research datasets.

The public bundle layout intentionally remains compatible with
``a_share_multifactor.decision_workflow.load_inputs``. A mutable catalog only
selects an immutable snapshot; consumers always receive an explicit snapshot
directory whose files are hash-bound by its manifest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd

from quant_data_kit._version import __version__
from quant_data_kit.calendar import load_sse_trade_dates
from quant_data_kit.providers._symbols import normalize_symbol
from quant_data_kit.providers.benchmark import fetch_hs300_benchmark
from quant_data_kit.providers.corporate_actions import fetch_etf_corporate_actions
from quant_data_kit.providers.prices import fetch_daily_prices
from quant_data_kit.providers.provider_registry import get_provider_spec
from quant_data_kit.research_coverage import SCHEMA as HISTORY_SCHEMA
from quant_data_kit.research_coverage import validate_history
from quant_data_kit.validate import validate_price_frame

SCHEMA_VERSION = "qdk.research-dataset/v1"
CATALOG_SCHEMA_VERSION = "qdk.research-dataset-catalog/v1"
DEFAULT_PROVIDER = "akshare_sina_etf"
FILE_NAMES = {
    "raw": "raw.parquet",
    "adjusted": "adjusted.parquet",
    "benchmark": "benchmark.parquet",
    "calendar": "calendar.parquet",
    "actions": "actions.parquet",
    "catalog": "catalog.csv",
    "history": "history/history.parquet",
}
ACTION_COLUMNS = [
    "event_id",
    "symbol",
    "announced_date",
    "record_date",
    "ex_date",
    "pay_date",
    "shares_available_date",
    "cash_per_share",
    "share_ratio",
    "source",
    "captured_at",
    "source_record",
]


def _canonical_bytes(payload: object) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "not-installed"


def _captured_at(value: str | None) -> pd.Timestamp:
    stamp = pd.Timestamp(value or datetime.now(timezone.utc).isoformat())
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("captured_at must include an explicit time zone")
    return stamp.tz_convert("UTC")


def _day(value: object, name: str) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is not None:
        raise ValueError(f"{name} must be a timezone-naive calendar date")
    return stamp.normalize()


def _empty_actions() -> pd.DataFrame:
    return pd.DataFrame(columns=ACTION_COLUMNS)


def _read_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path, dtype={"symbol": str})
    raise ValueError(f"Unsupported local input format: {path}")


def _local_file(source_dir: Path, name: str, *, required: bool = True) -> Path | None:
    candidates = [source_dir / f"{name}.parquet", source_dir / f"{name}.csv"]
    present = [path for path in candidates if path.is_file()]
    if len(present) > 1:
        raise ValueError(f"Local source contains ambiguous {name} inputs")
    if not present:
        if required:
            raise FileNotFoundError(f"Local source is missing {name}.parquet or {name}.csv")
        return None
    return present[0]


def _load_local_source(
    source_dir: Path,
    *,
    source_uri: str,
    source_version: str,
    license_note: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    if not all(value.strip() for value in (source_uri, source_version, license_note)):
        raise ValueError(
            "Local input requires nonempty source_uri, source_version and license_note"
        )
    source_dir = source_dir.resolve()
    paths = {
        name: _local_file(source_dir, name, required=name != "actions")
        for name in ("raw", "adjusted", "benchmark", "calendar", "actions")
    }
    frames = {
        name: _read_frame(path) if path is not None else _empty_actions()
        for name, path in paths.items()
    }
    declared = {
        name: {
            "file": path.name,
            "sha256": _sha256(path),
        }
        for name, path in paths.items()
        if path is not None
    }
    return frames, {
        "mode": "local_declared_source",
        "source_uri": source_uri,
        "source_version": source_version,
        "license_note": license_note,
        "files": declared,
    }


def _fetch_live_source(
    symbols: list[str],
    *,
    price_start: pd.Timestamp,
    dataset_start: pd.Timestamp,
    end: pd.Timestamp,
    captured_at: pd.Timestamp,
    provider: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    spec = get_provider_spec(provider)
    if not spec.single_source or not {"prices.raw", "prices.adjusted"}.issubset(
        spec.capabilities
    ):
        raise ValueError("Research ETF prices require one explicit raw/adjusted provider")
    request = {
        "symbols": symbols,
        "price_start": price_start.date().isoformat(),
        "adjusted_start": dataset_start.date().isoformat(),
        "dataset_start": dataset_start.date().isoformat(),
        "end": end.date().isoformat(),
    }
    kwargs = {
        "provider": provider,
        "max_workers": min(2, spec.max_workers),
        "max_retries": 3,
        "sleep_seconds": 0.2,
    }
    raw = fetch_daily_prices(
        symbols,
        request["price_start"],
        request["end"],
        adjust="",
        **kwargs,
    )
    adjusted = fetch_daily_prices(
        symbols,
        request["adjusted_start"],
        request["end"],
        adjust="qfq",
        **kwargs,
    )
    benchmark = fetch_hs300_benchmark(
        request["price_start"], request["end"], provider="akshare_sina"
    )
    calendar = pd.DataFrame({"date": load_sse_trade_dates(provider="akshare_sina")})
    actions = fetch_etf_corporate_actions(symbols, captured_at.isoformat())
    if not actions.empty:
        ex_dates = pd.to_datetime(actions["ex_date"]).dt.normalize()
        actions = actions[
            (ex_dates >= dataset_start) & (ex_dates <= end)
        ].reset_index(drop=True)
    frames = {
        "raw": raw,
        "adjusted": adjusted,
        "benchmark": benchmark,
        "calendar": calendar,
        "actions": actions,
    }
    return frames, {
        "mode": "live_public_api",
        "provider": provider,
        "provider_package": "akshare",
        "provider_package_version": _package_version("akshare"),
        "quant_data_kit_version": __version__,
        "endpoints": {
            "prices": (
                "akshare.fund_etf_hist_sina+akshare.fund_etf_dividend_sina"
                if provider == "akshare_sina_etf"
                else "akshare.fund_etf_hist_em"
            ),
            "dividends": "akshare.fund_open_fund_info_em:分红送配详情",
            "announcements": "akshare.fund_announcement_dividend_em",
            "benchmark": "akshare.stock_zh_index_daily:sh000300",
            "calendar": "akshare.tool_trade_date_hist_sina",
        },
        "request": request,
    }


def _normalize_frames(
    frames: dict[str, pd.DataFrame],
    *,
    symbols: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, pd.DataFrame]:
    ready = {name: frame.copy() for name, frame in frames.items()}
    for name in ("raw", "adjusted"):
        frame = ready[name]
        if "symbol" not in frame or "date" not in frame:
            raise ValueError(f"{name} input lacks symbol/date")
        frame["symbol"] = frame["symbol"].map(normalize_symbol)
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
        frame = frame[
            frame["symbol"].isin(symbols) & frame["date"].between(start, end)
        ]
        ready[name] = frame.sort_values(["symbol", "date"]).reset_index(drop=True)
    for name in ("benchmark", "calendar"):
        frame = ready[name]
        if "date" not in frame:
            raise ValueError(f"{name} input lacks date")
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
        ready[name] = frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    actions = ready.get("actions", _empty_actions())
    if actions.empty:
        actions = _empty_actions()
    else:
        missing = set(ACTION_COLUMNS) - set(actions)
        if missing:
            raise ValueError(f"actions input lacks fields: {sorted(missing)}")
        actions = actions[ACTION_COLUMNS].copy()
        actions["symbol"] = actions["symbol"].map(normalize_symbol)
        for column in (
            "announced_date",
            "record_date",
            "ex_date",
            "pay_date",
            "shares_available_date",
        ):
            actions[column] = pd.to_datetime(actions[column], errors="coerce").dt.normalize()
        actions = actions[
            actions["symbol"].isin(symbols) & actions["ex_date"].between(start, end)
        ]
    ready["actions"] = actions.sort_values(["symbol", "ex_date"]).reset_index(drop=True)
    return ready


def _merge_by_key(
    old: pd.DataFrame,
    new: pd.DataFrame,
    keys: list[str],
    *,
    end: pd.Timestamp,
) -> tuple[pd.DataFrame, int]:
    if old.empty:
        return new.copy(), 0
    if new.empty:
        raise ValueError("Incremental provider response is empty")
    overlap = old.merge(new, on=keys, how="inner", suffixes=("_old", "_new"))
    shared = sorted((set(old) & set(new)) - set(keys))
    revised = pd.Series(False, index=overlap.index)
    for column in shared:
        left = overlap[f"{column}_old"]
        right = overlap[f"{column}_new"]
        equal = left.eq(right) | (left.isna() & right.isna())
        if pd.api.types.is_numeric_dtype(left) and pd.api.types.is_numeric_dtype(right):
            equal = pd.Series(
                np.isclose(left, right, equal_nan=True, rtol=1e-12, atol=1e-12),
                index=overlap.index,
            )
        revised |= ~equal
    kept = old.merge(new[keys], on=keys, how="left", indicator=True)
    kept = kept[kept["_merge"] == "left_only"].drop(columns="_merge")
    combined = pd.concat([kept, new], ignore_index=True)
    date_key = "date" if "date" in keys else "ex_date"
    if date_key in combined:
        combined = combined[pd.to_datetime(combined[date_key]) <= end]
    return combined.sort_values(keys).reset_index(drop=True), int(revised.sum())


def _validate_actions(
    actions: pd.DataFrame,
    raw: pd.DataFrame,
) -> dict[str, Any]:
    if actions.empty:
        return {"rows": 0, "symbols": 0, "deferred_payments": 0}
    if actions.duplicated(["symbol", "ex_date"]).any() or actions.duplicated("event_id").any():
        raise ValueError("Corporate actions contain duplicate event identities or ex-dates")
    if actions[["announced_date", "record_date", "ex_date", "pay_date"]].isna().any().any():
        raise ValueError("Corporate actions contain unknown cash entitlement dates")
    deferred = 0
    for row in actions.itertuples():
        try:
            cash = Decimal(str(row.cash_per_share))
            ratio = Decimal(str(row.share_ratio))
        except InvalidOperation as exc:
            raise ValueError("Corporate actions contain invalid amounts") from exc
        if not cash.is_finite() or not ratio.is_finite() or cash < 0 or ratio <= 0:
            raise ValueError("Corporate actions contain invalid amounts")
        if row.announced_date >= row.ex_date or row.record_date >= row.ex_date:
            raise ValueError("Corporate-action announcement/record date must precede ex-date")
        previous = raw.loc[
            (raw["symbol"] == row.symbol) & (raw["date"] < row.ex_date), "date"
        ]
        if previous.empty or pd.Timestamp(previous.max()) != row.record_date:
            raise ValueError("Corporate-action record date is not the previous captured session")
        if row.pay_date < row.ex_date:
            raise ValueError("Corporate-action payment cannot precede ex-date")
        deferred += int(row.pay_date != row.ex_date)
    return {
        "rows": len(actions),
        "symbols": int(actions["symbol"].nunique()),
        "deferred_payments": deferred,
    }


def _validate_adjustments(
    raw: pd.DataFrame,
    adjusted: pd.DataFrame,
    actions: pd.DataFrame,
) -> dict[str, Any]:
    joined = raw[["symbol", "date", "close"]].merge(
        adjusted[["symbol", "date", "close"]],
        on=["symbol", "date"],
        how="outer",
        suffixes=("_raw", "_adjusted"),
        indicator=True,
        validate="one_to_one",
    )
    if not joined["_merge"].eq("both").all():
        raise ValueError("Raw and adjusted price keys differ")
    by_key = {
        (row.symbol, pd.Timestamp(row.ex_date)): (
            float(Decimal(str(row.cash_per_share))),
            float(Decimal(str(row.share_ratio))),
        )
        for row in actions.itertuples()
    }
    reports = []
    for symbol, group in joined.groupby("symbol", sort=True):
        group = group.sort_values("date").reset_index(drop=True)
        factor = group["close_adjusted"] / group["close_raw"]
        if not np.isfinite(factor).all() or (factor <= 0).any():
            raise ValueError(f"Invalid adjusted-price factor for {symbol}")
        multiplicative_errors = []
        for index in range(1, len(group)):
            day = pd.Timestamp(group.loc[index, "date"])
            cash, ratio = by_key.get((symbol, day), (0.0, 1.0))
            previous_close = float(group.loc[index - 1, "close_raw"])
            if cash >= previous_close:
                raise ValueError(f"Dividend cash exceeds previous close for {symbol} on {day.date()}")
            expected = previous_close * ratio / (previous_close - cash)
            observed = float(factor.iloc[index] / factor.iloc[index - 1])
            tolerance = max(0.003, 0.025 / min(float(group.loc[index, "close_raw"]), previous_close))
            multiplicative_errors.append(abs(observed - expected) / tolerance)
        multiplicative_ok = not multiplicative_errors or max(multiplicative_errors) <= 1

        multiplier = 1.0
        offset = float(group.iloc[-1].close_adjusted - group.iloc[-1].close_raw)
        affine_errors = []
        for index in range(len(group) - 1, -1, -1):
            row = group.iloc[index]
            affine_errors.append(
                abs(float(row.close_raw) * multiplier + offset - float(row.close_adjusted))
            )
            cash, ratio = by_key.get((symbol, pd.Timestamp(row.date)), (0.0, 1.0))
            offset -= multiplier * cash / ratio
            multiplier /= ratio
        affine_ok = not affine_errors or max(affine_errors) <= 0.025
        if not (multiplicative_ok or affine_ok):
            raise ValueError(
                f"Corporate-action/adjustment mismatch for {symbol}; a real matching cashflow feed is required"
            )
        reports.append(
            {
                "symbol": symbol,
                "model": "multiplicative" if multiplicative_ok else "affine",
                "factor_changes": int((factor.pct_change().abs().fillna(0) > 1e-12).sum()),
                "cash_actions": sum(key[0] == symbol for key in by_key),
                "max_normalized_multiplicative_error": (
                    float(max(multiplicative_errors)) if multiplicative_errors else 0.0
                ),
                "max_affine_price_error": float(max(affine_errors)) if affine_errors else 0.0,
            }
        )
    return {"passed": True, "symbols": reports}


def _validate_coverage(
    frames: dict[str, pd.DataFrame],
    *,
    symbols: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, Any]:
    calendar = pd.DatetimeIndex(frames["calendar"]["date"]).sort_values().unique()
    expected = pd.DatetimeIndex(calendar[(calendar >= start) & (calendar <= end)])
    if expected.empty or calendar.min() > start or calendar.max() <= end:
        raise ValueError("Trading calendar does not cover the requested interval")
    pre_listing_or_unavailable: dict[str, list[str]] = {}
    missing_after_first_bar: dict[str, list[str]] = {}
    for symbol in symbols:
        actual = set(frames["raw"].loc[frames["raw"]["symbol"] == symbol, "date"])
        if not actual:
            pre_listing_or_unavailable[symbol] = [day.date().isoformat() for day in expected]
            continue
        first_available = min(actual)
        leading = [
            day.date().isoformat()
            for day in expected
            if day < first_available and day not in actual
        ]
        gaps = [
            day.date().isoformat()
            for day in expected
            if day >= first_available and day not in actual
        ]
        if leading:
            pre_listing_or_unavailable[symbol] = leading
        if gaps:
            missing_after_first_bar[symbol] = gaps
    if pre_listing_or_unavailable or missing_after_first_bar:
        report = {
            "pre_listing_or_provider_history_unavailable": pre_listing_or_unavailable,
            "missing_after_first_available_bar": missing_after_first_bar,
            "listing_date_basis": "unavailable-not-inferred",
        }
        raise ValueError(f"ETF daily-price coverage failed: {json.dumps(report, sort_keys=True)}")
    if set(frames["raw"]["symbol"]) != set(symbols):
        raise ValueError("ETF price response does not cover the requested symbols")
    benchmark_dates = set(frames["benchmark"]["date"])
    benchmark_missing = [day.date().isoformat() for day in expected[1:] if day not in benchmark_dates]
    if benchmark_missing:
        raise ValueError(f"Benchmark gaps detected: {benchmark_missing}")
    future_sessions = calendar[calendar > end]
    if len(future_sessions) == 0:
        raise ValueError("Trading calendar lacks a session after the dataset end")
    return {
        "passed": True,
        "sessions": len(expected),
        "symbols": len(symbols),
        "next_session": pd.Timestamp(future_sessions[0]).date().isoformat(),
        "pre_listing_or_provider_history_unavailable": {},
        "missing_after_first_available_bar": {},
        "listing_date_basis": "unavailable-not-inferred",
    }


def validate_dataset_frames(
    frames: dict[str, pd.DataFrame],
    *,
    symbols: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, Any]:
    """Validate coverage, raw/adjusted identity and evidenced cash adjustments."""
    validate_price_frame(frames["raw"])
    validate_price_frame(frames["adjusted"])
    if not frames["raw"]["adjustment"].eq("none").all():
        raise ValueError("raw prices must be unadjusted")
    if not frames["adjusted"]["adjustment"].eq("qfq").all():
        raise ValueError("adjusted prices must use qfq")
    coverage = _validate_coverage(frames, symbols=symbols, start=start, end=end)
    actions = _validate_actions(frames["actions"], frames["raw"])
    adjustments = _validate_adjustments(
        frames["raw"], frames["adjusted"], frames["actions"]
    )
    return {
        "passed": True,
        "coverage": coverage,
        "corporate_actions": actions,
        "adjustment_consistency": adjustments,
    }


def _venue(symbol: str) -> str:
    code = normalize_symbol(symbol)
    if code.startswith("5"):
        return "SSE"
    if code.startswith("1"):
        return "SZSE"
    raise ValueError(f"ETF venue is not declared for symbol {code}")


def _instrument_catalog(
    symbols: list[str],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    captured_at: pd.Timestamp,
    raw: pd.DataFrame,
    source: dict[str, Any],
) -> pd.DataFrame:
    rows = []
    effective_to = end + pd.Timedelta(days=1)
    source_version = source.get(
        "source_version",
        f"{source.get('provider_package', 'provider')}={source.get('provider_package_version', 'unknown')}",
    )
    for symbol in symbols:
        observed = pd.DatetimeIndex(raw.loc[raw["symbol"] == symbol, "date"])
        if observed.empty:
            raise ValueError(f"Cannot catalog ETF without observed prices: {symbol}")
        rows.append(
            {
                "catalog_schema_version": "qdk.etf-instrument-catalog/v1",
                "catalog_source_version": source_version,
                "symbol": symbol,
                "asset_class": "equity",
                "product_type": "etf",
                "venue": _venue(symbol),
                "price_scale": 3,
                "price_tick": "0.001",
                "quantity_step": 1,
                "lot_size": 100,
                "commission_rate": 0,
                "stamp_duty_rate": 0,
                "effective_from": f"{start.date().isoformat()}T00:00:00Z",
                "effective_to": f"{effective_to.date().isoformat()}T00:00:00Z",
                "available_at": captured_at.isoformat(),
                "observed_first_session": observed.min().date().isoformat(),
                "observed_last_session": observed.max().date().isoformat(),
                "listing_date": "",
                "listing_date_basis": "unavailable-not-inferred",
            }
        )
    return pd.DataFrame(rows)


def _history(
    actions: pd.DataFrame,
    symbols: list[str],
    start: pd.Timestamp,
    captured_at: pd.Timestamp,
) -> pd.DataFrame:
    columns = ["domain", "symbol", "field", "effective_at", "available_at", "value"]
    rows = []
    effective_start = start.tz_localize("Asia/Shanghai").tz_convert("UTC")
    for symbol in symbols:
        for field, value in (
            ("product_type", "etf"),
            ("venue", _venue(symbol)),
        ):
            rows.append(
                {
                    "domain": "classification",
                    "symbol": symbol,
                    "field": field,
                    "effective_at": effective_start,
                    "available_at": captured_at,
                    "value": value,
                }
            )
    for row in actions.itertuples():
        effective = pd.Timestamp(row.ex_date).tz_localize("Asia/Shanghai").tz_convert("UTC")
        for field in ("cash_per_share", "share_ratio"):
            rows.append(
                {
                    "domain": "corporate_actions",
                    "symbol": row.symbol,
                    "field": f"{row.event_id}:{field}",
                    "effective_at": effective,
                    # This is a captured-current view. We do not invent an intraday
                    # historical publication timestamp from a date-only announcement.
                    "available_at": captured_at,
                    "value": str(getattr(row, field)),
                }
            )
    return validate_history(pd.DataFrame(rows, columns=columns))


def _history_source_payload(
    actions: pd.DataFrame,
    *,
    symbols: list[str],
    source: dict[str, Any],
) -> bytes:
    records = json.loads(actions.to_json(orient="records", date_format="iso", force_ascii=False))
    return _canonical_bytes(
        {
            "schema_version": "qdk.research-history-source/v1",
            "symbols": symbols,
            "source": source,
            "corporate_action_source_records": records,
            "classification_declaration": {
                symbol: {"product_type": "etf", "venue": _venue(symbol)}
                for symbol in symbols
            },
        }
    )


def _write_atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid4().hex}")
    temporary.write_bytes(_canonical_bytes(payload))
    os.replace(temporary, path)


def _manifest_identity_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in manifest.items()
        if key not in {"snapshot_id", "identity_sha256"}
    }


def _load_catalog(root: Path, *, required: bool) -> dict[str, Any]:
    path = root / "catalog.json"
    if not path.is_file():
        if required:
            raise FileNotFoundError(f"Research dataset catalog missing: {path}")
        return {
            "schema_version": CATALOG_SCHEMA_VERSION,
            "current_snapshot_id": None,
            "snapshots": [],
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != CATALOG_SCHEMA_VERSION:
        raise ValueError("Unsupported research dataset catalog schema")
    return payload


def load_research_snapshot(root: Path, snapshot_id: str | None = None) -> tuple[dict, dict]:
    """Load one explicit or catalog-selected immutable snapshot and verify all hashes."""
    root = root.resolve()
    catalog = _load_catalog(root, required=True)
    selected = snapshot_id or catalog.get("current_snapshot_id")
    if not isinstance(selected, str) or not selected.startswith("sha256-"):
        raise ValueError("Research dataset requires an explicit content-addressed snapshot")
    snapshot = root / "snapshots" / selected
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("snapshot_id") != selected:
        raise ValueError("Research dataset snapshot identity mismatch")
    frames = {}
    for name, entry in manifest["files"].items():
        path = (snapshot / entry["file"]).resolve()
        try:
            path.relative_to(snapshot.resolve())
        except ValueError as exc:
            raise ValueError(f"Research dataset snapshot path escapes: {name}") from exc
        if _sha256(path) != entry["sha256"]:
            raise ValueError(f"Research dataset snapshot integrity failed: {name}")
        file_format = entry.get("format", path.suffix.lstrip("."))
        if file_format == "parquet":
            frames[name] = pd.read_parquet(path)
        elif file_format == "csv":
            frames[name] = pd.read_csv(path, dtype={"symbol": str})
        else:
            raise ValueError(f"Unsupported research dataset file format: {file_format}")
    for name, entry in manifest.get("evidence_files", {}).items():
        path = (snapshot / entry["file"]).resolve()
        try:
            path.relative_to(snapshot.resolve())
        except ValueError as exc:
            raise ValueError(f"Research dataset evidence path escapes: {name}") from exc
        if _sha256(path) != entry["sha256"]:
            raise ValueError(f"Research dataset evidence integrity failed: {name}")
    identity = _manifest_identity_payload(manifest)
    digest = hashlib.sha256(_canonical_bytes(identity)).hexdigest()
    if manifest.get("identity_sha256") != digest or selected != f"sha256-{digest}":
        raise ValueError("Research dataset manifest identity changed")
    return manifest, frames


def _publish(
    root: Path,
    frames: dict[str, pd.DataFrame],
    *,
    symbols: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
    captured_at: pd.Timestamp,
    source: dict[str, Any],
    validation: dict[str, Any],
    parent_snapshot_id: str | None,
    update_evidence: dict[str, Any],
) -> dict[str, Any]:
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    staging = root / f".snapshot.tmp-{uuid4().hex}"
    staging.mkdir()
    try:
        history = _history(frames["actions"], symbols, start, captured_at)
        catalog_frame = _instrument_catalog(
            symbols,
            start=start,
            end=end,
            captured_at=captured_at,
            raw=frames["raw"],
            source=source,
        )
        frames = {**frames, "catalog": catalog_frame, "history": history}
        files = {}
        providers = {
            "raw": source.get("provider", source["mode"]),
            "adjusted": source.get("provider", source["mode"]),
            "benchmark": "akshare_sina" if source["mode"] == "live_public_api" else source["mode"],
            "calendar": "akshare_sina" if source["mode"] == "live_public_api" else source["mode"],
            "actions": (
                "akshare_eastmoney_fund_f10"
                if source["mode"] == "live_public_api"
                else source["mode"]
            ),
            "catalog": "qdk.etf-instrument-catalog/v1",
            "history": "derived_from_actions_captured_view",
        }
        for name, filename in FILE_NAMES.items():
            path = staging / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            file_format = path.suffix.lstrip(".")
            if file_format == "parquet":
                frames[name].to_parquet(path, index=False)
            elif file_format == "csv":
                frames[name].to_csv(path, index=False)
            else:
                raise ValueError(f"Unsupported research dataset output format: {file_format}")
            files[name] = {
                "file": filename,
                "sha256": _sha256(path),
                "rows": len(frames[name]),
                "provider": providers[name],
                "format": file_format,
            }
        history_root = staging / "history"
        history_source = history_root / "source.json"
        history_source.write_bytes(
            _history_source_payload(frames["actions"], symbols=symbols, source=source)
        )
        history_manifest = {
            "schema_version": HISTORY_SCHEMA,
            "provider": source.get("provider", source["mode"]),
            "source_uri": source.get(
                "source_uri", "akshare://sina-etf+eastmoney-fund-f10"
            ),
            "license_note": source.get(
                "license_note", "Public web sources; rights remain with source providers."
            ),
            "scope": "captured-current-source-declaration",
            "original": {"file": history_source.name, "sha256": _sha256(history_source)},
            "history": {
                "file": Path(FILE_NAMES["history"]).name,
                "sha256": files["history"]["sha256"],
            },
            "rows": len(history),
            "domains": sorted(history["domain"].unique()),
            "symbols": sorted(history["symbol"].unique()),
        }
        history_manifest_path = history_root / "manifest.json"
        history_manifest_path.write_bytes(_canonical_bytes(history_manifest))
        evidence_files = {
            "history_manifest": {
                "file": "history/manifest.json",
                "sha256": _sha256(history_manifest_path),
            },
            "history_source": {
                "file": "history/source.json",
                "sha256": _sha256(history_source),
            },
        }
        query = {
            "symbols": symbols,
            "start": start.date().isoformat(),
            "end": end.date().isoformat(),
        }
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "parent_snapshot_id": parent_snapshot_id,
            "origin": source["mode"],
            "captured_at": captured_at.isoformat(),
            "requested_start": query["start"],
            "requested_end": query["end"],
            "symbols": symbols,
            "query": query,
            "source": source,
            "files": files,
            "evidence_files": evidence_files,
            "validation": validation,
            "update_evidence": update_evidence,
            "warnings": [],
            "consumer_contract": {
                "asm_decision_workflow_load_inputs": True,
                "history_availability": "captured-current; no inferred intraday announcement time",
            },
        }
        digest = hashlib.sha256(_canonical_bytes(manifest)).hexdigest()
        snapshot_id = f"sha256-{digest}"
        manifest = {"snapshot_id": snapshot_id, "identity_sha256": digest, **manifest}
        (staging / "manifest.json").write_bytes(_canonical_bytes(manifest))
        destination = root / "snapshots" / snapshot_id
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            existing = json.loads((destination / "manifest.json").read_text(encoding="utf-8"))
            if existing != manifest:
                raise ValueError(f"Immutable research snapshot collision: {snapshot_id}")
            shutil.rmtree(staging)
        else:
            os.replace(staging, destination)

        catalog = _load_catalog(root, required=False)
        known = {entry["snapshot_id"] for entry in catalog["snapshots"]}
        if snapshot_id not in known:
            catalog["snapshots"].append(
                {
                    "snapshot_id": snapshot_id,
                    "parent_snapshot_id": parent_snapshot_id,
                    "captured_at": captured_at.isoformat(),
                    "start": query["start"],
                    "end": query["end"],
                    "manifest_sha256": _sha256(destination / "manifest.json"),
                }
            )
        catalog["current_snapshot_id"] = snapshot_id
        _write_atomic_json(root / "catalog.json", catalog)
        return manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def build_dataset(
    root: Path,
    *,
    symbols: list[str],
    start: str,
    end: str,
    captured_at: str | None = None,
    provider: str = DEFAULT_PROVIDER,
    source_dir: Path | None = None,
    source_uri: str = "",
    source_version: str = "",
    license_note: str = "",
) -> dict[str, Any]:
    """Build the first immutable ETF snapshot below ``root``."""
    root = root.resolve()
    if (root / "catalog.json").exists():
        raise FileExistsError(f"Research dataset already exists: {root}")
    normalized_symbols = list(dict.fromkeys(normalize_symbol(item) for item in symbols))
    if not normalized_symbols:
        raise ValueError("At least one ETF symbol is required")
    start_day, end_day = _day(start, "start"), _day(end, "end")
    if start_day > end_day:
        raise ValueError("start must not be after end")
    captured = _captured_at(captured_at)
    if end_day > captured.tz_convert("Asia/Shanghai").tz_localize(None).normalize():
        raise ValueError("Research dataset cannot include a future session")
    if source_dir is None:
        frames, source = _fetch_live_source(
            normalized_symbols,
            price_start=start_day,
            dataset_start=start_day,
            end=end_day,
            captured_at=captured,
            provider=provider,
        )
    else:
        frames, source = _load_local_source(
            source_dir,
            source_uri=source_uri,
            source_version=source_version,
            license_note=license_note,
        )
    frames = _normalize_frames(
        frames, symbols=normalized_symbols, start=start_day, end=end_day
    )
    validation = validate_dataset_frames(
        frames, symbols=normalized_symbols, start=start_day, end=end_day
    )
    return _publish(
        root,
        frames,
        symbols=normalized_symbols,
        start=start_day,
        end=end_day,
        captured_at=captured,
        source=source,
        validation=validation,
        parent_snapshot_id=None,
        update_evidence={"mode": "initial_build", "revised_rows": {}},
    )


def update_dataset(
    root: Path,
    *,
    end: str,
    captured_at: str | None = None,
    overlap_sessions: int = 5,
    source_dir: Path | None = None,
    source_uri: str = "",
    source_version: str = "",
    license_note: str = "",
) -> dict[str, Any]:
    """Refresh a bounded overlap and publish a new immutable child snapshot."""
    if overlap_sessions < 1:
        raise ValueError("overlap_sessions must be positive")
    previous, old = load_research_snapshot(root)
    symbols = list(previous["symbols"])
    start_day = _day(previous["requested_start"], "start")
    end_day = _day(end, "end")
    old_end = _day(previous["requested_end"], "previous end")
    if end_day < old_end:
        raise ValueError("Incremental update cannot shorten the dataset")
    captured = _captured_at(captured_at)
    if end_day > captured.tz_convert("Asia/Shanghai").tz_localize(None).normalize():
        raise ValueError("Research dataset cannot include a future session")
    old_dates = pd.DatetimeIndex(old["raw"]["date"].drop_duplicates().sort_values())
    price_start = old_dates[max(0, len(old_dates) - overlap_sessions)]
    if source_dir is None:
        prior_source = previous["source"]
        if prior_source.get("mode") != "live_public_api":
            raise ValueError("A local-source dataset update requires source_dir and declarations")
        frames, source = _fetch_live_source(
            symbols,
            price_start=price_start,
            dataset_start=start_day,
            end=end_day,
            captured_at=captured,
            provider=prior_source["provider"],
        )
    else:
        frames, source = _load_local_source(
            source_dir,
            source_uri=source_uri,
            source_version=source_version,
            license_note=license_note,
        )
    frames = _normalize_frames(frames, symbols=symbols, start=start_day, end=end_day)
    revised = {}
    merged = {}
    for name, keys in (
        ("raw", ["symbol", "date"]),
        ("adjusted", ["symbol", "date"]),
        ("benchmark", ["date"]),
    ):
        merged[name], revised[name] = _merge_by_key(old[name], frames[name], keys, end=end_day)
    merged["calendar"] = frames["calendar"]
    # The ETF action endpoints return the complete fund history. Treat the new
    # response as the authoritative current vintage and retain old rows only
    # when a declared local update contains a bounded fragment.
    if source["mode"] == "live_public_api":
        merged["actions"] = frames["actions"]
        old_action_ids = set(old["actions"].get("event_id", []))
        new_action_ids = set(frames["actions"].get("event_id", []))
        revised["actions"] = len(old_action_ids.symmetric_difference(new_action_ids))
    elif frames["actions"].empty:
        merged["actions"] = old["actions"].copy()
        revised["actions"] = 0
    else:
        merged["actions"], revised["actions"] = _merge_by_key(
            old["actions"], frames["actions"], ["symbol", "ex_date"], end=end_day
        )
    merged = _normalize_frames(merged, symbols=symbols, start=start_day, end=end_day)
    validation = validate_dataset_frames(
        merged, symbols=symbols, start=start_day, end=end_day
    )
    return _publish(
        root,
        merged,
        symbols=symbols,
        start=start_day,
        end=end_day,
        captured_at=captured,
        source=source,
        validation=validation,
        parent_snapshot_id=previous["snapshot_id"],
        update_evidence={
            "mode": "overlap_refresh",
            "overlap_sessions": overlap_sessions,
            "fetched_from": pd.Timestamp(price_start).date().isoformat(),
            "previous_end": old_end.date().isoformat(),
            "revised_rows": revised,
        },
    )


def inspect_dataset(root: Path, snapshot_id: str | None = None) -> dict[str, Any]:
    """Verify an immutable snapshot and return a concise evidence summary."""
    manifest, frames = load_research_snapshot(root, snapshot_id)
    validation = validate_dataset_frames(
        frames,
        symbols=list(manifest["symbols"]),
        start=_day(manifest["requested_start"], "start"),
        end=_day(manifest["requested_end"], "end"),
    )
    return {
        "snapshot_id": manifest["snapshot_id"],
        "parent_snapshot_id": manifest["parent_snapshot_id"],
        "captured_at": manifest["captured_at"],
        "source": manifest["source"],
        "query": manifest["query"],
        "files": manifest["files"],
        "validation": validation,
        "update_evidence": manifest["update_evidence"],
        "snapshot_path": str(Path(root).resolve() / "snapshots" / manifest["snapshot_id"]),
    }


def _source_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-dir", type=Path)
    parser.add_argument("--source-uri", default="")
    parser.add_argument("--source-version", default="")
    parser.add_argument("--license-note", default="")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m quant_data_kit.research_dataset",
        description="Build and maintain immutable real ETF research datasets",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--root", type=Path, required=True)
    build.add_argument("--symbols", nargs="+", required=True)
    build.add_argument("--start", required=True)
    build.add_argument("--end", required=True)
    build.add_argument("--captured-at")
    build.add_argument("--provider", default=DEFAULT_PROVIDER)
    _source_arguments(build)

    update = commands.add_parser("update")
    update.add_argument("--root", type=Path, required=True)
    update.add_argument("--end", required=True)
    update.add_argument("--captured-at")
    update.add_argument("--overlap-sessions", type=int, default=5)
    _source_arguments(update)

    inspect = commands.add_parser("inspect")
    inspect.add_argument("--root", type=Path, required=True)
    inspect.add_argument("--snapshot-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "build":
        manifest = build_dataset(
            args.root,
            symbols=args.symbols,
            start=args.start,
            end=args.end,
            captured_at=args.captured_at,
            provider=args.provider,
            source_dir=args.source_dir,
            source_uri=args.source_uri,
            source_version=args.source_version,
            license_note=args.license_note,
        )
        result = inspect_dataset(args.root, manifest["snapshot_id"])
    elif args.command == "update":
        manifest = update_dataset(
            args.root,
            end=args.end,
            captured_at=args.captured_at,
            overlap_sessions=args.overlap_sessions,
            source_dir=args.source_dir,
            source_uri=args.source_uri,
            source_version=args.source_version,
            license_note=args.license_note,
        )
        result = inspect_dataset(args.root, manifest["snapshot_id"])
    else:
        result = inspect_dataset(args.root, args.snapshot_id)
    print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
