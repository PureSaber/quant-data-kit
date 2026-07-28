"""Parquet I/O and dataset manifest tracking."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


@dataclass
class DataManifest:
    dataset: str
    path: str
    rows: int
    columns: list[str]
    date_min: str | None = None
    date_max: str | None = None
    symbol_count: int | None = None
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    extra: dict[str, str | int | float | bool] = field(default_factory=dict)


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def load_parquet(path: Path) -> pd.DataFrame:
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Parquet file not found: {path}")
    return pd.read_parquet(path)


def _date_bounds(df: pd.DataFrame, date_col: str = "date") -> tuple[str | None, str | None]:
    if df.empty or date_col not in df.columns:
        return None, None
    dates = pd.to_datetime(df[date_col])
    return dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")


def build_manifest(
    df: pd.DataFrame,
    dataset: str,
    path: Path,
    *,
    date_col: str = "date",
    symbol_col: str = "symbol",
    extra: dict[str, str | int | float | bool] | None = None,
) -> DataManifest:
    dmin, dmax = _date_bounds(df, date_col)
    sym_count = int(df[symbol_col].nunique()) if symbol_col in df.columns and not df.empty else None
    return DataManifest(
        dataset=dataset,
        path=str(path),
        rows=len(df),
        columns=list(df.columns),
        date_min=dmin,
        date_max=dmax,
        symbol_count=sym_count,
        extra=extra or {},
    )


def write_manifest(manifest: DataManifest, manifest_path: Path) -> None:
    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(asdict(manifest), indent=2, ensure_ascii=False), encoding="utf-8")


def save_manifest(
    df: pd.DataFrame,
    parquet_path: Path,
    *,
    dataset: str,
    manifest_path: Path | None = None,
    date_col: str = "date",
    symbol_col: str = "symbol",
    extra: dict[str, str | int | float | bool] | None = None,
) -> DataManifest:
    save_parquet(df, parquet_path)
    manifest = build_manifest(
        df, dataset, parquet_path, date_col=date_col, symbol_col=symbol_col, extra=extra
    )
    target = manifest_path or parquet_path.with_suffix(".manifest.json")
    write_manifest(manifest, target)
    return manifest


def load_manifest(path: Path) -> DataManifest:
    path = Path(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return DataManifest(**payload)


def cache_covers_range(
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
    *,
    date_col: str = "date",
) -> bool:
    if df.empty or date_col not in df.columns:
        return False
    dates = pd.to_datetime(df[date_col])
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    return dates.min() <= start and dates.max() >= end
