"""Shared quant data utilities: Parquet cache, calendar, validation, AKShare providers."""

__version__ = "0.1.0"

from quant_data_kit.calendar import trading_days_between
from quant_data_kit.storage import (
    DataManifest,
    load_manifest,
    load_parquet,
    save_manifest,
    save_parquet,
    write_manifest,
)
from quant_data_kit.validate import validate_price_frame

__all__ = [
    "__version__",
    "DataManifest",
    "load_manifest",
    "load_parquet",
    "save_manifest",
    "save_parquet",
    "trading_days_between",
    "validate_price_frame",
    "write_manifest",
]
