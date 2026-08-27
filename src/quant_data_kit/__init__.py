"""Shared quant data utilities: Parquet cache, calendar, validation, AKShare providers."""

__version__ = "0.3.0"

from quant_data_kit.calendar import trading_days_between
from quant_data_kit.panel import (
    add_industry_relative_strength,
    merge_earnings_to_panel,
    merge_northbound_to_panel,
)
from quant_data_kit.storage import (
    DataManifest,
    cache_covers_range,
    incremental_start_date,
    load_manifest,
    load_parquet,
    parse_date,
    save_manifest,
    save_parquet,
    should_refresh_cache,
    write_manifest,
)
from quant_data_kit.validate import validate_price_frame

__all__ = [
    "DataManifest",
    "__version__",
    "add_industry_relative_strength",
    "cache_covers_range",
    "incremental_start_date",
    "load_manifest",
    "load_parquet",
    "merge_earnings_to_panel",
    "merge_northbound_to_panel",
    "parse_date",
    "save_manifest",
    "save_parquet",
    "should_refresh_cache",
    "trading_days_between",
    "validate_price_frame",
    "write_manifest",
]
from quant_data_kit.snapshots import DatasetSnapshot, create_snapshot, load_snapshot
from quant_data_kit.temporal import TemporalAudit, audit_point_in_time, point_in_time_join

__all__ = [
    "DatasetSnapshot",
    "TemporalAudit",
    "audit_point_in_time",
    "create_snapshot",
    "load_snapshot",
    "point_in_time_join",
]
