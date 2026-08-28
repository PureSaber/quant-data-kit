"""Shared quant data utilities: Parquet cache, calendar, validation, AKShare providers."""

__version__ = "0.4.0"

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
from quant_data_kit.snapshots import DatasetSnapshot, create_snapshot, load_snapshot
from quant_data_kit.temporal import TemporalAudit, audit_point_in_time, point_in_time_join
from quant_data_kit.domain_v2 import (
    AssetClass,
    InstrumentSpec,
    MarginMode,
    SessionPhase,
    SymbolMapping,
    TradingSession,
    dataclass_payload,
)
from quant_data_kit.fixed_point import FixedPoint
from quant_data_kit.market_clock_v2 import MarketClock
from quant_data_kit.market_events_v2 import (
    AggressorSide,
    BarEvent,
    BookAction,
    BookDeltaEvent,
    BookLevel,
    BookSide,
    BookSnapshotEvent,
    CorporateActionEvent,
    FundingRateEvent,
    MarketEvent,
    MarkPriceEvent,
    QuoteEvent,
    StatusEvent,
    TradeEvent,
    market_event_payload,
)
from quant_data_kit.schemas_v2 import (
    SCHEMA_VERSION_V2,
    get_arrow_schema,
    get_json_schema,
    validate_arrow_table,
    validate_json_record,
)
from quant_data_kit.temporal_v2 import (
    BitemporalAudit,
    ensure_utc_datetime,
    point_in_time_join_bitemporal,
    validate_bitemporal_frame,
)

__all__ = [
    "AggressorSide",
    "AssetClass",
    "BarEvent",
    "BitemporalAudit",
    "BookAction",
    "BookDeltaEvent",
    "BookLevel",
    "BookSide",
    "BookSnapshotEvent",
    "CorporateActionEvent",
    "DataManifest",
    "DatasetSnapshot",
    "FixedPoint",
    "InstrumentSpec",
    "FundingRateEvent",
    "MarginMode",
    "MarketClock",
    "MarketEvent",
    "MarkPriceEvent",
    "QuoteEvent",
    "SCHEMA_VERSION_V2",
    "SessionPhase",
    "StatusEvent",
    "SymbolMapping",
    "TemporalAudit",
    "TradeEvent",
    "TradingSession",
    "__version__",
    "add_industry_relative_strength",
    "audit_point_in_time",
    "cache_covers_range",
    "create_snapshot",
    "dataclass_payload",
    "ensure_utc_datetime",
    "get_arrow_schema",
    "get_json_schema",
    "incremental_start_date",
    "load_manifest",
    "load_parquet",
    "load_snapshot",
    "market_event_payload",
    "merge_earnings_to_panel",
    "merge_northbound_to_panel",
    "parse_date",
    "point_in_time_join",
    "point_in_time_join_bitemporal",
    "save_manifest",
    "save_parquet",
    "should_refresh_cache",
    "trading_days_between",
    "validate_bitemporal_frame",
    "validate_arrow_table",
    "validate_json_record",
    "validate_price_frame",
    "write_manifest",
]
