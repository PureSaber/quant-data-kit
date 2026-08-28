"""Arrow and JSON schema registry for cross-asset data contracts."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pyarrow as pa
from jsonschema import Draft202012Validator, FormatChecker

from quant_data_kit.exceptions import ValidationError

SCHEMA_VERSION_V2 = "2.0.0"

INSTRUMENT_SPEC_SCHEMA_ID = "puresaber.instrument-spec"
SYMBOL_MAPPING_SCHEMA_ID = "puresaber.symbol-mapping"
TRADING_SESSION_SCHEMA_ID = "puresaber.trading-session"
QUOTE_EVENT_SCHEMA_ID = "puresaber.quote-event"
TRADE_EVENT_SCHEMA_ID = "puresaber.trade-event"
BAR_EVENT_SCHEMA_ID = "puresaber.bar-event"
BOOK_SNAPSHOT_EVENT_SCHEMA_ID = "puresaber.book-snapshot-event"
BOOK_DELTA_EVENT_SCHEMA_ID = "puresaber.book-delta-event"
FUNDING_RATE_EVENT_SCHEMA_ID = "puresaber.funding-rate-event"
MARK_PRICE_EVENT_SCHEMA_ID = "puresaber.mark-price-event"
CORPORATE_ACTION_EVENT_SCHEMA_ID = "puresaber.corporate-action-event"
STATUS_EVENT_SCHEMA_ID = "puresaber.status-event"

_UTC = pa.timestamp("ns", tz="UTC")
_FIXED_POINT = pa.struct(
    [pa.field("units", pa.int64(), nullable=False), pa.field("scale", pa.int16(), nullable=False)]
)
_BOOK_LEVEL = pa.struct(
    [
        pa.field("price", _FIXED_POINT, nullable=False),
        pa.field("quantity", _FIXED_POINT, nullable=False),
        pa.field("order_count", pa.int64(), nullable=True),
    ]
)


def _field(name: str, data_type: pa.DataType, *, nullable: bool = False) -> pa.Field:
    return pa.field(name, data_type, nullable=nullable)


_COMMON_EVENT_FIELDS = [
    _field("event_type", pa.string()),
    _field("event_id", pa.string()),
    _field("instrument_id", pa.string()),
    _field("event_time", _UTC),
    _field("received_at", _UTC),
    _field("available_at", _UTC),
    _field("source", pa.string()),
    _field("trading_day", pa.date32()),
    _field("session_id", pa.string()),
    _field("sequence", pa.int64(), nullable=True),
]

_ARROW_SCHEMAS: dict[str, pa.Schema] = {
    INSTRUMENT_SPEC_SCHEMA_ID: pa.schema(
        [
            _field("instrument_id", pa.string()),
            _field("asset_class", pa.string()),
            _field("product_type", pa.string()),
            _field("venue", pa.string()),
            _field("native_symbol", pa.string()),
            _field("base_currency", pa.string(), nullable=True),
            _field("quote_currency", pa.string(), nullable=True),
            _field("settlement_currency", pa.string()),
            _field("price_tick", _FIXED_POINT),
            _field("quantity_step", _FIXED_POINT),
            _field("contract_multiplier", _FIXED_POINT),
            _field("calendar_id", pa.string()),
            _field("margin_mode", pa.string()),
            _field("inverse", pa.bool_()),
            _field("effective_from", _UTC),
            _field("effective_to", _UTC, nullable=True),
            _field("available_at", _UTC),
            _field("superseded_at", _UTC, nullable=True),
            _field("underlying_id", pa.string(), nullable=True),
            _field("expiry_date", pa.date32(), nullable=True),
            _field("metadata", pa.map_(pa.string(), pa.string())),
        ]
    ),
    SYMBOL_MAPPING_SCHEMA_ID: pa.schema(
        [
            _field("source", pa.string()),
            _field("provider_symbol", pa.string()),
            _field("instrument_id", pa.string()),
            _field("effective_from", _UTC),
            _field("effective_to", _UTC, nullable=True),
            _field("available_at", _UTC),
            _field("superseded_at", _UTC, nullable=True),
        ]
    ),
    TRADING_SESSION_SCHEMA_ID: pa.schema(
        [
            _field("session_id", pa.string()),
            _field("calendar_id", pa.string()),
            _field("venue", pa.string()),
            _field("trading_day", pa.date32()),
            _field("phase", pa.string()),
            _field("opens_at", _UTC),
            _field("closes_at", _UTC),
            _field("available_at", _UTC),
            _field("superseded_at", _UTC, nullable=True),
        ]
    ),
    QUOTE_EVENT_SCHEMA_ID: pa.schema(
        _COMMON_EVENT_FIELDS
        + [
            _field("bid_price", _FIXED_POINT),
            _field("bid_quantity", _FIXED_POINT),
            _field("ask_price", _FIXED_POINT),
            _field("ask_quantity", _FIXED_POINT),
        ]
    ),
    TRADE_EVENT_SCHEMA_ID: pa.schema(
        _COMMON_EVENT_FIELDS
        + [
            _field("price", _FIXED_POINT),
            _field("quantity", _FIXED_POINT),
            _field("aggressor_side", pa.string()),
        ]
    ),
    BAR_EVENT_SCHEMA_ID: pa.schema(
        _COMMON_EVENT_FIELDS
        + [
            _field("bar_start", _UTC),
            _field("bar_end", _UTC),
            _field("open_price", _FIXED_POINT),
            _field("high_price", _FIXED_POINT),
            _field("low_price", _FIXED_POINT),
            _field("close_price", _FIXED_POINT),
            _field("volume", _FIXED_POINT),
            _field("is_complete", pa.bool_()),
        ]
    ),
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID: pa.schema(
        _COMMON_EVENT_FIELDS
        + [_field("bids", pa.list_(_BOOK_LEVEL)), _field("asks", pa.list_(_BOOK_LEVEL))]
    ),
    BOOK_DELTA_EVENT_SCHEMA_ID: pa.schema(
        _COMMON_EVENT_FIELDS
        + [
            _field("side", pa.string()),
            _field("action", pa.string()),
            _field("price", _FIXED_POINT),
            _field("quantity", _FIXED_POINT),
            _field("previous_sequence", pa.int64()),
        ]
    ),
    FUNDING_RATE_EVENT_SCHEMA_ID: pa.schema(
        _COMMON_EVENT_FIELDS
        + [
            _field("rate", pa.float64()),
            _field("interval_start", _UTC),
            _field("interval_end", _UTC),
        ]
    ),
    MARK_PRICE_EVENT_SCHEMA_ID: pa.schema(
        _COMMON_EVENT_FIELDS + [_field("price", _FIXED_POINT)]
    ),
    CORPORATE_ACTION_EVENT_SCHEMA_ID: pa.schema(
        _COMMON_EVENT_FIELDS
        + [
            _field("action_type", pa.string()),
            _field("effective_date", pa.date32()),
            _field("ratio", _FIXED_POINT, nullable=True),
            _field("cash_amount", _FIXED_POINT, nullable=True),
            _field("currency", pa.string(), nullable=True),
        ]
    ),
    STATUS_EVENT_SCHEMA_ID: pa.schema(
        _COMMON_EVENT_FIELDS
        + [_field("status", pa.string()), _field("reason", pa.string())]
    ),
}

_FIXED_POINT_JSON = {
    "type": "object",
    "additionalProperties": False,
    "required": ["units", "scale"],
    "properties": {
        "units": {"type": "integer", "minimum": -(2**63), "maximum": 2**63 - 1},
        "scale": {"type": "integer", "minimum": 0, "maximum": 18},
    },
}
_BOOK_LEVEL_JSON = {
    "type": "object",
    "additionalProperties": False,
    "required": ["price", "quantity", "order_count"],
    "properties": {
        "price": _FIXED_POINT_JSON,
        "quantity": _FIXED_POINT_JSON,
        "order_count": {"type": ["integer", "null"], "minimum": 0},
    },
}
_UTC_JSON = {"type": "string", "format": "date-time", "pattern": "Z$"}
_NULLABLE_UTC_JSON = {"oneOf": [_UTC_JSON, {"type": "null"}]}
_NULLABLE_STRING = {"type": ["string", "null"]}


def _object_schema(properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "properties": properties,
    }


_COMMON_EVENT_JSON = {
    "event_type": {"type": "string"},
    "event_id": {"type": "string", "minLength": 1},
    "instrument_id": {"type": "string", "minLength": 1},
    "event_time": _UTC_JSON,
    "received_at": _UTC_JSON,
    "available_at": _UTC_JSON,
    "source": {"type": "string", "minLength": 1},
    "trading_day": {"type": "string", "format": "date"},
    "session_id": {"type": "string", "minLength": 1},
    "sequence": {"type": ["integer", "null"], "minimum": 0},
}

_JSON_SCHEMAS: dict[str, dict[str, Any]] = {
    INSTRUMENT_SPEC_SCHEMA_ID: _object_schema(
        {
            "instrument_id": {"type": "string", "minLength": 1},
            "asset_class": {"type": "string"},
            "product_type": {"type": "string", "minLength": 1},
            "venue": {"type": "string", "minLength": 1},
            "native_symbol": {"type": "string", "minLength": 1},
            "base_currency": _NULLABLE_STRING,
            "quote_currency": _NULLABLE_STRING,
            "settlement_currency": {"type": "string", "minLength": 1},
            "price_tick": _FIXED_POINT_JSON,
            "quantity_step": _FIXED_POINT_JSON,
            "contract_multiplier": _FIXED_POINT_JSON,
            "calendar_id": {"type": "string", "minLength": 1},
            "margin_mode": {"type": "string"},
            "inverse": {"type": "boolean"},
            "effective_from": _UTC_JSON,
            "effective_to": _NULLABLE_UTC_JSON,
            "available_at": _UTC_JSON,
            "superseded_at": _NULLABLE_UTC_JSON,
            "underlying_id": _NULLABLE_STRING,
            "expiry_date": {
                "oneOf": [{"type": "string", "format": "date"}, {"type": "null"}]
            },
            "metadata": {"type": "object", "additionalProperties": {"type": "string"}},
        },
        list(_ARROW_SCHEMAS[INSTRUMENT_SPEC_SCHEMA_ID].names),
    ),
    SYMBOL_MAPPING_SCHEMA_ID: _object_schema(
        {
            "source": {"type": "string", "minLength": 1},
            "provider_symbol": {"type": "string", "minLength": 1},
            "instrument_id": {"type": "string", "minLength": 1},
            "effective_from": _UTC_JSON,
            "effective_to": _NULLABLE_UTC_JSON,
            "available_at": _UTC_JSON,
            "superseded_at": _NULLABLE_UTC_JSON,
        },
        list(_ARROW_SCHEMAS[SYMBOL_MAPPING_SCHEMA_ID].names),
    ),
    TRADING_SESSION_SCHEMA_ID: _object_schema(
        {
            "session_id": {"type": "string", "minLength": 1},
            "calendar_id": {"type": "string", "minLength": 1},
            "venue": {"type": "string", "minLength": 1},
            "trading_day": {"type": "string", "format": "date"},
            "phase": {"type": "string"},
            "opens_at": _UTC_JSON,
            "closes_at": _UTC_JSON,
            "available_at": _UTC_JSON,
            "superseded_at": _NULLABLE_UTC_JSON,
        },
        list(_ARROW_SCHEMAS[TRADING_SESSION_SCHEMA_ID].names),
    ),
}


def _event_schema(event_type: str, extra: dict[str, Any], schema_id: str) -> dict[str, Any]:
    properties = deepcopy(_COMMON_EVENT_JSON)
    properties["event_type"] = {"const": event_type}
    properties.update(extra)
    return _object_schema(properties, list(_ARROW_SCHEMAS[schema_id].names))


_JSON_SCHEMAS.update(
    {
        QUOTE_EVENT_SCHEMA_ID: _event_schema(
            "quote",
            {
                "bid_price": _FIXED_POINT_JSON,
                "bid_quantity": _FIXED_POINT_JSON,
                "ask_price": _FIXED_POINT_JSON,
                "ask_quantity": _FIXED_POINT_JSON,
            },
            QUOTE_EVENT_SCHEMA_ID,
        ),
        TRADE_EVENT_SCHEMA_ID: _event_schema(
            "trade",
            {
                "price": _FIXED_POINT_JSON,
                "quantity": _FIXED_POINT_JSON,
                "aggressor_side": {"enum": ["buy", "sell", "unknown"]},
            },
            TRADE_EVENT_SCHEMA_ID,
        ),
        BAR_EVENT_SCHEMA_ID: _event_schema(
            "bar",
            {
                "bar_start": _UTC_JSON,
                "bar_end": _UTC_JSON,
                "open_price": _FIXED_POINT_JSON,
                "high_price": _FIXED_POINT_JSON,
                "low_price": _FIXED_POINT_JSON,
                "close_price": _FIXED_POINT_JSON,
                "volume": _FIXED_POINT_JSON,
                "is_complete": {"type": "boolean"},
            },
            BAR_EVENT_SCHEMA_ID,
        ),
        BOOK_SNAPSHOT_EVENT_SCHEMA_ID: _event_schema(
            "book_snapshot",
            {
                "bids": {"type": "array", "minItems": 1, "items": _BOOK_LEVEL_JSON},
                "asks": {"type": "array", "minItems": 1, "items": _BOOK_LEVEL_JSON},
            },
            BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
        ),
        BOOK_DELTA_EVENT_SCHEMA_ID: _event_schema(
            "book_delta",
            {
                "side": {"enum": ["bid", "ask"]},
                "action": {"enum": ["upsert", "delete"]},
                "price": _FIXED_POINT_JSON,
                "quantity": _FIXED_POINT_JSON,
                "previous_sequence": {"type": "integer", "minimum": 0},
            },
            BOOK_DELTA_EVENT_SCHEMA_ID,
        ),
        FUNDING_RATE_EVENT_SCHEMA_ID: _event_schema(
            "funding_rate",
            {"rate": {"type": "number"}, "interval_start": _UTC_JSON, "interval_end": _UTC_JSON},
            FUNDING_RATE_EVENT_SCHEMA_ID,
        ),
        MARK_PRICE_EVENT_SCHEMA_ID: _event_schema(
            "mark_price", {"price": _FIXED_POINT_JSON}, MARK_PRICE_EVENT_SCHEMA_ID
        ),
        CORPORATE_ACTION_EVENT_SCHEMA_ID: _event_schema(
            "corporate_action",
            {
                "action_type": {"type": "string", "minLength": 1},
                "effective_date": {"type": "string", "format": "date"},
                "ratio": {"oneOf": [_FIXED_POINT_JSON, {"type": "null"}]},
                "cash_amount": {"oneOf": [_FIXED_POINT_JSON, {"type": "null"}]},
                "currency": _NULLABLE_STRING,
            },
            CORPORATE_ACTION_EVENT_SCHEMA_ID,
        ),
        STATUS_EVENT_SCHEMA_ID: _event_schema(
            "status",
            {"status": {"type": "string", "minLength": 1}, "reason": {"type": "string"}},
            STATUS_EVENT_SCHEMA_ID,
        ),
    }
)


def get_arrow_schema(schema_id: str, version: str = SCHEMA_VERSION_V2) -> pa.Schema:
    if version != SCHEMA_VERSION_V2:
        raise ValidationError(f"Unsupported schema version: {schema_id}@{version}")
    try:
        return _ARROW_SCHEMAS[schema_id]
    except KeyError as exc:
        raise ValidationError(f"Unknown schema ID: {schema_id}") from exc


def get_json_schema(schema_id: str, version: str = SCHEMA_VERSION_V2) -> dict[str, Any]:
    if version != SCHEMA_VERSION_V2:
        raise ValidationError(f"Unsupported schema version: {schema_id}@{version}")
    try:
        return deepcopy(_JSON_SCHEMAS[schema_id])
    except KeyError as exc:
        raise ValidationError(f"Unknown schema ID: {schema_id}") from exc


def validate_json_record(
    schema_id: str,
    payload: dict[str, Any],
    version: str = SCHEMA_VERSION_V2,
) -> None:
    validator = Draft202012Validator(
        get_json_schema(schema_id, version), format_checker=FormatChecker()
    )
    errors = sorted(validator.iter_errors(payload), key=lambda error: list(error.path))
    if errors:
        detail = "; ".join(error.message for error in errors)
        raise ValidationError(f"JSON schema validation failed for {schema_id}: {detail}")


def validate_arrow_table(
    schema_id: str,
    table: pa.Table,
    version: str = SCHEMA_VERSION_V2,
) -> None:
    expected = get_arrow_schema(schema_id, version)
    if table.schema != expected:
        raise ValidationError(
            f"Arrow schema mismatch for {schema_id}: expected={expected}, actual={table.schema}"
        )
