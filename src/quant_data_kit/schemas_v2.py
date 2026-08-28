"""Arrow and JSON schema registry for cross-asset data contracts."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
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
_BOOK_LEVEL_LIST = pa.list_(pa.field("item", _BOOK_LEVEL, nullable=False))


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
_SEQUENCED_EVENT_FIELDS = [
    *_COMMON_EVENT_FIELDS[:-1],
    _field("sequence", pa.int64()),
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
        _SEQUENCED_EVENT_FIELDS
        + [_field("bids", _BOOK_LEVEL_LIST), _field("asks", _BOOK_LEVEL_LIST)]
    ),
    BOOK_DELTA_EVENT_SCHEMA_ID: pa.schema(
        _SEQUENCED_EVENT_FIELDS
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
                "sequence": {"type": "integer", "minimum": 0},
                "bids": {"type": "array", "minItems": 1, "items": _BOOK_LEVEL_JSON},
                "asks": {"type": "array", "minItems": 1, "items": _BOOK_LEVEL_JSON},
            },
            BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
        ),
        BOOK_DELTA_EVENT_SCHEMA_ID: _event_schema(
            "book_delta",
            {
                "sequence": {"type": "integer", "minimum": 0},
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

_EVENT_SCHEMA_BY_TYPE = {
    "quote": QUOTE_EVENT_SCHEMA_ID,
    "trade": TRADE_EVENT_SCHEMA_ID,
    "bar": BAR_EVENT_SCHEMA_ID,
    "book_snapshot": BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    "book_delta": BOOK_DELTA_EVENT_SCHEMA_ID,
    "funding_rate": FUNDING_RATE_EVENT_SCHEMA_ID,
    "mark_price": MARK_PRICE_EVENT_SCHEMA_ID,
    "corporate_action": CORPORATE_ACTION_EVENT_SCHEMA_ID,
    "status": STATUS_EVENT_SCHEMA_ID,
}


def _timestamp(payload: Mapping[str, Any], field_name: str) -> datetime:
    value = datetime.fromisoformat(str(payload[field_name]).replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValidationError(f"{field_name} must be stored as UTC")
    return value


def _fixed_value(payload: Mapping[str, Any], field_name: str) -> Decimal:
    value = payload[field_name]
    return Decimal(value["units"]).scaleb(-value["scale"])


def _require_positive(payload: Mapping[str, Any], field_name: str) -> Decimal:
    value = _fixed_value(payload, field_name)
    if value <= 0:
        raise ValidationError(f"{field_name} must be positive")
    return value


def _require_non_negative(payload: Mapping[str, Any], field_name: str) -> Decimal:
    value = _fixed_value(payload, field_name)
    if value < 0:
        raise ValidationError(f"{field_name} must be non-negative")
    return value


def _validate_event_semantics(schema_id: str, payload: Mapping[str, Any]) -> None:
    event_time = _timestamp(payload, "event_time")
    received_at = _timestamp(payload, "received_at")
    available_at = _timestamp(payload, "available_at")
    if event_time > received_at:
        raise ValidationError("received_at must not be earlier than event_time")
    if received_at > available_at:
        raise ValidationError("available_at must not be earlier than received_at")

    if schema_id == QUOTE_EVENT_SCHEMA_ID:
        bid = _require_positive(payload, "bid_price")
        ask = _require_positive(payload, "ask_price")
        _require_non_negative(payload, "bid_quantity")
        _require_non_negative(payload, "ask_quantity")
        if payload["bid_price"]["scale"] != payload["ask_price"]["scale"]:
            raise ValidationError("bid and ask prices must use one scale")
        if bid > ask:
            raise ValidationError("quote is crossed")
    elif schema_id == TRADE_EVENT_SCHEMA_ID:
        _require_positive(payload, "price")
        _require_positive(payload, "quantity")
    elif schema_id == BAR_EVENT_SCHEMA_ID:
        bar_start = _timestamp(payload, "bar_start")
        bar_end = _timestamp(payload, "bar_end")
        if bar_end <= bar_start:
            raise ValidationError("bar_end must be later than bar_start")
        if event_time != bar_end:
            raise ValidationError("bar event_time must equal bar_end")
        price_fields = ("open_price", "high_price", "low_price", "close_price")
        prices = {name: _require_positive(payload, name) for name in price_fields}
        if len({payload[name]["scale"] for name in price_fields}) != 1:
            raise ValidationError("bar OHLC values must use one price scale")
        if prices["high_price"] < max(prices.values()):
            raise ValidationError("high_price is below another OHLC value")
        if prices["low_price"] > min(prices.values()):
            raise ValidationError("low_price is above another OHLC value")
        _require_non_negative(payload, "volume")
    elif schema_id == BOOK_SNAPSHOT_EVENT_SCHEMA_ID:
        levels = [*payload["bids"], *payload["asks"]]
        if len({level["price"]["scale"] for level in levels}) != 1:
            raise ValidationError("book prices must use one scale")
        for level in levels:
            _require_positive(level, "price")
            _require_non_negative(level, "quantity")
        bids = [level["price"]["units"] for level in payload["bids"]]
        asks = [level["price"]["units"] for level in payload["asks"]]
        if bids != sorted(bids, reverse=True) or len(bids) != len(set(bids)):
            raise ValidationError("book bids must be strictly ordered without duplicates")
        if asks != sorted(asks) or len(asks) != len(set(asks)):
            raise ValidationError("book asks must be strictly ordered without duplicates")
        if bids[0] >= asks[0]:
            raise ValidationError("book snapshot is locked or crossed")
    elif schema_id == BOOK_DELTA_EVENT_SCHEMA_ID:
        _require_positive(payload, "price")
        quantity = _require_non_negative(payload, "quantity")
        if payload["previous_sequence"] >= payload["sequence"]:
            raise ValidationError("previous_sequence must precede sequence")
        if payload["action"] == "delete" and quantity != 0:
            raise ValidationError("delete delta quantity must be zero")
        if payload["action"] == "upsert" and quantity == 0:
            raise ValidationError("upsert delta quantity must be positive")
    elif schema_id == FUNDING_RATE_EVENT_SCHEMA_ID:
        if not math.isfinite(float(payload["rate"])):
            raise ValidationError("funding rate must be finite")
        if _timestamp(payload, "interval_end") <= _timestamp(payload, "interval_start"):
            raise ValidationError("funding interval must be positive")
    elif schema_id == MARK_PRICE_EVENT_SCHEMA_ID:
        _require_positive(payload, "price")
    elif schema_id == CORPORATE_ACTION_EVENT_SCHEMA_ID:
        ratio = payload["ratio"]
        cash_amount = payload["cash_amount"]
        if ratio is None and cash_amount is None:
            raise ValidationError("corporate action requires ratio or cash_amount")
        if ratio is not None and _fixed_value(payload, "ratio") < 0:
            raise ValidationError("ratio must be non-negative")
        if cash_amount is not None:
            if _fixed_value(payload, "cash_amount") < 0:
                raise ValidationError("cash_amount must be non-negative")
            if not payload["currency"]:
                raise ValidationError("currency is required with cash_amount")
        elif payload["currency"] is not None:
            raise ValidationError("currency requires cash_amount")


def _validate_record_semantics(schema_id: str, payload: Mapping[str, Any]) -> None:
    if schema_id in _EVENT_SCHEMA_BY_TYPE.values():
        _validate_event_semantics(schema_id, payload)
    elif schema_id == INSTRUMENT_SPEC_SCHEMA_ID:
        for name in ("price_tick", "quantity_step", "contract_multiplier"):
            _require_positive(payload, name)
        if payload["effective_to"] is not None and _timestamp(
            payload, "effective_to"
        ) <= _timestamp(payload, "effective_from"):
            raise ValidationError("effective_to must be later than effective_from")
    elif schema_id == SYMBOL_MAPPING_SCHEMA_ID:
        if payload["effective_to"] is not None and _timestamp(
            payload, "effective_to"
        ) <= _timestamp(payload, "effective_from"):
            raise ValidationError("effective_to must be later than effective_from")
    elif schema_id == TRADING_SESSION_SCHEMA_ID and _timestamp(
        payload, "closes_at"
    ) <= _timestamp(payload, "opens_at"):
        raise ValidationError("closes_at must be later than opens_at")


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
    _validate_record_semantics(schema_id, payload)


def validate_event_stream(
    records: Iterable[Mapping[str, Any]],
    version: str = SCHEMA_VERSION_V2,
) -> None:
    """Validate event records in replay order, including sequence continuity."""
    last_sequences: dict[tuple[str, str, str, str], int] = {}
    event_ids: set[str] = set()
    for payload in records:
        event_type = payload.get("event_type")
        try:
            schema_id = _EVENT_SCHEMA_BY_TYPE[str(event_type)]
        except KeyError as exc:
            raise ValidationError(f"Unknown market event_type: {event_type!r}") from exc
        validate_json_record(schema_id, dict(payload), version)
        event_id = str(payload["event_id"])
        if event_id in event_ids:
            raise ValidationError(f"Duplicate event_id in stream: {event_id}")
        event_ids.add(event_id)
        sequence = payload["sequence"]
        if sequence is None:
            continue
        key = (
            str(payload["source"]),
            str(payload["instrument_id"]),
            str(payload["session_id"]),
            str(event_type),
        )
        previous = last_sequences.get(key)
        if previous is not None and sequence <= previous:
            raise ValidationError(
                f"Sequence must be strictly increasing for stream {key}: "
                f"previous={previous}, current={sequence}"
            )
        if (
            event_type == "book_delta"
            and previous is not None
            and payload["previous_sequence"] != previous
        ):
            raise ValidationError(
                f"book_delta previous_sequence must equal prior stream sequence: "
                f"expected={previous}, actual={payload['previous_sequence']}"
            )
        last_sequences[key] = sequence


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
