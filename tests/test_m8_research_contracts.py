from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone

import pyarrow as pa
import pytest

import quant_data_kit.research_contracts_v2 as contracts
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint
from quant_data_kit.research_contracts_v2 import (
    CURATED_AGGREGATION_SCHEMA_ID,
    MARKET_CONTEXT_SCHEMA_ID,
    VERIFIED_FACTOR_INPUT_SCHEMA_ID,
    CuratedAggregation,
    EventBarPartitionEvidence,
    EventSchemaRef,
    LineageRef,
    VerifiedFactorInput,
)
from quant_data_kit.schemas_v2 import (
    BAR_EVENT_SCHEMA_ID,
    BOOK_DELTA_EVENT_SCHEMA_ID,
    SCHEMA_VERSION_V2,
    TRADE_EVENT_SCHEMA_ID,
    get_arrow_schema,
)

HASH = "0" * 64
OTHER_HASH = "1" * 64
SNAPSHOT = f"sha256-{HASH}"
TRADE_SCHEMA = EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)


def evidence(
    *,
    first: int = 1,
    last: int = 2,
    digest: str = HASH,
    path: str = "date=2026-01-05/instrument=IF/data.parquet",
) -> EventBarPartitionEvidence:
    return EventBarPartitionEvidence(
        relative_path=path,
        source="fixture",
        instrument_id="IF",
        session_id="day",
        first_sequence=first,
        last_sequence=last,
        first_event_id=f"e{first}",
        last_event_id=f"e{last}",
        event_count=last - first + 1,
        source_selection_sha256=digest,
    )


def fixed_aggregation() -> CuratedAggregation:
    return CuratedAggregation(
        calendar_id="calendar-v1",
        session_policy_version="sessions-v1",
        kind="fixed_time_bar",
        recipe_version="recipe-v1",
        interval_ns=60_000_000_000,
        market_context_snapshot_id=SNAPSHOT,
        market_context_logical_sha256=HASH,
        source_event_schemas=(TRADE_SCHEMA,),
    )


def event_aggregation(
    items: tuple[EventBarPartitionEvidence, ...] | None = None,
) -> CuratedAggregation:
    return CuratedAggregation(
        calendar_id="calendar-v1",
        session_policy_version="sessions-v1",
        kind="event_bar",
        recipe_version="recipe-v1",
        event_bar_basis="trade_count",
        event_bar_threshold=FixedPoint(2, 0),
        market_context_snapshot_id=SNAPSHOT,
        market_context_logical_sha256=HASH,
        source_event_schemas=(TRADE_SCHEMA,),
        partition_evidence=items or (evidence(),),
    )


def test_schema_constants_and_round_trips_are_frozen() -> None:
    assert CURATED_AGGREGATION_SCHEMA_ID == "puresaber.curated-aggregation@1.0.0"
    assert MARKET_CONTEXT_SCHEMA_ID == "puresaber.market-context@1.0.0"
    assert VERIFIED_FACTOR_INPUT_SCHEMA_ID == "puresaber.verified-factor-input@1.0.0"
    assert EventSchemaRef.from_contract(TRADE_SCHEMA.to_contract()) == TRADE_SCHEMA
    aggregation = event_aggregation()
    assert CuratedAggregation.from_contract(aggregation.to_contract()) == aggregation
    assert EventBarPartitionEvidence.from_contract(evidence().to_contract()) == evidence()


@pytest.mark.parametrize(
    ("schema_id", "version"),
    [("", SCHEMA_VERSION_V2), ("foreign.trade", SCHEMA_VERSION_V2), (TRADE_EVENT_SCHEMA_ID, "v2")],
)
def test_event_schema_reference_rejects_invalid_identity(schema_id: str, version: str) -> None:
    with pytest.raises(ValidationError):
        EventSchemaRef(schema_id, version)


def test_closed_references_and_lineage_fail_closed() -> None:
    with pytest.raises(ValidationError, match="exactly"):
        EventSchemaRef.from_contract(
            {"schema_id": TRADE_EVENT_SCHEMA_ID, "schema_version": SCHEMA_VERSION_V2, "x": 1}
        )
    for args in (("", SNAPSHOT, HASH), ("market", "latest", HASH), ("market", SNAPSHOT, "X")):
        with pytest.raises(ValidationError):
            LineageRef(*args)


@pytest.mark.parametrize(
    "changes",
    [
        {"source": ""},
        {"first_sequence": True},
        {"first_sequence": -1},
        {"event_count": 0},
        {"first_sequence": 3, "last_sequence": 2},
        {"source_selection_sha256": "bad"},
    ],
)
def test_event_evidence_rejects_invalid_values(changes: dict) -> None:
    with pytest.raises(ValidationError):
        replace(evidence(), **changes)


@pytest.mark.parametrize("value", ["-0", "01", str(2**63), 1])
def test_event_evidence_requires_canonical_integer_strings(value: object) -> None:
    payload = evidence().to_contract()
    payload["first_sequence"] = value
    with pytest.raises(ValidationError, match="canonical|int64"):
        EventBarPartitionEvidence.from_contract(payload)


def test_aggregation_kind_conditions_and_order_are_strict() -> None:
    base = fixed_aggregation()
    with pytest.raises(ValidationError, match="unsupported"):
        replace(base, kind="other")
    with pytest.raises(ValidationError, match="source_event_schemas"):
        replace(base, source_event_schemas=())
    with pytest.raises(ValidationError, match="interval_ns"):
        replace(base, interval_ns=0)
    with pytest.raises(ValidationError, match="another"):
        replace(base, session_rollup="session")
    session = replace(base, kind="session_bar", interval_ns=None, session_rollup="session")
    assert session.session_rollup == "session"
    with pytest.raises(ValidationError, match="session_rollup"):
        replace(session, session_rollup=None)
    with pytest.raises(ValidationError, match="another"):
        replace(session, interval_ns=1)
    event = event_aggregation()
    with pytest.raises(ValidationError, match="basis"):
        replace(event, event_bar_basis="bad")
    with pytest.raises(ValidationError, match="threshold"):
        replace(event, event_bar_threshold=FixedPoint(0, 0))
    with pytest.raises(ValidationError, match="another"):
        replace(event, interval_ns=1)
    with pytest.raises(ValidationError, match="evidence"):
        replace(event, partition_evidence=())
    duplicate = evidence(first=3, last=4)
    with pytest.raises(ValidationError, match="partition paths"):
        replace(event, partition_evidence=(evidence(), duplicate))
    second = evidence(
        first=3,
        last=4,
        digest=OTHER_HASH,
        path="date=2026-01-05/instrument=IF/session=pm/data.parquet",
    )
    with pytest.raises(ValidationError, match="sorted"):
        replace(event, partition_evidence=(second, evidence()))
    overlap = evidence(
        first=2,
        last=3,
        digest=OTHER_HASH,
        path="date=2026-01-05/instrument=IF/session=pm/data.parquet",
    )
    with pytest.raises(ValidationError, match="overlap"):
        replace(event, partition_evidence=(evidence(), overlap))
    excessive_count = replace(
        second,
        first_sequence=3,
        last_sequence=3,
        event_count=2,
        first_event_id="e3",
        last_event_id="e3",
    )
    with pytest.raises(ValidationError, match="event_count"):
        replace(event, partition_evidence=(evidence(), excessive_count))
    bad_boundary = replace(second, first_sequence=3, last_sequence=3, event_count=1)
    with pytest.raises(ValidationError, match="boundary"):
        replace(event, partition_evidence=(evidence(), bad_boundary))


def test_aggregation_parser_rejects_noncanonical_and_open_payloads() -> None:
    payload = fixed_aggregation().to_contract()
    payload["extra"] = True
    with pytest.raises(ValidationError, match="exactly"):
        CuratedAggregation.from_contract(payload)
    payload = fixed_aggregation().to_contract()
    payload["interval_ns"] = "-0"
    with pytest.raises(ValidationError, match="canonical"):
        CuratedAggregation.from_contract(payload)
    payload = event_aggregation().to_contract()
    assert payload["event_bar_threshold"] is not None
    payload["event_bar_threshold"]["scale"] = True
    with pytest.raises(ValidationError, match="scale"):
        CuratedAggregation.from_contract(payload)
    payload = fixed_aggregation().to_contract()
    payload["source_event_schemas"] = "not-an-array"
    with pytest.raises(ValidationError, match="array"):
        CuratedAggregation.from_contract(payload)
    payload = fixed_aggregation().to_contract()
    payload["partition_evidence"] = "not-an-array"
    with pytest.raises(ValidationError, match="array"):
        CuratedAggregation.from_contract(payload)


def valid_verified_input(**changes) -> VerifiedFactorInput:
    table = pa.table(
        {
            "event_schema_id": [TRADE_EVENT_SCHEMA_ID],
            "value": [1],
        }
    )
    values = {
        "layer": "normalized",
        "source_snapshot_id": SNAPSHOT,
        "source_logical_sha256": HASH,
        "selection_logical_sha256": contracts._arrow_table_logical_sha256(table),
        "event_schemas": (TRADE_SCHEMA,),
        "table": table,
        "calendar_id": "calendar-v1",
        "session_policy_version": "sessions-v1",
        "market_context_snapshot_id": SNAPSHOT,
        "market_context_logical_sha256": HASH,
        "lineage": (
            LineageRef("market", SNAPSHOT, HASH),
            LineageRef("market_context", SNAPSHOT, HASH),
        ),
    }
    values.update(changes)
    if "table" in changes and "selection_logical_sha256" not in changes:
        values["selection_logical_sha256"] = contracts._arrow_table_logical_sha256(values["table"])
    return VerifiedFactorInput._from_certified_factory(**values)


def bar_table() -> pa.Table:
    timestamp = datetime(2026, 1, 5, 1, 31, tzinfo=timezone.utc)
    fixed = {"units": 1, "scale": 0}
    return pa.Table.from_pylist(
        [
            {
                "event_type": "bar",
                "event_id": "bar-1",
                "instrument_id": "IF",
                "event_time": timestamp,
                "received_at": timestamp,
                "available_at": timestamp,
                "source": "fixture",
                "trading_day": date(2026, 1, 5),
                "session_id": "day",
                "sequence": 1,
                "bar_start": datetime(2026, 1, 5, 1, 30, tzinfo=timezone.utc),
                "bar_end": timestamp,
                "open_price": fixed,
                "high_price": fixed,
                "low_price": fixed,
                "close_price": fixed,
                "volume": fixed,
                "is_complete": True,
            }
        ],
        schema=get_arrow_schema(BAR_EVENT_SCHEMA_ID),
    )


def test_verified_input_is_closed_nonempty_and_layer_safe() -> None:
    valid = valid_verified_input()
    assert len(valid.arrow_schema_sha256) == 64
    assert valid.to_contract()["rows"] == "1"
    cases = [
        {"schema_id": "wrong"},
        {"layer": "raw"},
        {"event_schemas": ()},
        {"table": pa.table({"value": pa.array([], type=pa.int64())})},
        {"lineage": ()},
        {"layer": "curated", "aggregation": None},
        {"aggregation": fixed_aggregation()},
    ]
    for changes in cases:
        with pytest.raises(ValidationError):
            valid_verified_input(**changes)
    curated_table = bar_table()
    curated = valid_verified_input(
        layer="curated",
        aggregation=fixed_aggregation(),
        event_schemas=(EventSchemaRef(BAR_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),),
        table=curated_table,
    )
    assert curated.to_contract()["aggregation"]["kind"] == "fixed_time_bar"
    with pytest.raises(ValidationError, match="certified factory"):
        VerifiedFactorInput(
            layer="normalized",
            source_snapshot_id=SNAPSHOT,
            source_logical_sha256=HASH,
            selection_logical_sha256=valid.selection_logical_sha256,
            event_schemas=(TRADE_SCHEMA,),
            table=valid.table,
            calendar_id="calendar-v1",
            session_policy_version="sessions-v1",
            market_context_snapshot_id=SNAPSHOT,
            market_context_logical_sha256=HASH,
            lineage=(LineageRef("market", SNAPSHOT, HASH),),
        )
    with pytest.raises(ValidationError, match="certified factory"):
        replace(valid, source_snapshot_id=f"sha256-{OTHER_HASH}")
    with pytest.raises(ValidationError, match="market lineage"):
        valid_verified_input(source_snapshot_id=f"sha256-{OTHER_HASH}")
    with pytest.raises(ValidationError, match="context differs from its lineage"):
        valid_verified_input(market_context_logical_sha256=OTHER_HASH)
    with pytest.raises(ValidationError, match="selection hash"):
        valid_verified_input(selection_logical_sha256=OTHER_HASH)
    with pytest.raises(ValidationError, match="context differs"):
        valid_verified_input(
            layer="curated",
            aggregation=fixed_aggregation(),
            event_schemas=(EventSchemaRef(BAR_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),),
            table=curated_table,
            calendar_id="other-calendar",
        )
    with pytest.raises(ValidationError, match="frozen Bar schema"):
        valid_verified_input(
            layer="curated",
            aggregation=fixed_aggregation(),
            table=curated_table,
        )


def test_schema_refs_must_be_sorted_when_multiple() -> None:
    book = EventSchemaRef(BOOK_DELTA_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)
    with pytest.raises(ValidationError, match="sorted"):
        valid_verified_input(event_schemas=(TRADE_SCHEMA, book))
