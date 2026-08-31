from __future__ import annotations

import hashlib
import json
from copy import copy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import quant_data_kit.curated as curated_module
import quant_data_kit.research_inputs_v2 as research_inputs
from quant_data_kit.curated import (
    build_event_bars,
    build_session_rollup_bars,
    curate_session_bars_from_snapshot,
    curate_trade_bars_from_snapshot,
    curate_trade_event_bars_from_snapshot,
)
from quant_data_kit.data_lake import (
    StoragePolicy,
    read_normalized_events,
    write_normalized_events,
    write_raw_bytes,
)
from quant_data_kit.domain_v2 import (
    AssetClass,
    InstrumentSpec,
    MarginMode,
    SessionPhase,
    TradingSession,
)
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.fixed_point import FixedPoint
from quant_data_kit.research_contracts_v2 import (
    CuratedAggregation,
    EventBarPartitionEvidence,
    EventSchemaRef,
)
from quant_data_kit.research_inputs_v2 import (
    create_market_context_snapshot,
    load_market_context_snapshot,
    load_verified_curated_bars,
    load_verified_normalized_events,
)
from quant_data_kit.schemas_v2 import (
    BAR_EVENT_SCHEMA_ID,
    BOOK_DELTA_EVENT_SCHEMA_ID,
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    QUOTE_EVENT_SCHEMA_ID,
    SCHEMA_VERSION_V2,
    TRADE_EVENT_SCHEMA_ID,
    get_arrow_schema,
)

UTC = timezone.utc
TEST_POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)
SESSION_ID = "CFFEX-IF-2026-01-05-DAY"


def trade(event_id: str, timestamp: str, sequence: int, price: int = 40001) -> dict:
    return {
        "event_type": "trade",
        "event_id": event_id,
        "instrument_id": "IF-CONT",
        "event_time": timestamp,
        "received_at": timestamp,
        "available_at": timestamp,
        "source": "cn-fixture",
        "trading_day": "2026-01-05",
        "session_id": SESSION_ID,
        "sequence": sequence,
        "price": {"units": price, "scale": 1},
        "quantity": {"units": 1, "scale": 0},
        "aggressor_side": "unknown",
    }


def normalized(root: Path, records: list[dict], key: str = "m8"):
    raw = write_raw_bytes(
        root,
        source="cn-fixture",
        request={"fixture": key},
        collected_at="2026-01-05T01:00:00Z",
        payload=key.encode(),
        idempotency_key=key,
        policy=TEST_POLICY,
    )
    result = write_normalized_events(
        root,
        records,
        provider="cn-fixture",
        venue="CFFEX",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    return result.snapshot


def market_context(root: Path):
    instrument = InstrumentSpec(
        instrument_id="IF-CONT",
        asset_class=AssetClass.FUTURE,
        product_type="index-future",
        venue="CFFEX",
        native_symbol="IF",
        settlement_currency="CNY",
        price_tick=FixedPoint(2, 1),
        quantity_step=FixedPoint(1, 0),
        contract_multiplier=FixedPoint(300, 0),
        calendar_id="cffex-v1",
        margin_mode=MarginMode.CROSS,
        effective_from=datetime(2025, 1, 1, tzinfo=UTC),
        available_at=datetime(2025, 1, 1, tzinfo=UTC),
    )
    session = TradingSession(
        session_id=SESSION_ID,
        calendar_id="cffex-v1",
        venue="CFFEX",
        trading_day=date(2026, 1, 5),
        phase=SessionPhase.CONTINUOUS,
        opens_at=datetime(2026, 1, 5, 1, 30, tzinfo=UTC),
        closes_at=datetime(2026, 1, 5, 2, 0, tzinfo=UTC),
        available_at=datetime(2025, 12, 1, tzinfo=UTC),
    )
    return create_market_context_snapshot(
        root,
        calendar_id="cffex-v1",
        session_policy_version="cffex-session-v1",
        instruments=[instrument],
        sessions=[session],
        policy=TEST_POLICY,
    )


def test_market_context_and_verified_normalized_input_are_content_bound(tmp_path: Path) -> None:
    source = normalized(
        tmp_path,
        [
            trade("t1", "2026-01-05T01:30:01Z", 1),
            trade("t2", "2026-01-05T01:30:02Z", 2),
        ],
    )
    context = market_context(tmp_path)
    assert load_market_context_snapshot(tmp_path, context.snapshot_id) == context
    verified = load_verified_normalized_events(
        tmp_path,
        source.snapshot_id,
        [EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)],
        context.snapshot_id,
    )
    assert verified.layer == "normalized"
    assert verified.table.num_rows == 2
    assert verified.table.column("event_schema_id").to_pylist() == [
        TRADE_EVENT_SCHEMA_ID,
        TRADE_EVENT_SCHEMA_ID,
    ]
    assert verified.to_contract()["rows"] == "2"
    assert verified.to_contract()["aggregation"] is None


def test_fixed_session_and_event_bars_all_load_through_certified_factory(
    tmp_path: Path,
) -> None:
    source = normalized(
        tmp_path,
        [
            trade("t1", "2026-01-05T01:30:01Z", 1, 40001),
            trade("t2", "2026-01-05T01:30:20Z", 2, 40003),
            trade("t3", "2026-01-05T01:31:01Z", 3, 40002),
            trade("t4", "2026-01-05T01:31:20Z", 4, 40004),
        ],
    )
    context = market_context(tmp_path)
    fixed = curate_trade_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="fixed-bars",
        revision_id="r1",
        recipe_version="fixed-v1",
        interval=timedelta(minutes=1),
        session_starts={SESSION_ID: datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
        market_context_snapshot_id=context.snapshot_id,
        policy=TEST_POLICY,
    )
    session = curate_session_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="session-bars",
        revision_id="r1",
        recipe_version="session-v1",
        session_rollup="session",
        market_context_snapshot_id=context.snapshot_id,
        policy=TEST_POLICY,
    )
    event = curate_trade_event_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="event-bars",
        revision_id="r1",
        recipe_version="event-v1",
        basis="trade_count",
        threshold=FixedPoint(2, 0),
        market_context_snapshot_id=context.snapshot_id,
        policy=TEST_POLICY,
    )
    fixed_input = load_verified_curated_bars(tmp_path, "fixed-bars", fixed.snapshot_id)
    session_input = load_verified_curated_bars(tmp_path, "session-bars", session.snapshot_id)
    event_input = load_verified_curated_bars(tmp_path, "event-bars", event.snapshot_id)
    assert fixed_input.table.num_rows == 2
    assert fixed_input.aggregation is not None
    assert fixed_input.aggregation.kind == "fixed_time_bar"
    assert session_input.table.num_rows == 1
    assert session_input.aggregation is not None
    assert session_input.aggregation.kind == "session_bar"
    assert event_input.table.num_rows == 2
    assert event_input.aggregation is not None
    assert event_input.aggregation.partition_evidence is not None
    assert event_input.to_contract()["event_schemas"] == [
        {"schema_id": BAR_EVENT_SCHEMA_ID, "schema_version": SCHEMA_VERSION_V2}
    ]


def test_legacy_curated_snapshot_cannot_be_promoted_to_m8(tmp_path: Path) -> None:
    source = normalized(tmp_path, [trade("t1", "2026-01-05T01:30:01Z", 1)])
    legacy = curate_trade_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="legacy",
        revision_id="r1",
        recipe_version="legacy-v1",
        interval=timedelta(minutes=1),
        session_starts={SESSION_ID: datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
        policy=TEST_POLICY,
    )
    with pytest.raises(ValidationError, match="legacy-curated-not-m8-certified"):
        load_verified_curated_bars(tmp_path, "legacy", legacy.snapshot_id)


def test_closed_schema_and_context_mutation_fail_closed(tmp_path: Path) -> None:
    source = normalized(tmp_path, [trade("t1", "2026-01-05T01:30:01Z", 1)])
    context = market_context(tmp_path)
    with pytest.raises(ValidationError, match="event_schemas"):
        load_verified_normalized_events(tmp_path, source.snapshot_id, [], context.snapshot_id)
    with pytest.raises(ValidationError, match="non-Bar"):
        load_verified_normalized_events(
            tmp_path,
            source.snapshot_id,
            [EventSchemaRef(BAR_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)],
            context.snapshot_id,
        )
    with pytest.raises(ValidationError, match="sorted"):
        load_verified_normalized_events(
            tmp_path,
            source.snapshot_id,
            [
                EventSchemaRef(BOOK_SNAPSHOT_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),
                EventSchemaRef(BOOK_DELTA_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),
            ],
            context.snapshot_id,
        )
    manifest_path = (
        tmp_path / "market-context" / "snapshots" / context.snapshot_id / "manifest.json"
    )
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["session_policy_version"] = "tampered"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="logical hash changed"):
        load_market_context_snapshot(tmp_path, context.snapshot_id)


def test_market_context_creation_rejects_invalid_members_and_is_idempotent(
    tmp_path: Path,
) -> None:
    context = market_context(tmp_path)
    instrument = context.instruments[0]
    session = context.sessions[0]
    repeated = create_market_context_snapshot(
        tmp_path,
        calendar_id=context.calendar_id,
        session_policy_version=context.session_policy_version,
        instruments=context.instruments,
        sessions=context.sessions,
        policy=TEST_POLICY,
    )
    assert repeated == context
    invalid_calls = [
        {"calendar_id": "", "instruments": [instrument], "sessions": [session]},
        {
            "calendar_id": context.calendar_id,
            "session_policy_version": "",
            "instruments": [instrument],
            "sessions": [session],
        },
        {"calendar_id": context.calendar_id, "instruments": [], "sessions": [session]},
        {
            "calendar_id": context.calendar_id,
            "instruments": [replace(instrument, calendar_id="other")],
            "sessions": [session],
        },
        {
            "calendar_id": context.calendar_id,
            "instruments": [instrument],
            "sessions": [replace(session, calendar_id="other")],
        },
        {
            "calendar_id": context.calendar_id,
            "instruments": [instrument, instrument],
            "sessions": [session],
        },
        {
            "calendar_id": context.calendar_id,
            "instruments": [instrument],
            "sessions": [session, session],
        },
    ]
    for values in invalid_calls:
        values.setdefault("session_policy_version", context.session_policy_version)
        with pytest.raises(ValidationError):
            create_market_context_snapshot(tmp_path / "invalid", policy=TEST_POLICY, **values)
    with pytest.raises(ValidationError, match="InstrumentSpec"):
        create_market_context_snapshot(
            tmp_path / "wrong-instrument",
            calendar_id=context.calendar_id,
            session_policy_version=context.session_policy_version,
            instruments=[object()],
            sessions=[session],
            policy=TEST_POLICY,
        )
    with pytest.raises(ValidationError, match="TradingSession"):
        create_market_context_snapshot(
            tmp_path / "wrong-session",
            calendar_id=context.calendar_id,
            session_policy_version=context.session_policy_version,
            instruments=[instrument],
            sessions=[object()],
            policy=TEST_POLICY,
        )


def test_market_context_loader_rejects_paths_shape_and_extra_files(tmp_path: Path) -> None:
    context = market_context(tmp_path)
    with pytest.raises(ValidationError, match="content-addressed"):
        load_market_context_snapshot(tmp_path, "latest")
    snapshot_dir = tmp_path / "market-context" / "snapshots" / context.snapshot_id
    (snapshot_dir / "unexpected.txt").write_text("x", encoding="utf-8")
    with pytest.raises(ValidationError, match="unexpected"):
        load_market_context_snapshot(tmp_path, context.snapshot_id)

    malformed_root = tmp_path / "malformed"
    malformed = market_context(malformed_root)
    malformed_manifest = (
        malformed_root / "market-context" / "snapshots" / malformed.snapshot_id / "manifest.json"
    )
    malformed_manifest.write_text("{", encoding="utf-8")
    with pytest.raises(ValidationError, match="malformed"):
        load_market_context_snapshot(malformed_root, malformed.snapshot_id)

    open_root = tmp_path / "open"
    opened = market_context(open_root)
    open_manifest = (
        open_root / "market-context" / "snapshots" / opened.snapshot_id / "manifest.json"
    )
    payload = json.loads(open_manifest.read_text(encoding="utf-8"))
    payload["extra"] = True
    open_manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="not closed"):
        load_market_context_snapshot(open_root, opened.snapshot_id)


def test_context_record_guards_cover_pit_session_and_venue(tmp_path: Path) -> None:
    context = market_context(tmp_path)
    record = trade("t1", "2026-01-05T01:30:01Z", 1)
    with pytest.raises(ValidationError, match="valid UTC"):
        research_inputs._utc("not-a-time", "time")
    with pytest.raises(ValidationError, match="UTC-aware"):
        research_inputs._utc("2026-01-05T01:30:01", "time")
    with pytest.raises(ValidationError, match="closed FixedPoint"):
        research_inputs._fixed({"units": 1, "scale": 0, "x": 1}, "value")
    with pytest.raises(ValidationError, match="no PIT-valid"):
        research_inputs._active_instrument(
            context,
            "missing",
            datetime(2026, 1, 5, 1, 30, 1, tzinfo=UTC),
            datetime(2026, 1, 5, 1, 30, 1, tzinfo=UTC),
        )
    duplicate = replace(context.instruments[0], available_at=datetime(2025, 2, 1, tzinfo=UTC))
    ambiguous = replace(context, instruments=(context.instruments[0], duplicate))
    with pytest.raises(ValidationError, match="ambiguous"):
        research_inputs._active_instrument(
            ambiguous,
            "IF-CONT",
            datetime(2026, 1, 5, 1, 30, 1, tzinfo=UTC),
            datetime(2026, 1, 5, 1, 30, 1, tzinfo=UTC),
        )
    with pytest.raises(ValidationError, match="exactly one"):
        research_inputs._context_session(
            context,
            "missing",
            datetime(2026, 1, 5, 1, 30, 1, tzinfo=UTC),
            datetime(2026, 1, 5, 1, 30, 1, tzinfo=UTC),
            date(2026, 1, 5),
            allow_close=False,
        )
    outside = dict(
        record,
        event_time="2026-01-05T02:00:00Z",
        received_at="2026-01-05T02:00:00Z",
        available_at="2026-01-05T02:00:00Z",
    )
    with pytest.raises(ValidationError, match="outside session"):
        research_inputs._validate_context_record(context, outside, bar=False)
    wrong_day = dict(record, trading_day="2026-01-06")
    with pytest.raises(ValidationError, match="trading_day"):
        research_inputs._validate_context_record(context, wrong_day, bar=False)
    future_session = replace(
        context,
        sessions=(
            replace(context.sessions[0], available_at=datetime(2026, 1, 5, 1, 31, tzinfo=UTC)),
        ),
    )
    with pytest.raises(ValidationError, match="not PIT-valid"):
        research_inputs._validate_context_record(future_session, record, bar=False)
    bad_pit = dict(record, received_at="2026-01-05T01:30:00Z")
    with pytest.raises(ValidationError, match="not monotonic"):
        research_inputs._validate_context_record(context, bad_pit, bar=False)
    wrong_calendar = replace(context, calendar_id="other")
    with pytest.raises(ValidationError, match="calendar"):
        research_inputs._validate_context_record(wrong_calendar, record, bar=False)
    wrong_venue = replace(context, instruments=(replace(context.instruments[0], venue="OTHER"),))
    with pytest.raises(ValidationError, match="venue"):
        research_inputs._validate_context_record(wrong_venue, record, bar=False)


def l2_event(event_type: str, event_id: str, sequence: int, timestamp: str) -> dict:
    common = {
        "event_type": event_type,
        "event_id": event_id,
        "instrument_id": "BTC-USDT",
        "event_time": timestamp,
        "received_at": timestamp,
        "available_at": timestamp,
        "source": "fixture",
        "trading_day": "2026-01-05",
        "session_id": "crypto-day",
        "sequence": sequence,
    }
    if event_type == "book_snapshot":
        return {
            **common,
            "bids": [
                {
                    "price": {"units": 100, "scale": 0},
                    "quantity": {"units": 1, "scale": 0},
                    "order_count": 1,
                }
            ],
            "asks": [
                {
                    "price": {"units": 101, "scale": 0},
                    "quantity": {"units": 1, "scale": 0},
                    "order_count": 1,
                }
            ],
        }
    return {
        **common,
        "side": "bid",
        "action": "upsert",
        "price": {"units": 99, "scale": 0},
        "quantity": {"units": 2, "scale": 0},
        "previous_sequence": sequence - 1,
    }


def test_event_order_union_and_schema_guards() -> None:
    trade_ref = EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)
    first = trade("same", "2026-01-05T01:30:01Z", 2)
    duplicate = trade("same", "2026-01-05T01:30:02Z", 3)
    with pytest.raises(ValidationError, match="duplicate event_id"):
        research_inputs._validate_event_order([(trade_ref, first), (trade_ref, duplicate)])
    earlier = trade("earlier", "2026-01-05T01:30:00Z", 3)
    with pytest.raises(ValidationError, match="not strictly ordered"):
        research_inputs._validate_event_order([(trade_ref, first), (trade_ref, earlier)])
    lower_sequence = trade("lower", "2026-01-05T01:30:02Z", 1)
    with pytest.raises(ValidationError, match="sequence does not advance"):
        research_inputs._validate_event_order([(trade_ref, first), (trade_ref, lower_sequence)])

    snapshot_ref = EventSchemaRef(BOOK_SNAPSHOT_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)
    delta_ref = EventSchemaRef(BOOK_DELTA_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)
    snapshot = l2_event("book_snapshot", "s1", 10, "2026-01-05T01:30:01Z")
    delta = l2_event("book_delta", "d1", 11, "2026-01-05T01:30:02Z")
    research_inputs._validate_event_order([(snapshot_ref, snapshot), (delta_ref, delta)])
    union = research_inputs._event_union_table([(snapshot_ref, snapshot), (delta_ref, delta)])
    assert union.num_rows == 2
    assert union.schema.field("bids").nullable
    with pytest.raises(ValidationError, match="failed replay"):
        research_inputs._validate_event_order([(delta_ref, delta)])
    with pytest.raises(ValidationError, match="version 2.0.0"):
        research_inputs._normalize_schema_refs([EventSchemaRef(TRADE_EVENT_SCHEMA_ID, "1.0.0")])
    assert research_inputs._normalize_schema_refs(
        [{"schema_id": TRADE_EVENT_SCHEMA_ID, "schema_version": SCHEMA_VERSION_V2}]
    ) == (trade_ref,)


def test_normalized_factory_missing_schema_and_toctou_guards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = normalized(tmp_path, [trade("t1", "2026-01-05T01:30:01Z", 1)])
    context = market_context(tmp_path)
    refs = [
        EventSchemaRef(QUOTE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),
        EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),
    ]
    with pytest.raises(ValidationError, match="lacks requested"):
        load_verified_normalized_events(tmp_path, source.snapshot_id, refs, context.snapshot_id)

    original = research_inputs.load_normalized_snapshot
    calls = 0

    def changed_snapshot(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = original(*args, **kwargs)
        return replace(result, logical_sha256="f" * 64) if calls == 2 else result

    monkeypatch.setattr(research_inputs, "load_normalized_snapshot", changed_snapshot)
    with pytest.raises(ValidationError, match="changed while"):
        load_verified_normalized_events(
            tmp_path,
            source.snapshot_id,
            [EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)],
            context.snapshot_id,
        )


def test_normalized_factory_context_toctou_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = normalized(tmp_path, [trade("t1", "2026-01-05T01:30:01Z", 1)])
    context = market_context(tmp_path)
    original = research_inputs.load_market_context_snapshot
    calls = 0

    def changed_context(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = original(*args, **kwargs)
        return replace(result, session_policy_version="changed") if calls == 2 else result

    monkeypatch.setattr(research_inputs, "load_market_context_snapshot", changed_context)
    with pytest.raises(ValidationError, match="context changed"):
        load_verified_normalized_events(
            tmp_path,
            source.snapshot_id,
            [EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)],
            context.snapshot_id,
        )


def test_payload_parsers_and_arrow_null_conversion_are_closed(tmp_path: Path) -> None:
    context = market_context(tmp_path)
    instrument_payload = context.identity()["instruments"][0]
    with pytest.raises(ValidationError, match="instrument context record"):
        research_inputs._instrument_from_payload({})
    bad_metadata = dict(instrument_payload, metadata=[])
    with pytest.raises(ValidationError, match="metadata"):
        research_inputs._instrument_from_payload(bad_metadata)
    with pytest.raises(ValidationError, match="session context record"):
        research_inputs._session_from_payload({})
    nullable_schema = pa.schema([pa.field("missing", pa.timestamp("ns", tz="UTC"), nullable=True)])
    assert research_inputs._arrow_ready({}, nullable_schema) == {}


def test_bar_validation_and_event_evidence_fail_closed(tmp_path: Path) -> None:
    source = normalized(
        tmp_path,
        [
            trade("t1", "2026-01-05T01:30:01Z", 1, 40001),
            trade("t2", "2026-01-05T01:30:20Z", 2, 40003),
            trade("t3", "2026-01-05T01:31:01Z", 3, 40002),
            trade("t4", "2026-01-05T01:31:20Z", 4, 40004),
        ],
    )
    context_snapshot = market_context(tmp_path)
    fixed = curate_trade_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="guard-fixed",
        revision_id="r1",
        recipe_version="fixed-v1",
        interval=timedelta(minutes=1),
        session_starts={SESSION_ID: datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
        market_context_snapshot_id=context_snapshot.snapshot_id,
        policy=TEST_POLICY,
    )
    event = curate_trade_event_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="guard-event",
        revision_id="r1",
        recipe_version="event-v1",
        basis="trade_count",
        threshold=FixedPoint(2, 0),
        market_context_snapshot_id=context_snapshot.snapshot_id,
        policy=TEST_POLICY,
    )
    fixed_input = load_verified_curated_bars(tmp_path, "guard-fixed", fixed.snapshot_id)
    event_input = load_verified_curated_bars(tmp_path, "guard-event", event.snapshot_id)
    context = load_market_context_snapshot(tmp_path, context_snapshot.snapshot_id)
    assert fixed_input.aggregation is not None
    assert event_input.aggregation is not None

    def bars(rows: list[dict]) -> pa.Table:
        return pa.Table.from_pylist(rows, schema=get_arrow_schema(BAR_EVENT_SCHEMA_ID))

    fixed_rows = fixed_input.table.to_pylist()
    incomplete = [dict(fixed_rows[0], is_complete=False)]
    with pytest.raises(ValidationError, match="incomplete"):
        research_inputs._validate_bar_rows(bars(incomplete), fixed_input.aggregation, context)
    bad_boundary = [dict(fixed_rows[0], event_time=fixed_rows[0]["bar_start"])]
    with pytest.raises(ValidationError, match="event_time"):
        research_inputs._validate_bar_rows(bars(bad_boundary), fixed_input.aggregation, context)
    with pytest.raises(ValidationError, match="duplicate"):
        research_inputs._validate_bar_rows(
            bars([fixed_rows[0], fixed_rows[0]]), fixed_input.aggregation, context
        )
    with pytest.raises(ValidationError, match="strictly ordered"):
        research_inputs._validate_bar_rows(
            bars(list(reversed(fixed_rows))), fixed_input.aggregation, context
        )
    wrong_interval = replace(fixed_input.aggregation, interval_ns=1)
    with pytest.raises(ValidationError, match="interval"):
        research_inputs._validate_bar_rows(bars([fixed_rows[0]]), wrong_interval, context)
    crossing_row = dict(
        fixed_rows[0],
        bar_start=datetime(2026, 1, 5, 1, 29, tzinfo=UTC),
        bar_end=datetime(2026, 1, 5, 1, 30, tzinfo=UTC),
        event_time=datetime(2026, 1, 5, 1, 30, tzinfo=UTC),
    )
    with pytest.raises(ValidationError, match="crosses"):
        research_inputs._validate_bar_rows(bars([crossing_row]), fixed_input.aggregation, context)

    source_rows = read_normalized_events(tmp_path, source.snapshot_id, event_type="trade")
    evidence = event_input.aggregation.partition_evidence
    assert evidence is not None
    with pytest.raises(ValidationError, match="event_count"):
        research_inputs._source_rows_for_evidence(
            source_rows, replace(evidence[0], event_count=evidence[0].event_count + 1)
        )
    with pytest.raises(ValidationError, match="boundaries"):
        research_inputs._source_rows_for_evidence(
            source_rows, replace(evidence[0], first_event_id="wrong")
        )
    with pytest.raises(ValidationError, match="selection hash"):
        research_inputs._source_rows_for_evidence(
            source_rows, replace(evidence[0], source_selection_sha256="f" * 64)
        )

    broken = copy(event_input.aggregation)
    object.__setattr__(broken, "partition_evidence", None)
    with pytest.raises(ValidationError, match="metadata is incomplete"):
        research_inputs._verify_event_bars(broken, source_rows, event_input.table, context)
    wrong_schema = replace(
        event_input.aggregation,
        source_event_schemas=(EventSchemaRef(BOOK_DELTA_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),),
    )
    with pytest.raises(ValidationError, match="Trade schema"):
        research_inputs._verify_event_bars(wrong_schema, source_rows, event_input.table, context)
    multiple_sources = event_input.table.to_pylist()
    multiple_sources[1] = dict(multiple_sources[1], source="other")
    with pytest.raises(ValidationError, match="one deterministic"):
        research_inputs._verify_event_bars(
            event_input.aggregation,
            source_rows,
            bars(multiple_sources),
            context,
        )
    changed = event_input.table.to_pylist()
    changed[0] = dict(changed[0])
    changed[0]["close_price"] = dict(changed[0]["close_price"], units=40002)
    with pytest.raises(ValidationError, match="do not recompute"):
        research_inputs._verify_event_bars(
            event_input.aggregation, source_rows, bars(changed), context
        )


def test_curated_factory_binding_and_toctou_guards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = normalized(tmp_path, [trade("t1", "2026-01-05T01:30:01Z", 1)])
    context = market_context(tmp_path)
    curated = curate_trade_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="toctou-bars",
        revision_id="r1",
        recipe_version="fixed-v1",
        interval=timedelta(minutes=1),
        session_starts={SESSION_ID: datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
        market_context_snapshot_id=context.snapshot_id,
        policy=TEST_POLICY,
    )
    original_curated = research_inputs.load_curated_snapshot
    calls = 0

    def changed_curated(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = original_curated(*args, **kwargs)
        return replace(result, created_at="2026-01-05T01:59:00Z") if calls == 2 else result

    monkeypatch.setattr(research_inputs, "load_curated_snapshot", changed_curated)
    with pytest.raises(ValidationError, match="Curated snapshot changed"):
        load_verified_curated_bars(tmp_path, "toctou-bars", curated.snapshot_id)


def test_curated_factory_context_and_lineage_guards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = normalized(tmp_path, [trade("t1", "2026-01-05T01:30:01Z", 1)])
    context = market_context(tmp_path)
    curated = curate_trade_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="binding-bars",
        revision_id="r1",
        recipe_version="fixed-v1",
        interval=timedelta(minutes=1),
        session_starts={SESSION_ID: datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
        market_context_snapshot_id=context.snapshot_id,
        policy=TEST_POLICY,
    )
    original_context = research_inputs.load_market_context_snapshot

    def wrong_context(*args, **kwargs):
        return replace(original_context(*args, **kwargs), logical_sha256="f" * 64)

    monkeypatch.setattr(research_inputs, "load_market_context_snapshot", wrong_context)
    with pytest.raises(ValidationError, match="binding changed"):
        load_verified_curated_bars(tmp_path, "binding-bars", curated.snapshot_id)


def test_curated_factory_rechecks_normalized_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = normalized(tmp_path, [trade("t1", "2026-01-05T01:30:01Z", 1)])
    context = market_context(tmp_path)
    curated = curate_trade_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="lineage-bars",
        revision_id="r1",
        recipe_version="fixed-v1",
        interval=timedelta(minutes=1),
        session_starts={SESSION_ID: datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
        market_context_snapshot_id=context.snapshot_id,
        policy=TEST_POLICY,
    )
    original = research_inputs.load_normalized_snapshot

    def changed_normalized(*args, **kwargs):
        return replace(original(*args, **kwargs), logical_sha256="f" * 64)

    monkeypatch.setattr(research_inputs, "load_normalized_snapshot", changed_normalized)
    with pytest.raises(ValidationError, match="lineage hash changed"):
        load_verified_curated_bars(tmp_path, "lineage-bars", curated.snapshot_id)


def test_curated_aggregation_builders_cover_all_kinds_and_reject_bad_inputs() -> None:
    start = datetime(2026, 1, 5, 1, 30, tzinfo=UTC)
    end = datetime(2026, 1, 5, 2, 0, tzinfo=UTC)
    records = [
        trade("t1", "2026-01-05T01:30:01Z", 1, 40001),
        trade("t2", "2026-01-05T01:30:20Z", 2, 40003),
    ]
    with pytest.raises(ValidationError, match="no trades"):
        curated_module._bar_from_trade_group(
            [],
            bar_start=start,
            bar_end=end,
            source="curated",
            recipe_version="r1",
            identity_extra={},
        )
    with pytest.raises(ValidationError, match="strictly positive"):
        curated_module._bar_from_trade_group(
            records,
            bar_start=end,
            bar_end=start,
            source="curated",
            recipe_version="r1",
            identity_extra={},
        )
    with pytest.raises(ValidationError, match="session_rollup"):
        build_session_rollup_bars(
            records, session_boundaries={SESSION_ID: (start, end)}, session_rollup="bad"
        )
    with pytest.raises(ValidationError, match="Missing authoritative"):
        build_session_rollup_bars(records, session_boundaries={}, session_rollup="session")
    with pytest.raises(ValidationError, match="trading-day boundary"):
        build_session_rollup_bars(
            records,
            session_boundaries={SESSION_ID: (start, end)},
            session_rollup="trading_day",
        )
    day_bars = build_session_rollup_bars(
        records,
        session_boundaries={SESSION_ID: (start, end)},
        session_rollup="trading_day",
        trading_day_boundaries={
            ("CFFEX", "2026-01-05"): (start, end, SESSION_ID),
        },
        instrument_venues={"IF-CONT": "CFFEX"},
    )
    assert len(day_bars) == 1
    assert day_bars[0]["bar_start"] == "2026-01-05T01:30:00Z"
    with pytest.raises(ValidationError, match="unsupported"):
        build_event_bars(
            records,
            basis="bad",
            threshold=FixedPoint(1, 0),
            session_starts={SESSION_ID: start},
        )
    with pytest.raises(ValidationError, match="positive"):
        build_event_bars(
            records,
            basis="trade_count",
            threshold=FixedPoint(0, 0),
            session_starts={SESSION_ID: start},
        )
    with pytest.raises(ValidationError, match="scale zero"):
        build_event_bars(
            records,
            basis="trade_count",
            threshold=FixedPoint(10, 1),
            session_starts={SESSION_ID: start},
        )
    with pytest.raises(ValidationError, match="Missing session start"):
        build_event_bars(
            records,
            basis="trade_count",
            threshold=FixedPoint(1, 0),
            session_starts={},
        )
    with pytest.raises(ValidationError, match="not strictly ordered"):
        build_event_bars(
            [records[0], records[0]],
            basis="trade_count",
            threshold=FixedPoint(2, 0),
            session_starts={SESSION_ID: start},
        )
    assert (
        len(
            build_event_bars(
                records,
                basis="base_volume",
                threshold=FixedPoint(1, 0),
                session_starts={SESSION_ID: start},
            )
        )
        == 2
    )
    assert (
        len(
            build_event_bars(
                records,
                basis="quote_notional",
                threshold=FixedPoint(40001, 1),
                session_starts={SESSION_ID: start},
            )
        )
        == 2
    )
    with pytest.raises(ValidationError, match="below threshold"):
        build_event_bars(
            records,
            basis="trade_count",
            threshold=FixedPoint(3, 0),
            session_starts={SESSION_ID: start},
        )


def two_session_context(
    root: Path,
    *,
    first_id: str = "z-morning",
    second_id: str = "a-afternoon",
):
    base = market_context(root)
    first = replace(
        base.sessions[0],
        session_id=first_id,
        opens_at=datetime(2026, 1, 5, 1, 30, tzinfo=UTC),
        closes_at=datetime(2026, 1, 5, 1, 40, tzinfo=UTC),
    )
    second = replace(
        base.sessions[0],
        session_id=second_id,
        opens_at=datetime(2026, 1, 5, 1, 40, tzinfo=UTC),
        closes_at=datetime(2026, 1, 5, 2, 0, tzinfo=UTC),
    )
    return create_market_context_snapshot(
        root,
        calendar_id=base.calendar_id,
        session_policy_version="cffex-two-session-v1",
        instruments=base.instruments,
        sessions=[first, second],
        policy=TEST_POLICY,
    )


def two_session_trades(first_id: str, second_id: str) -> list[dict]:
    rows = [
        trade("t1", "2026-01-05T01:30:01Z", 1, 40001),
        trade("t2", "2026-01-05T01:30:20Z", 2, 40003),
        trade("t3", "2026-01-05T01:40:01Z", 1, 40002),
        trade("t4", "2026-01-05T01:40:20Z", 2, 40004),
    ]
    for row in rows[:2]:
        row["session_id"] = first_id
    for row in rows[2:]:
        row["session_id"] = second_id
    return rows


def test_multi_session_event_bars_have_unique_certified_partitions(tmp_path: Path) -> None:
    context = two_session_context(tmp_path)
    source = normalized(tmp_path, two_session_trades("z-morning", "a-afternoon"))
    snapshot = curate_trade_event_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="multi-session-event-bars",
        revision_id="r1",
        recipe_version="event-v1",
        basis="trade_count",
        threshold=FixedPoint(2, 0),
        market_context_snapshot_id=context.snapshot_id,
        policy=TEST_POLICY,
    )
    verified = load_verified_curated_bars(
        tmp_path, "multi-session-event-bars", snapshot.snapshot_id
    )
    evidence = verified.aggregation.partition_evidence if verified.aggregation else None
    assert evidence is not None
    paths = [item.relative_path for item in evidence]
    assert len(paths) == len(set(paths)) == 2
    assert all("/source=cn-fixture/" in item for item in paths)
    assert all("/session=" in item for item in paths)
    assert {item.session_id for item in evidence} == {"z-morning", "a-afternoon"}


def test_normalized_factory_orders_by_event_time_not_session_text(tmp_path: Path) -> None:
    context = two_session_context(tmp_path)
    source = normalized(tmp_path, two_session_trades("z-morning", "a-afternoon"))
    verified = load_verified_normalized_events(
        tmp_path,
        source.snapshot_id,
        [EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)],
        context.snapshot_id,
    )
    assert verified.table.column("session_id").to_pylist() == [
        "z-morning",
        "z-morning",
        "a-afternoon",
        "a-afternoon",
    ]
    assert verified.table.column("event_id").to_pylist() == ["t1", "t2", "t3", "t4"]


def test_event_bar_writer_partitions_independent_sources_without_rejecting_them(
    tmp_path: Path,
) -> None:
    primary = [
        trade("p1", "2026-01-05T01:30:01Z", 1),
        trade("p2", "2026-01-05T01:30:02Z", 2),
    ]
    source = normalized(tmp_path, primary)
    context = market_context(tmp_path)
    secondary = [
        dict(trade("s1", "2026-01-05T01:30:03Z", 1), source="other"),
        dict(trade("s2", "2026-01-05T01:30:04Z", 2), source="other"),
    ]
    streams = [("cn-fixture", primary), ("other", secondary)]
    bars: list[dict] = []
    scopes: dict[str, tuple[str, str]] = {}
    evidence: list[EventBarPartitionEvidence] = []
    for upstream_source, rows in streams:
        stream_bars = build_event_bars(
            rows,
            basis="trade_count",
            threshold=FixedPoint(2, 0),
            session_starts={SESSION_ID: datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
            source="curated",
            recipe_version="multi-source-v1",
        )
        bars.extend(stream_bars)
        scopes.update({str(bar["event_id"]): (upstream_source, SESSION_ID) for bar in stream_bars})
        relative_path = (
            "date=2026-01-05/instrument=IF-CONT/"
            f"source={upstream_source}/session={SESSION_ID}/data.parquet"
        )
        evidence.append(
            EventBarPartitionEvidence(
                relative_path=relative_path,
                source=upstream_source,
                instrument_id="IF-CONT",
                session_id=SESSION_ID,
                first_sequence=1,
                last_sequence=2,
                first_event_id=str(rows[0]["event_id"]),
                last_event_id=str(rows[-1]["event_id"]),
                event_count=2,
                source_selection_sha256=curated_module._source_selection_sha256(rows),
            )
        )
    aggregation = CuratedAggregation(
        calendar_id=context.calendar_id,
        session_policy_version=context.session_policy_version,
        kind="event_bar",
        recipe_version="multi-source-v1",
        event_bar_basis="trade_count",
        event_bar_threshold=FixedPoint(2, 0),
        market_context_snapshot_id=context.snapshot_id,
        market_context_logical_sha256=context.logical_sha256,
        source_event_schemas=(EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),),
        partition_evidence=tuple(evidence),
    )
    snapshot = curated_module._write_curated_bars(
        tmp_path,
        bars,
        dataset="multi-source-writer",
        revision_id="r1",
        recipe_version="multi-source-v1",
        normalized_snapshot_id=source.snapshot_id,
        policy=TEST_POLICY,
        aggregation=aggregation,
        event_partition_scopes=scopes,
    )
    assert len(snapshot.partitions) == 2
    assert {item.relative_path for item in snapshot.partitions} == {
        item.relative_path for item in evidence
    }


def test_bound_reader_rejects_actual_consumed_bytes_that_do_not_match_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = normalized(tmp_path, [trade("t1", "2026-01-05T01:30:01Z", 1)])
    context = market_context(tmp_path)
    target = (
        tmp_path
        / "normalized"
        / "snapshots"
        / source.snapshot_id
        / source.partitions[0].relative_path
    )
    original_read_bytes = Path.read_bytes

    def changed_bytes(path: Path) -> bytes:
        payload = original_read_bytes(path)
        if path == target:
            return payload[:-1] + bytes([payload[-1] ^ 1])
        return payload

    monkeypatch.setattr(Path, "read_bytes", changed_bytes)
    with pytest.raises(ValidationError, match="bytes differ from its manifest"):
        load_verified_normalized_events(
            tmp_path,
            source.snapshot_id,
            [EventSchemaRef(TRADE_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2)],
            context.snapshot_id,
        )


def test_partition_row_binding_and_source_order_fail_closed(tmp_path: Path) -> None:
    source = normalized(
        tmp_path,
        [
            trade("t1", "2026-01-05T01:30:01Z", 1),
            trade("t2", "2026-01-05T01:30:02Z", 2),
        ],
    )
    normalized_path = (
        tmp_path
        / "normalized"
        / "snapshots"
        / source.snapshot_id
        / source.partitions[0].relative_path
    )
    normalized_table = pq.read_table(normalized_path)
    reversed_table = normalized_table.take(pa.array([1, 0]))
    with pytest.raises(ValidationError, match="strictly ordered"):
        research_inputs._validate_normalized_partition_table(source.partitions[0], reversed_table)
    wrong_normalized_rows = normalized_table.to_pylist()
    wrong_normalized_rows[0] = dict(wrong_normalized_rows[0], instrument_id="OTHER")
    wrong_normalized = pa.Table.from_pylist(wrong_normalized_rows, schema=normalized_table.schema)
    with pytest.raises(ValidationError, match="instrument differs"):
        research_inputs._validate_normalized_partition_table(source.partitions[0], wrong_normalized)

    context = market_context(tmp_path)
    curated = curate_trade_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="partition-binding",
        revision_id="r1",
        recipe_version="fixed-v1",
        interval=timedelta(minutes=1),
        session_starts={SESSION_ID: datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
        market_context_snapshot_id=context.snapshot_id,
        policy=TEST_POLICY,
    )
    curated_path = (
        tmp_path
        / "curated"
        / "partition-binding"
        / "snapshots"
        / curated.snapshot_id
        / curated.partitions[0].relative_path
    )
    curated_table = pq.read_table(curated_path)
    wrong_curated_rows = curated_table.to_pylist()
    wrong_curated_rows[0] = dict(wrong_curated_rows[0], trading_day=date(2026, 1, 6))
    wrong_curated = pa.Table.from_pylist(wrong_curated_rows, schema=curated_table.schema)
    with pytest.raises(ValidationError, match="trading_day"):
        curated_module._validate_curated_partition_table(
            curated.partitions[0], wrong_curated, curated.aggregation
        )


def test_partition_binding_helpers_cover_all_metadata_and_order_guards(tmp_path: Path) -> None:
    source = normalized(
        tmp_path,
        [
            trade("t1", "2026-01-05T01:30:01Z", 1),
            trade("t2", "2026-01-05T01:30:02Z", 2),
        ],
    )
    partition = source.partitions[0]
    path = tmp_path / "normalized" / "snapshots" / source.snapshot_id / partition.relative_path
    table = pq.read_table(path)
    with pytest.raises(ValidationError, match="row count"):
        research_inputs._validate_normalized_partition_table(
            replace(partition, rows=partition.rows + 1), table
        )
    metadata_cases = [
        (replace(partition, event_type="quote"), "event_type"),
        (replace(partition, trading_date="2026-01-06"), "trading_day"),
        (replace(partition, provider="other"), "source"),
    ]
    for changed_partition, message in metadata_cases:
        with pytest.raises(ValidationError, match=message):
            research_inputs._validate_normalized_partition_table(changed_partition, table)
    duplicate_sequence_rows = table.to_pylist()
    duplicate_sequence_rows[1] = dict(duplicate_sequence_rows[1], sequence=1)
    duplicate_sequence = pa.Table.from_pylist(duplicate_sequence_rows, schema=table.schema)
    with pytest.raises(ValidationError, match="sequence does not advance"):
        research_inputs._validate_normalized_partition_table(partition, duplicate_sequence)

    context = market_context(tmp_path)
    event = curate_trade_event_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="partition-helper-event",
        revision_id="r1",
        recipe_version="event-v1",
        basis="trade_count",
        threshold=FixedPoint(2, 0),
        market_context_snapshot_id=context.snapshot_id,
        policy=TEST_POLICY,
    )
    event_partition = event.partitions[0]
    event_path = (
        tmp_path
        / "curated"
        / "partition-helper-event"
        / "snapshots"
        / event.snapshot_id
        / event_partition.relative_path
    )
    event_table = pq.read_table(event_path)
    with pytest.raises(ValidationError, match="row count"):
        curated_module._validate_curated_partition_table(
            replace(event_partition, rows=event_partition.rows + 1),
            event_table,
            event.aggregation,
        )
    wrong_instrument_rows = event_table.to_pylist()
    wrong_instrument_rows[0] = dict(wrong_instrument_rows[0], instrument_id="OTHER")
    wrong_instrument = pa.Table.from_pylist(wrong_instrument_rows, schema=event_table.schema)
    with pytest.raises(ValidationError, match="instrument"):
        curated_module._validate_curated_partition_table(
            event_partition, wrong_instrument, event.aggregation
        )
    multiple_session_rows = event_table.to_pylist()
    multiple_session_rows.append(
        dict(
            multiple_session_rows[0],
            event_id="other-session",
            event_time=datetime(2026, 1, 5, 1, 31, tzinfo=UTC),
            received_at=datetime(2026, 1, 5, 1, 31, tzinfo=UTC),
            available_at=datetime(2026, 1, 5, 1, 31, tzinfo=UTC),
            bar_end=datetime(2026, 1, 5, 1, 31, tzinfo=UTC),
            sequence=event_table.num_rows + 1,
            session_id="other-session",
        )
    )
    multiple_session = pa.Table.from_pylist(multiple_session_rows, schema=event_table.schema)
    expanded_partition = replace(event_partition, rows=multiple_session.num_rows)
    with pytest.raises(ValidationError, match="exactly one session"):
        curated_module._validate_curated_partition_table(
            expanded_partition, multiple_session, event.aggregation
        )


def test_content_bound_reader_and_final_stamp_failure_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = normalized(tmp_path, [trade("t1", "2026-01-05T01:30:01Z", 1)])
    partition = source.partitions[0]
    path = tmp_path / "normalized" / "snapshots" / source.snapshot_id / partition.relative_path
    original_stamp = research_inputs._file_stamp
    calls = 0

    def changed_during_read(target: Path):
        nonlocal calls
        calls += 1
        stamp = original_stamp(target)
        return (*stamp[:-1], stamp[-1] + 1) if calls == 2 else stamp

    monkeypatch.setattr(research_inputs, "_file_stamp", changed_during_read)
    with pytest.raises(ValidationError, match="changed while reading"):
        research_inputs._read_content_bound_parquet(path, partition.content_sha256)
    monkeypatch.setattr(research_inputs, "_file_stamp", original_stamp)

    invalid = tmp_path / "invalid.parquet"
    invalid.write_bytes(b"not parquet")
    with pytest.raises(ValidationError, match="not readable Parquet"):
        research_inputs._read_content_bound_parquet(
            invalid, hashlib.sha256(b"not parquet").hexdigest()
        )
    with pytest.raises(ValidationError, match="changed during verified read"):
        research_inputs._assert_file_stamps({path: (0, 0, 0, 0, 0)})
    missing = tmp_path / "missing.parquet"
    with pytest.raises(ValidationError, match="disappeared"):
        research_inputs._assert_file_stamps({missing: (0, 0, 0, 0, 0)})


def test_trading_day_rollup_uses_all_authoritative_sessions(tmp_path: Path) -> None:
    morning_id = "CFFEX-IF-2026-01-05-AM"
    afternoon_id = "CFFEX-IF-2026-01-05-PM"
    source_records = [
        dict(
            trade("t1", "2026-01-05T01:31:00Z", 1),
            session_id=morning_id,
        ),
        dict(
            trade("t2", "2026-01-05T03:01:00Z", 1),
            session_id=afternoon_id,
        ),
    ]
    source = normalized(tmp_path, source_records, key="two-sessions")
    base_context = market_context(tmp_path)
    instrument = base_context.instruments[0]
    sessions = [
        TradingSession(
            session_id=morning_id,
            calendar_id="cffex-v1",
            venue="CFFEX",
            trading_day=date(2026, 1, 5),
            phase=SessionPhase.CONTINUOUS,
            opens_at=datetime(2026, 1, 5, 1, 30, tzinfo=UTC),
            closes_at=datetime(2026, 1, 5, 2, 0, tzinfo=UTC),
            available_at=datetime(2025, 12, 1, tzinfo=UTC),
        ),
        TradingSession(
            session_id=afternoon_id,
            calendar_id="cffex-v1",
            venue="CFFEX",
            trading_day=date(2026, 1, 5),
            phase=SessionPhase.CONTINUOUS,
            opens_at=datetime(2026, 1, 5, 3, 0, tzinfo=UTC),
            closes_at=datetime(2026, 1, 5, 4, 0, tzinfo=UTC),
            available_at=datetime(2025, 12, 1, tzinfo=UTC),
        ),
    ]
    context = create_market_context_snapshot(
        tmp_path,
        calendar_id="cffex-v1",
        session_policy_version="cffex-split-v1",
        instruments=[instrument],
        sessions=sessions,
        policy=TEST_POLICY,
    )
    curated = curate_session_bars_from_snapshot(
        tmp_path,
        normalized_snapshot_id=source.snapshot_id,
        dataset="trading-day-bars",
        revision_id="r1",
        recipe_version="trading-day-v1",
        session_rollup="trading_day",
        market_context_snapshot_id=context.snapshot_id,
        policy=TEST_POLICY,
    )
    verified = load_verified_curated_bars(tmp_path, "trading-day-bars", curated.snapshot_id)
    row = verified.table.to_pylist()[0]
    assert row["bar_start"] == datetime(2026, 1, 5, 1, 30, tzinfo=UTC)
    assert row["bar_end"] == datetime(2026, 1, 5, 4, 0, tzinfo=UTC)
    assert row["session_id"] == afternoon_id


def test_verified_normalized_l2_requires_snapshot_anchor_and_replays(tmp_path: Path) -> None:
    events = [
        dict(
            l2_event("book_snapshot", "s1", 10, "2026-01-05T01:30:01Z"),
            source="crypto-fixture",
        ),
        dict(
            l2_event("book_delta", "d1", 11, "2026-01-05T01:30:02Z"),
            source="crypto-fixture",
        ),
    ]
    raw = write_raw_bytes(
        tmp_path,
        source="crypto-fixture",
        request={"fixture": "l2"},
        collected_at="2026-01-05T01:00:00Z",
        payload=b"l2",
        idempotency_key="l2",
        policy=TEST_POLICY,
    )
    result = write_normalized_events(
        tmp_path,
        events,
        provider="crypto-fixture",
        venue="BINANCE",
        upstream_raw_references=[raw.reference()],
        policy=TEST_POLICY,
    )
    assert result.snapshot is not None
    instrument = InstrumentSpec(
        instrument_id="BTC-USDT",
        asset_class=AssetClass.CRYPTO,
        product_type="spot",
        venue="BINANCE",
        native_symbol="BTCUSDT",
        base_currency="BTC",
        quote_currency="USDT",
        settlement_currency="USDT",
        price_tick=FixedPoint(1, 2),
        quantity_step=FixedPoint(1, 6),
        contract_multiplier=FixedPoint(1, 0),
        calendar_id="crypto-24x7-v1",
        margin_mode=MarginMode.CASH,
        effective_from=datetime(2025, 1, 1, tzinfo=UTC),
        available_at=datetime(2025, 1, 1, tzinfo=UTC),
    )
    session = TradingSession(
        session_id="crypto-day",
        calendar_id="crypto-24x7-v1",
        venue="BINANCE",
        trading_day=date(2026, 1, 5),
        phase=SessionPhase.CONTINUOUS,
        opens_at=datetime(2026, 1, 5, tzinfo=UTC),
        closes_at=datetime(2026, 1, 6, tzinfo=UTC),
        available_at=datetime(2025, 12, 1, tzinfo=UTC),
    )
    context = create_market_context_snapshot(
        tmp_path,
        calendar_id="crypto-24x7-v1",
        session_policy_version="utc-day-v1",
        instruments=[instrument],
        sessions=[session],
        policy=TEST_POLICY,
    )
    verified = load_verified_normalized_events(
        tmp_path,
        result.snapshot.snapshot_id,
        [
            EventSchemaRef(BOOK_DELTA_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),
            EventSchemaRef(BOOK_SNAPSHOT_EVENT_SCHEMA_ID, SCHEMA_VERSION_V2),
        ],
        context.snapshot_id,
    )
    assert verified.table.num_rows == 2
    assert set(verified.table.column("event_schema_id").to_pylist()) == {
        BOOK_DELTA_EVENT_SCHEMA_ID,
        BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    }
