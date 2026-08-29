from __future__ import annotations

from copy import deepcopy

import pytest

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.l2_replay import L2BookReconstructor, L2ReplayError, replay_l2


def snapshot() -> dict:
    return {
        "event_type": "book_snapshot",
        "event_id": "snapshot-100",
        "instrument_id": "BTC-USDT-SPOT",
        "event_time": "2026-01-02T00:00:00Z",
        "received_at": "2026-01-02T00:00:00Z",
        "available_at": "2026-01-02T00:00:00Z",
        "source": "binance",
        "trading_day": "2026-01-02",
        "session_id": "binance-24x7-BTC-USDT-SPOT",
        "sequence": 100,
        "bids": [
            {
                "price": {"units": 100_000, "scale": 2},
                "quantity": {"units": 10, "scale": 3},
                "order_count": 2,
            },
            {
                "price": {"units": 99_900, "scale": 2},
                "quantity": {"units": 20, "scale": 3},
                "order_count": 3,
            },
        ],
        "asks": [
            {
                "price": {"units": 100_100, "scale": 2},
                "quantity": {"units": 15, "scale": 3},
                "order_count": 1,
            },
            {
                "price": {"units": 100_200, "scale": 2},
                "quantity": {"units": 30, "scale": 3},
                "order_count": 4,
            },
        ],
    }


def delta(
    sequence: int,
    previous_sequence: int,
    *,
    side: str = "bid",
    action: str = "upsert",
    price_units: int = 100_000,
    quantity_units: int = 25,
) -> dict:
    timestamp = f"2026-01-02T00:00:{sequence - 100:02d}Z"
    return {
        "event_type": "book_delta",
        "event_id": f"delta-{sequence}",
        "instrument_id": "BTC-USDT-SPOT",
        "event_time": timestamp,
        "received_at": timestamp,
        "available_at": timestamp,
        "source": "binance",
        "trading_day": "2026-01-02",
        "session_id": "binance-24x7-BTC-USDT-SPOT",
        "sequence": sequence,
        "side": side,
        "action": action,
        "price": {"units": price_units, "scale": 2},
        "quantity": {"units": quantity_units, "scale": 3},
        "previous_sequence": previous_sequence,
    }


def apply_atomic_group(
    reconstructor: L2BookReconstructor,
    events: list[dict],
) -> None:
    reconstructor._apply_validated_atomic_delta_group(
        source=str(events[0]["source"]),
        instrument_id=str(events[0]["instrument_id"]),
        session_id=str(events[0]["session_id"]),
        event_time=str(events[0]["event_time"]),
        sequences=[int(event["sequence"]) for event in events],
        previous_sequences=[int(event["previous_sequence"]) for event in events],
        sides=[str(event["side"]) for event in events],
        actions=[str(event["action"]) for event in events],
        price_units=[int(event["price"]["units"]) for event in events],
        price_scales=[int(event["price"]["scale"]) for event in events],
        quantity_units=[int(event["quantity"]["units"]) for event in events],
        quantity_scales=[int(event["quantity"]["scale"]) for event in events],
    )


def test_snapshot_delta_replay_and_checkpoint_hash_are_deterministic() -> None:
    events = [snapshot(), delta(101, 100), delta(102, 101, side="ask", price_units=100_100)]
    first = replay_l2(events)
    second = replay_l2(deepcopy(events))
    assert first == second
    assert first.final_checkpoint.sequence == 102
    assert len(first.final_checkpoint.state_sha256) == 64
    expected = {item.sequence: item.state_sha256 for item in first.checkpoints}
    assert replay_l2(events, expected_checkpoint_hashes=expected) == first
    compact = replay_l2(events, capture_all_checkpoints=False)
    assert compact.final_checkpoint == first.final_checkpoint
    assert compact.checkpoints == (first.final_checkpoint,)


@pytest.mark.parametrize(
    ("events", "message"),
    [
        ([snapshot(), delta(101, 99)], "gap, duplicate, or out-of-order"),
        ([snapshot(), delta(101, 100), delta(101, 100)], "Duplicate event_id"),
        ([snapshot(), delta(102, 100), delta(101, 100)], "gap, duplicate, or out-of-order"),
    ],
)
def test_gap_duplicate_and_out_of_order_are_rejected(events: list[dict], message: str) -> None:
    with pytest.raises(L2ReplayError, match=message):
        replay_l2(events)


def test_crossed_delta_is_transactional_and_does_not_pollute_state() -> None:
    reconstructor = L2BookReconstructor()
    initial = reconstructor.apply(snapshot())
    crossed = delta(101, 100, price_units=100_100)
    with pytest.raises(L2ReplayError, match="locked or crossed"):
        reconstructor.apply(crossed)
    assert reconstructor.checkpoint() == initial


def test_atomic_multi_level_group_matches_serial_golden_and_checks_book_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = [
        delta(101, 100, side="bid", price_units=100_000, quantity_units=26),
        delta(102, 101, side="ask", price_units=100_100, quantity_units=27),
        delta(
            103,
            102,
            side="bid",
            action="delete",
            price_units=99_900,
            quantity_units=0,
        ),
        delta(104, 103, side="bid", price_units=99_800, quantity_units=28),
    ]
    for event in events:
        for field in ("event_time", "received_at", "available_at"):
            event[field] = "2026-01-02T00:00:01Z"

    serial = L2BookReconstructor()
    serial.apply_snapshot(snapshot())
    for event in events:
        serial.apply_delta(event)

    atomic = L2BookReconstructor()
    atomic.apply_snapshot(snapshot())
    real_assert = L2BookReconstructor._assert_book_valid
    checks = 0

    def counted_assert(bids, asks) -> None:
        nonlocal checks
        checks += 1
        real_assert(bids, asks)

    monkeypatch.setattr(
        L2BookReconstructor,
        "_assert_book_valid",
        staticmethod(counted_assert),
    )
    apply_atomic_group(atomic, events)
    assert checks == 1
    assert atomic.checkpoint() == serial.checkpoint()


def test_atomic_multi_level_group_allows_transient_cross_but_commits_valid_final_book() -> None:
    events = [
        delta(101, 100, side="bid", price_units=100_150, quantity_units=25),
        delta(
            102,
            101,
            side="ask",
            action="delete",
            price_units=100_100,
            quantity_units=0,
        ),
    ]
    for event in events:
        for field in ("event_time", "received_at", "available_at"):
            event[field] = "2026-01-02T00:00:01Z"

    serial = L2BookReconstructor()
    serial.apply_snapshot(snapshot())
    with pytest.raises(L2ReplayError, match="locked or crossed"):
        serial.apply_delta(events[0])

    atomic = L2BookReconstructor()
    atomic.apply_snapshot(snapshot())
    apply_atomic_group(atomic, events)
    checkpoint = atomic.checkpoint()
    assert checkpoint.sequence == 102
    assert checkpoint.bids[0].price_units == 100_150
    assert checkpoint.asks[0].price_units == 100_200


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda events: events[1].update(previous_sequence=100), "gap, duplicate"),
        (lambda events: events[1].update(sequence=101), "strictly advance"),
        (lambda events: events[1].update(side="invalid"), "Unsupported L2 side"),
        (lambda events: events[1].update(action="invalid"), "Unsupported L2 action"),
        (lambda events: events[1]["price"].update(units=0), "price must be positive"),
        (lambda events: events[1]["price"].update(scale=3), "price scale changed"),
        (lambda events: events[1]["quantity"].update(units=-1), "non-negative"),
        (lambda events: events[1]["quantity"].update(scale=-1), "outside"),
        (lambda events: events[1]["quantity"].update(scale=19), "outside"),
        (
            lambda events: (
                events[1].update(action="delete"),
                events[1]["quantity"].update(units=1),
            ),
            "delete delta quantity",
        ),
        (lambda events: events[1]["quantity"].update(units=0), "upsert delta quantity"),
        (
            lambda events: (
                events[1].update(action="delete"),
                events[1]["price"].update(units=98_000),
                events[1]["quantity"].update(units=0),
            ),
            "absent price level",
        ),
        (lambda events: events[1]["price"].update(units=100_100), "locked or crossed"),
    ],
)
def test_atomic_multi_level_group_rejects_bad_row_and_rolls_back(
    mutate,
    message: str,
) -> None:
    events = [delta(101, 100), delta(102, 101, price_units=99_900)]
    for event in events:
        for field in ("event_time", "received_at", "available_at"):
            event[field] = "2026-01-02T00:00:01Z"
    mutate(events)
    reconstructor = L2BookReconstructor()
    initial = reconstructor.apply_snapshot(snapshot())
    with pytest.raises(L2ReplayError, match=message):
        apply_atomic_group(reconstructor, events)
    assert reconstructor.checkpoint() == initial


def test_atomic_multi_level_group_defensive_entry_guards_preserve_state() -> None:
    event = delta(101, 100)
    arguments = {
        "source": "binance",
        "instrument_id": "BTC-USDT-SPOT",
        "session_id": "binance-24x7-BTC-USDT-SPOT",
        "event_time": event["event_time"],
        "sequences": [101],
        "previous_sequences": [100],
        "sides": ["bid"],
        "actions": ["upsert"],
        "price_units": [100_000],
        "price_scales": [2],
        "quantity_units": [25],
        "quantity_scales": [3],
    }
    uninitialized = L2BookReconstructor()
    with pytest.raises(L2ReplayError, match="start from a BookSnapshot"):
        uninitialized._apply_validated_atomic_delta_group(**arguments)

    reconstructor = L2BookReconstructor()
    initial = reconstructor.apply_snapshot(snapshot())
    with pytest.raises(L2ReplayError, match="identity changed"):
        reconstructor._apply_validated_atomic_delta_group(**{**arguments, "source": "okx"})
    empty = {name: [] for name in (
        "sequences",
        "previous_sequences",
        "sides",
        "actions",
        "price_units",
        "price_scales",
        "quantity_units",
        "quantity_scales",
    )}
    with pytest.raises(L2ReplayError, match="must not be empty"):
        reconstructor._apply_validated_atomic_delta_group(**{**arguments, **empty})
    with pytest.raises(L2ReplayError, match="different lengths"):
        reconstructor._apply_validated_atomic_delta_group(
            **{**arguments, "quantity_scales": [3, 3]}
        )
    with pytest.raises(L2ReplayError, match="moved backwards"):
        reconstructor._apply_validated_atomic_delta_group(
            **{**arguments, "event_time": "2026-01-01T23:59:59Z"}
        )
    assert reconstructor.checkpoint() == initial


def test_delete_absent_level_and_price_scale_change_are_rejected() -> None:
    reconstructor = L2BookReconstructor()
    reconstructor.apply(snapshot())
    absent = delta(
        101,
        100,
        action="delete",
        price_units=98_000,
        quantity_units=0,
    )
    with pytest.raises(L2ReplayError, match="absent price level"):
        reconstructor.apply(absent)
    scale_change = delta(101, 100)
    scale_change["price"] = {"units": 1_000, "scale": 1}
    with pytest.raises(L2ReplayError, match="price scale changed"):
        reconstructor.apply(scale_change)


def test_checkpoint_mismatch_and_missing_checkpoint_fail() -> None:
    events = [snapshot(), delta(101, 100)]
    with pytest.raises(L2ReplayError, match="checkpoint hash mismatch"):
        replay_l2(events, expected_checkpoint_hashes={101: "0" * 64})
    with pytest.raises(L2ReplayError, match="not reached"):
        replay_l2(events, expected_checkpoint_hashes={999: "0" * 64})


def test_snapshot_duplicate_price_and_cross_are_rejected_by_frozen_contract() -> None:
    duplicate = snapshot()
    duplicate["bids"][1]["price"] = duplicate["bids"][0]["price"]
    with pytest.raises((L2ReplayError, ValidationError), match="duplicates"):
        replay_l2([duplicate])
    crossed = snapshot()
    crossed["bids"][0]["price"] = crossed["asks"][0]["price"]
    with pytest.raises((L2ReplayError, ValidationError), match="crossed"):
        replay_l2([crossed])


def test_resync_snapshot_cannot_switch_stream_identity() -> None:
    first = snapshot()
    second = snapshot()
    second["event_id"] = "snapshot-other"
    second["instrument_id"] = "ETH-USDT-SPOT"
    second["session_id"] = "binance-24x7-ETH-USDT-SPOT"
    second["sequence"] = 200
    with pytest.raises(L2ReplayError, match="identity changed"):
        replay_l2([first, second])


def test_uninitialized_unsupported_and_non_snapshot_entry_fail_closed() -> None:
    reconstructor = L2BookReconstructor()
    with pytest.raises(L2ReplayError, match="uninitialized"):
        reconstructor.checkpoint()
    with pytest.raises(L2ReplayError, match="Unsupported"):
        reconstructor.apply({"event_type": "trade"})
    with pytest.raises(L2ReplayError, match="start from a BookSnapshot"):
        reconstructor.apply_delta(delta(101, 100))
    with pytest.raises(L2ReplayError, match="at least one"):
        replay_l2([])
    with pytest.raises(L2ReplayError, match="begin with a BookSnapshot"):
        replay_l2([delta(101, 100)])


def test_snapshot_and_delta_time_and_sequence_must_strictly_advance() -> None:
    reconstructor = L2BookReconstructor()
    reconstructor.apply_snapshot(snapshot())
    same_snapshot = snapshot()
    same_snapshot["event_id"] = "snapshot-repeat"
    with pytest.raises(L2ReplayError, match="Snapshot sequence must advance"):
        reconstructor.apply_snapshot(same_snapshot)

    non_advancing = delta(100, 100)
    with pytest.raises((L2ReplayError, ValidationError), match="precede|strictly advance"):
        reconstructor.apply_delta(non_advancing)
    backwards = delta(101, 100)
    backwards["event_time"] = "2026-01-01T23:59:59Z"
    backwards["received_at"] = backwards["event_time"]
    backwards["available_at"] = backwards["event_time"]
    with pytest.raises(L2ReplayError, match="moved backwards"):
        reconstructor.apply_delta(backwards)


def test_validated_snapshot_and_batch_guards_preserve_state() -> None:
    reconstructor = L2BookReconstructor()
    initial = reconstructor.apply_snapshot(snapshot())

    resync = snapshot()
    resync["event_id"] = "snapshot-200"
    resync["sequence"] = 200
    resync["event_time"] = "2026-01-02T00:01:00Z"
    resync["received_at"] = resync["event_time"]
    resync["available_at"] = resync["event_time"]
    assert reconstructor.apply_snapshot(resync).sequence == 200

    empty_level = deepcopy(resync)
    empty_level["sequence"] = 201
    empty_level["bids"][0]["quantity"]["units"] = 0
    with pytest.raises(L2ReplayError, match="empty price levels"):
        reconstructor._apply_validated_snapshot_state(empty_level)

    mixed_scale = deepcopy(resync)
    mixed_scale["sequence"] = 201
    mixed_scale["asks"][0]["price"]["scale"] = 3
    with pytest.raises(L2ReplayError, match="price scales differ"):
        reconstructor._apply_validated_snapshot_state(mixed_scale)
    assert reconstructor.checkpoint().sequence == 200
    assert initial.sequence == 100


def test_private_validated_dispatch_and_uniform_batch_fail_closed() -> None:
    reconstructor = L2BookReconstructor()
    with pytest.raises(L2ReplayError, match="Unsupported"):
        reconstructor._apply_without_checkpoint({"event_type": "trade"})
    with pytest.raises(L2ReplayError, match="Unsupported"):
        reconstructor._apply_validated_without_checkpoint({"event_type": "trade"})
    with pytest.raises(L2ReplayError, match="start from a BookSnapshot"):
        reconstructor._apply_validated_uniform_upsert_batch(
            source="binance",
            instrument_id="BTC-USDT-SPOT",
            session_id="binance-24x7-BTC-USDT-SPOT",
            first_previous_sequence=100,
            final_sequence=101,
            first_event_time="2026-01-02T00:00:01Z",
            final_event_time="2026-01-02T00:00:01Z",
            side="bid",
            price_units=100_000,
            price_scale=2,
            quantity_units=25,
            quantity_scale=3,
        )

    reconstructor.apply_snapshot(snapshot())
    arguments = {
        "source": "binance",
        "instrument_id": "BTC-USDT-SPOT",
        "session_id": "binance-24x7-BTC-USDT-SPOT",
        "first_previous_sequence": 100,
        "final_sequence": 101,
        "first_event_time": "2026-01-02T00:00:01Z",
        "final_event_time": "2026-01-02T00:00:01Z",
        "side": "bid",
        "price_units": 100_000,
        "price_scale": 2,
        "quantity_units": 25,
        "quantity_scale": 3,
    }
    wrong_identity = {**arguments, "source": "okx"}
    with pytest.raises(L2ReplayError, match="identity changed"):
        reconstructor._apply_validated_uniform_upsert_batch(**wrong_identity)
    non_advancing = {**arguments, "final_sequence": 100}
    with pytest.raises(L2ReplayError, match="strictly advance"):
        reconstructor._apply_validated_uniform_upsert_batch(**non_advancing)
    wrong_scale = {**arguments, "price_scale": 3}
    with pytest.raises(L2ReplayError, match="price scale changed"):
        reconstructor._apply_validated_uniform_upsert_batch(**wrong_scale)
