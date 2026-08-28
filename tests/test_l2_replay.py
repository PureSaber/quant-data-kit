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
