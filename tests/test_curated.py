from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from quant_data_kit.curated import (
    build_session_bars,
    load_curated_snapshot,
    write_curated_bars,
)
from quant_data_kit.exceptions import ValidationError

UTC = timezone.utc


def trade(event_id: str, timestamp: str, price_units: int, quantity_units: int) -> dict:
    return {
        "event_type": "trade",
        "event_id": event_id,
        "instrument_id": "IF-CONT",
        "event_time": timestamp,
        "received_at": timestamp,
        "available_at": timestamp,
        "source": "cn-fixture",
        "trading_day": "2026-01-05",
        "session_id": "CFFEX-IF-2026-01-05-DAY",
        "sequence": None,
        "price": {"units": price_units, "scale": 1},
        "quantity": {"units": quantity_units, "scale": 0},
        "aggressor_side": "unknown",
    }


def test_session_bar_aggregation_is_deterministic_and_session_anchored() -> None:
    records = [
        trade("t1", "2026-01-05T01:30:01Z", 40001, 2),
        trade("t2", "2026-01-05T01:30:45Z", 40005, 3),
        trade("t3", "2026-01-05T01:31:01Z", 39999, 1),
    ]
    session_starts = {"CFFEX-IF-2026-01-05-DAY": datetime(2026, 1, 5, 1, 30, tzinfo=UTC)}
    first = build_session_bars(
        records,
        interval=timedelta(minutes=1),
        session_starts=session_starts,
        source="curated-fixture",
    )
    second = build_session_bars(
        records,
        interval=timedelta(minutes=1),
        session_starts=session_starts,
        source="curated-fixture",
    )
    assert first == second
    assert len(first) == 2
    assert first[0]["bar_start"] == "2026-01-05T01:30:00Z"
    assert first[0]["bar_end"] == "2026-01-05T01:31:00Z"
    assert first[0]["open_price"]["units"] == 40001
    assert first[0]["high_price"]["units"] == 40005
    assert first[0]["close_price"]["units"] == 40005
    assert first[0]["volume"] == {"units": 5, "scale": 0}


def test_session_bar_refuses_missing_or_future_session_start() -> None:
    record = trade("t1", "2026-01-05T01:30:01Z", 40001, 2)
    with pytest.raises(ValidationError, match="Missing session start"):
        build_session_bars([record], interval=timedelta(minutes=1), session_starts={})
    with pytest.raises(ValidationError, match="precedes its session start"):
        build_session_bars(
            [record],
            interval=timedelta(minutes=1),
            session_starts={
                record["session_id"]: datetime(2026, 1, 5, 1, 31, tzinfo=UTC)
            },
        )


def test_curated_revision_is_immutable_and_lineage_changes_snapshot(tmp_path: Path) -> None:
    bars = build_session_bars(
        [trade("t1", "2026-01-05T01:30:01Z", 40001, 2)],
        interval=timedelta(minutes=1),
        session_starts={"CFFEX-IF-2026-01-05-DAY": datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
    )
    first = write_curated_bars(
        tmp_path,
        bars,
        dataset="session-bars-1m",
        revision_id="revision-1",
        recipe_version="session-bars-v1",
        lineage={"normalized_snapshot": "sha256-upstream-1"},
    )
    repeated = write_curated_bars(
        tmp_path,
        bars,
        dataset="session-bars-1m",
        revision_id="revision-1",
        recipe_version="session-bars-v1",
        lineage={"normalized_snapshot": "sha256-upstream-1"},
    )
    revised = write_curated_bars(
        tmp_path,
        deepcopy(bars),
        dataset="session-bars-1m",
        revision_id="revision-2",
        recipe_version="session-bars-v1",
        lineage={"normalized_snapshot": "sha256-upstream-2"},
    )
    assert repeated.snapshot_id == first.snapshot_id
    assert revised.snapshot_id != first.snapshot_id
    assert load_curated_snapshot(
        tmp_path, "session-bars-1m", revised.snapshot_id
    ).lineage == {"normalized_snapshot": "sha256-upstream-2"}
    with pytest.raises(ValidationError, match="explicit content-addressed"):
        load_curated_snapshot(tmp_path, "session-bars-1m", "latest")


def test_curated_partition_mutation_is_detected(tmp_path: Path) -> None:
    bars = build_session_bars(
        [trade("t1", "2026-01-05T01:30:01Z", 40001, 2)],
        interval=timedelta(minutes=1),
        session_starts={"CFFEX-IF-2026-01-05-DAY": datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
    )
    snapshot = write_curated_bars(
        tmp_path,
        bars,
        dataset="bars",
        revision_id="r1",
        recipe_version="v1",
        lineage={"normalized_snapshot": "sha256-upstream"},
    )
    snapshot_dir = tmp_path / "curated" / "bars" / "snapshots" / snapshot.snapshot_id
    partition = snapshot_dir / snapshot.partitions[0].relative_path
    partition.write_bytes(partition.read_bytes() + b"changed")
    with pytest.raises(ValidationError, match="hash changed"):
        load_curated_snapshot(tmp_path, "bars", snapshot.snapshot_id)


def test_curated_reader_rejects_dataset_and_snapshot_path_traversal(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="Windows-safe"):
        load_curated_snapshot(tmp_path, "../outside", "sha256-" + "0" * 24)
    with pytest.raises(ValidationError, match="explicit content-addressed"):
        load_curated_snapshot(tmp_path, "bars", "sha256-../../outside")
