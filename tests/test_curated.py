from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from quant_data_kit.curated import (
    build_session_bars,
    curate_trade_bars_from_snapshot,
    load_curated_snapshot,
)
from quant_data_kit.data_lake import StoragePolicy, write_normalized_events, write_raw_bytes
from quant_data_kit.exceptions import ValidationError

UTC = timezone.utc
TEST_POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)


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


def normalized_trades(root: Path, records: list[dict], *, key: str):
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


def curate(root: Path, normalized_snapshot_id: str, *, revision_id: str, dataset: str = "bars"):
    return curate_trade_bars_from_snapshot(
        root,
        normalized_snapshot_id=normalized_snapshot_id,
        dataset=dataset,
        revision_id=revision_id,
        recipe_version="session-bars-v1",
        interval=timedelta(minutes=1),
        session_starts={"CFFEX-IF-2026-01-05-DAY": datetime(2026, 1, 5, 1, 30, tzinfo=UTC)},
        policy=TEST_POLICY,
    )


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
            session_starts={record["session_id"]: datetime(2026, 1, 5, 1, 31, tzinfo=UTC)},
        )


def test_curated_revision_is_immutable_and_lineage_changes_snapshot(tmp_path: Path) -> None:
    first_normalized = normalized_trades(
        tmp_path,
        [trade("t1", "2026-01-05T01:30:01Z", 40001, 2)],
        key="raw-1",
    )
    first = curate(
        tmp_path,
        first_normalized.snapshot_id,
        revision_id="revision-1",
        dataset="session-bars-1m",
    )
    repeated = curate(
        tmp_path,
        first_normalized.snapshot_id,
        revision_id="revision-1",
        dataset="session-bars-1m",
    )
    changed = trade("t2", "2026-01-05T01:30:01Z", 40002, 2)
    second_normalized = normalized_trades(tmp_path, [changed], key="raw-2")
    with pytest.raises(ValidationError, match="maps to different content"):
        curate(
            tmp_path,
            second_normalized.snapshot_id,
            revision_id="revision-1",
            dataset="session-bars-1m",
        )
    revised = curate(
        tmp_path,
        second_normalized.snapshot_id,
        revision_id="revision-2",
        dataset="session-bars-1m",
    )
    assert repeated.snapshot_id == first.snapshot_id
    assert revised.snapshot_id != first.snapshot_id
    assert (
        load_curated_snapshot(tmp_path, "session-bars-1m", revised.snapshot_id).lineage[
            "normalized_snapshot_id"
        ]
        == second_normalized.snapshot_id
    )
    with pytest.raises(ValidationError, match="explicit content-addressed"):
        load_curated_snapshot(tmp_path, "session-bars-1m", "latest")


def test_curated_partition_mutation_is_detected(tmp_path: Path) -> None:
    normalized = normalized_trades(
        tmp_path,
        [trade("t1", "2026-01-05T01:30:01Z", 40001, 2)],
        key="raw-mutation",
    )
    snapshot = curate(tmp_path, normalized.snapshot_id, revision_id="r1")
    snapshot_dir = tmp_path / "curated" / "bars" / "snapshots" / snapshot.snapshot_id
    partition = snapshot_dir / snapshot.partitions[0].relative_path
    partition.write_bytes(partition.read_bytes() + b"changed")
    with pytest.raises(ValidationError, match="hash changed"):
        load_curated_snapshot(tmp_path, "bars", snapshot.snapshot_id)


def test_curated_reader_rejects_dataset_and_snapshot_path_traversal(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="Windows-safe"):
        load_curated_snapshot(tmp_path, "../outside", "sha256-" + "0" * 64)
    with pytest.raises(ValidationError, match="explicit content-addressed"):
        load_curated_snapshot(tmp_path, "bars", "sha256-../../outside")
