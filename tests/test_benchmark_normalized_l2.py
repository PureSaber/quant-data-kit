from __future__ import annotations

from pathlib import Path

from quant_data_kit.data_lake import StoragePolicy
from quant_data_kit.schemas_v2 import validate_json_record
from tools.benchmark_normalized_l2 import run_benchmark, synthetic_l2_events


def test_synthetic_l2_fixture_is_sequential_and_schema_valid() -> None:
    events = list(synthetic_l2_events(4))
    assert [item["sequence"] for item in events] == [1, 2, 3, 4]
    assert [item.get("previous_sequence") for item in events] == [None, 1, 2, 3]
    for event in events:
        schema_id = (
            "puresaber.book-snapshot-event"
            if event["event_type"] == "book_snapshot"
            else "puresaber.book-delta-event"
        )
        validate_json_record(schema_id, event)


def test_small_benchmark_proves_end_to_end_determinism(tmp_path: Path) -> None:
    report = run_benchmark(
        tmp_path / "benchmark",
        rows=32,
        runs=2,
        minimum_events_per_second=0,
        maximum_peak_rss_gib=16,
        batch_rows=8,
        repository=Path(__file__).resolve().parents[1],
        policy=StoragePolicy(
            hot_quota_bytes=1024**3,
            minimum_free_bytes=1,
            minimum_free_fraction=0.000001,
        ),
    )
    assert report["passed"] is True
    assert all(report["gates"].values())
    assert report["run_results"][0]["snapshot_id"] == report["run_results"][1]["snapshot_id"]
