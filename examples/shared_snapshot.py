"""Publish deterministic synthetic prices and print a real public snapshot reference."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from quant_data_kit import ArtifactStore, SnapshotStore


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifacts = ArtifactStore(args.root, context={"run_id": "synthetic-example-v1"})
    snapshots = SnapshotStore(artifacts)
    with TemporaryDirectory() as temporary:
        path = Path(temporary) / "prices.parquet"
        frame = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
                "symbol": ["A", "B", "A", "B"],
                "close": [10.0, 20.0, 10.5, 19.5],
                "available_at": pd.to_datetime(
                    ["2024-01-01T18:00:00Z"] * 2 + ["2024-01-02T18:00:00Z"] * 2
                ),
            }
        )
        frame.to_parquet(path, index=False)
        manifest = snapshots.publish(
            {"prices": path},
            dataset="synthetic-prices",
            version="1",
            temporal={"timezone": "UTC", "calendar": "synthetic-daily", "adjustment": "none"},
            owner="run/synthetic-example-v1/inputs",
            available_at_column="available_at",
        )
    # The temporary source has already disappeared; the store owns independent bytes.
    artifacts.verify(manifest.artifact)
    assert (
        snapshots.read_table(
            manifest.artifact, symbols=["A"], as_of="2024-01-02T18:00:00Z"
        ).num_rows
        == 2
    )
    result = json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(result, encoding="utf-8")
    print(result)


if __name__ == "__main__":
    main()
