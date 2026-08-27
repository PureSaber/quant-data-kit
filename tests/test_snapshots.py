import pandas as pd
import pytest

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.snapshots import create_snapshot, load_snapshot


def test_snapshot_is_content_addressed_and_idempotent(tmp_path) -> None:
    frame = pd.DataFrame({"symbol": ["A"], "date": ["2024-01-02"], "close": [10.0]})
    first = create_snapshot(frame, tmp_path, dataset="prices", source="fixture")
    second = create_snapshot(frame, tmp_path, dataset="prices", source="fixture")
    assert first.snapshot_id == second.snapshot_id
    loaded = load_snapshot(tmp_path / "prices" / first.snapshot_id / "manifest.json")
    assert loaded.content_sha256 == first.content_sha256


def test_snapshot_detects_mutation(tmp_path) -> None:
    frame = pd.DataFrame({"symbol": ["A"], "date": ["2024-01-02"], "close": [10.0]})
    snapshot = create_snapshot(frame, tmp_path, dataset="prices", source="fixture")
    path = tmp_path / "prices" / snapshot.snapshot_id / "data.parquet"
    pd.DataFrame({"symbol": ["A"], "date": ["2024-01-02"], "close": [11.0]}).to_parquet(
        path, index=False
    )
    with pytest.raises(ValidationError, match="changed"):
        load_snapshot(path.parent / "manifest.json")
