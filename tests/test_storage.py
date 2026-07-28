from __future__ import annotations

import pandas as pd
import pytest

from quant_data_kit.storage import build_manifest, cache_covers_range, save_manifest
from quant_data_kit.validate import ValidationError, validate_price_frame


def test_validate_price_frame_ok():
    df = pd.DataFrame(
        {
            "symbol": ["000001", "000001"],
            "date": ["2024-01-02", "2024-01-03"],
            "open": [1.0, 1.1],
            "high": [1.2, 1.3],
            "low": [0.9, 1.0],
            "close": [1.1, 1.2],
            "volume": [100, 110],
        }
    )
    stats = validate_price_frame(df)
    assert stats["rows"] == 2
    assert stats["symbols"] == 1


def test_validate_price_frame_missing_column():
    df = pd.DataFrame({"symbol": ["000001"], "date": ["2024-01-02"]})
    with pytest.raises(ValidationError):
        validate_price_frame(df)


def test_cache_covers_range():
    df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=5)})
    assert cache_covers_range(df, "2024-01-02", "2024-01-04")
    assert not cache_covers_range(df, "2023-01-01", "2024-01-04")


def test_save_manifest(tmp_path):
    df = pd.DataFrame({"symbol": ["000001"], "date": ["2024-01-02"], "close": [1.0]})
    parquet = tmp_path / "x.parquet"
    manifest = save_manifest(df, parquet, dataset="test")
    assert manifest.rows == 1
    assert build_manifest(df, "test", parquet).dataset == "test"
