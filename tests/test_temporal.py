import pandas as pd
import pytest

from quant_data_kit.exceptions import ValidationError
from quant_data_kit.temporal import (
    apply_symbol_lifecycle,
    audit_point_in_time,
    point_in_time_join,
)


def test_point_in_time_join_uses_availability_not_event_date() -> None:
    observations = pd.DataFrame(
        {
            "symbol": ["A", "A", "A"],
            "date": pd.to_datetime(["2024-04-01", "2024-04-15", "2024-05-01"]),
        }
    )
    facts = pd.DataFrame(
        {
            "symbol": ["A"],
            "report_date": pd.to_datetime(["2024-03-31"]),
            "available_at": pd.to_datetime(["2024-04-15"]),
            "value": [10.0],
        }
    )
    merged = point_in_time_join(observations, facts, fact_columns=["value", "report_date"])
    assert pd.isna(merged.loc[0, "value"])
    assert merged.loc[1, "value"] == 10.0
    audit = audit_point_in_time(merged)
    assert audit.matched_rows == 2
    assert audit.unavailable_rows == 0


def test_audit_rejects_future_data() -> None:
    frame = pd.DataFrame({"date": ["2024-01-01"], "source_available_at": ["2024-01-02"]})
    with pytest.raises(ValidationError, match="future-data"):
        audit_point_in_time(frame)


def test_apply_symbol_lifecycle_keeps_delisted_history() -> None:
    panel = pd.DataFrame(
        {
            "symbol": ["A", "A", "A"],
            "date": pd.to_datetime(["2019-12-31", "2020-01-02", "2020-02-03"]),
        }
    )
    lifecycle = pd.DataFrame(
        {"symbol": ["A"], "listed_at": ["2020-01-01"], "delisted_at": ["2020-01-31"]}
    )
    result = apply_symbol_lifecycle(panel, lifecycle)
    assert result["date"].tolist() == [pd.Timestamp("2020-01-02")]
