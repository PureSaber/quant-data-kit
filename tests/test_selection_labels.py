import numpy as np
import pandas as pd
import pytest

from quant_data_kit.financial.labels import forward_labels, label_coverage
from quant_data_kit.financial.units import normalize_trading_units


def sample():
    days = pd.date_range("2024-01-01", periods=6)
    levels = pd.DataFrame(
        {
            "date": days,
            "instrument_id": "A",
            "value": [100, 101, 102, 103, 104, 105],
            "available_at": days.tz_localize("UTC") + pd.Timedelta(hours=20),
        }
    )
    selections = pd.DataFrame(
        {
            "sample_id": ["zero", "missing", "young"],
            "date": [days[0], days[1], days[-1]],
            "instrument_id": ["A", "B", "A"],
            "missing_reason": ["missing_price", "delisted_unknown_terminal", "missing_price"],
        }
    )
    return days, levels, selections


def test_zero_terminal_survives_and_unknown_terminal_has_only_explicit_scenarios():
    days, levels, selected = sample()
    terminal = pd.DataFrame(
        [
            {
                "sample_id": "zero",
                "date": days[2],
                "realized_return": -1.0,
                "available_at": "2024-01-03T20:00:00Z",
                "source": "evidenced-zero-cash",
            }
        ]
    )
    labels = forward_labels(
        levels,
        selected,
        days,
        horizon=3,
        as_of="2024-01-06T23:00:00Z",
        terminals=terminal,
        lower_return=-1,
        upper_return=0.5,
    )
    assert labels.status.tolist() == ["terminal", "delisted_unknown_terminal", "immature"]
    assert labels["return"].iloc[0] == -1
    assert np.isnan(labels["return"].iloc[1])
    assert labels.lower_return.iloc[1] == -1 and labels.upper_return.iloc[1] == 0.5
    coverage = label_coverage(labels)
    assert coverage["selected_samples"] == 3 and coverage["observed_or_terminal"] == 1
    assert coverage["lower_mean_scenario"] is None  # immature sample cannot be silently dropped


def test_future_terminal_evidence_cannot_backfill_current_labels():
    days, levels, selected = sample()
    terminal = pd.DataFrame(
        [
            {
                "sample_id": "zero",
                "date": days[2],
                "realized_return": -1.0,
                "available_at": "2024-02-01T00:00:00Z",
                "source": "late-evidence",
            }
        ]
    )
    labels = forward_labels(
        levels, selected, days, horizon=3, as_of="2024-01-06T23:00:00Z", terminals=terminal
    )
    assert labels.status.iloc[0] == "observed"
    assert labels["return"].iloc[0] == pytest.approx(0.03)
    assert labels.lower_return.iloc[1:].isna().all()


def test_interior_missing_price_is_not_a_shortened_horizon():
    days, levels, selected = sample()
    labels = forward_labels(
        levels.drop(index=1), selected, days, horizon=3, as_of="2024-01-06T23:00:00Z"
    )
    assert labels.status.iloc[0] == "missing_price" and labels["return"].isna().all()


def test_label_contract_guards_and_trading_unit_hand_differential():
    days, levels, selected = sample()
    for frame in (pd.concat([levels, levels.iloc[:1]]), levels.assign(value=0)):
        with pytest.raises(ValueError):
            forward_labels(frame, selected, days, horizon=3, as_of="2024-01-06T23:00:00Z")
    with pytest.raises(ValueError):
        forward_labels(
            levels, selected, days, horizon=3, as_of="2024-01-06T23:00:00Z", lower_return=-1
        )
    for lots in (1, 3, 10, 12345):
        actual = normalize_trading_units(
            pd.DataFrame({"volume": [lots], "amount": [2]}),
            volume_unit="lots",
            amount_unit="ten_thousand_currency",
            currency="CNY",
            share_basis="raw",
            lot_size=100,
        )
        assert actual.volume.iloc[0] == lots * 100
        assert actual.amount.iloc[0] == 20000
        assert actual.volume.iloc[0] != lots  # injected hand/share confusion would fail
