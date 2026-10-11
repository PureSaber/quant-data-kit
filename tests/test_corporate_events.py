import sqlite3

import pytest

from quant_data_kit.us_research.corporate_events import (
    asof_events,
    import_events,
    number,
    utc,
    validate,
)


def event(**changes):
    return {
        "event_id": "demo",
        "revision": 1,
        "event_type": "merger",
        "target": "T",
        "acquirer": "A",
        "currency": "usd",
        "announced_at": "2025-01-01T00:00:00Z",
        "available_at": "2025-01-01T00:00:01Z",
        "expected_close_at": "2025-06-01T00:00:00Z",
        "status": "announced",
        "cash_per_share": "50",
        "exchange_ratio": "1",
        "source_ref": "synthetic",
        "evidence_kind": "synthetic",
        **changes,
    }


def test_append_idempotency_and_asof(tmp_path):
    path = tmp_path / "events.sqlite"
    first, final = (
        event(),
        event(revision=2, available_at="2025-01-02T00:00:00Z", status="completed"),
    )
    assert import_events(path, [first, final])["inserted"] == 2
    assert import_events(path, [first])["inserted"] == 0
    assert asof_events(path, "2024-01-01T00:00:00Z") == []
    assert asof_events(path, "2025-01-01T01:00:00Z")[0]["revision"] == 1
    assert asof_events(path, "2025-01-03T00:00:00Z")[0]["status"] == "completed"
    with pytest.raises(ValueError, match="conflict"):
        import_events(path, [event(cash_per_share="51")])


@pytest.mark.parametrize(
    "changes",
    [
        {"extra": 1},
        {"event_id": ""},
        {"currency": "12"},
        {"revision": True},
        {"event_type": "split"},
        {"status": "unknown"},
        {"evidence_kind": "historical_pit"},
        {"available_at": "2024-01-01T00:00:00Z"},
        {"expected_close_at": "2024-01-01T00:00:00Z"},
        {"cash_per_share": "0", "exchange_ratio": "0"},
        {"acquirer": ""},
        {"acquirer": "T"},
        {"cash_per_share": "NaN"},
        {"cash_per_share": "-1"},
        {"cash_per_share": "x"},
    ],
)
def test_invalid_event(changes):
    with pytest.raises(ValueError):
        validate(event(**changes))


def test_invalid_types_and_times():
    with pytest.raises(TypeError):
        number(True, "quantity")
    with pytest.raises(TypeError):
        validate(event(acquirer=1))
    with pytest.raises(ValueError):
        utc("2025-01-01")
    with pytest.raises(ValueError):
        validate([])


@pytest.mark.parametrize(
    "changes",
    [
        {"revision": 3},
        {"available_at": "2025-01-01T00:00:01Z"},
        {"target": "OTHER"},
        {"status": "pending"},
    ],
)
def test_invalid_history_rolls_back(tmp_path, changes):
    path = tmp_path / "events.sqlite"
    import_events(path, [event(status="completed")])
    row = event(revision=2, status="completed", available_at="2025-01-02T00:00:00Z")
    row.update(changes)
    with pytest.raises(ValueError):
        import_events(path, [row])
    assert len(asof_events(path, "2025-02-01T00:00:00Z")) == 1


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE metadata SET schema='bad'",
        "UPDATE revisions SET sha256='bad'",
        "UPDATE revisions SET revision=3",
    ],
)
def test_tamper_rejected(tmp_path, sql):
    path = tmp_path / "events.sqlite"
    import_events(path, [event()])
    with sqlite3.connect(path) as db:
        db.execute(sql)
    with pytest.raises(ValueError):
        asof_events(path, "2025-02-01T00:00:00Z")


def test_empty_and_noninitial_import(tmp_path):
    with pytest.raises(ValueError, match="empty"):
        import_events(tmp_path / "e.sqlite", [])
    with pytest.raises(ValueError, match="first"):
        import_events(tmp_path / "e.sqlite", [event(revision=2)])
