from dataclasses import replace
from decimal import Decimal

import pandas as pd
import pytest

from quant_data_kit.financial import calendars, holdings, lifecycle, macro, reconciliation, status
from quant_data_kit.financial.actions import ActionTerms
from quant_data_kit.financial.common import utc
from quant_data_kit.financial.units import normalize_trading_units
from quant_data_kit.us_research.sec import quarterly_facts, ttm_facts

T0 = "2024-01-01T00:00:00Z"
T1 = "2024-02-01T00:00:00Z"
T2 = "2024-03-01T00:00:00Z"


def test_financial_utc_preserves_nanoseconds_and_rejects_sub_nanoseconds() -> None:
    expected = pd.Timestamp("2026-10-04T11:00:00.000000900Z")
    for value in (
        "2026-10-04T11:00:00.000000900Z",
        "2026-10-04T11:00:00.0000009000Z",
    ):
        assert utc(value).value == expected.value
    with pytest.raises(ValueError, match="finer than nanoseconds"):
        utc("2026-10-04T11:00:00.0000009001Z")


def lifecycle_row(kind, effective=T0, known=T0, **kw):
    return dict(
        event_id=f"{kind}:{effective}",
        instrument_id="stable-A",
        kind=kind,
        effective_at=effective,
        available_at=known,
        source="synthetic",
        evidence_id="fixture",
        symbol="OLD",
        venue="TEST",
        universe_id="U",
        successor_id="stable-B",
        **kw,
    )


def test_lifecycle_future_correction_exit_and_delisting_keep_obligations():
    events = pd.DataFrame(
        [
            lifecycle_row("listing"),
            lifecycle_row("entry"),
            lifecycle_row("exit", T1),
            lifecycle_row("delisting", T2),
        ]
    )
    before = lifecycle.select_universe(events, T0, T0, universe_id="U")
    assert before.eligible == {"stable-A"}
    after = lifecycle.select_universe(
        events, T1, T1, universe_id="U", holdings=["stable-A", "missing"]
    )
    assert not after.eligible and after.retained_holdings == {"stable-A", "missing"}
    assert after.unknown_holdings == {"missing"}
    assert lifecycle.lifecycle_asof(events, T1, T1).iloc[0].listed
    assert not lifecycle.lifecycle_asof(events, T2, T2).iloc[0].listed
    pd.testing.assert_frame_equal(
        lifecycle.lifecycle_asof(events, T0, T0), lifecycle.lifecycle_asof(events.iloc[:2], T0, T0)
    )


def test_lifecycle_symbol_change_has_stable_id_and_delayed_knowledge():
    change = lifecycle_row("symbol_change", T1, T2)
    change["symbol"] = "NEW"
    frame = pd.DataFrame([lifecycle_row("listing"), change])
    assert lifecycle.lifecycle_asof(frame, T1, T1).iloc[0].symbol == "OLD"
    latest = lifecycle.lifecycle_asof(frame, T2, T2).iloc[0]
    assert latest.symbol == "NEW" and latest.instrument_id == "stable-A"


def status_frame():
    return pd.DataFrame(
        [
            {
                "instrument_id": "A",
                "effective_from": T0,
                "effective_to": T2,
                "available_at": T0,
                "buy_status": "blocked",
                "sell_status": "tradable",
                "reason": "limit_up",
                "source": "exchange",
                "evidence_id": "fixture",
            }
        ]
    )


def test_status_expiry_side_specific_unknown_and_source_conflict():
    frame = status_frame()
    assert status.permission_asof(frame, "A", T1, T1).sell == "tradable"
    assert status.permission_asof(frame, "A", T2, T2).buy == "unknown"
    assert status.permission_asof(frame, "absent", T1, T1).buy == "unknown"
    conflict = frame.iloc[0].to_dict()
    conflict.update(source="vendor", buy_status="tradable")
    result = status.permission_asof(pd.concat([frame, pd.DataFrame([conflict])]), "A", T1, T1)
    assert result.reason == "conflicting_sources" and result.sell == "unknown"
    with pytest.raises(ValueError):
        status.validate_status(frame.assign(reason="no_restriction"))


def calendar(purpose="settlement", days=("2024-01-02", "2024-01-04", "2024-01-05")):
    return calendars.PurposeCalendar(
        "TEST", purpose, "v1", T0, "2024-01-01", "2024-01-10", days, "synthetic", "synthetic"
    )


def test_purpose_calendar_holiday_coverage_publication_and_revision():
    cal = calendar()
    assert cal.advance("2024-01-02", 1, at=T1).date().isoformat() == "2024-01-04"
    with pytest.raises(ValueError, match="purpose"):
        cal.advance("2024-01-02", 1, at=T1, purpose="trading")
    with pytest.raises(ValueError, match="coverage"):
        cal.advance("2024-01-05", 1, at=T1)
    with pytest.raises(ValueError, match="known"):
        cal.advance("2024-01-02", 1, at="2023-12-31T00:00:00Z")
    revised = replace(cal, version="v2", available_at=T1, open_days=("2024-01-02", "2024-01-05"))
    book = calendars.CalendarBook([cal, revised])
    assert book.asof("TEST", "settlement", T0, "2024-01-02").version == "v1"
    assert book.asof("TEST", "settlement", T2, "2024-01-02").version == "v2"


def test_macro_release_boundary_missing_revision_and_unit_drift():
    payload = [
        {"date": "2023-12-01", "realtime_start": "2024-01-05", "value": "2.1"},
        {"date": "2023-12-01", "realtime_start": "2024-02-02", "value": "2.3"},
    ]
    frame = macro.from_alfred(
        payload,
        series_id="TEST",
        unit="percent",
        source="synthetic",
        release_times={"2024-01-05": "2024-01-05T13:30:00Z", "2024-02-02": "2024-02-02T13:30:00Z"},
    )
    assert macro.macro_asof(frame, "2024-01-05T13:29:59Z").empty
    assert macro.macro_asof(frame, "2024-01-05T13:30:00Z").iloc[0].value == Decimal("2.1")
    assert macro.macro_asof(frame, T2).iloc[0].value == Decimal("2.3")
    with pytest.raises(ValueError, match="release timestamp"):
        macro.from_alfred(
            payload, series_id="TEST", unit="percent", source="synthetic", release_times={}
        )
    bad = frame.copy()
    bad.loc[1, "unit"] = "fraction"
    with pytest.raises(ValueError, match="unit drift"):
        macro.validate_macro(bad)


def holding(fund, instrument, weight, kind="security", **kw):
    row = {
        "disclosure_id": f"{fund}:1",
        "fund_id": fund,
        "holding_date": "2023-12-31",
        "available_at": T0,
        "instrument_id": instrument,
        "asset_type": kind,
        "currency": "CNY",
        "weight": weight,
        "source": "synthetic",
        "evidence_id": f"{fund}:{instrument}",
    }
    return {**row, **kw}


def test_lookthrough_nested_overlap_missing_cash_currency_and_conservation():
    frame = pd.DataFrame(
        [
            holding("F1", "F2", ".5", "fund"),
            holding("F1", "A", ".3"),
            holding("F2", "A", ".5"),
            holding("F2", "CASH:USD", ".5", "cash", currency="USD"),
        ]
    )
    leaves = holdings.look_through(frame, {"F1": 1}, T1)
    summary = holdings.exposure_summary(leaves)
    assert summary["known_weight"] == Decimal(".8")
    assert summary["unknown_weight"] == Decimal(".2")
    assert next(x for x in summary["exposures"] if x["instrument_id"] == "A")["weight"] == Decimal(
        ".55"
    )
    assert sum(leaves.weight) == 1
    assert (
        holdings.exposure_summary(holdings.look_through(frame, {"F1": 1}, T2, max_age_days=1))[
            "unknown_weight"
        ]
        == 1
    )


def test_holdings_cycle_depth_and_future_disclosure_not_silently_dropped():
    frame = pd.DataFrame([holding("F1", "F2", "1", "fund"), holding("F2", "F1", "1", "fund")])
    assert holdings.look_through(frame, {"F1": 1}, T1).iloc[0].reason == "cycle"
    assert holdings.look_through(frame, {"F1": 1}, T1, max_depth=1).iloc[0].reason == "depth_limit"
    future = frame.assign(available_at=T2)
    assert holdings.look_through(future, {"F1": 1}, T1).iloc[0].reason == "missing_disclosure"
    with pytest.raises(ValueError):
        holdings.validate_holdings(frame.assign(weight="1.1"))


def test_reconciliation_normalizes_units_preserves_snapshot_and_propagates_impact(tmp_path):
    base = {
        "instrument_id": "A",
        "field": "amount",
        "effective_at": T0,
        "available_at": T0,
        "currency": "CNY",
        "basis": "raw",
        "evidence_id": "fixture",
    }
    frame = pd.DataFrame(
        [
            dict(base, observation_id="a", source="exchange", value="1000", unit="currency"),
            dict(base, observation_id="b", source="vendor", value="1", unit="thousand_currency"),
        ]
    )
    assert not reconciliation.discrepancies(frame, at=T1)
    frame.loc[1, "value"] = "2"
    case = reconciliation.discrepancies(frame, at=T1)[0]
    decision = reconciliation.adjudicate(
        case,
        selected_observation_id="a",
        resolved_at=T1,
        reviewer="test",
        rationale="exchange evidence",
        evidence_uri="synthetic:document",
        prior_snapshot="old",
        dependencies={"factor": ["b"], "orders": ["factor"], "report": ["orders"]},
    )
    assert decision["impacted"] == ["factor", "orders", "report"]
    path = reconciliation.publish_decision(decision, tmp_path / "new.json")
    with pytest.raises(FileExistsError):
        reconciliation.publish_decision(decision, path)
    with pytest.raises(ValueError, match="hash"):
        reconciliation.publish_decision({**decision, "reviewer": "tampered"}, tmp_path / "bad.json")


def sec_fact(start, end, value, accession, known, concept="NetIncomeLoss"):
    return {
        "instrument_id": "A",
        "concept": concept,
        "unit": "USD",
        "period_start": pd.Timestamp(start),
        "period_end": pd.Timestamp(end),
        "value": value,
        "accession": accession,
        "form": "10-Q",
        "accepted_at": pd.Timestamp(known),
        "available_at": pd.Timestamp(known),
    }


def test_sec_cumulative_quarters_and_mixed_vintage_fail_closed():
    facts = pd.DataFrame(
        [
            sec_fact("2023-01-01", "2023-03-31", 10, "q1", "2023-05-01T00:00:00Z"),
            sec_fact("2023-01-01", "2023-06-30", 25, "q2", "2023-08-01T00:00:00Z"),
            sec_fact("2023-01-01", "2023-09-30", 45, "q3", "2023-11-01T00:00:00Z"),
            sec_fact("2023-01-01", "2023-12-31", 70, "fy", T1),
        ]
    )
    quarters = quarterly_facts(facts, T2)
    assert quarters.value.tolist() == [10, 15, 20, 25]
    assert ttm_facts(facts, T2).empty  # No silent cross-filing comparability assumption.
    assert ttm_facts(facts, T2, allow_mixed_vintages=True).iloc[0].value == 70
    revised = pd.DataFrame([sec_fact("2023-01-01", "2023-06-30", 30, "amended", T2)])
    extended = pd.concat([facts, revised], ignore_index=True)
    pd.testing.assert_frame_equal(quarterly_facts(facts, T1), quarterly_facts(extended, T1))


def test_sec_53_week_year_and_gap_not_fabricated():
    bounds = [
        ("2022-12-26", "2023-03-26"),
        ("2023-03-27", "2023-06-25"),
        ("2023-06-26", "2023-09-24"),
        ("2023-09-25", "2023-12-31"),
    ]
    facts = pd.DataFrame([sec_fact(a, b, 10, str(i), T1) for i, (a, b) in enumerate(bounds)])
    assert ttm_facts(facts, T2).iloc[0].value == 40
    assert ttm_facts(facts.iloc[1:], T2).empty


@pytest.mark.parametrize("unit,basis,lot", [("lots", "raw", None), ("shares", "adjusted", None)])
def test_units_reject_implicit_multiplier_or_adjusted_shares(unit, basis, lot):
    with pytest.raises(ValueError):
        normalize_trading_units(
            pd.DataFrame({"volume": [1], "amount": [2]}),
            volume_unit=unit,
            amount_unit="currency",
            currency="CNY",
            share_basis=basis,
            lot_size=lot,
        )


def test_actions_require_explicit_election_and_reject_adjustment_mix():
    args = {
        "event_id": "x",
        "instrument_id": "A",
        "kind": "rights_exercise",
        "effective_at": T1,
        "available_at": T0,
        "currency": "USD",
        "source": "synthetic",
        "evidence_id": "fixture",
        "target_id": "B",
        "target_mark": "10",
    }
    with pytest.raises(ValueError, match="election"):
        ActionTerms(**args)
    with pytest.raises(ValueError, match="raw"):
        ActionTerms(**{**args, "price_basis": "total_return"})
