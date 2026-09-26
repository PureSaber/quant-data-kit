import json
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from quant_data_kit.us_research.calendar import schedule, settlement_session
from quant_data_kit.us_research.prices import (
    fetch_yahoo,
    load_bundle,
    us_symbol,
    validate_prices,
    write_bundle,
)
from quant_data_kit.us_research.sec import (
    annual_quality,
    download_sec,
    fact_table,
    facts_asof,
    filing_table,
)
from quant_data_kit.us_research.universe import members_asof, validate_membership


@pytest.fixture
def panel():
    cal = schedule("2024-03-08", "2024-03-12")
    return pd.DataFrame(
        [
            {
                "instrument_id": "US:AAPL",
                "symbol": "AAPL",
                "session": day,
                "open": 100,
                "high": 102,
                "low": 99,
                "close": 101,
                "volume": 1000,
                "dividend": 0,
                "split_ratio": 1,
                "currency": "USD",
                "price_basis": "raw",
                "available_at": row.close,
                "source": "fixture",
            }
            for day, row in cal.iterrows()
        ]
    )


def test_calendar_dst_halfday_settlement():
    cal = schedule("2024-03-08", "2024-03-11")
    assert cal.iloc[0].open.hour == 14
    assert cal.iloc[1].open.hour == 13
    assert schedule("2024-11-29", "2024-11-29").iloc[0].close.hour == 18
    assert schedule("2024-07-04", "2024-07-04").empty
    assert settlement_session("2024-05-24") == "2024-05-29"
    assert settlement_session("2024-05-28") == "2024-05-29"
    with pytest.raises(ValueError):
        settlement_session("2024-05-27")
    with pytest.raises(ValueError):
        settlement_session("2017-01-03")
    with pytest.raises(ValueError):
        schedule("2024-05-30", "2024-05-01")


@pytest.mark.parametrize("value", ["AAPL", "BRK.B", "BRK-B", "SPY"])
def test_us_symbols_are_not_domestic(value):
    assert us_symbol(value) == value


@pytest.mark.parametrize("value", ["000001", "AAPL.SZ/", "../SPY", ""])
def test_invalid_symbols(value):
    with pytest.raises(ValueError):
        us_symbol(value)


@pytest.mark.parametrize(
    "column,value",
    [
        ("close", float("nan")),
        ("open", -1),
        ("high", 1),
        ("volume", -1),
        ("dividend", -1),
        ("split_ratio", 0),
        ("currency", "CNY"),
        ("price_basis", "adjusted"),
        ("source", ""),
        ("available_at", "2024-03-08T09:00:00Z"),
        ("available_at", "2024-03-08"),
        ("pay_date", "2020-01-01"),
    ],
)
def test_bad_panel_rejected(panel, column, value):
    panel[column] = panel[column].astype(object) if column in panel else None
    panel.loc[0, column] = value
    with pytest.raises((ValueError, TypeError)):
        validate_prices(panel)


def test_bundle_immutable_and_hash_checked(panel, tmp_path):
    path = tmp_path / "bundle"
    provenance = {
        "source": "fixture",
        "observed_at": "2024-03-13T00:00:00Z",
        "evidence_kind": "synthetic",
        "universe_kind": "fixed_cohort",
    }
    write_bundle(path, panel, provenance)
    manifest, tables = load_bundle(path)
    assert tables["prices"].currency.eq("USD").all()
    assert manifest["provenance"] == provenance
    with pytest.raises(FileExistsError):
        write_bundle(path, panel, provenance)
    with (path / "prices.csv").open("a") as handle:
        handle.write("corruption")
    with pytest.raises(ValueError, match="hash"):
        load_bundle(path)


def test_gaps_duplicates_and_basis_are_rejected(panel):
    for bad in (panel.drop(index=1), pd.concat([panel, panel.iloc[[0]]])):
        with pytest.raises(ValueError):
            validate_prices(bad)
    panel.loc[0, "price_basis"] = "split_normalized"
    with pytest.raises(ValueError):
        validate_prices(panel)


def test_yahoo_contract_has_no_double_split_or_pit_claim(monkeypatch):
    hist = pd.DataFrame(
        {
            "Open": [50],
            "High": [51],
            "Low": [49],
            "Close": [50],
            "Volume": [2000],
            "Dividends": [1],
            "Stock Splits": [2],
        },
        index=pd.DatetimeIndex(["2024-03-08"], tz="America/New_York"),
    )
    calls = []

    class Ticker:
        def __init__(self, symbol):
            calls.append(symbol)

        def history(self, **kwargs):
            assert not kwargs["auto_adjust"] and kwargs["actions"]
            return hist

        def get_history_metadata(self):
            return {"currency": "USD"}

    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(Ticker=Ticker, __version__="test"))
    prices, provenance = fetch_yahoo(["AAPL"], "2024-03-08", "2024-03-08")
    assert calls == ["AAPL"]
    assert prices.iloc[0].close == 50
    assert prices.iloc[0].price_basis == "split_normalized"
    assert provenance["evidence_kind"] == "retrospective"
    assert prices.pay_date.isna().all()


def test_membership_knowledge_time_not_current_constituents():
    events = validate_membership(
        pd.DataFrame(
            [
                {
                    "instrument_id": "OLD",
                    "effective_session": "2024-01-02",
                    "known_at": "2024-01-01T00:00Z",
                    "member": True,
                    "source": "archive",
                    "event_id": "1",
                },
                {
                    "instrument_id": "OLD",
                    "effective_session": "2024-02-01",
                    "known_at": "2024-02-03T00:00Z",
                    "member": False,
                    "source": "revision",
                    "event_id": "2",
                },
            ]
        )
    )
    assert members_asof(events, "2024-02-02", "2024-02-02T12:00Z") == {"OLD"}
    assert members_asof(events, "2024-02-04", "2024-02-04T12:00Z") == set()
    with pytest.raises(ValueError):
        validate_membership(pd.concat([events, events]))


def test_sec_restatement_vintages_and_periods():
    filings = filing_table(
        [
            {
                "accessionNumber": ["a", "b"],
                "acceptanceDateTime": ["2023-02-01T22:00Z", "2024-02-01T22:00Z"],
                "form": ["10-K", "10-K/A"],
                "filingDate": ["2023-02-01", "2024-02-01"],
            }
        ],
        "US:A",
    )
    concepts = {}
    for name, value in [
        ("Assets", 1000),
        ("NetIncomeLoss", 100),
        ("NetCashProvidedByUsedInOperatingActivities", 130),
    ]:
        values = [
            {"val": value, "end": "2022-12-31", "accn": "a"},
            {"val": value * 2, "end": "2022-12-31", "accn": "b"},
        ]
        if name != "Assets":
            for row in values:
                row["start"] = "2022-01-01"
        concepts[name] = {"units": {"USD": values}}
    facts, audit = fact_table({"facts": {"us-gaap": concepts}}, filings)
    assert audit["excluded_unknown_accessions"] == []
    assert facts_asof(facts, "2023-02-01T22:04Z").empty
    assert set(facts_asof(facts, "2023-02-02T00:00Z").accession) == {"a"}
    assert set(facts_asof(facts, "2024-02-02T00:00Z").accession) == {"b"}
    quality = annual_quality(facts, "2023-02-02T00:00Z")
    assert quality.iloc[0].roa_annual == pytest.approx(0.1)
    assert quality.iloc[0].accruals_annual == pytest.approx(-0.03)
    assert annual_quality(facts, "2026-01-01T00:00Z").empty


def test_sec_missing_acceptance_never_backdated():
    with pytest.raises(ValueError):
        filing_table([{"accessionNumber": ["a"]}], "US:A")
    with pytest.raises(ValueError, match="contact"):
        download_sec("123", "unused", "anonymous")
    with pytest.raises(ValueError, match="CIK"):
        download_sec("../x", "unused", "test@example.com")


def test_manifest_traversal_rejected(panel, tmp_path):
    path = tmp_path / "b"
    write_bundle(
        path,
        panel,
        {
            "source": "x",
            "observed_at": "2024-01-01T00:00Z",
            "evidence_kind": "synthetic",
            "universe_kind": "fixed_cohort",
        },
    )
    manifest = json.loads((path / "manifest.json").read_text())
    manifest["files"]["../prices.csv"] = manifest["files"].pop("prices.csv")
    (path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        load_bundle(path)
