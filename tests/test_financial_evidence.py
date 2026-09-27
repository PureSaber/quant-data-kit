import json
from pathlib import Path

import pandas as pd
import pytest

from quant_data_kit.financial import reconciliation as r
from quant_data_kit.financial.returns import total_return_panel
from quant_data_kit.us_research.sec import fact_table, filing_table, quarterly_facts


def test_real_apple_cumulative_cash_flow_and_acceptance_lag():
    data = json.loads(
        (Path(__file__).parent / "fixtures/financial/apple_cash_flow_extract.json").read_text()
    )
    rows = data["facts"]
    filings = filing_table(
        [
            {
                "accessionNumber": [x["accession"] for x in rows],
                "acceptanceDateTime": [x["accepted_at"] for x in rows],
                "form": ["10-Q"] * 3,
                "filingDate": [x["accepted_at"][:10] for x in rows],
            }
        ],
        "CIK:0000320193",
    )
    source = {
        "facts": {
            "us-gaap": {
                data["concept"]: {
                    "units": {
                        "USD": [
                            {
                                "start": data["period_start"],
                                "end": x["period_end"],
                                "val": x["value_millions"] * 1_000_000,
                                "accn": x["accession"],
                            }
                            for x in rows
                        ]
                    }
                }
            }
        }
    }
    facts, _ = fact_table(source, filings)
    boundary = pd.Timestamp(rows[2]["accepted_at"]) + pd.Timedelta(minutes=5)
    assert len(quarterly_facts(facts, boundary - pd.Timedelta(seconds=1))) == 2
    quarters = quarterly_facts(facts, boundary)
    assert quarters.value.tolist() == [39_895e6, 22_690e6, 28_858e6]
    assert quarters.vintage_consistent.tolist() == [True, False, False]
    # Fiscal Q2 starts in December: calendar-quarter bucketing would be wrong.
    assert quarters.iloc[1].period_start == pd.Timestamp("2023-12-31")


def test_resolution_materializes_new_snapshot_without_retroactive_knowledge(tmp_path):
    base = {
        "instrument_id": "A",
        "field": "close",
        "effective_at": "2024-01-01T00:00:00Z",
        "available_at": "2024-01-01T00:00:00Z",
        "currency": "USD",
        "basis": "raw",
        "unit": "currency",
        "evidence_id": "synthetic",
    }
    records = [
        dict(base, observation_id="a", source="exchange", value="10"),
        dict(base, observation_id="b", source="vendor", value="20"),
    ]
    parent = r.freeze_snapshot([records[1]])
    old_bytes = r.canonical(parent)
    case = r.discrepancies(pd.DataFrame(records), at="2024-02-01T00:00:00Z")[0]
    decision = r.adjudicate(
        case,
        selected_observation_id="a",
        resolved_at="2024-02-01T00:00:00Z",
        reviewer="test",
        rationale="exchange evidence",
        evidence_uri="synthetic:test",
        prior_snapshot=parent["snapshot_id"],
        dependencies={"factor": ["b"]},
    )
    path = tmp_path / "corrected.json"
    result = r.apply_decision_snapshot(parent, decision, path)
    assert r.canonical(parent) == old_bytes
    assert result["records"][0]["value"] == 10
    assert result["records"][0]["available_at"] == pd.Timestamp("2024-02-01T00:00:00Z")
    assert result["parent_snapshot"] == parent["snapshot_id"]
    with pytest.raises(FileExistsError):
        r.apply_decision_snapshot(parent, decision, path)


def test_split_total_return_is_not_raw_price_return_and_refuses_adjusted_quotes():
    # Apple's 2020 4-for-1 split terms; prices here are synthetic, not observed quotes.
    # Source: https://investor.apple.com/dividend-history/
    bars = pd.DataFrame(
        {
            "date": ["2020-08-28", "2020-08-31"],
            "symbol": ["AAPL"] * 2,
            "close": [400, 100],
            "adjustment": ["none"] * 2,
        }
    )
    action = {
        "event_id": "apple-split",
        "instrument_id": "AAPL",
        "kind": "split",
        "ratio": "4",
        "effective_at": "2020-08-31T13:30:00Z",
        "available_at": "2020-07-31T23:59:59Z",
        "currency": "USD",
        "source": "Apple IR",
        "evidence_id": "split-2020",
    }
    result = total_return_panel(bars, [action], timezone="America/New_York")
    assert result.return_close.iloc[-1] == result.return_close.iloc[0]
    with pytest.raises(ValueError):
        total_return_panel(
            bars.assign(adjustment="total_return"), [action], timezone="America/New_York"
        )
