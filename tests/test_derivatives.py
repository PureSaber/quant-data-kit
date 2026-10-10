import json
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from quant_data_kit.derivatives import load_bundle, write_bundle
from quant_data_kit.derivatives.demo import demo_records, write_demo
from quant_data_kit.derivatives.providers import fetch_databento, fetch_dataway


@pytest.mark.parametrize("kind", ["future", "option"])
def test_roundtrip_asof_and_tampering(tmp_path, kind):
    bundle = write_demo(tmp_path / kind, kind)
    first = bundle.quotes[0]
    assert bundle.asof(first.at) == {}
    assert first.instrument_id in bundle.asof(first.available_at)
    assert bundle.summary()["evidence_kind"] == "synthetic"
    assert load_bundle(bundle.root).identity == bundle.identity
    with pytest.raises(FileExistsError):
        write_demo(bundle.root, kind)
    (bundle.root / "quotes.csv").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        bundle.verify_unchanged()


def test_metadata_availability_and_bad_records(tmp_path):
    contracts, quotes = demo_records()
    c, q = contracts[0], quotes[0]
    with pytest.raises(ValueError, match="timezone-naive"):
        replace(c, known_at="2025-01-01")
    with pytest.raises(ValueError, match="OHLC"):
        replace(q, high="1")
    with pytest.raises(ValueError, match="required"):
        replace(q, close=None)
    with pytest.raises(ValueError, match="availability"):
        replace(q, available_at=q.at - timedelta(seconds=1))
    with pytest.raises(ValueError, match="finite"):
        replace(q, close="NaN")
    with pytest.raises(ValueError, match="duplicate"):
        write_bundle(
            tmp_path / "bad",
            [c],
            [q, q],
            provider="test",
            evidence_kind="synthetic",
            rights_note="test",
            limits="test",
        )
    late = replace(c, known_at=q.available_at + timedelta(days=1))
    b = write_bundle(
        tmp_path / "late",
        [late],
        [q],
        provider="test",
        evidence_kind="synthetic",
        rights_note="test",
        limits="test",
    )
    assert not b.asof(q.available_at)


def test_empty_collection_cannot_publish(tmp_path):
    contracts, _ = demo_records()
    rows, source = fetch_dataway(
        contracts,
        "2025-01-02",
        "2025-01-02",
        fetch=lambda url: b"c_contract,n_close\n",
        base_url="https://data.example.test/cube",
    )
    assert not rows and len(source["raw_responses"]) == 1
    with pytest.raises(ValueError, match="empty"):
        write_bundle(
            tmp_path / "empty",
            contracts,
            rows,
            provider="dataway",
            evidence_kind="retrospective",
            rights_note="authorized",
            limits="retrospective",
        )
    assert not (tmp_path / "empty").exists()


def test_paid_provider_cost_gate_and_secret_redaction():
    contracts, _ = demo_records()
    calls = []

    def expensive(url, **kwargs):
        calls.append(url)
        return b"2.5"

    with pytest.raises(ValueError, match="exceeds"):
        fetch_databento(
            contracts,
            "2025-01-02",
            "2025-01-02",
            dataset="GLBX.MDP3",
            max_cost_usd="1",
            key="secret-not-for-provenance",
            fetch=expensive,
        )
    assert len(calls) == 1 and "metadata.get_cost" in calls[0]

    def allowed(url, **kwargs):
        if "metadata.get_cost" in url:
            return b"0.1"
        assert b"stype_in=raw_symbol" in kwargs["data"]
        return b"ts_event,symbol,open,high,low,close,volume\n2025-01-02,DEMOH,100,102,99,101,42\n"

    quotes, source = fetch_databento(
        contracts[:1],
        "2025-01-02",
        "2025-01-02",
        dataset="GLBX.MDP3",
        max_cost_usd="1",
        key="secret",
        fetch=allowed,
    )
    assert quotes[0].close == Decimal(101)
    assert "secret" not in json.dumps(source)
    assert quotes[0].available_at.isoformat() == "2025-01-03T00:00:00+00:00"


def test_manifest_must_be_object(tmp_path):
    (tmp_path / "manifest.json").write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="object"):
        load_bundle(tmp_path)
