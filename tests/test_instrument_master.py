import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from quant_data_kit.instrument_master import (
    CATALOG_SCHEMA,
    import_instrument_master,
    load_instrument_master,
)


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source(root: Path, *, available_at="2023-02-17T08:00:00Z", late_rule=False) -> Path:
    root.mkdir()
    documents = root / "documents"
    documents.mkdir()
    payloads = {
        "listing": b"official historical listing notice",
        "rule-2023": b"official 2023 trading rule",
        "rule-2026": b"official 2026 trading rule",
    }
    for name, payload in payloads.items():
        (documents / f"{name}.txt").write_bytes(payload)
    catalog = pd.DataFrame(
        [
            {
                "catalog_schema_version": CATALOG_SCHEMA,
                "catalog_source_version": "official-test-v1",
                "symbol": "510300",
                "asset_class": "etf",
                "product_type": "etf",
                "venue": "SSE",
                "price_scale": "3",
                "price_tick": "0.001",
                "quantity_step": "1",
                "lot_size": "100",
                "commission_rate": "0",
                "stamp_duty_rate": "0",
                "effective_from": "2025-01-02T00:00:00Z",
                "effective_to": "2026-09-25T00:00:00Z",
                "available_at": available_at,
                "listing_date": "2012-05-28",
                "listing_date_basis": "official-published-document",
                "listing_evidence_id": "listing",
                "trading_rule_evidence_ids": "rule-2023;rule-2026",
                "price_limit_evidence_ids": "rule-2023;rule-2026",
                "price_limit_rate": "0.10",
            }
        ]
    )
    catalog_path = root / "catalog.csv"
    catalog.to_csv(catalog_path, index=False)
    declaration = {
        "schema_version": "qdk.historical-instrument-master-source/v1",
        "captured_at": "2026-09-26T00:00:00Z",
        "rights_note": "test fixture",
        "coverage": {
            "instrument_master": "field-evidence-complete",
            "daily_trading_status": "unavailable-no-positive-state-inference",
        },
        "catalog_file": "catalog.csv",
        "catalog_sha256": _hash(catalog_path),
        "documents": [
            {
                "document_id": "listing",
                "kind": "listing",
                "publisher": "official exchange",
                "title": "listing notice",
                "source_uri": "https://exchange.example/listing",
                "published_at": "2012-05-23T08:00:00Z",
                "effective_from": "2012-05-28T00:00:00Z",
                "effective_to": "2100-01-01T00:00:00Z",
                "file": "documents/listing.txt",
                "sha256": _hash(documents / "listing.txt"),
            },
            {
                "document_id": "rule-2023",
                "kind": "trading_rule",
                "publisher": "official exchange",
                "title": "2023 rule",
                "source_uri": "https://exchange.example/rule-2023",
                "published_at": "2023-02-17T08:00:00Z",
                "effective_from": "2023-04-10T00:00:00Z",
                "effective_to": "2026-07-06T00:00:00Z",
                "file": "documents/rule-2023.txt",
                "sha256": _hash(documents / "rule-2023.txt"),
            },
            {
                "document_id": "rule-2026",
                "kind": "trading_rule",
                "publisher": "official exchange",
                "title": "2026 rule",
                "source_uri": "https://exchange.example/rule-2026",
                "published_at": ("2026-07-07T08:00:00Z" if late_rule else "2026-04-24T08:00:00Z"),
                "effective_from": "2026-07-06T00:00:00Z",
                "effective_to": "2100-01-01T00:00:00Z",
                "file": "documents/rule-2026.txt",
                "sha256": _hash(documents / "rule-2026.txt"),
            },
        ],
    }
    (root / "declaration.json").write_text(
        json.dumps(declaration, ensure_ascii=False), encoding="utf-8"
    )
    return root


def test_import_verifies_source_identity_hashes_and_rule_intervals(tmp_path):
    source = _source(tmp_path / "source")
    bundle = tmp_path / "bundle"
    manifest = import_instrument_master(source, bundle)
    loaded, catalog = load_instrument_master(bundle)
    assert loaded == manifest
    assert catalog.loc[0, "symbol"] == "510300"
    assert loaded["coverage"]["daily_trading_status"].startswith("unavailable")

    document = bundle / loaded["documents"][0]["file"]
    document.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_instrument_master(bundle)


def test_manifest_source_identity_change_is_rejected(tmp_path):
    bundle = tmp_path / "bundle"
    import_instrument_master(_source(tmp_path / "source"), bundle)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["documents"][0]["source_uri"] = "https://different.example/listing"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="manifest identity"):
        load_instrument_master(bundle)


def test_catalog_rejects_future_availability_and_late_rule_publication(tmp_path):
    with pytest.raises(ValueError, match="before its initial rules"):
        import_instrument_master(
            _source(tmp_path / "early", available_at="2023-02-16T08:00:00Z"),
            tmp_path / "early-bundle",
        )
    with pytest.raises(ValueError, match="not public before it became effective"):
        import_instrument_master(
            _source(tmp_path / "late", late_rule=True),
            tmp_path / "late-bundle",
        )


def test_etf_catalog_rejects_equity_asset_class(tmp_path):
    source = _source(tmp_path / "wrong-class")
    catalog_path = source / "catalog.csv"
    catalog = pd.read_csv(catalog_path, dtype=str)
    catalog.loc[0, "asset_class"] = "equity"
    catalog.to_csv(catalog_path, index=False)
    declaration_path = source / "declaration.json"
    declaration = json.loads(declaration_path.read_text(encoding="utf-8"))
    declaration["catalog_sha256"] = _hash(catalog_path)
    declaration_path.write_text(json.dumps(declaration), encoding="utf-8")

    with pytest.raises(ValueError, match="ETF product_type requires ETF asset_class"):
        import_instrument_master(source, tmp_path / "wrong-class-bundle")
