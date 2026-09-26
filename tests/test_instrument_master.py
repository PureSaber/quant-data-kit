import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from quant_data_kit.instrument_master import (
    CATALOG_SCHEMA,
    CURRENT_VALIDITY,
    MASTER_SCOPE,
    SOURCE_SCHEMA,
    UNAVAILABLE_FEE_SCOPE,
    import_instrument_master,
    instrument_master_prefix,
    load_instrument_master,
    resolve_instrument_catalog,
)

CAPTURED_AT = "2026-09-26T00:00:00Z"


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assertion(symbol: str, **fields: str) -> dict:
    return {
        "symbol": symbol,
        "fields": {
            field: {
                "value": value,
                "locator": f"fixture:{field}",
                "excerpt": f"fixture declares {field}={value}",
                "method": "direct",
            }
            for field, value in fields.items()
        },
    }


def _source(root: Path, *, late_rule: bool = False) -> Path:
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

    common = {
        "catalog_schema_version": CATALOG_SCHEMA,
        "catalog_source_version": "official-test-v2",
        "symbol": "510300",
        "asset_class": "etf",
        "product_type": "etf",
        "venue": "SSE",
        "price_scale": "3",
        "price_tick": "0.001",
        "quantity_step": "1",
        "lot_size": "100",
        "commission_rate": "",
        "stamp_duty_rate": "",
        "fee_fields_scope": UNAVAILABLE_FEE_SCOPE,
        "fee_evidence_id": "",
        "listing_date": "2012-05-28",
        "listing_date_basis": "official-published-document",
        "listing_evidence_id": "listing",
        "price_limit_rate": "0.10",
        "master_scope": MASTER_SCOPE,
    }
    catalog = pd.DataFrame(
        [
            {
                **common,
                "effective_from": "2023-04-10T00:00:00Z",
                "effective_to": "2026-07-06T00:00:00Z",
                "available_at": "2023-02-17T08:00:00Z",
                "trading_rule_evidence_id": "rule-2023",
                "price_limit_evidence_id": "rule-2023",
            },
            {
                **common,
                "effective_from": "2026-07-06T00:00:00Z",
                "effective_to": CAPTURED_AT,
                "available_at": "2026-04-25T00:00:00Z",
                "trading_rule_evidence_id": "rule-2026",
                "price_limit_evidence_id": "rule-2026",
            },
        ]
    )
    catalog_path = root / "catalog.csv"
    catalog.to_csv(catalog_path, index=False)
    listing_assertion = _assertion(
        "510300",
        asset_class="etf",
        product_type="etf",
        venue="SSE",
        listing_date="2012-05-28",
    )
    rule_assertion = _assertion(
        "510300",
        venue="SSE",
        price_scale="3",
        price_tick="0.001",
        quantity_step="1",
        lot_size="100",
        price_limit_rate="0.10",
    )
    declaration = {
        "schema_version": SOURCE_SCHEMA,
        "captured_at": CAPTURED_AT,
        "rights_note": "test fixture",
        "coverage": {
            "instrument_master": "typed-field-evidence-complete",
            "fees": UNAVAILABLE_FEE_SCOPE,
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
                "published_at": "2012-05-23T00:00:00Z",
                "available_at": "2012-05-23T08:00:00Z",
                "effective_from": "2012-05-28T00:00:00Z",
                "effective_to": CAPTURED_AT,
                "validity_basis": CURRENT_VALIDITY,
                "availability_basis": "official-page-time",
                "file": "documents/listing.txt",
                "sha256": _hash(documents / "listing.txt"),
                "assertions": [listing_assertion],
            },
            {
                "document_id": "rule-2023",
                "kind": "trading_rule",
                "publisher": "official exchange",
                "title": "2023 rule",
                "source_uri": "https://exchange.example/rule-2023",
                "published_at": "2023-02-17T00:00:00Z",
                "available_at": "2023-02-17T08:00:00Z",
                "effective_from": "2023-04-10T00:00:00Z",
                "effective_to": "2026-07-06T00:00:00Z",
                "validity_basis": "superseded-by-next-version",
                "availability_basis": "official-page-time",
                "file": "documents/rule-2023.txt",
                "sha256": _hash(documents / "rule-2023.txt"),
                "assertions": [rule_assertion],
            },
            {
                "document_id": "rule-2026",
                "kind": "trading_rule",
                "publisher": "official exchange",
                "title": "2026 rule",
                "source_uri": "https://exchange.example/rule-2026",
                "published_at": "2026-04-24T00:00:00Z",
                "available_at": ("2026-07-07T00:00:00Z" if late_rule else "2026-04-25T00:00:00Z"),
                "effective_from": "2026-07-06T00:00:00Z",
                "effective_to": CAPTURED_AT,
                "validity_basis": CURRENT_VALIDITY,
                "availability_basis": "official-page-date-next-utc-day",
                "file": "documents/rule-2026.txt",
                "sha256": _hash(documents / "rule-2026.txt"),
                "assertions": [rule_assertion],
            },
        ],
    }
    (root / "declaration.json").write_text(
        json.dumps(declaration, ensure_ascii=False), encoding="utf-8"
    )
    return root


def _update_catalog_hash(source: Path) -> None:
    declaration_path = source / "declaration.json"
    declaration = json.loads(declaration_path.read_text(encoding="utf-8"))
    declaration["catalog_sha256"] = _hash(source / "catalog.csv")
    declaration_path.write_text(json.dumps(declaration, ensure_ascii=False), encoding="utf-8")


def test_import_verifies_source_identity_assertions_and_rule_versions(tmp_path):
    source = _source(tmp_path / "source")
    bundle = tmp_path / "bundle"
    manifest = import_instrument_master(source, bundle)
    loaded, catalog = load_instrument_master(bundle)
    assert loaded == manifest
    assert len(catalog) == 2
    assert catalog["symbol"].tolist() == ["510300", "510300"]
    assert loaded["refresh_required_after"] == "2026-09-26T00:00:00+00:00"
    assert (bundle / "source" / "declaration.json").read_bytes() == (
        source / "declaration.json"
    ).read_bytes()

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


def test_catalog_rejects_late_rule_and_unobserved_future_validity(tmp_path):
    with pytest.raises(ValueError, match="not available before"):
        import_instrument_master(
            _source(tmp_path / "late", late_rule=True), tmp_path / "late-bundle"
        )

    source = _source(tmp_path / "future")
    declaration_path = source / "declaration.json"
    declaration = json.loads(declaration_path.read_text(encoding="utf-8"))
    declaration["documents"][-1]["effective_to"] = "2099-01-01T00:00:00Z"
    declaration_path.write_text(json.dumps(declaration), encoding="utf-8")
    with pytest.raises(ValueError, match="extends beyond its observed capture horizon"):
        import_instrument_master(source, tmp_path / "future-bundle")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("asset_class", "equity"),
        ("product_type", "stock"),
        ("venue", "FAKE"),
        ("price_tick", "0.002"),
        ("lot_size", "7"),
        ("price_limit_rate", "9.99"),
    ],
)
def test_catalog_rejects_values_not_bound_to_document_assertions(tmp_path, field, value):
    source = _source(tmp_path / field)
    catalog_path = source / "catalog.csv"
    catalog = pd.read_csv(catalog_path, dtype=str, keep_default_na=False)
    catalog.loc[:, field] = value
    catalog.to_csv(catalog_path, index=False)
    _update_catalog_hash(source)
    with pytest.raises(ValueError):
        import_instrument_master(source, tmp_path / f"{field}-bundle")


def test_catalog_schema_is_closed_and_unavailable_fees_remain_empty(tmp_path):
    source = _source(tmp_path / "extra")
    catalog_path = source / "catalog.csv"
    catalog = pd.read_csv(catalog_path, dtype=str, keep_default_na=False)
    catalog["unverified_field"] = "accepted-before-fix"
    catalog.to_csv(catalog_path, index=False)
    _update_catalog_hash(source)
    with pytest.raises(ValueError, match="schema mismatch"):
        import_instrument_master(source, tmp_path / "extra-bundle")

    source = _source(tmp_path / "fees")
    catalog_path = source / "catalog.csv"
    catalog = pd.read_csv(catalog_path, dtype=str, keep_default_na=False)
    catalog.loc[:, "commission_rate"] = "0"
    catalog.to_csv(catalog_path, index=False)
    _update_catalog_hash(source)
    with pytest.raises(ValueError, match="unavailable fee fields must remain empty"):
        import_instrument_master(source, tmp_path / "fees-bundle")

    source = _source(tmp_path / "unlocated-claim")
    declaration_path = source / "declaration.json"
    declaration = json.loads(declaration_path.read_text(encoding="utf-8"))
    del declaration["documents"][0]["assertions"][0]["fields"]["venue"]["locator"]
    declaration_path.write_text(json.dumps(declaration), encoding="utf-8")
    with pytest.raises(ValueError, match="claim schema is invalid"):
        import_instrument_master(source, tmp_path / "unlocated-claim-bundle")


def test_flat_view_preserves_initial_availability_and_rejects_rule_changes(tmp_path):
    bundle = tmp_path / "bundle"
    import_instrument_master(_source(tmp_path / "source"), bundle)
    _, catalog = load_instrument_master(bundle)
    flat = resolve_instrument_catalog(
        catalog,
        symbols=["510300"],
        start=pd.Timestamp("2025-01-02"),
        end=pd.Timestamp("2026-09-24"),
    )
    assert flat.loc[0, "available_at"] == "2023-02-17T08:00:00Z"
    assert flat.loc[0, "rule_versions"] == "rule-2023|rule-2023;rule-2026|rule-2026"

    source = _source(tmp_path / "changed")
    catalog_path = source / "catalog.csv"
    changed = pd.read_csv(catalog_path, dtype=str, keep_default_na=False)
    changed.loc[1, "price_tick"] = "0.002"
    changed.to_csv(catalog_path, index=False)
    _update_catalog_hash(source)
    declaration_path = source / "declaration.json"
    declaration = json.loads(declaration_path.read_text(encoding="utf-8"))
    declaration["documents"][-1]["assertions"][0]["fields"]["price_tick"]["value"] = "0.002"
    declaration_path.write_text(json.dumps(declaration), encoding="utf-8")
    changed_bundle = tmp_path / "changed-bundle"
    import_instrument_master(source, changed_bundle)
    _, changed_catalog = load_instrument_master(changed_bundle)
    with pytest.raises(ValueError, match="cannot represent.*price_tick"):
        resolve_instrument_catalog(
            changed_catalog,
            symbols=["510300"],
            start=pd.Timestamp("2025-01-02"),
            end=pd.Timestamp("2026-09-24"),
        )


def test_prefix_ignores_only_verified_current_horizon_extension(tmp_path):
    first_source = _source(tmp_path / "first-source")
    first_bundle = tmp_path / "first-bundle"
    import_instrument_master(first_source, first_bundle)

    second_source = _source(tmp_path / "second-source")
    declaration_path = second_source / "declaration.json"
    declaration = json.loads(declaration_path.read_text(encoding="utf-8"))
    declaration["captured_at"] = "2026-09-27T00:00:00Z"
    for document in declaration["documents"]:
        if document["effective_to"] == CAPTURED_AT:
            document["effective_to"] = declaration["captured_at"]
    catalog_path = second_source / "catalog.csv"
    catalog = pd.read_csv(catalog_path, dtype=str, keep_default_na=False)
    catalog.loc[catalog["effective_to"].eq(CAPTURED_AT), "effective_to"] = declaration[
        "captured_at"
    ]
    catalog.to_csv(catalog_path, index=False)
    declaration["catalog_sha256"] = _hash(catalog_path)
    declaration_path.write_text(json.dumps(declaration), encoding="utf-8")
    second_bundle = tmp_path / "second-bundle"
    import_instrument_master(second_source, second_bundle)

    cutoff = CAPTURED_AT
    assert instrument_master_prefix(first_bundle, cutoff=cutoff) == instrument_master_prefix(
        second_bundle, cutoff=cutoff
    )

    changed_source = _source(tmp_path / "changed-history-source")
    changed_catalog_path = changed_source / "catalog.csv"
    changed_catalog = pd.read_csv(changed_catalog_path, dtype=str, keep_default_na=False)
    changed_catalog.loc[0, "price_tick"] = "0.002"
    changed_catalog.to_csv(changed_catalog_path, index=False)
    changed_declaration_path = changed_source / "declaration.json"
    changed_declaration = json.loads(changed_declaration_path.read_text(encoding="utf-8"))
    changed_declaration["catalog_sha256"] = _hash(changed_catalog_path)
    changed_declaration["documents"][1]["assertions"][0]["fields"]["price_tick"][
        "value"
    ] = "0.002"
    changed_declaration_path.write_text(json.dumps(changed_declaration), encoding="utf-8")
    changed_bundle = tmp_path / "changed-history-bundle"
    import_instrument_master(changed_source, changed_bundle)
    assert instrument_master_prefix(first_bundle, cutoff=cutoff) != instrument_master_prefix(
        changed_bundle, cutoff=cutoff
    )
    with pytest.raises(ValueError, match="cutoff exceeds"):
        instrument_master_prefix(first_bundle, cutoff="2026-09-27T00:00:00Z")
