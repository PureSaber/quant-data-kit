"""Evidence-bound, point-in-time historical instrument-master imports.

Catalog rows are rule versions rather than flattened symbol records. Every
consumer field is bound to a human-extracted typed assertion with a locator and
short excerpt on a hash-verified official attachment, or is explicitly
declared unavailable. Validation proves the attachment and declared assertion
have not changed; it does not claim to parse or semantically certify the source
text. Current observations end at the capture time.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

SOURCE_SCHEMA = "qdk.historical-instrument-master-source/v2"
BUNDLE_SCHEMA = "qdk.historical-instrument-master/v2"
CATALOG_SCHEMA = "qdk.etf-instrument-catalog/v3"
CONSUMER_CATALOG_SCHEMA = "qdk.etf-instrument-catalog-view/v1"

CATALOG_COLUMNS = (
    "catalog_schema_version",
    "catalog_source_version",
    "symbol",
    "asset_class",
    "product_type",
    "venue",
    "price_scale",
    "price_tick",
    "quantity_step",
    "lot_size",
    "commission_rate",
    "stamp_duty_rate",
    "fee_fields_scope",
    "fee_evidence_id",
    "effective_from",
    "effective_to",
    "available_at",
    "listing_date",
    "listing_date_basis",
    "listing_evidence_id",
    "trading_rule_evidence_id",
    "price_limit_evidence_id",
    "price_limit_rate",
    "master_scope",
)
BASE_COLUMNS = set(CATALOG_COLUMNS)
DOCUMENT_KINDS = {
    "calendar",
    "fee_schedule",
    "listing",
    "price_limit",
    "status_observation",
    "trading_rule",
}
DOCUMENT_COLUMNS = {
    "document_id",
    "kind",
    "publisher",
    "title",
    "source_uri",
    "published_at",
    "available_at",
    "effective_from",
    "effective_to",
    "validity_basis",
    "availability_basis",
    "file",
    "sha256",
    "assertions",
}
ASSERTION_FIELDS = {
    "asset_class",
    "product_type",
    "venue",
    "listing_date",
    "price_scale",
    "price_tick",
    "quantity_step",
    "lot_size",
    "commission_rate",
    "stamp_duty_rate",
    "price_limit_rate",
}
LISTING_FIELDS = ("asset_class", "product_type", "venue", "listing_date")
TRADING_RULE_FIELDS = ("venue", "price_scale", "price_tick", "quantity_step", "lot_size")
PRICE_LIMIT_FIELDS = ("price_limit_rate",)
FEE_FIELDS = ("commission_rate", "stamp_duty_rate")
UNAVAILABLE_FEE_SCOPE = "unavailable-requires-consumer-strategy-cost-model"
CERTIFIED_FEE_SCOPE = "official-document-rates"
MASTER_SCOPE = "retrospective-fixed-pool"
CURRENT_VALIDITY = "observed-through-capture-refresh-required"
HISTORICAL_VALIDITIES = {"superseded-by-next-version", "bounded-event"}
ASSERTION_METHODS = {"direct", "derived"}
SOURCE_COLUMNS = {
    "schema_version",
    "captured_at",
    "rights_note",
    "coverage",
    "catalog_file",
    "catalog_sha256",
    "documents",
}
BUNDLE_COLUMNS = {
    "schema_version",
    "captured_at",
    "refresh_required_after",
    "rights_note",
    "coverage",
    "semantic_validation",
    "source",
    "catalog",
    "documents",
    "bundle_sha256",
}


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(path: Path) -> str:
    return _digest_bytes(path.read_bytes())


def _timestamp(value: object, field: str) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{field} requires an explicit time zone")
    return stamp.tz_convert("UTC")


def _nonempty(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")
    return value.strip()


def _relative_file(root: Path, value: object, field: str) -> Path:
    relative = Path(_nonempty(value, field))
    if relative.is_absolute():
        raise ValueError(f"{field} must be relative")
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"{field} escapes the evidence root") from exc
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    return resolved


def _validate_source_schema(declaration: dict[str, Any]) -> None:
    if set(declaration) != SOURCE_COLUMNS:
        missing = sorted(SOURCE_COLUMNS - set(declaration))
        extra = sorted(set(declaration) - SOURCE_COLUMNS)
        raise ValueError(f"Source declaration schema mismatch: missing={missing}, extra={extra}")
    coverage = declaration["coverage"]
    if not isinstance(coverage, dict) or not coverage:
        raise ValueError("Source declaration coverage must be a nonempty object")
    for field, value in coverage.items():
        _nonempty(field, "coverage field")
        _nonempty(value, f"coverage.{field}")


def _assertion_index(raw: object, document_id: str) -> dict[str, dict[str, dict[str, str]]]:
    if not isinstance(raw, list):
        raise TypeError(f"{document_id}.assertions must be a list")
    indexed: dict[str, dict[str, dict[str, str]]] = {}
    for position, assertion in enumerate(raw):
        if not isinstance(assertion, dict) or set(assertion) != {"symbol", "fields"}:
            raise ValueError(f"{document_id}.assertions[{position}] has an invalid schema")
        symbol = _nonempty(assertion["symbol"], f"{document_id}.assertions[{position}].symbol")
        fields = assertion["fields"]
        if not isinstance(fields, dict) or not fields or not set(fields) <= ASSERTION_FIELDS:
            raise ValueError(f"{document_id}.assertions[{position}].fields is invalid")
        normalized = {}
        for field, claim in fields.items():
            if not isinstance(claim, dict) or set(claim) != {
                "value",
                "locator",
                "excerpt",
                "method",
            }:
                raise ValueError(f"{document_id}.{symbol}.{field} claim schema is invalid")
            method = _nonempty(claim["method"], f"{document_id}.{symbol}.{field}.method")
            if method not in ASSERTION_METHODS:
                raise ValueError(f"{document_id}.{symbol}.{field}.method is unsupported")
            normalized[field] = {
                "value": _nonempty(claim["value"], f"{document_id}.{symbol}.{field}.value"),
                "locator": _nonempty(claim["locator"], f"{document_id}.{symbol}.{field}.locator"),
                "excerpt": _nonempty(claim["excerpt"], f"{document_id}.{symbol}.{field}.excerpt"),
                "method": method,
            }
        duplicate = set(indexed.get(symbol, {})) & set(normalized)
        if duplicate:
            raise ValueError(f"{document_id} repeats assertions for {symbol}: {sorted(duplicate)}")
        indexed.setdefault(symbol, {}).update(normalized)
    return indexed


def _document_index(declaration: dict[str, Any], root: Path) -> dict[str, dict[str, Any]]:
    if declaration.get("schema_version") != SOURCE_SCHEMA:
        raise ValueError("Unsupported historical instrument-master source schema")
    captured_at = _timestamp(declaration.get("captured_at"), "captured_at")
    documents = declaration.get("documents")
    if not isinstance(documents, list) or not documents:
        raise ValueError("Historical instrument master requires evidence documents")
    indexed: dict[str, dict[str, Any]] = {}
    for raw in documents:
        if not isinstance(raw, dict) or set(raw) != DOCUMENT_COLUMNS:
            extra = sorted(set(raw) - DOCUMENT_COLUMNS) if isinstance(raw, dict) else []
            missing = sorted(DOCUMENT_COLUMNS - set(raw)) if isinstance(raw, dict) else []
            raise ValueError(f"Evidence document schema mismatch: missing={missing}, extra={extra}")
        document_id = _nonempty(raw["document_id"], "document_id")
        if document_id in indexed:
            raise ValueError(f"Duplicate evidence document ID: {document_id}")
        kind = _nonempty(raw["kind"], f"{document_id}.kind")
        if kind not in DOCUMENT_KINDS:
            raise ValueError(f"Unsupported evidence document kind: {kind}")
        publisher = _nonempty(raw["publisher"], f"{document_id}.publisher")
        title = _nonempty(raw["title"], f"{document_id}.title")
        source_uri = _nonempty(raw["source_uri"], f"{document_id}.source_uri")
        if not source_uri.startswith("https://"):
            raise ValueError(f"{document_id}.source_uri must use HTTPS")
        published_at = _timestamp(raw["published_at"], f"{document_id}.published_at")
        available_at = _timestamp(raw["available_at"], f"{document_id}.available_at")
        effective_from = _timestamp(raw["effective_from"], f"{document_id}.effective_from")
        effective_to = _timestamp(raw["effective_to"], f"{document_id}.effective_to")
        validity_basis = _nonempty(raw["validity_basis"], f"{document_id}.validity_basis")
        availability_basis = _nonempty(
            raw["availability_basis"], f"{document_id}.availability_basis"
        )
        if not published_at <= available_at <= captured_at:
            raise ValueError(f"Evidence {document_id} has noncausal publication availability")
        if not effective_from < effective_to <= captured_at:
            raise ValueError(f"Evidence {document_id} extends beyond its observed capture horizon")
        if kind in {"trading_rule", "price_limit", "fee_schedule"} and (
            available_at > effective_from
        ):
            raise ValueError(f"Evidence {document_id} was not available before it became effective")
        if effective_to == captured_at:
            if validity_basis != CURRENT_VALIDITY:
                raise ValueError(f"Evidence {document_id} must declare its refresh boundary")
        elif validity_basis not in HISTORICAL_VALIDITIES:
            raise ValueError(f"Evidence {document_id} has an unsupported historical validity basis")
        source_file = _relative_file(root, raw["file"], f"{document_id}.file")
        expected_hash = _nonempty(raw["sha256"], f"{document_id}.sha256").lower()
        if len(expected_hash) != 64 or any(c not in "0123456789abcdef" for c in expected_hash):
            raise ValueError(f"Evidence {document_id} has an invalid SHA-256")
        if _digest(source_file) != expected_hash:
            raise ValueError(f"Evidence {document_id} hash mismatch")
        assertions = _assertion_index(raw["assertions"], document_id)
        if kind in {"listing", "trading_rule", "price_limit", "fee_schedule"} and not assertions:
            raise ValueError(f"Evidence {document_id} requires typed field assertions")
        indexed[document_id] = {
            **raw,
            "document_id": document_id,
            "kind": kind,
            "publisher": publisher,
            "title": title,
            "source_uri": source_uri,
            "published_at": published_at,
            "available_at": available_at,
            "effective_from": effective_from,
            "effective_to": effective_to,
            "validity_basis": validity_basis,
            "availability_basis": availability_basis,
            "source_file": source_file,
            "sha256": expected_hash,
            "assertion_index": assertions,
        }
    return indexed


def _decimal(value: object, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite decimal")
    return result


def _equal_field(field: str, left: object, right: object) -> bool:
    if field in {
        "price_scale",
        "price_tick",
        "quantity_step",
        "lot_size",
        "commission_rate",
        "stamp_duty_rate",
        "price_limit_rate",
    }:
        return _decimal(left, field) == _decimal(right, field)
    if field == "listing_date":
        return pd.Timestamp(left).date() == pd.Timestamp(right).date()
    return str(left) == str(right)


def _document(
    documents: dict[str, dict[str, Any]],
    document_id: object,
    *,
    symbol: str,
    field: str,
    kinds: set[str],
) -> dict[str, Any]:
    evidence_id = _nonempty(document_id, f"{symbol}.{field}")
    document = documents.get(evidence_id)
    if document is None or document["kind"] not in kinds:
        raise ValueError(f"{symbol}.{field} references missing or wrong-kind evidence")
    return document


def _assert_fields(
    row: pd.Series,
    document: dict[str, Any],
    fields: tuple[str, ...],
) -> None:
    symbol = str(row["symbol"])
    assertions = document["assertion_index"].get(symbol, {})
    missing = set(fields) - set(assertions)
    if missing:
        raise ValueError(
            f"Evidence {document['document_id']} lacks {symbol} assertions: {sorted(missing)}"
        )
    for field in fields:
        if not _equal_field(field, row[field], assertions[field]["value"]):
            raise ValueError(
                f"{symbol}.{field} differs from evidence {document['document_id']} assertion"
            )


def _validate_row(
    row: pd.Series,
    documents: dict[str, dict[str, Any]],
    captured_at: pd.Timestamp,
) -> None:
    symbol = _nonempty(row["symbol"], "symbol")
    if row["asset_class"] != "etf" or row["product_type"] != "etf":
        raise ValueError(f"{symbol} must be explicitly evidenced as an ETF")
    if row["venue"] not in {"SSE", "SZSE"}:
        raise ValueError(f"{symbol} has an unsupported ETF venue")
    if row["master_scope"] != MASTER_SCOPE:
        raise ValueError(f"{symbol} has an unsupported master_scope")
    effective_from = _timestamp(row["effective_from"], f"{symbol}.effective_from")
    effective_to = _timestamp(row["effective_to"], f"{symbol}.effective_to")
    available_at = _timestamp(row["available_at"], f"{symbol}.available_at")
    if not available_at <= effective_from < effective_to <= captured_at:
        raise ValueError(f"{symbol} has noncausal availability or unobserved future validity")
    listing_date = pd.Timestamp(row["listing_date"])
    if pd.isna(listing_date) or listing_date.tzinfo is not None:
        raise ValueError(f"{symbol}.listing_date must be a calendar date")
    if listing_date.date() > effective_from.date():
        raise ValueError(f"{symbol} has an invalid listing date")
    if row["listing_date_basis"] != "official-published-document":
        raise ValueError(f"{symbol} listing date lacks official-document basis")

    price_scale = int(_decimal(row["price_scale"], f"{symbol}.price_scale"))
    price_tick = _decimal(row["price_tick"], f"{symbol}.price_tick")
    quantity_step = _decimal(row["quantity_step"], f"{symbol}.quantity_step")
    lot_size = int(_decimal(row["lot_size"], f"{symbol}.lot_size"))
    price_limit_rate = _decimal(row["price_limit_rate"], f"{symbol}.price_limit_rate")
    if (
        price_scale < 0
        or price_tick <= 0
        or quantity_step <= 0
        or lot_size <= 0
        or not 0 < price_limit_rate < 1
        or price_tick * (Decimal(10) ** price_scale) % 1
    ):
        raise ValueError(f"{symbol} has invalid tick, lot or price-limit fields")

    listing = _document(
        documents,
        row["listing_evidence_id"],
        symbol=symbol,
        field="listing_evidence_id",
        kinds={"listing"},
    )
    trading = _document(
        documents,
        row["trading_rule_evidence_id"],
        symbol=symbol,
        field="trading_rule_evidence_id",
        kinds={"trading_rule"},
    )
    price_limit = _document(
        documents,
        row["price_limit_evidence_id"],
        symbol=symbol,
        field="price_limit_evidence_id",
        kinds={"price_limit", "trading_rule"},
    )
    for field, document in (
        ("listing_evidence_id", listing),
        ("trading_rule_evidence_id", trading),
        ("price_limit_evidence_id", price_limit),
    ):
        if (
            not document["effective_from"]
            <= effective_from
            < effective_to
            <= document["effective_to"]
        ):
            raise ValueError(f"{symbol}.{field} does not cover its rule version")
    _assert_fields(row, listing, LISTING_FIELDS)
    _assert_fields(row, trading, TRADING_RULE_FIELDS)
    _assert_fields(row, price_limit, PRICE_LIMIT_FIELDS)

    evidence_available = max(
        listing["available_at"], trading["available_at"], price_limit["available_at"]
    )
    scope = row["fee_fields_scope"]
    if scope == UNAVAILABLE_FEE_SCOPE:
        if row["commission_rate"] or row["stamp_duty_rate"] or row["fee_evidence_id"]:
            raise ValueError(f"{symbol} unavailable fee fields must remain empty")
    elif scope == CERTIFIED_FEE_SCOPE:
        fee = _document(
            documents,
            row["fee_evidence_id"],
            symbol=symbol,
            field="fee_evidence_id",
            kinds={"fee_schedule"},
        )
        if not fee["effective_from"] <= effective_from < effective_to <= fee["effective_to"]:
            raise ValueError(f"{symbol}.fee_evidence_id does not cover its rule version")
        _assert_fields(row, fee, FEE_FIELDS)
        for field in FEE_FIELDS:
            if not 0 <= _decimal(row[field], f"{symbol}.{field}") < 1:
                raise ValueError(f"{symbol}.{field} is outside [0, 1)")
        evidence_available = max(evidence_available, fee["available_at"])
    else:
        raise ValueError(f"{symbol} has an unsupported fee_fields_scope")
    if available_at != evidence_available:
        raise ValueError(f"{symbol}.available_at must equal its latest referenced evidence")


def validate_instrument_catalog(
    catalog: pd.DataFrame,
    documents: dict[str, dict[str, Any]],
    *,
    captured_at: pd.Timestamp,
) -> pd.DataFrame:
    """Validate a closed, versioned catalog against typed document assertions."""
    if catalog.empty or set(catalog.columns) != BASE_COLUMNS:
        missing = sorted(BASE_COLUMNS - set(catalog.columns))
        extra = sorted(set(catalog.columns) - BASE_COLUMNS)
        raise ValueError(f"Instrument catalog schema mismatch: missing={missing}, extra={extra}")
    out = catalog.loc[:, CATALOG_COLUMNS].copy().fillna("")
    for column in BASE_COLUMNS - {"commission_rate", "stamp_duty_rate", "fee_evidence_id"}:
        if out[column].astype(str).str.strip().eq("").any():
            raise ValueError(f"Instrument catalog contains empty {column}")
    if not out["catalog_schema_version"].eq(CATALOG_SCHEMA).all():
        raise ValueError("Unsupported instrument catalog schema")
    for index, row in out.iterrows():
        _validate_row(row, documents, captured_at)
        out.loc[index, "symbol"] = str(row["symbol"]).strip()
    effective = pd.to_datetime(out["effective_from"], format="mixed", utc=True)
    if pd.DataFrame({"symbol": out["symbol"], "effective_from": effective}).duplicated().any():
        raise ValueError("Instrument catalog repeats a symbol rule version")
    out = out.assign(_effective_from=effective).sort_values(["symbol", "_effective_from"])
    for symbol, versions in out.groupby("symbol", sort=False):
        static_fields = (
            "asset_class",
            "product_type",
            "venue",
            "listing_date",
            "listing_date_basis",
            "listing_evidence_id",
            "master_scope",
        )
        if any(versions[field].nunique(dropna=False) != 1 for field in static_fields):
            raise ValueError(f"{symbol} changes static instrument fields across rule versions")
        starts = pd.to_datetime(versions["effective_from"], format="mixed", utc=True).tolist()
        ends = pd.to_datetime(versions["effective_to"], format="mixed", utc=True).tolist()
        for previous_end, next_start in zip(ends, starts[1:], strict=False):
            if previous_end != next_start:
                raise ValueError(f"{symbol} has a rule-version gap or overlap")
    return out.drop(columns="_effective_from").reset_index(drop=True)


def resolve_instrument_catalog(
    catalog: pd.DataFrame,
    *,
    symbols: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """Create a flat consumer view only when all covered versions are execution-equivalent."""
    first_open = (
        start.tz_localize("Asia/Shanghai") + pd.Timedelta(hours=9, minutes=25)
    ).tz_convert("UTC")
    last_close = (end.tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15)).tz_convert("UTC")
    rows: list[dict[str, Any]] = []
    value_fields = (
        "asset_class",
        "product_type",
        "venue",
        "price_scale",
        "price_tick",
        "quantity_step",
        "lot_size",
        "commission_rate",
        "stamp_duty_rate",
        "fee_fields_scope",
        "master_scope",
        "listing_date",
        "listing_date_basis",
    )
    for symbol in symbols:
        versions = catalog[catalog["symbol"].eq(symbol)].copy()
        if versions.empty:
            raise ValueError(f"Historical instrument master is missing symbol {symbol}")
        versions["_start"] = pd.to_datetime(versions["effective_from"], format="mixed", utc=True)
        versions["_end"] = pd.to_datetime(versions["effective_to"], format="mixed", utc=True)
        versions = versions[(versions["_end"] > first_open) & (versions["_start"] <= last_close)]
        versions = versions.sort_values("_start")
        if (
            versions.empty
            or versions.iloc[0]["_start"] > first_open
            or versions.iloc[-1]["_end"] <= last_close
        ):
            raise ValueError(
                f"Historical instrument master does not cover {symbol} dataset interval"
            )
        for previous_end, next_start in zip(
            versions["_end"].tolist(), versions["_start"].tolist()[1:], strict=False
        ):
            if previous_end != next_start:
                raise ValueError(f"Historical instrument master has a {symbol} coverage gap")
        changed = [field for field in value_fields if versions[field].nunique(dropna=False) != 1]
        if changed:
            raise ValueError(
                f"Flat research consumer cannot represent {symbol} rule changes: {changed}"
            )
        first = versions.iloc[0]
        rows.append(
            {
                "catalog_schema_version": CONSUMER_CATALOG_SCHEMA,
                "catalog_source_version": first["catalog_source_version"],
                "symbol": symbol,
                **{field: first[field] for field in value_fields},
                "effective_from": first["effective_from"],
                "effective_to": versions.iloc[-1]["effective_to"],
                "available_at": first["available_at"],
                "rule_versions": ";".join(
                    f"{row.trading_rule_evidence_id}|{row.price_limit_evidence_id}"
                    for row in versions.itertuples(index=False)
                ),
            }
        )
    return pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)


def instrument_master_prefix(
    root: Path,
    *,
    cutoff: str | pd.Timestamp,
) -> dict[str, Any]:
    """Return the canonical evidence state known at ``cutoff``.

    The same cutoff can compare two independently captured bundles. Extending
    an unchanged current observation beyond the cutoff has no effect, while a
    changed historical value, availability time, version boundary, evidence
    identity, file hash or typed assertion remains visible.
    """
    manifest, catalog = load_instrument_master(root)
    cutoff_at = _timestamp(cutoff, "cutoff")
    captured_at = _timestamp(manifest["captured_at"], "captured_at")
    if cutoff_at > captured_at:
        raise ValueError("Instrument-master prefix cutoff exceeds the verified capture")

    known = catalog[
        pd.to_datetime(catalog["available_at"], format="mixed", utc=True) <= cutoff_at
    ].copy()
    rows: list[dict[str, Any]] = []
    referenced_documents: set[str] = set()
    for row in known.sort_values(["symbol", "effective_from"]).to_dict(orient="records"):
        start = _timestamp(row["effective_from"], "effective_from")
        end = _timestamp(row["effective_to"], "effective_to")
        normalized = {
            key: value
            for key, value in row.items()
            if key not in {"catalog_schema_version", "catalog_source_version", "effective_to"}
        }
        if start < cutoff_at:
            normalized["effective_to"] = min(end, cutoff_at).isoformat()
            normalized["knowledge_state"] = "effective-by-cutoff"
        else:
            normalized["effective_to"] = None
            normalized["knowledge_state"] = "scheduled-after-cutoff"
        rows.append(normalized)
        referenced_documents.update(
            evidence_id
            for evidence_id in (
                row["listing_evidence_id"],
                row["trading_rule_evidence_id"],
                row["price_limit_evidence_id"],
                row["fee_evidence_id"],
            )
            if evidence_id
        )

    documents = []
    for document in sorted(manifest["documents"], key=lambda item: item["document_id"]):
        if document["document_id"] not in referenced_documents:
            continue
        available_at = _timestamp(document["available_at"], "document.available_at")
        if available_at > cutoff_at:
            raise ValueError("Known catalog version references evidence unavailable at cutoff")
        effective_from = _timestamp(document["effective_from"], "document.effective_from")
        effective_to = _timestamp(document["effective_to"], "document.effective_to")
        normalized = {
            key: value
            for key, value in document.items()
            if key not in {"file", "effective_to", "validity_basis"}
        }
        if effective_from < cutoff_at:
            normalized["effective_to"] = min(effective_to, cutoff_at).isoformat()
            normalized["knowledge_state"] = "effective-by-cutoff"
        else:
            normalized["effective_to"] = None
            normalized["knowledge_state"] = "scheduled-after-cutoff"
        documents.append(normalized)
    return {
        "schema_version": "qdk.historical-instrument-master-prefix/v1",
        "cutoff": cutoff_at.isoformat(),
        "catalog_schema_version": CATALOG_SCHEMA,
        "rows": rows,
        "documents": documents,
    }


def import_instrument_master(source_root: Path, output: Path) -> dict[str, Any]:
    """Publish an immutable, self-contained bundle from declared official documents."""
    source_root = source_root.resolve()
    output = output.resolve()
    if output.exists():
        raise FileExistsError(output)
    declaration_path = source_root / "declaration.json"
    declaration = json.loads(declaration_path.read_text(encoding="utf-8"))
    _validate_source_schema(declaration)
    documents = _document_index(declaration, source_root)
    captured_at = _timestamp(declaration["captured_at"], "captured_at")
    catalog_path = _relative_file(source_root, declaration.get("catalog_file"), "catalog_file")
    expected_catalog_hash = _nonempty(declaration.get("catalog_sha256"), "catalog_sha256").lower()
    if _digest(catalog_path) != expected_catalog_hash:
        raise ValueError("Instrument catalog source hash mismatch")
    catalog = validate_instrument_catalog(
        pd.read_csv(catalog_path, dtype=str, keep_default_na=False),
        documents,
        captured_at=captured_at,
    )
    staging = output.with_name(f".{output.name}.tmp-{uuid4().hex}")
    staging.mkdir(parents=True)
    try:
        normalized_catalog = staging / "catalog.csv"
        catalog.to_csv(normalized_catalog, index=False)
        source_directory = staging / "source"
        source_directory.mkdir()
        shutil.copyfile(declaration_path, source_directory / "declaration.json")
        shutil.copyfile(catalog_path, source_directory / "catalog.csv")
        copied_documents = []
        for document_id, document in sorted(documents.items()):
            suffix = document["source_file"].suffix.lower()
            destination = staging / "documents" / f"{document_id}{suffix}"
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(document["source_file"], destination)
            original_destination = source_directory / Path(document["file"])
            original_destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(document["source_file"], original_destination)
            copied_documents.append(
                {
                    key: value
                    for key, value in document.items()
                    if key
                    not in {
                        "source_file",
                        "assertion_index",
                        "published_at",
                        "available_at",
                        "effective_from",
                        "effective_to",
                        "file",
                    }
                }
                | {
                    "file": destination.relative_to(staging).as_posix(),
                    "published_at": document["published_at"].isoformat(),
                    "available_at": document["available_at"].isoformat(),
                    "effective_from": document["effective_from"].isoformat(),
                    "effective_to": document["effective_to"].isoformat(),
                }
            )
        manifest = {
            "schema_version": BUNDLE_SCHEMA,
            "captured_at": captured_at.isoformat(),
            "refresh_required_after": captured_at.isoformat(),
            "rights_note": _nonempty(declaration.get("rights_note"), "rights_note"),
            "coverage": declaration.get("coverage", {}),
            "semantic_validation": {
                "assertion_origin": "human-extracted-with-locator-and-short-excerpt",
                "attachment_integrity": "sha256-verified",
                "attachment_text_semantically_verified_by_code": False,
            },
            "source": {
                "declaration_file": "source/declaration.json",
                "declaration_sha256": _digest(source_directory / "declaration.json"),
                "catalog_file": "source/catalog.csv",
                "catalog_sha256": _digest(source_directory / "catalog.csv"),
            },
            "catalog": {
                "file": "catalog.csv",
                "sha256": _digest(normalized_catalog),
                "rows": len(catalog),
                "symbols": sorted(catalog["symbol"].unique().tolist()),
            },
            "documents": copied_documents,
        }
        manifest["bundle_sha256"] = _digest_bytes(_canonical_bytes(manifest))
        (staging / "manifest.json").write_bytes(_canonical_bytes(manifest))
        os.replace(staging, output)
        return manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def load_instrument_master(root: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    """Load a bundle after verifying its source, documents and typed assertions."""
    root = root.resolve()
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if set(manifest) != BUNDLE_COLUMNS:
        missing = sorted(BUNDLE_COLUMNS - set(manifest))
        extra = sorted(set(manifest) - BUNDLE_COLUMNS)
        raise ValueError(
            f"Instrument-master bundle schema mismatch: missing={missing}, extra={extra}"
        )
    if manifest.get("schema_version") != BUNDLE_SCHEMA:
        raise ValueError("Unsupported historical instrument-master bundle schema")
    identity = {key: value for key, value in manifest.items() if key != "bundle_sha256"}
    if manifest.get("bundle_sha256") != _digest_bytes(_canonical_bytes(identity)):
        raise ValueError("Historical instrument-master manifest identity mismatch")
    captured_at = _timestamp(manifest.get("captured_at"), "captured_at")
    if _timestamp(manifest.get("refresh_required_after"), "refresh_required_after") != captured_at:
        raise ValueError("Historical instrument-master refresh boundary mismatch")
    for field in ("declaration", "catalog"):
        path = _relative_file(root, manifest["source"][f"{field}_file"], f"source.{field}_file")
        if _digest(path) != manifest["source"][f"{field}_sha256"]:
            raise ValueError(f"Historical instrument-master source {field} hash mismatch")
    source_declaration = json.loads(
        (root / manifest["source"]["declaration_file"]).read_text(encoding="utf-8")
    )
    _validate_source_schema(source_declaration)
    if _timestamp(source_declaration.get("captured_at"), "source.captured_at") != captured_at:
        raise ValueError("Historical instrument-master source capture changed")
    if source_declaration.get("catalog_sha256") != manifest["source"]["catalog_sha256"]:
        raise ValueError("Historical instrument-master source catalog identity changed")
    source_documents = _document_index(
        source_declaration, (root / manifest["source"]["declaration_file"]).parent
    )
    source_catalog = validate_instrument_catalog(
        pd.read_csv(root / manifest["source"]["catalog_file"], dtype=str, keep_default_na=False),
        source_documents,
        captured_at=captured_at,
    )
    catalog_path = _relative_file(root, manifest["catalog"]["file"], "catalog.file")
    if _digest(catalog_path) != manifest["catalog"]["sha256"]:
        raise ValueError("Historical instrument-master catalog hash mismatch")
    declaration = {
        "schema_version": SOURCE_SCHEMA,
        "captured_at": manifest["captured_at"],
        "documents": manifest["documents"],
    }
    documents = _document_index(declaration, root)
    catalog = validate_instrument_catalog(
        pd.read_csv(catalog_path, dtype=str, keep_default_na=False),
        documents,
        captured_at=captured_at,
    )
    if not source_catalog.equals(catalog):
        raise ValueError("Historical instrument-master normalized catalog differs from source")
    if (
        len(catalog) != manifest["catalog"]["rows"]
        or sorted(catalog["symbol"].unique().tolist()) != manifest["catalog"]["symbols"]
    ):
        raise ValueError("Historical instrument-master manifest differs from catalog")
    return manifest, catalog


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m quant_data_kit.instrument_master",
        description="Import or inspect evidence-bound historical instrument masters",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    publish = commands.add_parser("import")
    publish.add_argument("--source", type=Path, required=True)
    publish.add_argument("--output", type=Path, required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--root", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "import":
        result = import_instrument_master(args.source, args.output)
    else:
        result, _ = load_instrument_master(args.root)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
