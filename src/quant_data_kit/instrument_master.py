"""Evidence-bound historical instrument-master imports.

The bundle proves only the fields and intervals declared in its catalog.  It
does not turn a current security list, an empty suspension query, or observed
volume into historical tradability.
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

SOURCE_SCHEMA = "qdk.historical-instrument-master-source/v1"
BUNDLE_SCHEMA = "qdk.historical-instrument-master/v1"
CATALOG_SCHEMA = "qdk.etf-instrument-catalog/v2"

BASE_COLUMNS = {
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
    "effective_from",
    "effective_to",
    "available_at",
    "listing_date",
    "listing_date_basis",
    "listing_evidence_id",
    "trading_rule_evidence_ids",
    "price_limit_evidence_ids",
}
DOCUMENT_KINDS = {
    "calendar",
    "listing",
    "price_limit",
    "status_observation",
    "trading_rule",
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


def _evidence_ids(value: object, field: str) -> list[str]:
    ids = [item.strip() for item in _nonempty(value, field).split(";") if item.strip()]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError(f"{field} must contain unique semicolon-separated IDs")
    return ids


def _document_index(declaration: dict[str, Any], root: Path) -> dict[str, dict[str, Any]]:
    if declaration.get("schema_version") != SOURCE_SCHEMA:
        raise ValueError("Unsupported historical instrument-master source schema")
    captured_at = _timestamp(declaration.get("captured_at"), "captured_at")
    documents = declaration.get("documents")
    if not isinstance(documents, list) or not documents:
        raise ValueError("Historical instrument master requires evidence documents")
    indexed: dict[str, dict[str, Any]] = {}
    for raw in documents:
        if not isinstance(raw, dict):
            raise TypeError("Evidence document entries must be objects")
        document_id = _nonempty(raw.get("document_id"), "document_id")
        if document_id in indexed:
            raise ValueError(f"Duplicate evidence document ID: {document_id}")
        kind = _nonempty(raw.get("kind"), f"{document_id}.kind")
        if kind not in DOCUMENT_KINDS:
            raise ValueError(f"Unsupported evidence document kind: {kind}")
        publisher = _nonempty(raw.get("publisher"), f"{document_id}.publisher")
        source_uri = _nonempty(raw.get("source_uri"), f"{document_id}.source_uri")
        if not source_uri.startswith("https://"):
            raise ValueError(f"{document_id}.source_uri must use HTTPS")
        published_at = _timestamp(raw.get("published_at"), f"{document_id}.published_at")
        effective_from = _timestamp(raw.get("effective_from"), f"{document_id}.effective_from")
        effective_to = _timestamp(raw.get("effective_to"), f"{document_id}.effective_to")
        if published_at > captured_at:
            raise ValueError(f"Evidence {document_id} was published after capture")
        if kind in {"trading_rule", "price_limit"} and published_at > effective_from:
            raise ValueError(f"Evidence {document_id} was not public before it became effective")
        if effective_from >= effective_to:
            raise ValueError(f"Evidence {document_id} has an invalid effective interval")
        source_file = _relative_file(root, raw.get("file"), f"{document_id}.file")
        expected_hash = _nonempty(raw.get("sha256"), f"{document_id}.sha256").lower()
        if len(expected_hash) != 64 or any(c not in "0123456789abcdef" for c in expected_hash):
            raise ValueError(f"Evidence {document_id} has an invalid SHA-256")
        if _digest(source_file) != expected_hash:
            raise ValueError(f"Evidence {document_id} hash mismatch")
        indexed[document_id] = {
            **raw,
            "document_id": document_id,
            "kind": kind,
            "publisher": publisher,
            "source_uri": source_uri,
            "published_at": published_at,
            "effective_from": effective_from,
            "effective_to": effective_to,
            "source_file": source_file,
            "sha256": expected_hash,
        }
    return indexed


def _validate_interval_coverage(
    row: pd.Series,
    *,
    ids: list[str],
    documents: dict[str, dict[str, Any]],
    kinds: set[str],
    field: str,
) -> None:
    start = _timestamp(row["effective_from"], "effective_from")
    end = _timestamp(row["effective_to"], "effective_to")
    intervals = []
    for document_id in ids:
        document = documents.get(document_id)
        if document is None:
            raise ValueError(f"Unknown evidence ID {document_id} in {field}")
        if document["kind"] not in kinds:
            raise ValueError(f"Evidence {document_id} has the wrong kind for {field}")
        intervals.append((document["effective_from"], document["effective_to"]))
    cursor = start
    for interval_start, interval_end in sorted(intervals):
        if interval_end <= cursor:
            continue
        if interval_start > cursor:
            raise ValueError(f"{field} has an evidence gap at {cursor.isoformat()}")
        cursor = interval_end
        if cursor >= end:
            return
    raise ValueError(f"{field} does not cover effective_to")


def validate_instrument_catalog(
    catalog: pd.DataFrame, documents: dict[str, dict[str, Any]]
) -> pd.DataFrame:
    """Validate a flat consumer catalog against versioned source evidence."""
    missing = BASE_COLUMNS - set(catalog.columns)
    if missing or catalog.empty:
        raise ValueError(f"Instrument catalog is empty or missing fields: {sorted(missing)}")
    out = catalog.copy()
    for column in BASE_COLUMNS:
        if out[column].isna().any():
            raise ValueError(f"Instrument catalog contains null {column}")
    if not out["catalog_schema_version"].eq(CATALOG_SCHEMA).all():
        raise ValueError("Unsupported instrument catalog schema")
    if out["symbol"].duplicated().any():
        raise ValueError("Instrument catalog must have one flattened row per symbol")
    for index, row in out.iterrows():
        symbol = _nonempty(row["symbol"], "symbol")
        for field in ("catalog_source_version", "asset_class", "product_type", "venue"):
            _nonempty(row[field], f"{symbol}.{field}")
        if row["product_type"] == "etf" and row["asset_class"] != "etf":
            raise ValueError(f"{symbol} ETF product_type requires ETF asset_class")
        effective_from = _timestamp(row["effective_from"], f"{symbol}.effective_from")
        effective_to = _timestamp(row["effective_to"], f"{symbol}.effective_to")
        available_at = _timestamp(row["available_at"], f"{symbol}.available_at")
        if not (available_at <= effective_from < effective_to):
            raise ValueError(f"{symbol} has noncausal availability or invalid validity")
        listing_date = pd.Timestamp(row["listing_date"])
        if pd.isna(listing_date) or listing_date.date() > effective_from.date():
            raise ValueError(f"{symbol} has an invalid listing date")
        if row["listing_date_basis"] != "official-published-document":
            raise ValueError(f"{symbol} listing date lacks official-document basis")
        try:
            price_scale = int(row["price_scale"])
            price_tick = Decimal(str(row["price_tick"]))
            quantity_step = Decimal(str(row["quantity_step"]))
            lot_size = int(row["lot_size"])
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"{symbol} has invalid tick/lot fields") from exc
        if (
            price_scale < 0
            or price_tick <= 0
            or quantity_step <= 0
            or lot_size <= 0
            or price_tick * (Decimal(10) ** price_scale) % 1
        ):
            raise ValueError(f"{symbol} has invalid tick/lot fields")
        listing_id = _nonempty(row["listing_evidence_id"], f"{symbol}.listing_evidence_id")
        listing = documents.get(listing_id)
        if listing is None or listing["kind"] != "listing":
            raise ValueError(f"{symbol} listing evidence is missing or has the wrong kind")
        if listing["published_at"] > available_at:
            raise ValueError(f"{symbol} was marked available before its listing evidence")
        rule_ids = _evidence_ids(
            row["trading_rule_evidence_ids"], f"{symbol}.trading_rule_evidence_ids"
        )
        limit_ids = _evidence_ids(
            row["price_limit_evidence_ids"], f"{symbol}.price_limit_evidence_ids"
        )
        first_rules = [
            documents[item]
            for item in rule_ids
            if documents[item]["effective_from"] <= effective_from < documents[item]["effective_to"]
        ]
        first_limits = [
            documents[item]
            for item in limit_ids
            if documents[item]["effective_from"] <= effective_from < documents[item]["effective_to"]
        ]
        if not first_rules or not first_limits:
            raise ValueError(f"{symbol} lacks evidence effective at catalog start")
        if any(document["published_at"] > available_at for document in first_rules + first_limits):
            raise ValueError(f"{symbol} was marked available before its initial rules")
        _validate_interval_coverage(
            row,
            ids=rule_ids,
            documents=documents,
            kinds={"trading_rule"},
            field=f"{symbol}.trading_rule_evidence_ids",
        )
        _validate_interval_coverage(
            row,
            ids=limit_ids,
            documents=documents,
            kinds={"trading_rule", "price_limit"},
            field=f"{symbol}.price_limit_evidence_ids",
        )
        out.loc[index, "symbol"] = symbol
    return out.sort_values("symbol").reset_index(drop=True)


def import_instrument_master(source_root: Path, output: Path) -> dict[str, Any]:
    """Publish an immutable, hash-bound source bundle from declared official documents."""
    source_root = source_root.resolve()
    output = output.resolve()
    if output.exists():
        raise FileExistsError(output)
    declaration_path = source_root / "declaration.json"
    declaration = json.loads(declaration_path.read_text(encoding="utf-8"))
    documents = _document_index(declaration, source_root)
    catalog_path = _relative_file(source_root, declaration.get("catalog_file"), "catalog_file")
    expected_catalog_hash = _nonempty(declaration.get("catalog_sha256"), "catalog_sha256").lower()
    if _digest(catalog_path) != expected_catalog_hash:
        raise ValueError("Instrument catalog source hash mismatch")
    catalog = validate_instrument_catalog(pd.read_csv(catalog_path, dtype=str), documents)
    staging = output.with_name(f".{output.name}.tmp-{uuid4().hex}")
    staging.mkdir(parents=True)
    try:
        normalized_catalog = staging / "catalog.csv"
        catalog.to_csv(normalized_catalog, index=False)
        copied_documents = []
        for document_id, document in sorted(documents.items()):
            suffix = document["source_file"].suffix.lower()
            destination = staging / "documents" / f"{document_id}{suffix}"
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(document["source_file"], destination)
            copied_documents.append(
                {
                    key: value
                    for key, value in document.items()
                    if key
                    not in {
                        "source_file",
                        "published_at",
                        "effective_from",
                        "effective_to",
                        "file",
                    }
                }
                | {
                    "file": destination.relative_to(staging).as_posix(),
                    "published_at": document["published_at"].isoformat(),
                    "effective_from": document["effective_from"].isoformat(),
                    "effective_to": document["effective_to"].isoformat(),
                }
            )
        manifest = {
            "schema_version": BUNDLE_SCHEMA,
            "captured_at": _timestamp(declaration["captured_at"], "captured_at").isoformat(),
            "rights_note": _nonempty(declaration.get("rights_note"), "rights_note"),
            "coverage": declaration.get("coverage", {}),
            "source_declaration_sha256": _digest(declaration_path),
            "source_catalog_sha256": expected_catalog_hash,
            "catalog": {
                "file": "catalog.csv",
                "sha256": _digest(normalized_catalog),
                "rows": len(catalog),
                "symbols": sorted(catalog["symbol"].tolist()),
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
    """Load a published bundle after verifying every file and semantic time constraint."""
    root = root.resolve()
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != BUNDLE_SCHEMA:
        raise ValueError("Unsupported historical instrument-master bundle schema")
    identity = {key: value for key, value in manifest.items() if key != "bundle_sha256"}
    if manifest.get("bundle_sha256") != _digest_bytes(_canonical_bytes(identity)):
        raise ValueError("Historical instrument-master manifest identity mismatch")
    catalog_path = _relative_file(root, manifest["catalog"]["file"], "catalog.file")
    if _digest(catalog_path) != manifest["catalog"]["sha256"]:
        raise ValueError("Historical instrument-master catalog hash mismatch")
    documents = {}
    declaration = {
        "schema_version": SOURCE_SCHEMA,
        "captured_at": manifest["captured_at"],
        "documents": manifest["documents"],
    }
    documents = _document_index(declaration, root)
    catalog = validate_instrument_catalog(pd.read_csv(catalog_path, dtype=str), documents)
    if (
        len(catalog) != manifest["catalog"]["rows"]
        or sorted(catalog["symbol"].tolist()) != manifest["catalog"]["symbols"]
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
