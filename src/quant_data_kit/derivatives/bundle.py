"""Immutable normalized bundles with source identity and read-only validation."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .models import Contract, Quote, payload, record, text, utc

SCHEMA = "qdk.derivatives/v1"
MAX_FILE_BYTES = 128 * 1024 * 1024


def json_bytes(value):
    return (
        json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def quote_bytes(quotes):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, list(Quote.__dataclass_fields__), lineterminator="\n")
    writer.writeheader()
    writer.writerows(payload(q) for q in quotes)
    return stream.getvalue().encode("utf-8")


def parse_quotes(data):
    reader = csv.DictReader(io.StringIO(data.decode("utf-8-sig"), newline=""))
    if reader.fieldnames != list(Quote.__dataclass_fields__):
        raise ValueError("quote CSV columns must match the documented schema/order")
    rows = []
    for raw in reader:
        if None in raw:
            raise ValueError("malformed quote CSV row")
        rows.append(record(Quote, {k: None if v == "" else v for k, v in raw.items()}))
    return tuple(rows)


def validate_records(contracts, quotes):
    if not contracts or not quotes:
        raise ValueError("empty derivative dataset is not a successful collection")
    lookup = {c.instrument_id: c for c in contracts}
    if len(lookup) != len(contracts):
        raise ValueError(
            "duplicate contract IDs; split changed rule versions into separate bundles"
        )
    seen = set()
    previous = {}
    for q in quotes:
        if q.instrument_id not in lookup:
            raise ValueError(f"quote references unknown contract: {q.instrument_id}")
        c = lookup[q.instrument_id]
        if not c.listed_at <= q.at <= c.expiry:
            raise ValueError("quote falls outside the declared contract lifecycle")
        if c.kind == "option" and min(q.low, q.bid or 0, q.settlement or 0) < 0:
            raise ValueError("negative option premium")
        key = (q.instrument_id, q.at)
        if key in seen or (q.instrument_id in previous and q.at <= previous[q.instrument_id]):
            raise ValueError("duplicate or out-of-order observations")
        previous[q.instrument_id] = q.at
        seen.add(key)
    if set(previous) != set(lookup):
        raise ValueError("every declared contract needs at least one observation")


@dataclass(frozen=True)
class DerivativeBundle:
    root: Path
    manifest: dict
    contracts: tuple[Contract, ...]
    quotes: tuple[Quote, ...]
    identity: str

    def contract(self, instrument_id):
        try:
            return next(c for c in self.contracts if c.instrument_id == instrument_id)
        except StopIteration as exc:
            raise ValueError(f"unknown contract: {instrument_id}") from exc

    def asof(self, at, *, product=None, venue=None):
        at = utc(at)
        latest = {}
        contracts = {c.instrument_id: c for c in self.contracts}
        for q in self.quotes:
            c = contracts[q.instrument_id]
            if (
                q.available_at <= at
                and c.known_at <= at < c.expiry
                and (product is None or c.product == product)
                and (venue is None or c.venue == venue)
                and (q.instrument_id not in latest or q.at > latest[q.instrument_id].at)
            ):
                latest[q.instrument_id] = q
        return latest

    def verify_unchanged(self):
        if load_bundle(self.root).identity != self.identity:
            raise ValueError("input bundle changed during research")

    def summary(self):
        return {
            "schema_version": "qdk.derivatives.preflight/v1",
            "status": "passed",
            "read_only": True,
            "evidence_kind": self.manifest["evidence_kind"],
            "contracts": len(self.contracts),
            "rows": len(self.quotes),
            "instrument_ids": [c.instrument_id for c in self.contracts],
            "currencies": sorted({c.currency for c in self.contracts}),
            "venues": sorted({c.venue for c in self.contracts}),
            "input_sha256": self.identity,
            "limits": self.manifest["limits"],
        }


def load_bundle(root):
    root = Path(root).resolve()
    raw_manifest = (root / "manifest.json").read_bytes()
    if len(raw_manifest) > 1_000_000:
        raise ValueError("manifest too large")
    manifest = json.loads(raw_manifest)
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")  # noqa: TRY004 - invalid serialized data
    if manifest.get("schema") != SCHEMA or manifest.get("status") != "complete":
        raise ValueError("unsupported or incomplete derivative bundle")
    if manifest.get("evidence_kind") not in {"synthetic", "retrospective"}:
        raise ValueError(
            "source must be labeled synthetic or retrospective; no PIT certification implied"
        )
    for name in ("provider", "rights_note", "limits"):
        if not isinstance(manifest.get(name), str) or not manifest[name].strip():
            raise ValueError(f"{name} must be declared")
    utc(manifest["captured_at"])
    files = manifest.get("files", {})
    if set(files) != {"contracts.json", "quotes.csv"}:
        raise ValueError("unexpected bundle file set")
    contents = {}
    for name, expected in files.items():
        path = (root / name).resolve()
        if path.parent != root or not path.is_file() or path.stat().st_size > MAX_FILE_BYTES:
            raise ValueError("missing, escaped or oversized input file")
        contents[name] = path.read_bytes()
        if digest(contents[name]) != expected:
            raise ValueError(f"input hash mismatch: {name}")
    raw_contracts = json.loads(contents["contracts.json"])
    if not isinstance(raw_contracts, list):
        raise ValueError("contracts must be a list")  # noqa: TRY004 - invalid serialized data
    contracts = tuple(record(Contract, raw) for raw in raw_contracts)
    quotes = parse_quotes(contents["quotes.csv"])
    validate_records(contracts, quotes)
    identity = digest(raw_manifest + contents["contracts.json"] + contents["quotes.csv"])
    return DerivativeBundle(root, manifest, contracts, quotes, identity)


def write_bundle(
    output, contracts, quotes, *, provider, evidence_kind, rights_note, limits, source=None
):
    contracts, quotes = tuple(contracts), tuple(quotes)
    validate_records(contracts, quotes)
    text(provider, "provider")
    if (
        evidence_kind not in {"synthetic", "retrospective"}
        or not rights_note.strip()
        or not limits.strip()
    ):
        raise ValueError("declare source evidence, rights and research limits")
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError("immutable bundle destination already exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".derivatives-", dir=output.parent))
    try:
        contents = {
            "contracts.json": json_bytes([payload(c) for c in contracts]),
            "quotes.csv": quote_bytes(quotes),
        }
        for name, data in contents.items():
            (stage / name).write_bytes(data)
        manifest = {
            "schema": SCHEMA,
            "status": "complete",
            "provider": provider,
            "evidence_kind": evidence_kind,
            "rights_note": rights_note,
            "limits": limits,
            "source": source or {},
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "symbols": [c.instrument_id for c in contracts],
            "start": min(q.session for q in quotes),
            "end": max(q.session for q in quotes),
            "files": {name: digest(data) for name, data in contents.items()},
        }
        (stage / "manifest.json").write_bytes(json_bytes(manifest))
        load_bundle(stage)
        os.rename(stage, output)
    finally:
        if stage.exists() and stage.resolve().parent == output.parent:
            shutil.rmtree(stage)
    return load_bundle(output)
