"""Append-only, revision-aware corporate-event research store (SQLite).

Manual/source-reviewed imports only. A source reference and timestamp are input
claims, not independent historical-PIT certification. No network or order API.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import quote

SCHEMA = "qdk.corporate-events/1"
FIELDS = {
    "event_id",
    "revision",
    "event_type",
    "target",
    "acquirer",
    "currency",
    "announced_at",
    "available_at",
    "status",
    "cash_per_share",
    "exchange_ratio",
    "expected_close_at",
    "source_ref",
    "evidence_kind",
}


def utc(value):
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("timezone-aware timestamp required")
    return stamp.astimezone(timezone.utc)


def number(value, name, *, minimum=None):
    if isinstance(value, bool):
        raise TypeError(f"{name}: boolean is not numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"{name}: decimal required") from exc
    if not result.is_finite() or (minimum is not None and result < minimum):
        raise ValueError(f"{name}: invalid finite range")
    return result


def validate(raw):
    if not isinstance(raw, dict) or set(raw) != FIELDS:
        raise ValueError(f"corporate event requires exactly {sorted(FIELDS)}")
    row = dict(raw)
    for name in ("event_id", "target", "currency", "source_ref"):
        if not isinstance(row[name], str) or not row[name].strip():
            raise ValueError(f"nonempty {name} required")
    if len(row["currency"]) != 3 or not row["currency"].isalpha():
        raise ValueError("three-letter currency required")
    row["currency"] = row["currency"].upper()
    if type(row["revision"]) is not int or row["revision"] < 1:
        raise ValueError("positive integer revision required")
    if row["event_type"] != "merger":
        raise ValueError("v1 supports announced cash/fixed-ratio mergers only")
    if row["status"] not in {"announced", "pending", "completed", "terminated"}:
        raise ValueError("unsupported merger status")
    if row["evidence_kind"] not in {"synthetic", "retrospective"}:
        raise ValueError("manual event records cannot certify historical_pit")
    for name in ("announced_at", "available_at", "expected_close_at"):
        row[name] = utc(row[name]).isoformat()
    if utc(row["available_at"]) < utc(row["announced_at"]):
        raise ValueError("availability cannot precede announcement")
    if utc(row["expected_close_at"]) <= utc(row["announced_at"]):
        raise ValueError("expected completion must follow announcement")
    for name in ("cash_per_share", "exchange_ratio"):
        row[name] = str(number(row[name], name, minimum=0))
    if number(row["cash_per_share"], "cash") + number(row["exchange_ratio"], "ratio") <= 0:
        raise ValueError("positive consideration required")
    if not isinstance(row["acquirer"], str):
        raise TypeError("acquirer must be a string; empty only for cash deals")
    if number(row["exchange_ratio"], "ratio") > 0 and not row["acquirer"].strip():
        raise ValueError("stock consideration requires an acquirer instrument")
    if row["target"] == row["acquirer"]:
        raise ValueError("target and acquirer must differ")
    return row


def canonical(row):
    return json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _connect(path, *, readonly=False):
    path = Path(path).resolve()
    if readonly:
        # pathlib encodes Windows \\?\ as URI authority %3F, which SQLite rejects.
        # SQLite's Windows VFS handles extended filenames after URI decoding.
        name = path.as_posix()
        if name.startswith("//?/UNC/"):
            name = "//" + name[8:]
        elif name.startswith("//?/"):
            name = name[4:]
        uri = "file:" + ("//" if name.startswith("//") else "") + quote(name, safe="/:")
        db = sqlite3.connect(uri + "?mode=ro", uri=True)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(path)
        db.execute("CREATE TABLE IF NOT EXISTS metadata (schema TEXT NOT NULL)")
        if not db.execute("SELECT schema FROM metadata").fetchall():
            db.execute("INSERT INTO metadata VALUES (?)", (SCHEMA,))
        db.execute(
            "CREATE TABLE IF NOT EXISTS revisions (event_id TEXT, revision INTEGER, "
            "available_at TEXT NOT NULL, payload TEXT NOT NULL, sha256 TEXT NOT NULL, "
            "PRIMARY KEY(event_id, revision))"
        )
        db.commit()
    if db.execute("SELECT schema FROM metadata").fetchall() != [(SCHEMA,)]:
        db.close()
        raise ValueError("unsupported corporate-event database schema")
    return db


def _records(db):
    result = []
    for event_id, revision, available, payload, digest in db.execute(
        "SELECT event_id,revision,available_at,payload,sha256 FROM revisions "
        "ORDER BY event_id,revision"
    ):
        if hashlib.sha256(payload.encode()).hexdigest() != digest:
            raise ValueError("corporate-event record checksum mismatch")
        row = validate(json.loads(payload))
        if (row["event_id"], row["revision"], row["available_at"]) != (
            event_id,
            revision,
            available,
        ):
            raise ValueError("corporate-event index disagrees with payload")
        result.append(row)
    _validate_history(result)
    return result


def _validate_history(rows):
    previous = {}
    for row in sorted(rows, key=lambda r: (r["event_id"], r["revision"])):
        old = previous.get(row["event_id"])
        if old:
            if row["revision"] != old["revision"] + 1:
                raise ValueError("event revisions must be contiguous")
            if utc(row["available_at"]) <= utc(old["available_at"]):
                raise ValueError("revisions must become available strictly later")
            if any(
                row[k] != old[k]
                for k in ("target", "announced_at", "event_type", "currency", "evidence_kind")
            ):
                raise ValueError("revision changes event identity")
            if old["status"] in {"completed", "terminated"} and row["status"] != old["status"]:
                raise ValueError("terminal status cannot reopen; use a new event ID")
        elif row["revision"] != 1:
            raise ValueError("first event revision must be 1")
        previous[row["event_id"]] = row


def import_events(path, rows):
    rows = [validate(row) for row in rows]
    if not rows:
        raise ValueError("empty import")
    db = _connect(path)
    inserted = 0
    try:
        with db:
            current = {(r["event_id"], r["revision"]): r for r in _records(db)}
            for row in rows:
                key = (row["event_id"], row["revision"])
                if key in current:
                    if canonical(current[key]) != canonical(row):
                        raise ValueError("immutable revision conflict; append a new revision")
                    continue
                current[key] = row
                payload = canonical(row)
                db.execute(
                    "INSERT INTO revisions VALUES (?,?,?,?,?)",
                    (
                        *key,
                        row["available_at"],
                        payload,
                        hashlib.sha256(payload.encode()).hexdigest(),
                    ),
                )
                inserted += 1
            _validate_history(current.values())
    finally:
        db.close()
    return {"schema": SCHEMA, "inserted": inserted, "submitted": len(rows)}


def asof_events(path, at):
    at = utc(at)
    db = _connect(path, readonly=True)
    try:
        latest = {}
        for row in _records(db):
            if utc(row["available_at"]) <= at:
                latest[row["event_id"]] = row
        return [latest[key] for key in sorted(latest)]
    finally:
        db.close()
