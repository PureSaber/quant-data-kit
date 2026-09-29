"""Content-addressed historical halt calendar.

Sessions that are not in the supplied observations fail closed. A current
Eastmoney ST/halt snapshot cannot be stored as this calendar.
"""

from __future__ import annotations

import hashlib
import json

import pandas as pd

_STATUSES = {"tradable", "suspended", "risk_warning", "delisted"}
_NOT_TRADABLE = {"suspended", "delisted"}
_SCHEMA = "quant.halt-calendar/v1"


def freeze_halt_calendar(observations: pd.DataFrame, *, source: str) -> dict:
    if not isinstance(source, str) or not source.strip():
        raise ValueError("halt calendar source is required")
    source = source.strip()
    if source.startswith("akshare.eastmoney.current"):
        raise ValueError("current ST/halt snapshot cannot certify a historical calendar")
    required = {"symbol", "session", "status"}
    if not required.issubset(observations.columns):
        raise ValueError("halt calendar requires symbol, session and status")
    rows = []
    seen: set[tuple[str, str]] = set()
    for record in observations.to_dict("records"):
        if pd.isna(record["symbol"]):
            raise ValueError("halt observation has an empty symbol, date or status")
        symbol = str(record["symbol"]).strip()
        session = pd.to_datetime(record["session"], errors="coerce")
        status = str(record["status"])
        if not symbol or pd.isna(session) or status not in _STATUSES:
            raise ValueError("halt observation has an empty symbol, date or status")
        day = pd.Timestamp(session).strftime("%Y-%m-%d")
        key = (symbol, day)
        if key in seen:
            raise ValueError(f"duplicate halt observation for {symbol} on {day}")
        seen.add(key)
        rows.append({"symbol": symbol, "session": day, "status": status})
    rows.sort(key=lambda item: (item["symbol"], item["session"]))
    payload = {"schema": _SCHEMA, "source": source, "observations": rows}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
    return payload


def lookup(calendar: dict, symbol: str, session) -> str:
    if calendar.get("schema") != _SCHEMA or "sha256" not in calendar:
        raise ValueError("halt calendar schema is not recognized")
    payload = {key: value for key, value in calendar.items() if key != "sha256"}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if hashlib.sha256(encoded.encode()).hexdigest() != calendar["sha256"]:
        raise ValueError("halt calendar SHA-256 mismatch")
    day = pd.Timestamp(session).strftime("%Y-%m-%d")
    for row in calendar.get("observations", []):
        if row["symbol"] == symbol and row["session"] == day:
            if row["status"] not in _STATUSES:
                raise ValueError("unsupported halt status")
            return row["status"]
    raise ValueError(f"session is not covered by the halt calendar: {symbol} {day}")


def is_tradable(calendar: dict, symbol: str, session) -> bool:
    status = lookup(calendar, symbol, session)
    if status in _NOT_TRADABLE:
        return False
    if status in {"tradable", "risk_warning"}:
        return True
    raise ValueError(f"unsupported halt status: {status}")
