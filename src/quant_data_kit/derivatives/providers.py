"""Bounded read-only acquisition. Contract rules are explicit user inputs.

Daily vendor bars are retrospective observations. Availability is conservatively
set to the following UTC day; this is an assumption, never historical PIT proof.
No network request is issued on import. No credentials enter bundle provenance.
"""

from __future__ import annotations

import base64
import csv
import io
import json
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from .bundle import digest, write_bundle
from .models import Quote, decimal

MAX_RESPONSE = 32 * 1024 * 1024
LIMITS = (
    "Retrospective daily bars; availability assumed at next UTC day, not PIT certified. "
    "Contract and margin rules are supplied explicitly; no exchange licensing or rule verification implied."
)


def request(url, *, headers=None, data=None):
    """No retries on metered requests; redact response bodies and request URLs."""
    try:
        with urlopen(Request(url, data=data, headers=headers or {}), timeout=90) as response:
            raw = response.read(MAX_RESPONSE + 1)
    except HTTPError as exc:
        raise ValueError(f"data provider HTTP {exc.code}; check account and request") from None
    except (URLError, TimeoutError, OSError):
        raise ValueError("data provider unavailable; no bundle was published") from None
    if len(raw) > MAX_RESPONSE:
        raise ValueError("response exceeds 32 MiB; narrow symbols or dates")
    return raw


def window(start, end):
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    if not 0 <= (end - start).days <= 31:
        raise ValueError("inclusive acquisition window must be between 1 and 32 days")
    return start, end


def symbols(contracts):
    values = [c.symbol for c in contracts]
    if not 1 <= len(values) <= 40 or len(set(values)) != len(values):
        raise ValueError("request 1–40 distinct explicit contract symbols")
    if any(not re.fullmatch(r"[A-Za-z0-9_. -]{1,64}", s) for s in values):
        raise ValueError("invalid contract symbol")
    return values


def daily_rows(raw, contracts, mapping, *, fixed_day=None, symbol_field="symbol"):
    lookup = {c.symbol: c for c in contracts}
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    if not reader.fieldnames or symbol_field not in reader.fieldnames:
        raise ValueError("provider response lacks symbol column")
    result = []
    for row in reader:
        if row.get(symbol_field) not in lookup:
            raise ValueError("provider returned an unrequested symbol")
        day = fixed_day or date.fromisoformat(row[mapping["session"]][:10])
        at = datetime.combine(day, time(23, 59, 59), timezone.utc)
        fields = {
            name: row.get(column) or None for name, column in mapping.items() if name != "session"
        }
        result.append(
            Quote(
                instrument_id=lookup[row[symbol_field]].instrument_id,
                at=at,
                available_at=at + timedelta(seconds=1),
                session=day.isoformat(),
                **fields,
            )
        )
    return result


def fetch_dataway(contracts, start, end, *, fetch=request, base_url=None):
    """Company-authorized Dataway only; returns rows and raw-source hashes."""
    start, end = window(start, end)
    base_url = base_url or os.environ.get("DATAWAY_BASE_URL")
    parsed = urlparse(base_url or "")
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.query
        or parsed.fragment
        or parsed.username
    ):
        raise ValueError("configure DATAWAY_BASE_URL with your authorized gateway endpoint")
    values = symbols(contracts)
    kinds = {c.kind for c in contracts}
    if len(kinds) != 1:
        raise ValueError("collect futures and options separately")
    option = kinds == {"option"}
    table = "b_option_marketday" if option else "b_future_globalmarketday"
    mapping = {name: f"n_{name}" for name in ("open", "high", "low", "close", "volume")}
    mapping.update(
        open_interest="n_openInterest", settlement="n_settlement" if option else "n_settlementPrice"
    )
    hashes, quotes = [], []
    day = start
    while day <= end:
        query = {
            "tableName": table,
            "begDate": day.isoformat(),
            "endDate": day.isoformat(),
            "fields": ",".join(["c_contract", *mapping.values()]),
            "c_contract": ",".join(values),
            "n_flag": 1,
        }
        raw = fetch(base_url + "?" + urlencode(query))
        hashes.append({"session": day.isoformat(), "sha256": digest(raw)})
        quotes.extend(daily_rows(raw, contracts, mapping, fixed_day=day, symbol_field="c_contract"))
        day += timedelta(days=1)
    return sorted(quotes, key=lambda q: (q.at, q.instrument_id)), {
        "table": table,
        "raw_responses": hashes,
        "date_basis": "one-day provider query",
    }


def fetch_databento(contracts, start, end, *, dataset, max_cost_usd, fetch=request, key=None):
    """Estimate first, then download exact raw-symbol OHLCV-1d request once.

    The budget is a pre-request estimate ceiling, not a provider-enforced spending
    cap. Raw symbols must be per-contract: continuous/parent symbology is excluded.
    See https://databento.com/docs/api-reference-historical?historical=http.
    """
    start, end = window(start, end)
    values = symbols(contracts)
    budget = decimal(max_cost_usd)
    if not 0 <= budget <= 100:
        raise ValueError("explicit cost ceiling between 0 and 100 USD required")
    if not re.fullmatch(r"[A-Z0-9]+\.[A-Z0-9]+", dataset):
        raise ValueError("invalid dataset")
    key = key or os.environ.get("DATABENTO_API_KEY")
    if not key:
        raise ValueError("set DATABENTO_API_KEY in the local environment")
    auth = {"Authorization": "Basic " + base64.b64encode((key + ":").encode()).decode()}
    query = {
        "dataset": dataset,
        "symbols": ",".join(values),
        "schema": "ohlcv-1d",
        "stype_in": "raw_symbol",
        "start": start.isoformat(),
        "end": (end + timedelta(days=1)).isoformat(),
    }
    root = "https://hist.databento.com/v0/"
    cost = decimal(json.loads(fetch(root + "metadata.get_cost?" + urlencode(query), headers=auth)))
    if cost < 0 or cost > budget:
        raise ValueError(
            f"provider cost estimate {cost} USD exceeds authorized ceiling {budget} USD"
        )
    query.update(encoding="csv", pretty_px="true", pretty_ts="true", map_symbols="true")
    raw = fetch(root + "timeseries.get_range", headers=auth, data=urlencode(query).encode())
    mapping = {name: name for name in ("open", "high", "low", "close", "volume")}
    mapping["session"] = "ts_event"
    quotes = daily_rows(raw, contracts, mapping)
    if any(not start.isoformat() <= q.session <= end.isoformat() for q in quotes):
        raise ValueError("provider returned observations outside requested range")
    return sorted(quotes, key=lambda q: (q.at, q.instrument_id)), {
        "dataset": dataset,
        "schema": "ohlcv-1d",
        "raw_sha256": digest(raw),
        "estimated_cost_usd": str(cost),
        "authorized_ceiling_usd": str(budget),
        "date_basis": "UTC daily bar; not exchange settlement session",
    }


def collect(output, contracts, start, end, *, provider, rights_note, **kwargs):
    if provider == "dataway":
        quotes, source = fetch_dataway(contracts, start, end, **kwargs)
    elif provider == "databento":
        quotes, source = fetch_databento(contracts, start, end, **kwargs)
    else:
        raise ValueError("unsupported derivative provider")
    return write_bundle(
        output,
        contracts,
        quotes,
        provider=provider,
        evidence_kind="retrospective",
        rights_note=rights_note,
        limits=LIMITS,
        source=source,
    )
