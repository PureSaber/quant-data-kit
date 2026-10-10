"""Bounded public-file/API ingestion; never bypass a source access rejection."""

import csv
import io
import json
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .bundle import digest
from .models import decimal

MAX_BYTES = 32 * 1024 * 1024
PROVIDERS = {"cboe-vix", "nse-fo", "deribit"}


class PublicSourceError(ValueError):
    def __init__(self, status):
        self.status = status
        super().__init__(f"public source HTTP {status}; use official files if access is restricted")


def public_request(url):
    try:
        req = Request(url, headers={"User-Agent": "QuantResearch/1.0 (read-only research)"})
        with urlopen(req, timeout=30) as response:
            raw = response.read(MAX_BYTES + 1)
    except HTTPError as exc:
        raise PublicSourceError(exc.code) from None
    except (URLError, OSError, TimeoutError):
        raise ValueError(
            "public data source unavailable; no automatic retry or partial publication"
        ) from None
    if len(raw) > MAX_BYTES:
        raise ValueError("public response exceeds 32 MiB; narrow the request")
    return raw


def selections(value):
    values = value.split(",") if isinstance(value, str) else list(value)
    values = [v.strip() for v in values]
    if not 1 <= len(values) <= 20 or len(set(values)) != len(values):
        raise ValueError("select 1-20 distinct contracts, separated by commas")
    if any(not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", v) for v in values):
        raise ValueError("invalid contract selection")
    return values


def validate_request(provider, kind, selection, start, end, source_mode="daily"):
    if provider not in PROVIDERS or kind not in {"future", "option"}:
        raise ValueError("unsupported free source or instrument kind")
    values = selections(selection)
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if not 0 <= (last - first).days <= 31 or last > datetime.now(timezone.utc).date():
        raise ValueError("select 1-32 inclusive completed/current UTC dates")
    if provider == "cboe-vix":
        if kind != "future" or source_mode != "daily":
            raise ValueError("Cboe adapter supports VX futures daily contract files")
        for value in values:
            expiry = date.fromisoformat(value)
            if last >= expiry:
                raise ValueError(
                    "Cboe sample must end before the selected expiry; final fixing is separate"
                )
    elif provider == "nse-fo" and source_mode != "daily":
        raise ValueError("NSE adapter supports daily UDiFF files")
    elif provider == "deribit":
        if kind != "option" or source_mode not in {"trades", "snapshot"}:
            raise ValueError("Deribit supports BTC/ETH option trades or analytical snapshots")
        if len(values) > 10:
            raise ValueError("Deribit collection is bounded to ten explicit option contracts")
        if any(
            not re.fullmatch(r"(?:BTC|ETH)-\d{1,2}[A-Z]{3}\d{2}-\d+(?:d\d+)?-[CP]", v)
            for v in values
        ):
            raise ValueError("select explicit BTC/ETH coin-quoted vanilla option symbols")
        if len({v.split("-")[0] for v in values}) != 1:
            raise ValueError("one underlying currency per Deribit request")
        if source_mode == "snapshot" and (
            first != last or last != datetime.now(timezone.utc).date()
        ):
            raise ValueError("a current snapshot requires today's UTC date; it cannot be backdated")
    return values, first, last


def csv_rows(raw, required):
    if len(raw) > MAX_BYTES:
        raise ValueError("CSV exceeds 32 MiB")
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
    if (
        not reader.fieldnames
        or len(set(reader.fieldnames)) != len(reader.fieldnames)
        or not set(required) <= set(reader.fieldnames)
    ):
        raise ValueError("public CSV schema changed or required fields are absent")
    rows = list(reader)
    if any(None in row for row in rows):
        raise ValueError("malformed public CSV record")
    return rows


def daily_time(day, zone, hour, minute=0):
    at = datetime.combine(day, time(hour, minute), ZoneInfo(zone)).astimezone(timezone.utc)
    # Explicit conservative research assumption, not historical publication evidence.
    available = datetime.combine(day + timedelta(days=1), time(9), ZoneInfo(zone)).astimezone(
        timezone.utc
    )
    return at, available


def margin_values(initial=None, maintenance=None):
    if initial is None and maintenance is None:
        return None, None
    if initial is None or maintenance is None:
        raise ValueError("declare both research margin amounts or neither")
    initial, maintenance = decimal(initial), decimal(maintenance)
    if not 0 <= maintenance <= initial or initial <= 0:
        raise ValueError("margin requires 0 <= maintenance <= positive initial")
    return initial, maintenance


class Resources:
    def __init__(self, *, fetch=public_request, directory=None):
        self.fetch = fetch
        self.directory = Path(directory).resolve() if directory else None
        self.receipts = []

    def read(self, filename, url):
        if self.directory:
            path = (self.directory / filename).resolve()
            if (
                path.parent != self.directory
                or not path.is_file()
                or path.stat().st_size > MAX_BYTES
            ):
                raise ValueError(f"missing, escaped or oversized official source file: {filename}")
            raw = path.read_bytes()
        else:
            raw = self.fetch(url)
        if len(raw) > MAX_BYTES:
            raise ValueError("public source response exceeds 32 MiB")
        self.receipts.append(
            {
                "file": filename,
                "url": url,
                "bytes": len(raw),
                "sha256": digest(raw),
                "transport": "local-file" if self.directory else "https",
            }
        )
        return raw

    def rpc(self, filename, url):
        data = json.loads(self.read(filename, url))
        if not isinstance(data, dict) or "error" in data or "result" not in data:
            raise ValueError("public API returned a JSON-RPC error or changed schema")
        return data["result"]
