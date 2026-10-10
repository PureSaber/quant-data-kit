"""Deterministic synthetic markets for engineering acceptance, never market evidence."""

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from .bundle import write_bundle
from .models import Contract, Quote, utc


def demo_records(kind="future"):
    start = utc("2025-01-02T21:00:00Z")
    contracts = []
    for month, expiry in [("H", "2025-01-24T21:00:00Z"), ("M", "2025-02-21T21:00:00Z")]:
        base = Contract(
            instrument_id="DEMO:" + month,
            symbol="DEMO" + month,
            product="DEMO",
            venue="SYNTHETIC",
            currency="USD",
            kind="future",
            multiplier="10",
            tick="0.01",
            listed_at=start - timedelta(days=100),
            last_trade_at=utc(expiry),
            expiry=utc(expiry),
            known_at=start - timedelta(days=100),
            timezone="America/New_York",
            settlement="cash",
            initial_margin="100",
            maintenance_margin="80",
            rules_source="Synthetic fixture assumptions; not exchange contract rules",
        )
        if kind == "future":
            contracts.append(base)
        elif kind == "option":
            for strike in (95, 100, 105):
                for right in ("call", "put"):
                    code = f"{month}-{right[0].upper()}{strike}"
                    contracts.append(
                        replace(
                            base,
                            instrument_id="DEMO:" + code,
                            symbol="DEMO" + code,
                            kind="option",
                            underlying="DEMO-SPOT",
                            option_right=right,
                            strike=Decimal(strike),
                            exercise_style="european",
                            initial_margin=Decimal(300),
                            maintenance_margin=Decimal(240),
                        )
                    )
        else:
            raise ValueError("demo kind must be future or option")
    quotes = []
    for i in range(36):
        at = start + timedelta(days=i)
        if at.weekday() >= 5:
            continue
        underlying = Decimal(100) + Decimal(i) / 10
        for c in contracts:
            if at > c.expiry:
                continue
            front = c.symbol.startswith("DEMOH")
            if kind == "future":
                close = underlying + (Decimal("0.20") if front else Decimal("1.20"))
            else:
                intrinsic = max(
                    Decimal(0), (underlying - c.strike) * (1 if c.option_right == "call" else -1)
                )
                close = intrinsic + Decimal((c.expiry - at).days) / Decimal(25)
            previous = max(Decimal(0), close - Decimal("0.10"))
            volume = Decimal(1000 - i * 30 if front else 200 + i * 35)
            quotes.append(
                Quote(
                    c.instrument_id,
                    at,
                    at + timedelta(seconds=1),
                    str(at.date()),
                    previous,
                    close + Decimal("0.20"),
                    max(Decimal(0), previous - Decimal("0.20")),
                    close,
                    max(volume, Decimal(1)),
                    Decimal(5000),
                    close,
                    max(Decimal(0), close - Decimal("0.01")),
                    close + Decimal("0.01"),
                    underlying,
                )
            )
    return contracts, quotes


def write_demo(output, kind):
    contracts, quotes = demo_records(kind)
    return write_bundle(
        output,
        contracts,
        quotes,
        provider="deterministic-demo",
        evidence_kind="synthetic",
        rights_note="Generated locally; no vendor market data",
        limits="Engineering fixture only. Prices, liquidity, calendars and margin are synthetic.",
    )
