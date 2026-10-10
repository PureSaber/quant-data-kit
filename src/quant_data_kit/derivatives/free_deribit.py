"""Coin-quoted options projected to USD for analysis, never ledger replay."""

from collections import defaultdict
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from urllib.parse import urlencode

from .models import Contract, Quote, decimal

HISTORY = "https://history.deribit.com/api/v2/public/"
CURRENT = "https://www.deribit.com/api/v2/public/"


def timestamp(ms):
    return datetime.fromtimestamp(int(ms) / 1000, timezone.utc)


def instrument(resources, name, *, history):
    base = HISTORY if history else CURRENT
    raw = resources.rpc(
        f"instrument-{name}.json", base + "get_instrument?" + urlencode({"instrument_name": name})
    )
    coin = name.split("-")[0]
    if (
        raw.get("instrument_name") != name
        or raw.get("kind") != "option"
        or raw.get("base_currency") != coin
        or raw.get("quote_currency") != coin
        or raw.get("settlement_currency") != coin
        or raw.get("counter_currency") != "USD"
    ):
        raise ValueError("Deribit metadata is not the selected coin-quoted USD-strike option")
    if raw.get("option_type") not in {"call", "put"}:
        raise ValueError("invalid Deribit option right")
    return raw


def contract(raw, tick):
    name = raw["instrument_name"]
    expiry, listed = timestamp(raw["expiration_timestamp"]), timestamp(raw["creation_timestamp"])
    return Contract(
        "DERIBIT:" + name,
        name,
        raw["base_currency"],
        "DERIBIT",
        "USD",
        "option",
        raw["contract_size"],
        tick,
        listed,
        expiry,
        expiry,
        listed,
        "UTC",
        "cash",
        underlying=raw["base_currency"],
        option_right=raw["option_type"],
        strike=raw["strike"],
        exercise_style="european",
        rules_source="Deribit metadata; ANALYTICAL USD projection of coin-settled contract, not a USD cash contract; tick is analytical only",
    )


def historical_trades(resources, names, first, last, *, tick):
    contracts, quotes, metadata, statistics = [], [], {}, {}
    start = int(datetime.combine(first, time(), timezone.utc).timestamp() * 1000)
    end = (
        int(datetime.combine(last + timedelta(days=1), time(), timezone.utc).timestamp() * 1000) - 1
    )
    for name in names:
        info = instrument(resources, name, history=True)
        c = contract(info, tick)
        if end >= info["expiration_timestamp"]:
            raise ValueError("historical option analysis must end before expiry")
        metadata[name] = info
        contracts.append(c)
        rows, previous, finished = [], None, False
        params = {
            "instrument_name": name,
            "start_timestamp": start,
            "end_timestamp": end,
            "count": 1000,
            "sorting": "asc",
            "include_old": "true",
        }
        method = "get_last_trades_by_instrument_and_time"
        for page in range(20):
            result = resources.rpc(
                f"trades-{name}-{page:02d}.json", HISTORY + method + "?" + urlencode(params)
            )
            trades = result.get("trades")
            if not isinstance(trades, list) or not isinstance(result.get("has_more"), bool):
                raise ValueError("Deribit trade page schema changed")  # noqa: TRY004  # noqa: TRY004
            # Time-sorted responses can reorder trades sharing a millisecond.
            # Sequence order is deterministic; gaps still fail closed, including
            # an ambiguous page boundary that cuts through such a group.
            trades = sorted(trades, key=lambda t: int(t["trade_seq"]))
            for trade in trades:
                if trade.get("instrument_name") != name:
                    raise ValueError("Deribit returned an unrequested instrument")
                sequence = int(trade["trade_seq"])
                if previous is not None and sequence != previous + 1:
                    raise ValueError(
                        "Deribit trade sequence gap/overlap; incomplete input rejected"
                    )
                previous = sequence
                if int(trade["timestamp"]) < start:
                    raise ValueError("Deribit returned data preceding requested interval")
                if int(trade["timestamp"]) > end:
                    finished = True
                    break
                rows.append(trade)
            if finished or not result["has_more"]:
                finished = True
                break
            if not trades or previous is None:
                raise ValueError("Deribit pagination made no progress")
            method = "get_last_trades_by_instrument"
            params = {
                "instrument_name": name,
                "start_seq": previous + 1,
                "count": 1000,
                "sorting": "asc",
                "include_old": "true",
            }
        if not finished:
            raise ValueError("Deribit history exceeded 20-page bound; narrow the date window")
        if not rows:
            raise ValueError(f"no trades for selected option {name}; no zero bars manufactured")
        grouped = defaultdict(list)
        for row in rows:
            grouped[timestamp(row["timestamp"]).date()].append(row)
        for day, trades in sorted(grouped.items()):
            prices, amounts = [], []
            previous_time = None
            for row in trades:
                observed = timestamp(row["timestamp"])
                if previous_time is not None and observed < previous_time:
                    raise ValueError("Deribit trade timestamps moved backwards")
                previous_time = observed
                index, premium, amount = (
                    decimal(row["index_price"]),
                    decimal(row["price"]),
                    decimal(row["amount"]),
                )
                if index <= 0 or premium < 0 or amount <= 0:
                    raise ValueError("invalid Deribit trade price/index/amount")
                prices.append(premium * index)
                amounts.append(amount / c.multiplier)
            at = timestamp(trades[-1]["timestamp"])
            available = datetime.combine(day + timedelta(days=1), time(), timezone.utc)
            quotes.append(
                Quote(
                    c.instrument_id,
                    at,
                    available,
                    day.isoformat(),
                    prices[0],
                    max(prices),
                    min(prices),
                    prices[-1],
                    sum(amounts),
                    underlying_price=trades[-1]["index_price"],
                )
            )
        statistics[name] = {
            "trades": len(rows),
            "first_sequence": rows[0]["trade_seq"],
            "last_sequence": rows[-1]["trade_seq"],
            "sessions": len(grouped),
        }
    return (
        contracts,
        quotes,
        {
            "native_instruments": metadata,
            "trade_coverage": statistics,
            "price_basis": "observed trades projected to USD using each trade's index_price",
            "date_basis": "UTC daily aggregation retains actual last-trade timestamp and underlying; next UTC day availability assumed",
            "synchronization": "different legs may have asynchronous last trades; portfolio preflight rejects unsynchronized legs",
            "allowed_modes": ["analysis"],
        },
    )


def current_snapshot(resources, names, *, tick, now=None):
    started = datetime.now(timezone.utc) if now is None else now
    metadata = {name: instrument(resources, name, history=False) for name in names}
    coin = next(iter(metadata.values()))["base_currency"]
    summaries = resources.rpc(
        "option-summary.json",
        CURRENT + "get_book_summary_by_currency?" + urlencode({"currency": coin, "kind": "option"}),
    )
    index = resources.rpc(
        "index-price.json",
        CURRENT + "get_index_price?" + urlencode({"index_name": coin.lower() + "_usd"}),
    )
    fixing = decimal(index["index_price"])
    if fixing <= 0:
        raise ValueError("Deribit conversion fixing must be positive")
    finished = datetime.now(timezone.utc) if now is None else now
    if (finished - started).total_seconds() > 120:
        raise ValueError("snapshot collection exceeded the two-minute freshness bound")
    selected = [r for r in summaries if r.get("instrument_name") in names]
    if len(selected) != len(names) or len({r["instrument_name"] for r in selected}) != len(names):
        raise ValueError("Deribit option snapshot is incomplete or duplicated")
    contracts, quotes, native_rows = [], [], []
    for row in selected:
        name = row["instrument_name"]
        source_time = timestamp(row["creation_timestamp"])
        if not -5 <= (finished - source_time).total_seconds() <= 120:
            raise ValueError("stale/future Deribit summary; refresh the snapshot")
        c = contract(metadata[name], tick)
        if finished >= c.expiry:
            raise ValueError("expired instrument in current snapshot")
        premium = decimal(row["mark_price"]) * fixing
        if premium <= 0:
            raise ValueError("nonpositive Deribit mark price")
        # A calculation snapshot, not an OHLC bar or synchronized executable quote.
        quotes.append(
            Quote(
                c.instrument_id,
                finished,
                finished,
                finished.date().isoformat(),
                premium,
                premium,
                premium,
                premium,
                Decimal(0),
                open_interest=decimal(row["open_interest"]) / c.multiplier,
                underlying_price=fixing,
            )
        )
        contracts.append(c)
        native_rows.append(
            {
                k: row.get(k)
                for k in (
                    "instrument_name",
                    "creation_timestamp",
                    "mark_price",
                    "mark_iv",
                    "bid_price",
                    "ask_price",
                    "underlying_price",
                    "underlying_index",
                    "volume",
                    "open_interest",
                )
            }
        )
    return (
        contracts,
        quotes,
        {
            "native_instruments": metadata,
            "native_observations": native_rows,
            "conversion_index_price": str(fixing),
            "capture_started": started.isoformat(),
            "capture_finished": finished.isoformat(),
            "price_basis": "vendor mark price projected to USD using a shared current index fixing; not executed trades",
            "synchronization": "bounded sequential capture; common calculation time/fixing, not an atomic market snapshot",
            "allowed_modes": ["analysis"],
        },
    )
