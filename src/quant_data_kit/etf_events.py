"""Issuer-evidenced ETF unit splits and deterministic cash/share price adjustment."""

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pandas as pd

SPLIT_COLUMNS = ["credit_at", "known_at", "fractional_rule", "evidence_sha256", "evidence_source"]


def normalize_etf_splits(declarations, *, evidence_root, captured_at):
    from quant_data_kit.providers.corporate_actions import _ETF_ACTION_COLUMNS

    rows = []
    required = {
        "event_id",
        "symbol",
        "announced_date",
        "record_date",
        "ex_date",
        "credit_at",
        "known_at",
        "sellable_date",
        "ratio",
        "fractional_rule",
        "source",
        "file",
        "sha256",
    }
    root = Path(evidence_root).resolve()
    for item in declarations:
        if set(item) != required:
            raise ValueError("split requires a complete issuer evidence declaration")
        path = (root / item["file"]).resolve()
        if (
            root not in path.parents
            or hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]
        ):
            raise ValueError("split issuer attachment identity mismatch")
        ratio = Decimal(str(item["ratio"]))
        if not ratio.is_finite() or ratio <= 1 or ratio.as_tuple().exponent < -8:
            raise ValueError(
                "supported split requires exact expansion ratio; consolidations remain blocked"
            )
        if item["fractional_rule"] not in {"floor", "ceil", "exact_pro_rata"} or not item["source"]:
            raise ValueError("split requires an explicit deterministic fractional rule and source")
        known, credit, captured = map(
            pd.Timestamp, (item["known_at"], item["credit_at"], captured_at)
        )
        if (
            any(t.tzinfo is None for t in (known, credit, captured))
            or not known <= credit <= captured
        ):
            raise ValueError("split requires ordered aware knowledge, credit and capture times")
        record, ex, sellable = map(
            pd.Timestamp, (item["record_date"], item["ex_date"], item["sellable_date"])
        )
        announced = pd.Timestamp(item["announced_date"])
        if not announced < ex or known.tz_convert("Asia/Shanghai").date() < announced.date():
            raise ValueError("split knowledge cannot predate publication")
        if (
            record.date() != credit.tz_convert("Asia/Shanghai").date()
            or credit.tz_convert("Asia/Shanghai").hour < 15
        ):
            raise ValueError("daily ETF split requires record-date end-of-day credit")
        if not record < ex or sellable != ex:
            raise ValueError("split requires the first post-split trading/sellable date")
        rows.append(
            {
                "event_id": item["event_id"],
                "symbol": item["symbol"],
                "announced_date": announced,
                "record_date": record,
                "ex_date": ex,
                "pay_date": ex,
                "shares_available_date": sellable,
                "cash_per_share": "0",
                "share_ratio": str(ratio),
                "source": item["source"],
                "captured_at": captured_at,
                "source_record": json.dumps(item, sort_keys=True),
                "credit_at": item["credit_at"],
                "known_at": item["known_at"],
                "fractional_rule": item["fractional_rule"],
                "evidence_sha256": item["sha256"],
                "evidence_source": item["source"],
            }
        )
    frame = pd.DataFrame(rows, columns=[*_ETF_ACTION_COLUMNS, *SPLIT_COLUMNS])
    if frame.duplicated(["symbol", "ex_date"]).any() or frame.event_id.duplicated().any():
        raise ValueError("duplicate issuer split")
    return frame


def adjust_etf_prices(raw, actions):
    """Adjust OHLC from raw closes; account rounding never changes the market series."""
    if not raw["adjustment"].eq("none").all() or raw.duplicated(["symbol", "date"]).any():
        raise ValueError("adjustment requires unique unadjusted bars")
    if actions.duplicated(["symbol", "ex_date"]).any():
        raise ValueError("duplicate cash/share event")
    adjusted = raw.copy()
    for row in actions.sort_values(["symbol", "ex_date"]).itertuples():
        cash, ratio = Decimal(str(row.cash_per_share)), Decimal(str(row.share_ratio))
        previous = raw[(raw.symbol == row.symbol) & (raw.date < row.ex_date)].sort_values("date")
        if previous.empty:
            raise ValueError("corporate-action adjustment lacks the preceding raw close")
        close = Decimal(str(previous.iloc[-1].close))
        if not cash.is_finite() or not ratio.is_finite() or not 0 <= cash < close or ratio <= 0:
            raise ValueError("invalid cash/share adjustment")
        factor = float((close - cash) / (close * ratio))
        selected = adjusted.symbol.eq(row.symbol) & (adjusted.date < row.ex_date)
        adjusted.loc[selected, ["open", "high", "low", "close"]] *= factor
    adjusted["adjustment"] = "qfq"
    adjusted["source"] = adjusted["source"].astype(str) + "+evidenced-cash-and-unit-events"
    return adjusted
