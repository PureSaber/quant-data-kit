"""CNInfo dividends: keep announcement, record, ex and payment dates distinct.

AKShare documents all three distribution ratios as per TEN shares:
https://akshare.akfamily.xyz/data/stock/stock.html#id249
Missing ratios are inferred as zero only when the source description omits that action.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal

import pandas as pd

from quant_data_kit.providers._fetch import fetch_with_retries
from quant_data_kit.providers._network import configure_network
from quant_data_kit.providers._symbols import normalize_symbol


def normalize_actions(frame: pd.DataFrame, symbol: str, captured_at: str) -> pd.DataFrame:
    columns = [
        "event_id",
        "symbol",
        "announced_date",
        "record_date",
        "ex_date",
        "pay_date",
        "shares_available_date",
        "cash_per_share",
        "share_ratio",
        "source",
        "captured_at",
        "source_record",
    ]
    required = {
        "实施方案公告日期",
        "股权登记日",
        "除权日",
        "派息日",
        "股份到账日",
        "送股比例",
        "转增比例",
        "派息比例",
        "实施方案分红说明",
    }
    if not required.issubset(frame.columns):
        raise ValueError("CNInfo corporate-action schema changed or response is incomplete")
    rows = []
    for record in frame.to_dict("records"):
        description = str(record["实施方案分红说明"])

        def amount(key, token, record=record, description=description):
            value = record[key]
            if pd.isna(value):
                if token in description:
                    raise ValueError(f"Missing {key} in a described corporate action")
                return Decimal(0)
            value = Decimal(str(value))
            if not value.is_finite() or value < 0:
                raise ValueError(f"Invalid {key}")
            return value / 10

        cash = amount("派息比例", "派")
        ratio = 1 + amount("送股比例", "送") + amount("转增比例", "转")
        if cash == 0 and ratio == 1:
            continue
        raw = json.dumps(
            {k: None if pd.isna(v) else str(v) for k, v in record.items()},
            ensure_ascii=False,
            sort_keys=True,
        )
        row = {
            "event_id": "cninfo:" + hashlib.sha256((symbol + raw).encode()).hexdigest()[:24],
            "symbol": normalize_symbol(symbol),
            "cash_per_share": str(cash),
            "share_ratio": str(ratio),
            "source": "akshare.cninfo",
            "captured_at": captured_at,
            "source_record": raw,
        }
        for src, dst in [
            ("实施方案公告日期", "announced_date"),
            ("股权登记日", "record_date"),
            ("除权日", "ex_date"),
            ("派息日", "pay_date"),
            ("股份到账日", "shares_available_date"),
        ]:
            row[dst] = pd.to_datetime(record[src], errors="coerce")
        rows.append(row)
    result = pd.DataFrame(rows, columns=columns)
    if result.duplicated("event_id").any():
        raise ValueError("Duplicate corporate-action source record")
    merged = []
    for _, group in result.groupby(["symbol", "ex_date"], dropna=False, sort=False):
        if len(group) == 1:
            merged.append(group.iloc[0].to_dict())
            continue
        sources = [json.loads(value) for value in sorted(group.source_record)]
        kinds = {record.get("分红类型") for record in sources}
        if (
            len(kinds) != len(group)
            or not kinds.issubset({"年度分红", "中期分红", "特别分红"})
            or not group.share_ratio.eq("1").all()
            or any(
                group[key].nunique(dropna=False) != 1
                for key in ("announced_date", "record_date", "pay_date")
            )
        ):
            raise ValueError("Duplicate/conflicting actions on one ex-date")
        # Separate ordinary and special cash dividends with identical entitlement dates.
        combined = group.iloc[0].to_dict()
        combined["cash_per_share"] = str(
            sum((Decimal(value) for value in group.cash_per_share), Decimal(0))
        )
        combined["source_record"] = json.dumps(sources, ensure_ascii=False, sort_keys=True)
        combined["event_id"] = (
            "cninfo:"
            + hashlib.sha256((symbol + combined["source_record"]).encode()).hexdigest()[:24]
        )
        merged.append(combined)
    result = pd.DataFrame(merged, columns=columns)
    return result


def fetch_corporate_actions(symbols: list[str], captured_at: str) -> pd.DataFrame:
    import akshare as ak

    configure_network()
    frames = [
        normalize_actions(
            fetch_with_retries(
                lambda symbol=symbol: ak.stock_dividend_cninfo(symbol=normalize_symbol(symbol)),
                max_retries=3,
                sleep_seconds=0.5,
                error_message=f"CNInfo dividend feed unavailable for {symbol}",
            ),
            symbol,
            captured_at,
        )
        for symbol in symbols
    ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
