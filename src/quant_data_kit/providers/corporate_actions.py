"""CNInfo dividends: keep announcement, record, ex and payment dates distinct.

AKShare documents all three distribution ratios as per TEN shares:
https://akshare.akfamily.xyz/data/stock/stock.html#id249
Missing ratios are inferred as zero only when the source description omits that action.
"""

from __future__ import annotations

import hashlib
import json
import re
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


_ETF_ACTION_COLUMNS = [
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


def normalize_etf_actions(
    dividends: pd.DataFrame,
    announcements: pd.DataFrame,
    symbol: str,
    captured_at: str,
) -> pd.DataFrame:
    """Join ETF entitlement dates to their real dividend announcements.

    Eastmoney exposes cash, record, ex and payment dates on the fund F10 page,
    while its announcement endpoint supplies the publication date and source
    document id. A row is admitted only when both source records can be joined;
    the missing announcement is never inferred from an entitlement date.
    """
    if dividends.empty:
        return pd.DataFrame(columns=_ETF_ACTION_COLUMNS)
    required_dividends = {"权益登记日", "除息日", "每10份分红", "分红发放日"}
    announcement_id_column = (
        "报告ID" if "报告ID" in announcements else "公告ID" if "公告ID" in announcements else None
    )
    required_announcements = {"公告日期", "公告标题"}
    if (
        not required_dividends.issubset(dividends)
        or not required_announcements.issubset(announcements)
        or announcement_id_column is None
    ):
        raise ValueError("Eastmoney ETF dividend/announcement schema changed")

    published = announcements.copy()
    published["公告日期"] = pd.to_datetime(published["公告日期"], errors="coerce").dt.normalize()
    if published[["公告日期", "公告标题", announcement_id_column]].isna().any().any():
        raise ValueError("ETF dividend announcement contains unknown source fields")
    published = published.sort_values(["公告日期", announcement_id_column]).reset_index(drop=True)
    used_announcements: set[str] = set()
    rows = []
    for source in dividends.to_dict("records"):
        record_date = pd.to_datetime(source["权益登记日"], errors="coerce").normalize()
        ex_date = pd.to_datetime(source["除息日"], errors="coerce").normalize()
        pay_date = pd.to_datetime(source["分红发放日"], errors="coerce").normalize()
        if any(pd.isna(value) for value in (record_date, ex_date, pay_date)):
            raise ValueError("ETF dividend contains an unknown entitlement/payment date")
        description = str(source["每10份分红"])
        matched_amount = re.search(r"每\s*10\s*份.*?([0-9]+(?:\.[0-9]+)?)\s*元", description)
        if matched_amount is None:
            raise ValueError(f"Unsupported ETF dividend description: {description}")
        cash = Decimal(matched_amount.group(1)) / Decimal(10)
        if not cash.is_finite() or cash <= 0:
            raise ValueError("ETF cash dividend must be positive and finite")

        candidates = published[
            (published["公告日期"] < ex_date)
            & ~published[announcement_id_column].astype(str).isin(used_announcements)
        ]
        if candidates.empty:
            raise ValueError(
                f"ETF dividend {normalize_symbol(symbol)} {ex_date.date()} lacks a prior announcement"
            )
        announcement = candidates.iloc[-1]
        announcement_id = str(announcement[announcement_id_column])
        used_announcements.add(announcement_id)
        source_payload = {
            "dividend": {
                key: None if pd.isna(value) else str(value) for key, value in source.items()
            },
            "announcement": {
                "date": announcement["公告日期"].date().isoformat(),
                "title": str(announcement["公告标题"]),
                "id": announcement_id,
            },
        }
        source_record = json.dumps(source_payload, ensure_ascii=False, sort_keys=True)
        code = normalize_symbol(symbol)
        rows.append(
            {
                "event_id": "eastmoney-etf:"
                + hashlib.sha256((code + source_record).encode()).hexdigest()[:24],
                "symbol": code,
                "announced_date": announcement["公告日期"],
                "record_date": record_date,
                "ex_date": ex_date,
                "pay_date": pay_date,
                "shares_available_date": pd.NaT,
                "cash_per_share": str(cash.normalize()),
                "share_ratio": "1",
                "source": "akshare:eastmoney:fund-f10",
                "captured_at": captured_at,
                "source_record": source_record,
            }
        )
    result = pd.DataFrame(rows, columns=_ETF_ACTION_COLUMNS)
    if result.duplicated(["symbol", "ex_date"]).any() or result.duplicated("event_id").any():
        raise ValueError("Duplicate/conflicting ETF corporate actions")
    return result.sort_values(["symbol", "ex_date"]).reset_index(drop=True)


def fetch_etf_corporate_actions(symbols: list[str], captured_at: str) -> pd.DataFrame:
    """Fetch evidenced ETF cash dividends from two Eastmoney fund endpoints."""
    import akshare as ak

    configure_network()
    frames = []
    for symbol in symbols:
        code = normalize_symbol(symbol)
        dividends = fetch_with_retries(
            lambda code=code: ak.fund_open_fund_info_em(symbol=code, indicator="分红送配详情"),
            max_retries=3,
            sleep_seconds=0.5,
            error_message=f"Eastmoney ETF dividend feed unavailable for {code}",
        )
        if dividends.empty:
            frames.append(pd.DataFrame(columns=_ETF_ACTION_COLUMNS))
            continue
        announcements = fetch_with_retries(
            lambda code=code: ak.fund_announcement_dividend_em(symbol=code),
            max_retries=3,
            sleep_seconds=0.5,
            error_message=f"Eastmoney ETF announcement feed unavailable for {code}",
        )
        frames.append(normalize_etf_actions(dividends, announcements, code, captured_at))
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=_ETF_ACTION_COLUMNS)
    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["symbol", "ex_date"])
        .reset_index(drop=True)
    )
