"""Turn evidenced corporate-action rows into execution ActionTerms.

CNInfo distributions already carry cash, bonus shares and transfers. Rights,
mergers and spin-offs are not inferred from that feed: they enter only through
an explicit record that satisfies ActionTerms.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

import pandas as pd

from quant_data_kit.financial.actions import KINDS, ActionTerms

_EXPLICIT_KINDS = {"merger", "spin_off", "rights_distribution", "rights_exercise"}


def _utc(value, field: str) -> str:
    stamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(stamp):
        raise ValueError(f"corporate action is missing {field}")
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC")
    else:
        stamp = stamp.tz_localize("UTC")
    return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _date(value, field: str) -> str:
    stamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(stamp):
        raise ValueError(f"corporate action is missing {field}")
    return pd.Timestamp(stamp).strftime("%Y-%m-%d")


def distribution_terms(row, *, instrument_id: str, currency: str = "CNY") -> list[ActionTerms]:
    """Split one normalized CNInfo distribution into dividend and split terms."""
    cash = Decimal(str(row.cash_per_share))
    ratio = Decimal(str(row.share_ratio))
    if not cash.is_finite() or not ratio.is_finite() or cash < 0 or ratio <= 0:
        raise ValueError("distribution amounts must be non-negative with a positive share ratio")
    if cash == 0 and ratio == 1:
        raise ValueError("distribution has neither cash nor a share change")
    event_id = str(row.event_id)
    source = str(row.source)
    record_date = _date(row.record_date, "record_date")
    ex_at = _utc(row.ex_date, "ex_date")
    # A historical date in a feed is not evidence that we had the row then.
    available_at = _utc(row.captured_at, "captured_at")
    terms: list[ActionTerms] = []
    if cash > 0:
        pay_at = _utc(row.pay_date, "pay_date")
        common = {
            "instrument_id": instrument_id,
            "currency": currency,
            "source": source,
            "cash_per_unit": str(cash),
            "entitlement_date": record_date,
        }
        terms.append(
            ActionTerms(
                event_id=event_id + ":entitlement",
                kind="dividend_entitlement",
                effective_at=ex_at,
                available_at=available_at,
                evidence_id=event_id,
                **common,
            )
        )
        terms.append(
            ActionTerms(
                event_id=event_id + ":payment",
                kind="dividend_payment",
                effective_at=pay_at,
                available_at=available_at,
                evidence_id=event_id,
                **common,
            )
        )
    if ratio != 1:
        available = _date(row.shares_available_date, "shares_available_date")
        ex_date = _date(row.ex_date, "ex_date")
        if available != ex_date:
            raise ValueError("deferred share delivery is not bridged without a receivables ledger")
        terms.append(
            ActionTerms(
                event_id=event_id + ":split",
                instrument_id=instrument_id,
                kind="split",
                effective_at=ex_at,
                available_at=available_at,
                currency=currency,
                source=source,
                evidence_id=event_id,
                ratio=str(ratio),
            )
        )
    return terms


def explicit_terms(fields: Mapping) -> ActionTerms:
    """Accept rights, merger and spin-off terms only when every field is supplied."""
    if not isinstance(fields, Mapping):
        raise TypeError("explicit corporate action must be a mapping")
    kind = fields.get("kind")
    if kind not in _EXPLICIT_KINDS:
        raise ValueError(
            "rights, merger and spin-off require an explicit evidenced record; "
            f"supported kinds: {sorted(_EXPLICIT_KINDS)}"
        )
    if kind not in KINDS:
        raise ValueError(f"unsupported action kind: {kind}")
    return ActionTerms(**dict(fields))
