"""Versioned financial facts; explicit knowledge time, provenance and coverage.

These contracts validate supplied evidence, not completeness of market history.
No provider is contacted implicitly and missing facts are never guessed.
"""

from .dividends import (
    DIVIDEND_LIFECYCLE_SCHEMA_ID,
    UNSUPPORTED_DIVIDEND_FEATURES,
    CashDeduction,
    CurrencyReference,
    DividendEntitlement,
    DividendLifecycle,
    DividendPayment,
    DividendPaymentElection,
    DividendProposal,
    EvidenceTiming,
    IssuerFxConversion,
    PaymentPolicy,
    PhaseEvidence,
    PublishedAmount,
    RoundingPolicy,
)
from .fx import PIT_FX_SCHEMA_ID, FxAsOf, PitFxRate, fx_rate_asof, select_pit_fx

SCHEMA_VERSION = "puresaber.financial-foundations/1"

__all__ = [
    "DIVIDEND_LIFECYCLE_SCHEMA_ID",
    "PIT_FX_SCHEMA_ID",
    "SCHEMA_VERSION",
    "UNSUPPORTED_DIVIDEND_FEATURES",
    "CashDeduction",
    "CurrencyReference",
    "DividendEntitlement",
    "DividendLifecycle",
    "DividendPayment",
    "DividendPaymentElection",
    "DividendProposal",
    "EvidenceTiming",
    "FxAsOf",
    "IssuerFxConversion",
    "PaymentPolicy",
    "PhaseEvidence",
    "PitFxRate",
    "PublishedAmount",
    "RoundingPolicy",
    "fx_rate_asof",
    "select_pit_fx",
]
