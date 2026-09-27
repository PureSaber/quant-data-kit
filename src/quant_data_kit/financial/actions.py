"""Explicit corporate-action instructions; no inferred elections or adjustment mixes."""

from dataclasses import asdict, dataclass
from decimal import Decimal

from .common import day, number, utc
from .reconciliation import canonical

KINDS = {
    "split",
    "dividend_entitlement",
    "dividend_payment",
    "cash_in_lieu",
    "merger",
    "spin_off",
    "rights_distribution",
    "rights_exercise",
    "terminal_cash",
}


@dataclass(frozen=True)
class ActionTerms:
    event_id: str
    instrument_id: str
    kind: str
    effective_at: str
    available_at: str
    currency: str
    source: str
    evidence_id: str
    ratio: str = "1"
    cash_per_unit: str = "0"
    target_id: str | None = None
    cost_fraction: str | None = None
    election_quantity: str | None = None
    retired_quantity: str | None = None
    target_mark: str | None = None
    parent_mark: str | None = None
    entitlement_date: str | None = None
    price_basis: str = "raw"

    def __post_init__(self):
        if self.kind not in KINDS or self.price_basis != "raw":
            raise ValueError("explicit raw-price action with a supported kind required")
        if not all(
            isinstance(x, str) and x.strip()
            for x in (
                self.event_id,
                self.instrument_id,
                self.currency,
                self.source,
                self.evidence_id,
            )
        ):
            raise ValueError("corporate action identity, currency and source evidence required")
        if len(self.currency) != 3 or not self.currency.isupper():
            raise ValueError("explicit action currency required")
        utc(self.effective_at)
        utc(self.available_at)
        if self.cost_fraction is None:
            if self.kind in {"merger", "spin_off", "rights_distribution"}:
                raise ValueError("conversion/distribution requires explicit cost allocation")
            object.__setattr__(self, "cost_fraction", "0")
        for name in ("ratio", "cash_per_unit", "cost_fraction"):
            number(getattr(self, name), nonnegative=True)
        if number(self.cost_fraction) > 1:
            raise ValueError("cost allocation fraction must be in [0,1]")
        if self.kind in {"merger", "spin_off", "rights_distribution", "rights_exercise"}:
            if not self.target_id or self.target_id == self.instrument_id:
                raise ValueError("conversion requires a different target instrument")
            if self.target_mark is None or number(self.target_mark) <= 0 or number(self.ratio) <= 0:
                raise ValueError("conversion requires positive ratio and evidenced target mark")
        if self.kind in {"spin_off", "rights_distribution"} and (
            self.parent_mark is None or number(self.parent_mark) <= 0
        ):
            raise ValueError("distribution requires an explicit ex-event parent mark")
        if self.kind == "rights_exercise" and (
            self.election_quantity is None or number(self.election_quantity) <= 0
        ):
            raise ValueError("rights exercise requires an explicit positive election")
        if self.kind.startswith("dividend_"):
            if self.entitlement_date is None:
                raise ValueError("dividend phase requires the original entitlement date")
            if day(self.entitlement_date).date() > utc(self.effective_at).date():
                raise ValueError("dividend cannot precede entitlement date")
        if self.kind == "split" and number(self.ratio) <= Decimal(0):
            raise ValueError("split ratio must be positive")
        if self.kind == "cash_in_lieu" and (
            self.retired_quantity is None or number(self.retired_quantity) <= 0
        ):
            raise ValueError("cash-in-lieu requires explicit retired share quantity")

    def fingerprint(self):
        return canonical(asdict(self))
