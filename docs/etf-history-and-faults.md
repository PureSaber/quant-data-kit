# ETF history and evidence faults

`etf_history.audit_etf_history` inventories missing sessions, invalid traded bars,
cross-provider OHLC/volume/amount conflicts and historical rule coverage. It does
not fill bars or shorten a requested interval. Absolute volume/amount tolerances
are explicit shares/CNY rounding allowances. A full-session suspension declaration
needs a hash-verified original attachment and pre-open knowledge. A suspension
explains absence of a trade; it does not supply a held account's valuation.

`etf_events.normalize_etf_splits` requires original issuer bytes, exact decimal
ratio, publication/record/ex/sellable dates, aware knowledge and end-of-record-day
credit times, and an explicit fractional rule. Expansion splits support per-holder
floor, ceil or exact pro-rata. Reverse splits remain blocked. `rule_determined`
entitlements are deterministic legal rules, distinct from registry-confirmed
quantities and provisional sensitivity branches. No unsupported ceiling branch is
created for ordinary-stock distributions. `adjust_etf_prices` adjusts OHLC using
original raw preceding closes and exact cash/share terms, without modifying volume
or the source frame. Account rounding never changes market adjustment factors.

`failure_policy.assess_fault` separates NAV/weight/risk evidence, target publication,
trade permission and A/B/C credit. Hard stops cannot be downgraded by template
review or candidate-exclusion flags. `EvidenceFault` carries that classification
into native research failure receipts. Solver-reported infeasibility is distinct
from bounded search failure and from externally proved infeasibility.

`provisional_valuation` is a separate read-only analytical envelope: evidenced
suspensions, bounded stale age and externally justified positive mark ranges are
required. Every corner must satisfy the caller's convex constraint checker.
Unknown/nonconvex checks must decline evaluation. The envelope never grants trade
permission, publishes a target, writes to an exact ledger or counts toward gates.
Candidate exclusion is available only to consumers binding a preregistered policy;
the existing strict replay continues to block unresolved missing inputs.
