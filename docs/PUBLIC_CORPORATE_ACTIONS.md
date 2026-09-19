# Public daily corporate actions and tradability

`providers.corporate_actions.fetch_corporate_actions` captures CNInfo distributions through the existing AKShare dependency. Source records are retained. Announcement, record, ex, payment and share-delivery dates remain separate. All three source ratios are per ten shares, normalized to per-share cash and a share multiplier. Unknown dates remain unknown. Missing amounts are zero only when the plan text does not describe that component.

Ordinary and special CASH dividends on the same ex-date are added only when their announcement, record and payment dates agree and their source types differ. Exact duplicates, conflicting records, and combined ambiguous share distributions fail. Revisions produce new event identities; a running account must reconcile changes rather than rewrite its past.

`providers.tradability.fetch_tradability` captures the current risk-warning board and the requested date's suspension records. `no_reported_restriction` means only that these two responses report no restriction. It does not certify historical listing status, delistings, the next session or price-limit execution. Missing schemas fail rather than turning into an empty restriction list.

Primary API field references: [AKShare stock documentation](https://akshare.akfamily.xyz/data/stock/stock.html), sections `stock_dividend_cninfo`, `stock_zh_a_st_em`, and `stock_tfp_em`.

2026-09-19 integration observation: CNInfo returned 109 normalized distribution records across the four example symbols. Eastmoney's current ST endpoint disconnected; no replacement status was fabricated. Tencent's adjusted series and CNInfo cash amounts disagree for the 000333 June 2026 distribution (price adjustment 3.72 versus cash 3.80 per share). Consumers must keep this discrepancy visible; the daily simulation refuses an unexplained adjustment window.
