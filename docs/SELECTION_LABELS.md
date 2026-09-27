# Selection-preserving labels (17, 19)

`quant_data_kit.financial.labels.forward_labels(levels, selections, sessions, horizon=...,
as_of=..., terminals=None, lower_return=None, upper_return=None)` keeps every selected sample.

* `levels`: date, instrument_id, positive economic return-index value, aware available_at.
* `selections`: unique sample_id, date, instrument_id; optional missing_reason is
  suspended, delisted_unknown_terminal or missing_price.
* `sessions`: explicit ordered unique exchange-session dates (not per-security row numbers).
* `terminals`: unique selected sample_id, effective date, holding-period realized_return,
  aware available_at and nonempty source. Return −1 requires evidence; it is not imputation.

Labels are observed, terminal, immature, suspended, delisted_unknown_terminal or missing_price.
Ordinary horizon labels require all intermediate economic levels, avoiding a shortened horizon
across a gap. Only evidence available at the cutoff is usable. Terminal proceeds remain cash
at zero interest to the label endpoint; the adapter must convert distributions/splits/share
basis consistently before supplying terminal holding-period returns. Unknown terminal values
are never defaulted to zero or last price. Effective dates and selected identities are strict.

Explicit lower/upper returns are **scenario assumptions**, not statistical confidence bounds.
`label_coverage` keeps the full denominator and refuses a whole-selection scenario mean while
any row lacks a bound (including immature outcomes). Report counts beside observed-only IC.

Tests include evidenced zero, late terminal evidence, missing interior prices, unsupported
bounds and independent hand/share and amount-unit conversions. Input data economics are still
the caller's responsibility; this API does not manufacture exchange calendars or terminal cash.
