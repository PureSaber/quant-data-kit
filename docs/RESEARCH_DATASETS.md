# Real ETF research datasets

`quant_data_kit.research_dataset` builds a real, inspectable ETF input dataset
without treating a provider fallback or inferred dividend as evidence. Live
prices use the explicit `akshare_sina_etf` provider: raw bars come from Sina and
qfq prices are deterministically derived from Sina's separate cumulative cash
series. Cash distributions join two independent Eastmoney fund records exposed
by AKShare: the F10 entitlement table supplies record, ex and payment dates and
the dividend announcement list supplies a prior publication date and source
document ID. The independent cash sources must agree through the adjustment
equation before publication.

The Python API is available directly from the module:

```python
from quant_data_kit.research_dataset import (
    bind_instrument_master,
    build_dataset,
    inspect_dataset,
    load_research_snapshot,
    update_dataset,
)
```

## Build, update and inspect

```powershell
$env:PYTHONPATH = (Resolve-Path src)
python -m quant_data_kit.research_dataset build `
  --root ..\artifacts\etf-research `
  --symbols 510300 510500 `
  --start 2026-01-01 `
  --end 2026-09-24

python -m quant_data_kit.research_dataset update `
  --root ..\artifacts\etf-research `
  --end 2026-09-25 `
  --overlap-sessions 5

python -m quant_data_kit.research_dataset inspect `
  --root ..\artifacts\etf-research
```

The root `catalog.json` selects a current snapshot and retains every prior
snapshot ID. Actual bundles live below `snapshots/sha256-.../`; update never
edits an old directory. The refresh refetches a bounded raw-price/benchmark
overlap, records changed provider rows in
`manifest.json:update_evidence.revised_rows`, merges new sessions and publishes
a new child snapshot. Adjusted prices are refreshed over the full dataset
because a newly declared distribution legitimately rebases the whole qfq
vintage; refreshing only the tail would create a false factor break.

Each snapshot also contains `catalog.csv`, with explicit ETF venue, tick,
quantity, lot and validity fields accepted by ASM's instrument-master loader.
The `history/` directory has its own `qdk.research-history/v1` manifest,
hash-bound `source.json` and Parquet history, so
`quant_data_kit.research_coverage.load_history(snapshot / "history")` works
without adapting a loose file.

The default catalog's `available_at` is the real snapshot capture time. QDK
does not backdate instrument-master availability to the research start. A
structurally loadable catalog is not evidence that the rules were known at each
historical decision time.

## Bind a historical instrument master

`quant_data_kit.instrument_master` imports an explicit source declaration,
catalog and official documents into one immutable bundle. Import rejects a
changed source hash, an unknown evidence ID, a rule published after its claimed
effective time, a coverage gap and catalog availability before its initial
evidence. Every source needs an HTTPS identity, publication time, effective
interval and exact SHA-256.

```powershell
python -m quant_data_kit.instrument_master import `
  --source ..\artifacts\pit-sources\source `
  --output ..\artifacts\pit-sources\bundle

python -m quant_data_kit.research_dataset bind-master `
  --root ..\artifacts\etf-research `
  --instrument-master ..\artifacts\pit-sources\bundle
```

Binding publishes a child snapshot. It copies already verified market data and
actions without a provider request, replaces the current-capture catalog with
the evidence-bound catalog, and retains the complete master bundle below
`instrument_master/`. A later incremental update automatically carries that
bundle forward and rechecks its symbol set, validity interval and availability.

The bundle proves only its declared fields and intervals. A retrospective
listing document becomes usable at the document publication time; it is not
backdated to the listing day. An empty present-day suspension query is not
expanded into daily `tradable=true`, and volume does not prove all-day
tradability. If the source declares a fixed retrospective pool, missing daily
status or missing dynamic-universe history, consumers must preserve those
limits rather than label the result a complete dynamic PIT backtest.

Every live manifest records the AKShare package version, QDK version, endpoint
names, normalized request and capture time. Every Parquet file has an exact
SHA-256. `inspect` checks those hashes, recomputes the snapshot identity and
reruns coverage, dividend and adjustment validation.

## Local declared input

A local source directory may contain Parquet or CSV files named `raw`,
`adjusted`, `benchmark`, `calendar` and optionally `actions`. Price files must
already follow the canonical QDK price columns and units. Missing actions are
accepted only when the raw/adjusted relationship has no unexplained factor
change.

```powershell
python -m quant_data_kit.research_dataset build `
  --root ..\artifacts\vendor-etf `
  --symbols 510300 `
  --start 2026-01-01 `
  --end 2026-09-24 `
  --source-dir D:\exports\etf-v7 `
  --source-uri vendor://licensed/export/etf-v7 `
  --source-version 2026.09.24-r2 `
  --license-note "Internal licensed research export"
```

The three source declarations are mandatory and the original input hashes are
stored in the snapshot. QDK does not describe a local file as public provider
data or fill a missing cash distribution.

## Validation boundary

Publication requires exact raw/adjusted symbol-date keys, every SSE session for
every requested ETF, benchmark coverage, a later calendar session and valid
OHLCV in shares. Adjustment changes must match one whole-window multiplicative
or affine adjustment model using the supplied cash and share actions. A factor
change without a matching action aborts publication.

When the provider's first bar is later than the requested start, the coverage
error reports those leading sessions separately as
`pre_listing_or_provider_history_unavailable`; it does not claim a listing date
without a source. Missing sessions on or after the first available bar are
reported as `missing_after_first_available_bar`. Neither class is filled from a
current constituent pool or forward-filled prices.

Real payment dates are retained even when cash is paid after the ex-date. This
solves the data-side cause of the earlier “matching cashflow feed” failure: ETF
dividends were previously queried through a stock-only CNInfo path and the
adjustment change had no matching ETF cash record. The dataset does not rewrite
a delayed payment to the ex-date. A downstream simulator that cannot represent
dividend receivables should continue to fail until it implements that ledger.

`history.parquet` uses the snapshot capture time as `available_at`. The source
announcement only provides a calendar date, so the dataset does not invent an
intraday historical availability timestamp. It is a captured-current history,
not a historical-vintage PIT feed.
