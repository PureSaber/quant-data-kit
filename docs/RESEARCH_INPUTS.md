# Optional equity providers and frozen research inputs

`quant-data-kit` keeps provider code outside strategy repositories. The input
builder selects exactly one primary for each data domain, normalizes units, and
atomically publishes Parquet files plus a hash-bound `manifest.json`. Price
shadows are captured below `shadows/` and are diagnostic only: they never fill,
replace, or vote on primary observations.

## Provider extras

Install only the adapters needed by a deployment:

```bash
pip install 'quant-data-kit[akshare]'
pip install 'quant-data-kit[baostock]'
pip install 'quant-data-kit[tushare]'
pip install 'quant-data-kit[yahoo]'
pip install 'quant-data-kit[alpha-vantage]'
```

`TUSHARE_TOKEN` and `ALPHAVANTAGE_API_KEY` are read from the environment only.
They are never accepted as CLI arguments or written to manifests. Use
`qdk-providers` to inspect capabilities and whether required credentials are
configured.

Supported daily-price selectors are `akshare_eastmoney`, `akshare_tencent`,
`baostock`, `tushare`, `yahoo`, and `alpha_vantage`. `akshare_auto` remains for
backward compatibility, but frozen input policies reject it because it can
change source during a run.

Provider data is not semantically identical. The canonical contract converts
volume to shares, amount to CNY when available, normalizes dates and A-share
symbols, and records source units, adjustment, provider, and endpoint. BaoStock
and Tushare adjustment methods, Yahoo data rights, and Alpha Vantage entitlements
still apply; adapter availability does not grant a data license.

## Build a bundle

Copy `configs/research_inputs.yaml`, choose the primary and optional shadows,
then run:

```bash
qdk-research-inputs \
  --config configs/research_inputs.yaml \
  --output inputs/2026-09-18 \
  --symbols 000001 000333 600036 601318 \
  --start 2024-09-18 \
  --end 2026-09-18
```

The output directory must not already exist. A primary-price, benchmark,
calendar, or required-domain failure aborts publication. Optional status and
shadow failures are recorded with an error type; error text is omitted so URLs
or credentials from third-party exceptions cannot enter evidence.

The stable consumer contract is:

- `raw.parquet`: unadjusted prices for execution and valuation.
- `adjusted.parquet`: adjusted prices for research features.
- `benchmark.parquet`: HS300 returns.
- `calendar.parquet`: SSE sessions including the next session.
- `actions.parquet`: corporate actions when configured.
- `status.parquet`: current trading restrictions when available.
- `manifest.json`: policy, provider identity, hashes, capture time, warnings,
  and separate shadow comparison results.
