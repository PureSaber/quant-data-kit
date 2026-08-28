# quant-data-kit

Shared data layer for PureSaber quant research repos: Parquet cache, manifest tracking, schema validation, and AKShare providers.

## Install

```bash
cd quant-data-kit
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[akshare,dev]"
```

## Features

| Module | Purpose |
|--------|---------|
| `storage` | Parquet I/O, content/schema hashes, manifest tracking, cache helpers |
| `snapshots` | Immutable content-addressed Parquet snapshots |
| `temporal` | PIT joins, availability audits, listing/delisting lifecycle filters |
| `validate` | OHLCV logic, missing, duplicate and trading-calendar coverage checks |
| `calendar` | SSE trading calendar helpers |
| `panel` | PIT merge helpers for alt-data |
| `providers.prices` | Daily OHLCV with thread pool + tx fallback |
| `providers.fundamentals` | Valuation metrics (PE/PB/market cap) |
| `providers.universe` | HS300 constituents + membership history |
| `providers.benchmark` | HS300 index returns |
| `providers.earnings_forecast` | Quarterly forecasts with effective_date (PIT) |
| `providers.northbound` | Stock Connect holdings with disclosure lag |
| `providers.industry` | Industry board returns |
| `providers.akshare` | Backward-compatible re-exports |
| `domain_v2` | Stable instrument IDs, PIT symbol mappings and versioned sessions |
| `market_clock_v2` | Explicit UTC-aware session and trading-day resolution |
| `market_events_v2` | Bars, trades, quotes, L2, funding, marks, actions and status events |
| `fixed_point` | Exact integer-unit prices, quantities, cash and fees |
| `schemas_v2` | Frozen JSON Schema and Arrow registry for v2 contracts |
| `temporal_v2` | Strict bitemporal validation and PIT joins without silent fallback |

## PIT rules

- **Earnings forecasts**: `effective_date = announce_date + 1 business day`
- **Northbound holdings**: `shift(1)` on publish date to reflect T-1 disclosure
- **Fundamentals**: causal joins use `available_at`, not report/event date
- **Historical universe**: missing history raises by default; current-universe fallback is explicit only

## CLI

```bash
qdk-validate data/prices.parquet
qdk-manifest data/prices.manifest.json
qdk-catalog list
qdk-catalog register hs300_prices data/prices.parquet
```

## Python API

```python
from quant_data_kit import create_snapshot, point_in_time_join, save_parquet
from quant_data_kit.providers import fetch_daily_prices, fetch_earnings_forecasts
from quant_data_kit.providers.akshare import fetch_hs300_constituents
```

## Used by

- [a-share-multifactor](../a-share-multifactor)
- [sklearn-stock-trend](../sklearn-stock-trend)
- [quant-lab](../quant-lab)
