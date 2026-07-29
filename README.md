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
| `storage` | Parquet I/O, manifest tracking, cache helpers |
| `validate` | OHLCV schema / missing / duplicate checks |
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

## PIT rules

- **Earnings forecasts**: `effective_date = announce_date + 1 business day`
- **Northbound holdings**: `shift(1)` on publish date to reflect T-1 disclosure

## CLI

```bash
qdk-validate data/prices.parquet
qdk-manifest data/prices.manifest.json
qdk-catalog list
qdk-catalog register hs300_prices data/prices.parquet
```

## Python API

```python
from quant_data_kit import save_parquet, merge_earnings_to_panel, should_refresh_cache
from quant_data_kit.providers import fetch_daily_prices, fetch_earnings_forecasts
from quant_data_kit.providers.akshare import fetch_hs300_constituents
```

## Used by

- [a-share-multifactor](../a-share-multifactor)
- [sklearn-stock-trend](../sklearn-stock-trend)
- [quant-lab](../quant-lab)
