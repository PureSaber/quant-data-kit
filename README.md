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
| `storage` | Parquet I/O + `*.manifest.json` dataset metadata |
| `validate` | OHLCV schema / missing / duplicate checks |
| `calendar` | SSE trading calendar helpers |
| `providers.akshare` | HS300 constituents + daily price fetch with retry |

## CLI

```bash
qdk-validate data/prices.parquet
qdk-manifest data/prices.manifest.json
```

## Python API

```python
from quant_data_kit import save_parquet, write_manifest, validate_price_frame
from quant_data_kit.providers.akshare import fetch_daily_prices, fetch_hs300_constituents
```

## Used by

- [a-share-multifactor](../a-share-multifactor)
- [sklearn-stock-trend](../sklearn-stock-trend)
- [quant-lab](../quant-lab)
