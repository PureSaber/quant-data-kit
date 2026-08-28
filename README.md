# quant-data-kit

Shared data layer for PureSaber quant research repos: immutable Raw/Normalized/Curated storage, pinned DuckDB snapshots, deterministic L2 replay, cross-venue fixture adapters, Parquet contracts, and AKShare providers.

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
| `data_lake` | Immutable Raw bytes, strict partitioned Normalized Parquet, quarantine, pinned DuckDB reads and storage stop policy |
| `curated` | Session-aware bar aggregation and immutable revision/lineage snapshots |
| `l2_replay` | Deterministic Snapshot+Delta reconstruction, sequence/cross checks and checkpoint hashes |
| `adapters_v2` | Binance, OKX and supplier-neutral domestic desensitized fixture adapters |

## M2 data-lake guarantees

- Raw persists exact provider bytes with source, request, UTC collection time, SHA-256 and a 30-day hot-retention deadline. Reusing an object key is either byte-for-byte idempotent or an explicit conflict.
- Normalized writes only frozen`standard/v2`Arrow schemas under`provider/venue/event_type/date/instrument`partitions. Invalid records, duplicate IDs, bad sequences and L2 reconstruction failures quarantine the complete affected stream.
- Curated revisions include recipe and lineage in a content-addressed snapshot ID; corrected data creates a new snapshot and never overwrites an old one.
- DuckDB requires an explicit`snapshot_id`, verifies manifest/partition hashes and schemas first, and exposes read-only query methods. There is no`latest`alias.
- Collection stops with a visible`COLLECTION_STOPPED`error if hot data would exceed150GB or free space would fall below`max(volume*20%,100GB)`.
- Raw cleanup requires all of: the30-day window elapsed, explicit confirmation, archive hash equality and a successful restore hash. No background or silent deletion path exists.

## M2 fixture certification scope

Binance and OKX fixtures cover Trade、BBO、BookSnapshot/Delta、FundingRate和MarkPrice for mapped BTC/ETH spot and BTC perpetual instruments. The domestic supplier-neutral L2 fixture is deliberately marked`fixture-certified-not-market-data-certified`; it must not be described as real domestic market-data certification.

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
