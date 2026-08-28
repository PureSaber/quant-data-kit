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

- Raw persists exact provider bytes through lake-local staging and atomic rename. Its integrity anchor binds source, request, UTC collection time, object/key path identity, SHA-256 and the30-day retention policy. A crash-released process lock and immutable key claim make concurrent writes, crash recovery and cleanup serialize on the same idempotency key. Every write, read and cleanup rejects path escapes and Windows reparse points below the lake root.
- Normalized requires resolvable, hash-verified Raw references and writes only frozen`standard/v2`Arrow schemas under`provider/venue/event_type/date/instrument`partitions. A sharded persistent claim index binds every lake-wide`event_id`to its Arrow-normalized logical event hash. Same-ID/same-content reuse is idempotent; conflicting content, bad sequences and L2 reconstruction failures cannot enter research snapshots.
- The certified Curated entry is`curate_trade_bars_from_snapshot`: it reads trades from one explicit verified Normalized snapshot, constructs session-aware bars and binds the exact lineage. One`dataset+revision_id`maps to one snapshot; corrected data requires a new revision and never overwrites history.
- Normalized and Curated snapshot identities bind Arrow-canonical logical rows and physical Parquet hashes. DuckDB verifies the fixed snapshot, copies Arrow data into in-memory tables, then disables external access; user SQL cannot call file readers or resolve`latest`/`main`.
- Collection stops with a visible`COLLECTION_STOPPED`error if hot data would exceed150GB or free space would fall below`max(volume*20%,100GB)`.
- Raw cleanup requires all of: the30-day window elapsed, explicit confirmation, an accessible real local archive object, archive hash equality and a successful restore-hash exercise. Cleanup publishes an immutable audit tombstone and resumes an explicit`deleting`state after interruption; local unlink failures remain visible to callers. Remote archives have no M2 verifier and therefore stop cleanup. No background or silent deletion path exists.
- Normalized and Curated writers also apply the storage gate; they only consume Raw data that already passed acquisition admission. Without a verified archive, capacity pressure stops ingestion instead of deleting data.

## M2 fixture certification scope

For both Binance and OKX, the certified fixture set is deliberately narrow: BTC spot covers Trade、BBO、BookSnapshot和BookDelta; ETH spot covers Trade only; BTC and ETH perpetuals cover FundingRate and MarkPrice only. It does not claim every event type for all four instruments. OKX books use the provider's signed CRC32 checksum; Binance has no equivalent field, so its gate is U/u/pu continuity plus immutable Raw SHA-256. The domestic supplier-neutral L2 fixture is deliberately marked`fixture-certified-not-market-data-certified`; it must not be described as real domestic market-data certification.

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
