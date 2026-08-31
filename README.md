# quant-data-kit

Shared data layer for PureSaber quant research repos: immutable Raw/Normalized/Curated storage, pinned DuckDB snapshots, deterministic L2 replay, cross-venue fixture adapters, Parquet contracts, and AKShare providers.

## Install

```bash
cd quant-data-kit
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.lock
python -m pip install -e . --no-deps --no-build-isolation
```

AKShare providers are an explicit optional integration and are deliberately excluded from the
audited runtime-and-development lock. Install `.[akshare]` only in an environment that needs live
AKShare access; the core package and CI do not require it.

## M6 dependency governance

The package declares the `data` layer through `[tool.quant-workspace]`, publishes
`puresaber.market-events@2.0.0` and `puresaber.instrument-master@2.0.0`, and identifies
`requirements.lock` as its externally resolved dependency set. The lock covers the base runtime
dependencies, the `dev` extra, and editable-build requirements for Python3.10-3.12. Every package
is resolved to one exact version, while the workspace manifest records the complete lock-file
SHA-256; CI must not resolve from unconstrained extras.

The direct `pandas<3` and `numpy<2.3` upper bounds preserve the package's declared Python3.10
support. Raising either bound requires a coordinated Python support review and a successful CI
matrix, not merely a lock refresh on a newer local interpreter.

Regenerate the lock only after reviewing changes to `pyproject.toml`:

```bash
python -m pip install "pip-tools==7.6.1"
pip-compile --extra dev --build-deps-for editable --allow-unsafe --strip-extras \
  --resolver backtracking --index-url https://pypi.org/simple \
  --constraint requirements-constraints.txt \
  --output-file requirements.lock pyproject.toml
```

Then install the lock in a clean Python3.10,3.11, or3.12 environment, run `pip check`, and install
the package with `python -m pip install -e . --no-deps --no-build-isolation`. Updating the lock and
the dependency declaration or resolver constraints is one review unit; never hand-edit an isolated
transitive pin. `requirements-constraints.txt` contains only cross-interpreter resolver limits and
is not an additional installation input.

Rollback is a Git revert of the governance commit, restoring both `pyproject.toml` and
`requirements.lock` together. Existing tags and historical lock hashes remain immutable; do not
move or rebuild a release tag to repair a dependency resolution.

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
| `research_contracts_v2` | Closed M8 Curated aggregation and verified-factor-input contracts |
| `research_inputs_v2` | Content-addressed market context plus fail-closed Curated/Normalized factor-input factories |
| `l2_replay` | Deterministic Snapshot+Delta reconstruction, sequence/cross checks and checkpoint hashes |
| `adapters_v2` | Binance, OKX and supplier-neutral domestic desensitized fixture adapters |
| `capture_v2` | Fail-closed public Binance/OKX L2 capture, immutable batched Raw segments, snapshot synchronization, independent archive/restore verification and the Raw-to-Normalized bridge |

The M7 Arrow batch entrypoint, bounded-memory validation architecture, benchmark scope, and
the explicit gap between frozen fixtures and current OKX/Binance live-book semantics are documented
in [`docs/m7-data-performance.md`](docs/m7-data-performance.md).
The public-feed collector's exact eight-stream scope, explicit storage configuration, state machine,
safe CLI modes and non-certification boundary are documented in
[`docs/m7-crypto-l2-capture.md`](docs/m7-crypto-l2-capture.md).

## M8 certified research inputs

M8 factor code must consume one of two public factories instead of promoting an arbitrary Arrow
table or the ordinary `read_normalized_events` result:

```python
from quant_data_kit import (
    EventSchemaRef,
    create_market_context_snapshot,
    load_verified_curated_bars,
    load_verified_normalized_events,
)

bars = load_verified_curated_bars(root, "bars-1m", curated_snapshot_id)
events = load_verified_normalized_events(
    root,
    normalized_snapshot_id,
    [EventSchemaRef("puresaber.trade-event", "2.0.0")],
    market_context_snapshot_id,
)
```

`create_market_context_snapshot` content-addresses immutable `InstrumentSpec` and
`TradingSession` values together with one explicit calendar and session-policy version. A certified
Curated snapshot additionally binds `puresaber.curated-aggregation@1.0.0`: fixed interval,
session/trading-day rollup, or event-Bar threshold and exact source-range evidence. The loaders
verify the complete source snapshot, physical and logical partition hashes, PIT timestamps,
context membership, ordering, L2 snapshot/delta replay, selection hash and a second post-read
snapshot check. Legacy Curated manifests remain readable through `load_curated_snapshot` but fail
with `legacy-curated-not-m8-certified` at the certified factory.

## M2 data-lake guarantees

- Raw persists exact provider bytes through lake-local staging and atomic rename. Its integrity anchor binds source, request, UTC collection time, object/key path identity, SHA-256 and the30-day retention policy. A crash-released process lock and immutable key claim make concurrent writes, crash recovery and cleanup serialize on the same idempotency key. Every write, read and cleanup rejects path escapes and Windows reparse points below the lake root.
- Normalized requires resolvable, hash-verified Raw references and writes only frozen`standard/v2`Arrow schemas under`provider/venue/event_type/date/instrument`partitions. Capture epochs persist`PREPARED`before snapshot publication and finish as`COMMITTED`or`ABORTED`; startup uses the frozen stream configuration as an independent identity anchor, enforces closed terminal JSON fields and strict types, recomputes partition rows, logical hashes, the available-time maximum and the final L2 state from the immutable journal, and rejects any receipt bound to a different snapshot before network startup. A sharded persistent claim index binds every lake-wide`event_id`to its Arrow-normalized logical event hash. Same-ID/same-content reuse is idempotent; conflicting content, bad sequences and L2 reconstruction failures cannot enter research snapshots.
- Certified Curated producers are`curate_trade_bars_from_snapshot`、`curate_session_bars_from_snapshot`and`curate_trade_event_bars_from_snapshot`: they read trades from one explicit verified Normalized snapshot, construct authoritative fixed/session/event Bars and bind exact lineage plus an immutable market-context snapshot. One`dataset+revision_id`maps to one snapshot; corrected data requires a new revision and never overwrites history.
- A Normalized snapshot is provider-bound. Binance and OKX certification therefore uses separate immutable snapshots and separate verified inputs; a Curated writer can partition evidence by`source/session`, but the certified loader rejects any source that is not present in the snapshot lineage.
- Normalized and Curated snapshot identities bind Arrow-canonical logical rows and physical Parquet hashes. DuckDB verifies the fixed snapshot, copies Arrow data into in-memory tables, then disables external access; user SQL cannot call file readers or resolve`latest`/`main`.
- Collection stops with a visible`COLLECTION_STOPPED`error if hot data would exceed150GB or free space would fall below`max(volume*20%,100GB)`.
- Raw cleanup requires all of: the30-day window elapsed, explicit confirmation, an accessible real local archive object, archive hash equality and a successful restore-hash exercise. Cleanup publishes an immutable audit tombstone and resumes an explicit`deleting`state after interruption; local unlink failures remain visible to callers. Remote archives have no M2 verifier and therefore stop cleanup. No background or silent deletion path exists.
- Normalized and Curated writers also apply the storage gate; they only consume Raw data that already passed acquisition admission. Without a verified archive, capacity pressure stops ingestion instead of deleting data.

## M2 fixture certification scope

For both Binance and OKX, the certified fixture set is deliberately narrow: BTC spot covers Trade、BBO、BookSnapshot和BookDelta; ETH spot covers Trade only; BTC and ETH perpetuals cover FundingRate and MarkPrice only. It does not claim every event type for all four instruments. The bundled OKX nonzero signed-CRC32 book sample is a historical golden fixture only: since 2026-06-23 the live JSON field is fixed to0 and is not an integrity gate. A current OKX collector must use TLS, enforce`seqId/prevSeqId`continuity, consume empty equal-sequence heartbeats without emitting`BookDelta`, and terminate admission on maintenance resets until a fresh snapshot. The separate`capture_v2`module can open only public Binance/OKX market-data endpoints after physical-volume, capacity and archive/restore preflight succeeds. Its deterministic tests certify the transport and synchronization implementation, not a continuous real-market dataset. Binance has no equivalent checksum field, so its fixture gate is U/u/pu continuity plus immutable Raw SHA-256. The domestic supplier-neutral L2 fixture is deliberately marked`fixture-certified-not-market-data-certified`; it must not be described as real domestic market-data certification.

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
qdk-capture capture.json --mode preflight
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
