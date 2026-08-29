# M7 Arrow normalization and market-feed boundary

## Public entrypoints

- `write_normalized_events(...)` remains the compatibility API for
  `Iterable[Mapping]`. It preserves stream-level quarantine behavior.
- `write_normalized_batches(...)` accepts `Iterable[pyarrow.RecordBatch]` or a
  `pyarrow.RecordBatchReader`. Every non-empty batch must have the exact frozen
  v2 Arrow schema and be homogeneous by event type, instrument, and trading day.
  Invalid batch input fails the normalization transaction closed; it does not
  publish a partial snapshot or a quarantine object.
- The fast L2 path is limited to a vector-proven sequence of same-side,
  same-price upserts. Any delete, mixed price/side/action, requested interior
  checkpoint, or non-uniform stream uses the serial reconstructor.

Both entrypoints enforce trusted Raw references, schema and PIT rules, strict
sequence continuity, deterministic L2 reconstruction, duplicate event IDs,
lake-wide event-claim conflicts, capacity stops, immutable publication, and
post-publication verification.

## Versioned storage algorithms

Normalized layout `3.0.0` records these independently versioned algorithms:

- Mapping partitions use `canonical-json-array-v1`, preserving legacy logical
  partition hashes.
- Arrow partitions use `arrow-ipc-record-batch-v1`. External loads re-read the
  Parquet data and recompute this digest; the manifest never substitutes for
  verification.
- Claim indexes use `streaming-parquet-v2` with claim version `3.1.0`. Individual
  `event_id_hash`, `event_sha256`, and `claim_sha256` values retain the frozen v2
  canonical JSON definitions. A versioned, order-independent
  `sha256-multiset-u64x4-v1` shard digest removes the former unbounded sorted
  string aggregation. Physical file hashes and a manifest hash remain mandatory.

Historical v2 snapshots are not rewritten. A missing v3 claim acceleration file
can be rebuilt from immutable partitions; a changed file or manifest fails closed.

## 2026 exchange-feed boundary

This release does not implement real network collectors. The following official
feed semantics are deliberately not folded into the frozen normalized event
contract:

- OKX deprecated the JSON `checksum` for `books`, `books-l2-tbt`, and
  `books50-l2-tbt` on 2026-06-23. The retained field is zero and is not a current
  integrity signal. A future live adapter must require TLS `wss` transport and
  strict `seqId`/`prevSeqId` processing. Sources:
  [checksum deprecation](https://www.okx.com/en-us/help/okx-order-book-channels-checksum-field-deprecation)
  and [OKX API v5](https://www.okx.com/docs-v5).
- An OKX no-change heartbeat can have empty `asks`/`bids` and
  `seqId == prevSeqId`. It is transport liveness, not a frozen `BookDelta`, and
  must not enter the normalized writer. A maintenance reset with
  `seqId < prevSeqId` must terminate the current admission transaction, discard
  deltas until a fresh snapshot, and start a new normalization transaction.
  The current v2 event contract intentionally rejects both equal-sequence deltas
  and an in-transaction sequence rollback.
- Binance USDⓈ-M local-book admission must first bridge
  `U <= lastUpdateId <= u`, then require each event's `pu` to equal the prior `u`.
  Quantities are absolute and zero means deletion. Binance documents that deleting
  an absent local price can be normal. A future adapter must treat that raw event
  as a measured no-op before emitting normalized deltas, because the current
  frozen reconstructor rejects an absent-level delete. Source:
  [Binance USDⓈ-M local order book](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly).
- Existing OKX CRC32 fixture tests remain historical fixture-integrity tests only.
  They are not labeled as 2026 OKX market-data certification.

These adapter policies require a serial contract review before real market-data
certification. They are not reasons to weaken the normalized batch gates.

## Performance gate

The formal command runs each repetition in a new Python process. Raw admission
setup is excluded; RecordBatch generation and the complete normalized write are
inside the timed scope.

```powershell
$env:TEMP = 'F:\puresaber-m7-temp'
$env:TMP = 'F:\puresaber-m7-temp'
python tools/benchmark_normalized_l2.py `
  --work-root .m7\quant-data-kit-10m-final-clean2-009a361 `
  --output validation/performance/m7-data-arrow-10m-final-okx-contract.json `
  --rows 10000000 --runs 3 --batch-rows 262144 `
  --minimum-events-per-second 100000 --maximum-peak-rss-gib 16
```

The report records the machine, Python and dependency versions, Git identity,
timed scope, three run results, deterministic artifact fields, C/H/F disk space,
actual temporary directory, and whether generated synthetic data was removed.
The formal run retains all three lake directories under the recorded H-drive
work root; cleanup is a separate, explicit operator decision after evidence review.

The final clean run used commit`009a36162a2ec1a48fc4f96b93b2e675196e9263`with
`dirty=false`. Its three throughputs were155,932.27、155,071.20 and153,097.30events/s;
maximum peak RSS was2.967GiB. All30,000,000 rows were accepted, no row was quarantined,
strict reload passed, and snapshot/partition/claim/L2-checkpoint hashes were identical across
fresh processes. The three retained runs contain3,310,487,778bytes. `TEMP`, `TMP`, and Python's
actual `tempfile.gettempdir()` all resolved to`F:\puresaber-m7-temp`; C was not the temp volume.
The report SHA-256 is`69416eeba389ff520043c9382ed7e1ff7380f4d5937030a02cb84cb1ab80c08f`.
