# M7 public Crypto L2 capture

## Frozen scope and safety boundary

`quant_data_kit.capture_v2`collects public market data only. It has no credential, account or order
API, rejects credential-like configuration keys recursively, and sends no orders. The default and
machine-certified scope is exactly these eight streams:

| Provider | Market | Instruments | Feed |
|---|---|---|---|
| Binance | Spot | BTCUSDT、ETHUSDT | Public depth WebSocket plus public REST depth snapshot |
| Binance | USDT linear perpetual | BTCUSDT、ETHUSDT | Public USD-M depth WebSocket plus public REST depth snapshot |
| OKX | Spot | BTC-USDT、ETH-USDT | Public`books`WebSocket |
| OKX | USDT perpetual | BTC-USDT-SWAP、ETH-USDT-SWAP | Public`books`WebSocket |

The run report always emits sorted providers`["binance","okx"]`and the sorted capabilities
`btc-spot-l2`、`btc-usdt-perpetual-l2`、`eth-spot-l2`and
`eth-usdt-perpetual-l2`. Configuration may list the streams explicitly, but M7 rejects any change
to provider、market、native symbol、stable instrument、stream ID、venue、channel or the parsed
official WSS/HTTPS host、port、path and query identity.

This implementation is not evidence of30 continuous capture days and is not
`market-data-certified`. A bounded probe proves only that preflight, public transport and admission
work for that invocation.

## State and synchronization rules

Every stream has the explicit state sequence`CONNECTING→BUFFERING→SNAPSHOT_SYNC→LIVE`and can
terminate in`RESYNC`or`PAUSED`. State transitions、failures and retries are committed first to a
dedicated content-addressed durable audit chain; only a successful reload/hash check allows the
in-memory state to change. Illegal transitions are durably audited and then rejected. High-rate
book observations remain counted, with the first and each1024th occurrence durably summarized so
that operational audit does not become per-market-message fsync I/O.

Binance messages are buffered before requesting the HTTPS snapshot. The bridge discards stale
updates, requires the first admitted update to satisfy`U<=lastUpdateId<=u`, then enforces the USD-M
`pu`chain or the applicable Spot update-ID continuity. Quantities are absolute; zero deletes a
level. Deleting an absent level is retained in Raw and audited as a documented no-op, not reported
as book corruption.

OKX requires TLS and a fresh`action=snapshot`before updates. It enforces
`seqId/prevSeqId`continuity. A zero checksum is not integrity evidence, an equal-sequence empty
heartbeat creates no Normalized event, and a maintenance reset forces`RESYNC`and blocks deltas
until a new snapshot.

## Immutable Raw and existing Normalized contract

Each WebSocket or HTTPS response is preserved byte-for-byte in one`RawFrame`; frames are batched by
message count, exact wire-byte count or age into immutable Raw segments. A segment binds provider,
stream, receive/event time ranges, native sequence ranges, message count, wire bytes, SHA-256,
collector commit and previous-segment lineage. A segment is never one file per network message and
an existing immutable path cannot be overwritten with different bytes.

Admitted snapshots and deltas are spooled in bounded Normalized epochs. Publication uses the
existing strict Arrow-batch path, PIT checks, schemas, quarantine rules and Raw references. Every
finalize attempt persists an immutable`PREPARED`transaction before publishing a visible snapshot,
then ends in a content-addressed`COMMITTED`receipt or`ABORTED`failure. Before any network startup,
the coordinator anchors every journal to the frozen stream configuration, scans all journal
parents, enforces closed JSON fields, strict types and filename attempt binding for every terminal
record, then recomputes partition rows, logical hashes, the available-time maximum and the final L2
state from the immutable journal. A receipt that points to another individually valid snapshot is
rejected before network startup. Unresolved`PREPARED`or retryable`ABORTED`transactions replay
idempotently.
A real spawned-process test terminates with`os._exit`after snapshot publication but before receipt
publication and proves that a fresh process commits the transaction before any network runner is
created. A transaction without a durable PREPARED identity blocks startup instead of being skipped.
Gaps or connection failures explicitly abort the current epoch before resynchronization.

## Capacity and independent archive controls

`hot_root`、`archive_root`and`restore_root`must be explicit absolute existing directories. The hot
and archive roots are resolved through an injectable physical-volume probe; different path strings
on the same physical device fail preflight. All three roots and each final parent reject symbolic
links、Windows junctions/reparse points and resolved escapes. The bootstrap lock itself is created
and checked as a regular non-reparse object before the file-lock backend may open it. These rules
apply to journal creation, parts, transaction recovery, receipts and abort records as well as
Raw/archive paths. Collection takes an exact startup
baseline, reserves every projected write under a thread-safe incremental counter, and repeats real
tree/capacity probes at bounded message、byte or monotonic-time intervals. It pauses if projected
hot data exceeds150GiB or if free space falls below
`max(volume capacity*20%,100GiB)`. Archive reserve failures also pause collection. No capture path
deletes or evicts data automatically.

Preflight writes an immutable archive probe, copies it to the archive volume, restores it under the
explicit restore root and recomputes SHA-256. Every Raw segment then repeats copy, manifest and
payload hashing, temporary restore, model re-read and hash comparison. The destination physical
identity is rechecked before and after immutable create-if-absent publication. Only a successful receipt
sets`archive_restore_verified=true`and`eligible_for_cleanup=true`; it always records
`cleanup_performed=false`. Cleanup is outside this collector and requires a separate explicit
operator action.

## Configuration and CLI

Example configuration, with operator-selected absolute paths:

```json
{
  "hot_root": "<absolute-hot-directory>",
  "archive_root": "<absolute-archive-directory-on-a-different-physical-volume>",
  "restore_root": "<absolute-temporary-restore-parent>",
  "collector_commit": "<git-commit>",
  "archive_reserve_bytes": 161061273600,
  "rotation": {
    "max_messages": 2000,
    "max_wire_bytes": 16777216,
    "max_age_seconds": 30
  },
  "retry": {
    "max_attempts": 5,
    "base_delay_seconds": 0.5,
    "maximum_delay_seconds": 8.0,
    "jitter_fraction": 0.2
  },
  "durability": {
    "capacity_probe_messages": 256,
    "capacity_probe_bytes": 4194304,
    "capacity_probe_seconds": 1.0,
    "normalized_flush_records": 256,
    "normalized_flush_bytes": 1048576,
    "normalized_flush_seconds": 1.0,
    "probe_timeout_seconds": 30.0
  }
}
```

Safe preflight is the default and opens no network connection:

```bash
qdk-capture capture.json
qdk-capture capture.json --mode preflight
```

A bounded public probe is explicit and still runs archive preflight first. Its message budget covers
the whole post-connect synchronization phase, including OKX control frames; both timeout and message
budget exhaustion are audited failures:

```bash
qdk-capture capture.json --mode probe --max-messages 3
```

Unbounded collection additionally requires`--confirm-long-running`. It still refuses to connect if
physical independence, free-space or archive/restore verification fails:

```bash
qdk-capture capture.json --mode run --confirm-long-running
```

Do not use`run`as a service readiness claim. Operational certification still requires retained
continuous evidence, data-quality review and an independently reviewed capacity/retention plan.
