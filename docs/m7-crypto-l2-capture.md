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
`eth-usdt-perpetual-l2`. Configuration may replace endpoint details, but it must still describe
the same frozen eight-stream certification scope.

This implementation is not evidence of30 continuous capture days and is not
`market-data-certified`. A bounded probe proves only that preflight, public transport and admission
work for that invocation.

## State and synchronization rules

Every stream has the explicit state sequence`CONNECTING→BUFFERING→SNAPSHOT_SYNC→LIVE`and can
terminate in`RESYNC`or`PAUSED`. Every transition, failure, retry, heartbeat, stale update and normal
absent-level deletion becomes an immutable audit Raw frame. Illegal transitions raise and emit an
alert instead of changing state silently.

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
existing`write_normalized_events`path, PIT checks, schemas, quarantine rules and Raw references.
Gaps or connection failures close the current epoch visibly before resynchronization.

## Capacity and independent archive controls

`hot_root`、`archive_root`and`restore_root`must be explicit absolute existing directories. The hot
and archive roots are resolved through an injectable physical-volume probe; different path strings
on the same physical device fail preflight. Collection checks capacity before network startup and
before every write. It pauses if projected hot data exceeds150GiB or if free space falls below
`max(volume capacity*20%,100GiB)`. Archive reserve failures also pause collection. No capture path
deletes or evicts data automatically.

Preflight writes an immutable archive probe, copies it to the archive volume, restores it under the
explicit restore root and recomputes SHA-256. Every Raw segment then repeats copy, manifest and
payload hashing, temporary restore, model re-read and hash comparison. Only a successful receipt
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
  }
}
```

Safe preflight is the default and opens no network connection:

```bash
qdk-capture capture.json
qdk-capture capture.json --mode preflight
```

A bounded public probe is explicit and still runs archive preflight first:

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
