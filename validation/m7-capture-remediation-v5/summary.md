# M7 epoch journal-binding remediation candidate evidence

## Decision boundary

- Source commit:`13e004a80296bb3a49c4bd54e64cb64670e56b01`; every source-dependent run
  recorded a clean working tree and package`0.7.4`.
- Branch:`codex/cross-asset-v2-m7-data-performance`; PR`PureSaber/quant-data-kit#6`
  remains open, unmerged and untagged.
- This candidate supersedes the failed software/fixture decision from
  `validation/m7-capture-remediation-v4`; historical evidence is unchanged.
- Software, deterministic fixture and local performance gates pass pending exact-head GitHub CI
  and a new independent read-only validation. Public network capture,30-day retention and domestic
  market-data certification remain unperformed.
- No credentials, authenticated endpoint, account route, order route, live order or cleanup was
  used.

## Closed independent findings

Recovery no longer accepts a merely self-consistent snapshot with matching provider, venue, row
count and Raw references. It replays the current sealed epoch journal through the same strict Arrow
and L2 validation path, recomputes every logical partition's identity, row count and canonical row
digest, the available-time maximum and the final L2 checkpoint set, then compares those values to
the loaded snapshot. A negative test creates a second valid same-lineage/same-row-count snapshot,
rewrites and rehashes the COMMITTED receipt, and proves reconciliation rejects the rebind.

PREPARED, COMMITTED, explicit ABORTED and retryable failure JSON objects now have closed field sets.
Unknown or missing fields fail closed. Hashes, snapshot IDs, attempts, row counts, exception,
message, restart instruction, retryable flags and abort reasons have strict non-coercing types.
Sequential artifact filenames must encode the payload attempt exactly; receipt and abort filenames
must match their complete content hash exactly.

The prior real spawned-process crash boundary, independent stream identity anchor, terminal-state
conflict rules, Raw lineage gates and bootstrap-lock reparse protections remain covered. The
historical v4 candidate is not promoted by this evidence.

## Quality gates

Environment: isolated`.venv-m7-verify`, Python3.12.5, package0.7.4,
`requirements.lock`SHA-256
`fac9239809b2bb42c2d7ee99417a3053075a177eb9b08131a954288566de6d88`.

| Gate | Result |
|---|---|
|`pip check` | PASS;no broken requirements |
|`ruff check .` | PASS |
|`ruff format --check .` | PASS;92 files unchanged |
| Mutually exclusive pytest partitions | PASS;431 passed,0 failed,1 existing platform skip |
| Full source pure branch coverage | PASS;2217/2546=87.08%,required>=80% |
| Every configured core module | PASS;all17 modules>=90% |
|`capture_v2/epoch.py` |251/276=90.94% |
|`capture_v2/collector.py` |85/94=90.43% |
|`capture_v2/storage.py` |125/136=91.91% |

The main partition has430 passes, one existing skip and one deliberately deselected native
hard-exit case. The isolated partition executes that one case and passes. Together they cover the
full collection without overlap.

## Deterministic eight-stream capture benchmarks

Every fixture round includes Raw frames, Raw publication, Normalized publication, archive copy,
restore-hash verification and complete worker shutdown. Reports bind the exact source commit,
`working_tree_dirty=false`, consistency true and gate true.

| Scenario | Rounds | Messages/round | Normalized rows/round | Messages/s | p99 | Safety multiple |
|---|---:|---:|---:|---:|---:|---:|
| Dense20+20 levels/update |3 |8,000 |319,688 |310.60/299.17/309.99 |37.73/36.54/36.97ms |3.88/3.74/3.87x |
| Sparse1+1 level/update |3 |4,000 |7,992 |575.18/578.13/573.59 |15.97/15.45/15.17ms |7.19/7.23/7.17x |

All rounds pass the240messages/s, scenario-density3x and100ms p99 gates. The slowest Dense round is
about2.3% below the previous candidate's slowest round, within the15% regression limit.

## Three independent10-million-event runs

All runs execute in independent processes, accept all10,000,000 events, quarantine0, pass strict
reload and deterministic artifact comparison, and retain3,310,487,778bytes without cleanup.

| Run | Events/s | Peak RSS | Result |
|---:|---:|---:|---|
|1 |188,411.35 |3.11GiB | PASS |
|2 |188,473.94 |3.10GiB | PASS |
|3 |182,097.14 |3.14GiB | PASS |

Minimum throughput remains82.1% above the100,000events/s gate and peak memory remains below16GiB.

## Capacity refusal

Installed`qdk-capture.exe`was invoked in safe`preflight`mode and explicit
`run --confirm-long-running`mode. Both returned exit code2 and immutable
`PAUSED_PREFLIGHT_FAILED`reports before network startup. After the150GiB reserve, projected archive
capacity was about56.36GB versus the199,710,735,564-byte floor. All eight streams report0WebSocket
messages and`long_running_capture_started=false`.

## Remaining certification risks

- GitHub Python3.10/3.11/3.12 CI and a new independent read-only verdict are required before this
  candidate can pass software/fixture acceptance.
- Public Binance/OKX TLS, reconnect/resync and real-message quality remain capacity-blocked.
- Current archive capacity cannot support the retained30-day eight-stream run.
- Legally authorized domestic L2 remains unavailable and fixture-certified only.
- Journal-to-snapshot validation intentionally rereads immutable journal and snapshot rows at
  startup; future scale work may add a safe cache, but cannot weaken semantic recomputation.

All17 artifacts are content-addressed and byte-preserved by the local`.gitattributes`.
