# M7 Normalized epoch reconciliation candidate evidence

## Decision boundary

- Source commit:`a4a5a16542a2c4c74edf8205b982c681248517a3`, clean working tree at every
  source-dependent test and benchmark.
- Branch:`codex/cross-asset-v2-m7-data-performance`; PR`PureSaber/quant-data-kit#6`
  remains open and unmerged.
- This candidate supersedes the software acceptance decision for
  `validation/m7-capture-remediation-v2`; that historical evidence remains immutable.
- Software, deterministic fixture and local performance gates pass pending a new independent
  read-only validation of the two previously reported P2 findings.
- Public Binance/OKX network capture was not run. Continuous days remain0 and
  `market_data_certified=false`.
- No API key, account endpoint, order route, live order or destructive cleanup was used.

## Closed P2 findings and invariants

Normalized publication now uses an immutable`PREPARED -> COMMITTED | ABORTED`protocol. A fully
validated PREPARED record is persisted before any snapshot becomes visible. COMMITTED binds the
receipt to the PREPARED hash; a publication failure or explicit abort seals ABORTED. Startup scans
all journals and reconciles recoverable PREPARED or retryable ABORTED transactions before creating
any network runner. Invalid identities, hashes, filenames, policies, parts, Raw lineage, terminal
records or state transitions fail closed.

The journal root, directory creation, opens, reads, seals and recovery path all use the same
component-wise path guard. Symbolic links, reparse points and Windows junctions are rejected. Sealed
parts are create-if-absent hard links followed by a hash check, so an existing immutable name is not
overwritten. Tests include a real Windows`mklink /J`junction and a separate-process termination
after snapshot publication but before the COMMITTED receipt; restart reconciliation commits that
pending transaction idempotently before network startup.

## Quality gates

Environment: isolated`.venv-m7-verify`, Python3.12.5, package0.7.2,
`requirements.lock`SHA-256
`fac9239809b2bb42c2d7ee99417a3053075a177eb9b08131a954288566de6d88`.

| Gate | Result |
|---|---|
| Isolated`pip check` | PASS; no broken requirements |
|`ruff check .` | PASS |
|`ruff format --check .` | PASS;88 files unchanged |
| Mutually exclusive pytest partitions | PASS;380 passed,0 failed,1 existing platform skip |
| Full source pure branch coverage | PASS;2105/2420=86.98%, required>=80% |
| Every configured core module | PASS;all17 modules>=90% |
|`capture_v2/epoch.py` |144/154=93.51% |
|`capture_v2/collector.py` |85/94=90.43% |
|`capture_v2/storage.py` |122/132=92.42% |

The formal coverage run uses two non-overlapping partitions. The main partition has379 passes and
one existing skip; the isolated native hard-exit case has one pass. This is deliberate because a
single Windows Coverage process can intermittently receive native exit`0xC0000005`instead of the
test's intended hard-exit code in the unrelated existing process-integrity case. The full
process-integrity file and isolated case passed in diagnostics. No failed attempt is promoted as
evidence, and the two JUnit files prove that every collected test belongs to exactly one passing
partition.

## Deterministic eight-stream capture benchmarks

Both fixture scenarios include exact Raw frames, Raw segment publication, Normalized publication,
archive copy, restore hash verification and complete worker shutdown. Each run reports source
commit`a4a5a165...`, `working_tree_dirty=false`, consistency true and gate true.

| Scenario | Rounds | Messages/round | Normalized rows/round | Messages/s | Rows/s | p99 |
|---|---:|---:|---:|---:|---:|---:|
| Dense20+20 levels/update |3 |8,000 |319,688 |305.35/300.15/313.42 |12,202.29/11,994.19/12,524.51 |36.37/41.68/36.88ms |
| Sparse1+1 level/update |3 |4,000 |7,992 |573.11/586.55/589.12 |1,145.07/1,171.92/1,177.06 |17.38/16.06/16.22ms |

The dense gate requires at least240messages/s, scenario-density rows at3x the frozen80messages/s
live offer rate, and p99 at most100ms. Dense safety multiples are3.75-3.92; sparse safety multiples
are7.16-7.36. All six rounds pass.

## Three independent10-million-event runs

All runs use the exact candidate source in clean state, execute in independent processes, accept
all10,000,000 valid events, quarantine0, pass strict reload, and retain3,310,487,778bytes without
cleanup.

| Run | Events/s | Peak RSS | Result |
|---:|---:|---:|---|
|1 |186,268.54 |3.128GiB | PASS |
|2 |189,471.96 |3.137GiB | PASS |
|3 |188,457.43 |3.112GiB | PASS |

The deterministic snapshot, manifest and final L2 checkpoint hashes match across all three runs.
The minimum throughput has86.3% headroom over the100,000events/s gate and peak memory stays below
the16GiB limit.

## Capacity refusal and exact CLI evidence

The exact candidate was invoked through the installed`qdk-capture.exe`entry point in safe
`preflight`mode and explicit`run --confirm-long-running`mode. Both returned exit code2 and immutable
`PAUSED_PREFLIGHT_FAILED`reports before network startup because the archive projection after the
150GiB reserve was approximately63.0GB, below the199,710,735,564-byte safety floor. All eight
streams report0WebSocket messages and`long_running_capture_started=false`.

The content-addressed`cli-invocation-results`artifact records exact argv, config hash, exit codes,
run IDs and report hashes. This closes the previous evidence-only observation that the generated
run report did not itself preserve CLI argv and process exit code.

## Remaining certification risks

- Public Binance/OKX TLS, snapshot bridge, reconnect/resync and real-message data quality remain
  unverified by this candidate evidence.
- Current archive capacity cannot support the required retained30-day eight-stream run; fail-closed
  behavior is correct, but the external storage blocker remains.
- No30-day continuous retained data, restore drill or cross-source real-market quality report exists.
- Domestic L2 still requires legally authorized market data and remains fixture-certified only.
- The Windows native hard-exit/Coverage interaction remains a test-infrastructure risk; CI does not
  skip the test, and the formal evidence executes it in a separate mutually exclusive partition.
- GitHub Python3.10/3.11/3.12 CI on the evidence commit and a new independent read-only validation
  are still required before the software/fixture status can be promoted from pending.

All13 evidence artifacts are content-addressed, byte-preserved by a local`.gitattributes`, and
enumerated in`evidence-manifest.json`. Existing
`validation/m7-capture`, `validation/m7-capture-remediation-v2`and historical performance evidence
were not rewritten.
