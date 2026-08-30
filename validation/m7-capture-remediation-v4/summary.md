# M7 capture recovery and terminal-binding candidate evidence

## Decision boundary

- Source commit:`dfb53b02d1bf01ff592b4948475ebdc8e6dd301c`; every source-dependent
  quality, performance and capacity run recorded a clean working tree.
- Branch:`codex/cross-asset-v2-m7-data-performance`; PR`PureSaber/quant-data-kit#6`
  remains open, unmerged and untagged.
- This candidate supersedes only the software/fixture acceptance decision from
  `validation/m7-capture-remediation-v3`. Historical evidence is unchanged.
- Software, deterministic fixture and local performance gates pass pending GitHub CI and a new
  independent read-only validation of the three previously reported P2 findings.
- Public Binance/OKX capture was not run. Continuous days remain0 and
  `market_data_certified=false`. No credentials, authenticated API, account endpoint, order route,
  live order or destructive cleanup was used.

## Closed findings and enforced invariants

The Normalized epoch protocol persists immutable`PREPARED -> COMMITTED | ABORTED`states. A real
spawned child process publishes a snapshot, persists a boundary marker and calls`os._exit`before
the COMMITTED receipt. A fresh parent process proves that PREPARED and the snapshot survived,
reconciles them exactly once before network-runner construction, and then proves a second
reconciliation is a no-op. On this Windows/PyArrow host an abrupt native termination may be
reported as a nonzero native status instead of the requested87; the marker and durable filesystem
state bind the tested crash window without accepting a normal exit.

Recovery now anchors every stream to the frozen collector configuration rather than trusting a
journal-provided provider or venue. It validates exact schema versions, canonical UTC time, strict
integer and Boolean types, policy, attempt, provider, venue, records, sealed part hashes and row
counts, available and unique Raw lineage, snapshot identity and row arithmetic for every PREPARED,
COMMITTED, explicit ABORTED and retryable failure record. Duplicate PREPARED states, multiple or
conflicting terminal states, terminal-then-pending work, orphan journals and legacy failures with no
durable PREPARED identity all block startup.

The path-bootstrap lock is validated before the file-lock backend may open it, created exclusively,
fsynced, then revalidated as a regular non-reparse file before and inside the process lock. A real
Windows directory junction at the lock path is rejected before the mocked lock backend is entered;
the outside sentinel is unchanged and the requested capture directory is not created.

## Quality gates

Environment: isolated`.venv-m7-verify`, Python3.12.5, package0.7.3,
`requirements.lock`SHA-256
`fac9239809b2bb42c2d7ee99417a3053075a177eb9b08131a954288566de6d88`.

| Gate | Result |
|---|---|
|`pip check` | PASS;no broken requirements |
|`ruff check .` | PASS |
|`ruff format --check .` | PASS;90 files unchanged |
| Mutually exclusive pytest partitions | PASS;421 passed,0 failed,1 existing platform skip |
| Full source pure branch coverage | PASS;2171/2494=87.05%,required>=80% |
| Every configured core module | PASS;all17 modules>=90% |
|`capture_v2/epoch.py` |207/224=92.41% |
|`capture_v2/collector.py` |85/94=90.43% |
|`capture_v2/storage.py` |125/136=91.91% |

The main partition has420 passes, one existing skip and one deliberately deselected native
hard-exit case. The isolated partition executes that one case and passes. The JUnit artifacts prove
the two partitions are mutually exclusive and together cover the full collection.

## Deterministic eight-stream capture benchmarks

Each fixture round includes exact Raw frames, Raw segment publication, Normalized publication,
archive copy, restore hash verification and complete worker shutdown. Every report records the
exact source commit,`working_tree_dirty=false`, consistency true and gate true.

| Scenario | Rounds | Messages/round | Normalized rows/round | Messages/s | p99 | Safety multiple |
|---|---:|---:|---:|---:|---:|---:|
| Dense20+20 levels/update |3 |8,000 |319,688 |306.30/308.89/310.79 |37.70/37.61/37.07ms |3.83/3.86/3.88x |
| Sparse1+1 level/update |3 |4,000 |7,992 |572.64/571.50/575.56 |16.44/16.32/18.28ms |7.16/7.14/7.19x |

The dense gate requires at least240messages/s, scenario-density rows at3x the frozen80messages/s
live offer rate, and p99 at most100ms. All six rounds pass.

## Three independent10-million-event runs

All runs execute in independent processes, accept all10,000,000 valid events, quarantine0, pass
strict reload, and retain3,310,487,778bytes without cleanup.

| Run | Events/s | Peak RSS | Result |
|---:|---:|---:|---|
|1 |188,642.01 |3.122GiB | PASS |
|2 |188,341.75 |3.107GiB | PASS |
|3 |190,168.20 |3.101GiB | PASS |

The snapshot logical hash, snapshot manifest hash and final L2 checkpoint hash match across all
three runs. Minimum throughput has88.3% headroom over the100,000events/s gate and peak memory is
well below16GiB.

## Capacity refusal and CLI evidence

The installed`qdk-capture.exe`was invoked in safe`preflight`mode and explicit
`run --confirm-long-running`mode. Both returned exit code2 and immutable
`PAUSED_PREFLIGHT_FAILED`reports before network startup. After the150GiB archive reserve, projected
capacity was about59.68GB versus the199,710,735,564-byte safety floor. All eight streams report0
WebSocket messages and`long_running_capture_started=false`.

The content-addressed CLI invocation artifact binds exact argv, config hash, source commit, exit
codes, run IDs and both report hashes.

## Remaining certification risks

- Public Binance/OKX TLS, snapshot bridge, reconnect/resync and real-message quality remain
  unverified because fail-closed capacity preflight correctly blocked network startup.
- Current archive capacity cannot support the required retained30-day eight-stream run. No30-day
  continuous dataset, archive restore drill or cross-source real-market quality report exists.
- Legally authorized domestic L2 remains unavailable and is fixture-certified only.
- The Windows native hard-exit/Coverage interaction remains an infrastructure risk; the critical
  crash test is not skipped and validates a real nonzero child termination plus durable boundary.
- GitHub Python3.10/3.11/3.12 CI and an independent read-only review are required before promoting
  software/fixture status from pending.

All17 artifacts are content-addressed and byte-preserved by the local`.gitattributes`. Historical
evidence directories were not rewritten.
