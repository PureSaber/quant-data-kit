# M7 capture core remediation candidate evidence

## Decision boundary

- Source commit:`a11a0c2c7fe50f4e0cb7d2bcc0a8466d4a11d21c`, clean working tree.
- Branch:`codex/cross-asset-v2-m7-data-performance`; PR`PureSaber/quant-data-kit#6`
  remains open and unmerged.
- Software, deterministic fixture and local performance gates pass pending independent read-only
  validation.
- Public Binance/OKX network capture was not run. Continuous days remain0 and
  `market_data_certified=false`.
- No API key, account endpoint, order route, live order or destructive cleanup was used.

## Remediated invariants

The candidate makes Raw audit durability precede state publication, persists honest preflight and
terminal outcomes, fails the aggregate run if any stream fails, cancels and drains peer tasks,
publishes or aborts Normalized epochs transactionally, rejects cross-process path replacement and
reparse/junction targets, freezes the exact eight-stream identity, and enforces a bounded
coordination budget. Dense same-sequence multi-level L2 updates are now applied atomically with one
post-group book validity check and complete rollback on failure.

## Quality gates

Environment: isolated`.venv-m7-verify`, Python3.12.5, package0.7.1,
`requirements.lock`SHA-256
`fac9239809b2bb42c2d7ee99417a3053075a177eb9b08131a954288566de6d88`.

| Gate | Result |
|---|---|
| Isolated`pip check` | PASS; no broken requirements |
|`ruff check .` | PASS |
|`ruff format --check .` | PASS;86 files unchanged |
| Full pytest | PASS;356 tests,0 failures,0 errors,1 existing Windows/POSIX skip |
| Full source pure branch coverage | PASS;2035/2342=86.89%, required>=80% |
| Every configured core module | PASS;>=90% pure branch coverage |
|`normalized_v3.py` |498/548=90.88% |
|`l2_replay.py` |103/106=97.17% |
|`adapters_v2/base.py` |30/32=93.75% |

## Deterministic eight-stream capture benchmarks

Both fixture scenarios include exact Raw frames, Raw segment publication, Normalized publication,
archive copy, restore hash verification and complete worker shutdown. Each run reports source
commit`a11a0c2...`, `working_tree_dirty=false`, consistency true and gate true.

| Scenario | Rounds | Messages/round | Normalized rows/round | Messages/s | Rows/s | p99 |
|---|---:|---:|---:|---:|---:|---:|
| Dense20+20 levels/update |3 |8,000 |319,688 |263.83/273.02/293.09 |10,542.72/10,909.99/11,712.10 |46.01/47.88/42.19ms |
| Sparse1+1 level/update |3 |4,000 |7,992 |605.37/626.68/628.98 |1,209.52/1,252.11/1,256.70 |17.92/15.54/15.59ms |

The dense gate requires at least240messages/s, scenario-density rows at3x the frozen80messages/s
live offer rate, and p99 at most100ms. All three dense rounds pass. A prior smaller dense500
diagnostic included one237.52messages/s result; it is retained outside this candidate set and is not
silently promoted to passing evidence.

## Three independent10-million-event runs

All runs use the exact candidate source in clean state, execute in independent processes, accept
all10,000,000 valid events, quarantine0, pass strict reload, and retain3,310,487,778bytes without
cleanup.

| Run | Events/s | Peak RSS | Result |
|---:|---:|---:|---|
|1 |117,722.24 |3.092GiB | PASS |
|2 |186,347.92 |3.116GiB | PASS |
|3 |195,061.58 |3.110GiB | PASS |

The deterministic snapshot, manifest and final L2 checkpoint hashes match across all three runs.
The first run has only17.7% throughput headroom over the100,000events/s gate and remains a visible
performance risk rather than a hidden exception.

## Capacity refusal

The exact candidate was invoked in both safe`preflight`mode and explicit`run
--confirm-long-running`mode. Both returned exit code2 and immutable
`PAUSED_PREFLIGHT_FAILED`reports before network startup because the archive projection after the
150GiB reserve was66,346,708,992bytes, below the199,710,735,564-byte safety floor. All eight
streams report0WebSocket messages and`long_running_capture_started=false`.

## Remaining certification risks

- Public Binance/OKX TLS, snapshot bridge, reconnect/resync and real-message data quality remain
  unverified by this candidate evidence.
- Current archive capacity cannot support the required retained30-day eight-stream run; fail-closed
  behavior is correct, but the external storage blocker remains.
- No30-day continuous retained data or cross-source real-market quality report exists yet.
- Domestic L2 still requires legally authorized market data and remains fixture-certified only.
- A rejected rolling-epoch experiment showed claim/lock growth and was fully reverted before the
  candidate commit. The retained diagnostics are not part of the passing artifact set.
- GitHub Python3.10/3.11/3.12 CI and independent read-only validation are still required before the
  software/fixture status can be promoted from pending.

All evidence files are content-addressed and enumerated in`evidence-manifest.json`. Existing
`validation/m7-capture`and historical performance evidence were not rewritten.
