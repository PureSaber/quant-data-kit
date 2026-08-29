# M7 data certification handoff

## Scope and acceptance

Scope is limited to `quant-data-kit`: schema-exact Arrow batch normalization, PIT/sequence/L2
validation, Parquet partitions, lake-wide event claims, immutable publish/recovery, strict reload,
current OKX sequence semantics and the data performance gate. It does not implement or claim a
real network collector or30-day market-data certification.

Acceptance requires three independent10-million-row processes from one clean final commit; every
run must reach at least100,000events/second, remain below16GiB peak RSS, accept all valid rows,
produce deterministic artifacts, pass strict reload, retain its data and avoid the nearly full C
volume for temporary files.

## Modified files

- Core:`src/quant_data_kit/normalized_v3.py`, `data_lake.py`, `l2_replay.py`,
  `adapters_v2/base.py`, `adapters_v2/okx.py`, `__init__.py`, `_version.py`.
- Build/governance:`pyproject.toml`, `requirements.lock`, `.gitignore`, `.gitattributes`.
- Tests:`tests/test_normalized_v3.py`, `test_normalized_v3_failures.py`,
  `test_integrity_branch_gates.py`, `test_l2_replay.py`, `test_adapters_v2.py`,
  `test_benchmark_normalized_l2.py`, `test_m2_integration.py`,
  `test_m2_process_integrity.py`, and `tests/fixtures/providers/index.json`.
- Tools/docs:`tools/check_branch_coverage.py`, `tools/benchmark_normalized_l2.py`, `README.md`,
  `docs/m7-data-performance.md`, this handoff and the final JSON report.

Historical v2 snapshots and existing evidence were not rewritten. Rollback is a Git revert to the
v0.6.1 default-branch implementation; no old tag is moved or rebuilt.

## Tests, CI and performance evidence

```text
python -m pip check
python -m ruff check src tests tools
python -m ruff format --check src tests tools
coverage run --branch -m pytest -q
coverage json -o coverage.json
python tools/check_branch_coverage.py coverage.json
python -m pip wheel . --no-deps --no-build-isolation
```

- Ruff and `pip check`:PASS.
- Full suite:263 passed,1 skipped; skip is the declared Windows/POSIX symbolic-link case.
- Total source branch coverage:83.98%.
- Pure branch coverage:`normalized_v3.py`90.45%, `data_lake.py`90.32%,
  `l2_replay.py`95.83%; every configured core module≥90%.
- Wheel/runtime/import metadata all report`quant-data-kit 0.6.1`.
- GitHub Actions run`33251583464`:Python3.10/3.11/3.12 all SUCCESS.

Formal source commit:`009a36162a2ec1a48fc4f96b93b2e675196e9263`.

```text
TEMP=F:\puresaber-m7-temp
TMP=F:\puresaber-m7-temp
python tools/benchmark_normalized_l2.py \
  --work-root .m7\quant-data-kit-10m-final-clean2-009a361 \
  --output validation\performance\m7-data-arrow-10m-final-okx-contract.json \
  --rows 10000000 --runs 3 --batch-rows 262144 \
  --minimum-events-per-second 100000 --maximum-peak-rss-gib 16
```

| Run | Events/s | Peak RSS | Accepted/quarantined | Strict reload |
|---:|---:|---:|---:|---|
| 1 | 155,932.27 | 2.943GiB | 10,000,000/0 | PASS |
| 2 | 155,071.20 | 2.949GiB | 10,000,000/0 | PASS |
| 3 | 153,097.30 | 2.967GiB | 10,000,000/0 | PASS |

- All per-run throughput and memory gates:PASS; median155,071.20events/second.
- Git identity:`009a361...`, `dirty=false`; repetitions are independent processes.
- Snapshot logical SHA-256:`bc73be94fc984ec61fa6874b15b278a68a3b1e791919eb0fe434428c0d59bb6f`.
- Snapshot manifest SHA-256:`16c1b30dea551950c8d2f564c2adfab5a9f2c11ef83c8f895b90c6d5cbee58e4`.
- Final L2 checkpoint SHA-256:`3b4334614477d065224e446933cf5084c16b8c63caf17db7e2ffdb34cbb9710c`.
- Report SHA-256:`69416eeba389ff520043c9382ed7e1ff7380f4d5937030a02cb84cb1ab80c08f`.
- Retained data:3,310,487,778bytes under the recorded H-drive work root; cleanup=false.
- `TEMP`, `TMP`, and actual`tempfile.gettempdir()` all equal`F:\puresaber-m7-temp`.

## Remaining risks and dependencies

- The benchmark is schema-exact synthetic Binance-style L2:one snapshot plus sequential upserts.
  It certifies the normalization implementation, not real exchange data or all market regimes.
- Real Binance/OKX TLS collectors, snapshot bridging, reconnect/resync, eight target streams,
  continuous30-day evidence and archive/restore are the next serial task.
- OKX`checksum=0`is not an integrity signal. Non-zero CRC32 remains a labelled legacy fixture only;
  current admission relies on`seqId/prevSeqId`, handles equal-sequence empty heartbeats outside
  normalized deltas, and requires a fresh snapshot after maintenance reset.
- H contains multiple retained10M calibration/formal data sets. No automatic deletion occurred;
  cleanup requires a separate exact-target review and is not authorized by this handoff.
- PR#6 must remain open until independent read-only validation and cross-repository certification
  accept this evidence. No merge or tag is authorized here.

- PR:[PureSaber/quant-data-kit#6](https://github.com/PureSaber/quant-data-kit/pull/6).
- Final JSON:`validation/performance/m7-data-arrow-10m-final-okx-contract.json`.
