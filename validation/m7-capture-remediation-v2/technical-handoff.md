# M7 capture remediation technical handoff

## 1. Scope and acceptance conditions

Scope is the fail-closed public Crypto L2 collector, its immutable Raw/Normalized/archive path,
atomic dense L2 admission and the impacted Normalized performance gate. Acceptance requires all
identified durability, concurrency, path-safety and aggregate-failure invariants to be tested;
three deterministic dense and sparse eight-stream fixture runs; full quality and branch gates; and
three independent retained10-million-event Normalized runs on one clean source commit.

It explicitly excludes real orders, authenticated APIs,30-day live capture, domestic market-data
certification and any claim of GA readiness.

## 2. Modified files

The remediation commit series`6078f6f..a11a0c2`changes:

- Capture core:`src/quant_data_kit/capture_v2/{cli,collector,epoch,models,storage}.py`and exports.
- Dense atomic admission:`src/quant_data_kit/adapters_v2/base.py`,
  `src/quant_data_kit/l2_replay.py`, `src/quant_data_kit/normalized_v3.py`.
- Benchmark:`tools/benchmark_capture_v2.py`.
- Tests:`tests/test_benchmark_capture_v2.py`, capture core/remediation tests,
  `tests/test_l2_replay.py`, `tests/test_normalized_v3.py`,
  `tests/test_normalized_v3_failures.py`and the impacted integration test.
- Documentation/version:`docs/m7-crypto-l2-capture.md`,
  `src/quant_data_kit/_version.py`.

This evidence commit only adds`validation/m7-capture-remediation-v2`; it does not rewrite old
evidence. Rollback is a Git revert of the remediation commits; no data migration or tag movement is
required.

## 3. Test commands and evidence

Executed commands include:

```text
.venv-m7-verify/Scripts/python -m pip check
python -m ruff check .
python -m ruff format --check .
python tools/check_branch_coverage.py <content-addressed coverage.json>
python tools/benchmark_capture_v2.py --scenario dense-burst --messages-per-stream 1000 ...
python tools/benchmark_capture_v2.py --scenario sparse --messages-per-stream 500 ...
python -m quant_data_kit.capture_v2.cli <config> --mode preflight
python -m quant_data_kit.capture_v2.cli <config> --mode run --confirm-long-running
```

The full pytest/coverage and3x10M benchmark commands are captured by their immutable reports. Exact
results, hashes and artifact locations are in`summary.md`and`evidence-manifest.json`.

## 4. Remaining risks and dependencies

- Provision an independent archive target that preserves the150GiB reserve while staying above the
 20%/100GiB floor, then repeat preflight before any bounded public probe.
- Confirm public Binance/OKX TLS access on the capture host, then run bounded real-message probes.
- Complete retained30-day capture, restore drill and cross-source quality review.
- Obtain legally authorized domestic L2 before real-market certification.
- The first10M run has17.7% throughput headroom; monitor performance regression against this floor.
- The original delegated technical-lead task handle became unavailable after a host refresh. The
  project lead therefore re-ran and packaged the actual source-dependent evidence above; no missing
  subagent narrative is treated as acceptance evidence.
