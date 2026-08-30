# M7 Normalized epoch reconciliation technical handoff

## 1. Scope and acceptance conditions

Scope is the two P2 findings from the prior independent validation: eliminate the visible
Normalized-snapshot-before-receipt crash window, and apply full reparse/junction safety to every
epoch-journal filesystem operation. Acceptance requires immutable transaction states, deterministic
startup reconciliation before network creation, fail-closed validation of all bindings, a real
Windows junction rejection test, a process-termination recovery test, full quality and branch
gates, three dense and three sparse fixture benchmarks, three retained10-million-event runs, and
capacity refusal through the real CLI.

It explicitly excludes real orders, authenticated APIs, public-network certification,30-day live
capture, domestic market-data certification and any claim of GA readiness.

## 2. Modified files

Source commit`a4a5a16542a2c4c74edf8205b982c681248517a3`changes:

- Transaction protocol and reconciliation:`src/quant_data_kit/capture_v2/epoch.py`.
- Pre-network startup reconciliation:`src/quant_data_kit/capture_v2/collector.py`.
- Safe idempotent directory creation:`src/quant_data_kit/capture_v2/storage.py`.
- Negative, crash, retry, lineage and junction tests:
  `tests/test_capture_v2_remediation.py`, `tests/test_benchmark_capture_v2.py`, and the impacted
  integration test.
- Version and documentation:`src/quant_data_kit/_version.py`, `README.md`, and
  `docs/m7-crypto-l2-capture.md`.

This evidence commit only adds`validation/m7-capture-remediation-v3`; it does not rewrite historical
evidence. Rollback is a Git revert of the source and evidence commits; no data migration, tag
movement or deletion is required.

## 3. Test commands, results and evidence

Executed command families include:

```text
.venv-m7-verify/Scripts/python -m pip check
.venv-m7-verify/Scripts/python -m ruff check .
.venv-m7-verify/Scripts/python -m ruff format --check .
.venv-m7-verify/Scripts/python -m coverage run --branch -m pytest <main partition> --junitxml=pytest-main-junit.xml
.venv-m7-verify/Scripts/python -m coverage run --append --branch -m pytest tests/test_m2_process_integrity.py::test_atomic_normalized_and_curated_staging_recover_after_hard_exit --junitxml=pytest-hardexit-junit.xml
.venv-m7-verify/Scripts/python tools/check_branch_coverage.py coverage.json
.venv-m7-verify/Scripts/python tools/benchmark_capture_v2.py --scenario dense-burst --messages-per-stream 1000 --output-dir <round>
.venv-m7-verify/Scripts/python tools/benchmark_capture_v2.py --scenario sparse --messages-per-stream 500 --output-dir <round>
.venv-m7-verify/Scripts/python tools/benchmark_normalized_l2.py --rows 10000000 --runs 3 --batch-rows 262144 --minimum-events-per-second 100000 --maximum-peak-rss-gib 16 ...
.venv-m7-verify/Scripts/qdk-capture.exe <config> --mode preflight
.venv-m7-verify/Scripts/qdk-capture.exe <config> --mode run --confirm-long-running
```

Results:380 passed,1 existing platform skip,0 failures; all17 configured core modules exceed90%
pure branch coverage and all source is86.98%; all six capture rounds pass; all three10M runs pass;
both CLI invocations exit2 before network and report0WebSocket messages. Logs and exact hashes are
in`summary.md`, `evidence-manifest.json`, both JUnit artifacts, the coverage artifact, the benchmark
reports, the two run reports and the CLI invocation artifact.

## 4. Remaining risks and dependencies

- Provision an independent archive target that preserves the150GiB reserve while staying above the
 20%/100GiB floor, then repeat preflight before any bounded public probe.
- Confirm public Binance/OKX TLS access on the capture host, then run bounded real-message probes.
- Complete retained30-day capture, archive restore drill and cross-source quality review.
- Obtain legally authorized domestic L2 before real-market certification.
- Keep the native Windows hard-exit test isolated under formal Coverage until the
  `0xC0000005`interaction is explained or eliminated; do not skip it in CI.
- GitHub Python3.10/3.11/3.12 CI and independent read-only validation are required before accepting
  this remediation candidate.
