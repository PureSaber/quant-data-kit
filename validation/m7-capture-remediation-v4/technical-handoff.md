# M7 capture recovery remediation technical handoff

## 1. Scope and acceptance conditions

Scope is the independent validator's three P2 findings against the previous candidate:

1. prove the snapshot-before-receipt crash window with an actually terminated child process;
2. validate complete and independently anchored PREPARED/terminal bindings, including conflicts and
   legacy no-PREPARED failure records;
3. reject a reparse/junction bootstrap lock before the lock backend opens it.

Acceptance additionally requires the complete quality and branch gates, three dense and three
sparse fixture rounds, three retained10-million-event runs, real CLI capacity refusal, exact-source
GitHub CI and a new independent read-only verdict with no P1/P2 findings. Public network capture,
30-day retention, domestic market-data certification and real orders are explicitly excluded.

## 2. Modified files

Source commit`dfb53b02d1bf01ff592b4948475ebdc8e6dd301c`modifies:

- Recovery protocol, terminal bindings and reconciliation:
  `src/quant_data_kit/capture_v2/epoch.py`.
- Independent stream identity injection before runner construction:
  `src/quant_data_kit/capture_v2/collector.py`.
- Bootstrap-lock reparse safety:`src/quant_data_kit/capture_v2/storage.py`.
- Crash, mutation, type/schema, lineage, conflict, orphan and real-junction tests:
  `tests/test_capture_v2_remediation.py`and the impacted integration test.
- Package version and operator documentation:`src/quant_data_kit/_version.py`,
  `README.md`and`docs/m7-crypto-l2-capture.md`.

This evidence-only follow-up adds`validation/m7-capture-remediation-v4`. Rollback is a Git revert
of the source and evidence commits; no tag movement, history rewrite, data migration or deletion is
required.

## 3. Test commands, results and evidence

Executed command families:

```text
.venv-m7-verify/Scripts/python -m pip check
.venv-m7-verify/Scripts/python -m ruff check .
.venv-m7-verify/Scripts/python -m ruff format --check .
.venv-m7-verify/Scripts/python -m coverage run --branch -m pytest tests --deselect=<native-hard-exit> --junitxml=<main.xml>
.venv-m7-verify/Scripts/python -m coverage run --append --branch -m pytest <native-hard-exit> --junitxml=<hardexit.xml>
.venv-m7-verify/Scripts/python tools/check_branch_coverage.py <coverage.json>
.venv-m7-verify/Scripts/python tools/benchmark_capture_v2.py --scenario dense-burst --messages-per-stream 1000 --output-dir <round>
.venv-m7-verify/Scripts/python tools/benchmark_capture_v2.py --scenario sparse --messages-per-stream 500 --output-dir <round>
.venv-m7-verify/Scripts/python tools/benchmark_normalized_l2.py --rows 10000000 --runs 3 --batch-rows 262144 --minimum-events-per-second 100000 --maximum-peak-rss-gib 16 ...
.venv-m7-verify/Scripts/qdk-capture.exe <config> --mode preflight
.venv-m7-verify/Scripts/qdk-capture.exe <config> --mode run --confirm-long-running
```

Results:421 passed,1 existing platform skip,0 failures; all17 core modules exceed90% pure branch
coverage and all source is87.05%; all six capture rounds pass; all three10M runs pass strict reload,
determinism, throughput, memory and retention gates; both CLI calls exit2 before network startup and
report0WebSocket messages. Exact hashes and byte lengths are in`evidence-manifest.json`.

## 4. Remaining risks and dependencies

- Provision an independent archive target that preserves the150GiB reserve and remains above the
  20%/100GiB floor; then rerun preflight before any bounded public probe.
- After capacity passes, verify public Binance/OKX TLS and run bounded real-message probes, followed
  by retained30-day capture, archive restore and cross-source quality review.
- Obtain legally authorized domestic L2 before any real-market domestic certification.
- Retain the isolated native hard-exit partition until the Windows/PyArrow status-code interaction
  is explained; never skip the critical child-termination test.
- Require GitHub Python3.10/3.11/3.12 CI and a new independent read-only verdict before accepting
  this remediation candidate.
