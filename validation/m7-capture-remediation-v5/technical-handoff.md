# M7 epoch journal-binding remediation technical handoff

## 1. Scope and acceptance conditions

Scope is the two P2 findings from the independent v4 review:

1. bind a COMMITTED or failure snapshot to the actual logical content of the current sealed journal;
2. enforce closed fields, strict types and exact filename/attempt binding for every durable state.

Acceptance also retains the existing crash, path, lineage, quality, performance, capacity and
determinism gates. Public network capture,30-day retention, domestic real-market certification and
all order activity are excluded.

## 2. Modified files

Source commit`13e004a80296bb3a49c4bd54e64cb64670e56b01`modifies:

-`src/quant_data_kit/capture_v2/epoch.py`:closed state schemas, exact filename binding and
  deterministic journal-to-snapshot content verification.
-`tests/test_capture_v2_remediation.py`:valid-snapshot rebind reproduction, unknown/malformed
  terminal fields, filename attempt mismatch and helper branch tests.
-`src/quant_data_kit/_version.py`and`tests/test_m2_integration.py`:package0.7.4.
-`README.md`and`docs/m7-crypto-l2-capture.md`:operator-facing invariants.

The evidence-only follow-up adds`validation/m7-capture-remediation-v5`. Rollback is a Git revert;
no tag movement, history rewrite, data migration or deletion is required.

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

Results:431 passed,1 existing platform skip,0 failures; all17 core modules exceed90% pure branch
coverage and all source is87.08%; all six capture rounds pass; all three10M runs pass strict reload,
determinism, throughput, memory and retention gates; both CLI calls exit2 before network startup and
report0WebSocket messages. Exact files, hashes and byte lengths are in`evidence-manifest.json`.

## 4. Remaining risks and dependencies

- Independent validation and exact evidence-head GitHub CI remain release gates.
- Provision archive capacity above the150GiB reserve and20%/100GiB floor before any public probe.
- After capacity passes, execute bounded public probes, then retained30-day capture, restore drill
  and cross-source quality review.
- Obtain legally authorized domestic L2 before real-market domestic certification.
- Preserve semantic journal/snapshot recomputation if startup validation is later optimized.
