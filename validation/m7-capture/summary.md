# M7 public Crypto L2 capture verification

## Scope and boundary

- Source baseline:`bb796ad49f5c69e9b31d1813d9ca12641755f876`.
- Branch:`codex/cross-asset-v2-m7-data-performance`.
- Pull request:`PureSaber/quant-data-kit#6`; it must remain open.
- Public market data only: no API key, account endpoint, order or cleanup operation was used.
- The eight-stream deterministic integration fixture covers Binance/OKX BTC/ETH Spot and USDT
  perpetual L2 from Raw exact bytes through independent archive/restore verification into the
  existing Normalized contract.
- This evidence does not represent30 continuous capture days and does not establish
  `market-data-certified`status.

## Local quality gates

Environment: isolated`.venv-m7-verify`, Python3.12.5, locked dependencies from
`requirements.lock`, editable package0.7.0.

| Gate | Result |
|---|---|
| Locked install | PASS;`websockets==15.0.1`installed from the regenerated lock |
|`pip check`before and after editable install | PASS; no broken requirements |
| Runtime/build metadata | PASS; both report`0.7.0` |
|`ruff check src tests tools` | PASS |
|`ruff format --check src tests tools` | PASS |
| Full pytest | PASS;299 passed、1 skipped、0 failed |
| Skip review | Existing Windows skip only:`POSIX symbolic-link regression`; no`capture_v2`test skipped |
| Full source pure branch coverage | PASS;1800/2092=86.04%, required>=80% |

New capture core pure branch coverage:

| Module | Covered/total | Result |
|---|---:|---|
|`capture_v2/models.py` |72/74=97.30% | PASS |
|`capture_v2/storage.py` |71/78=91.03% | PASS |
|`capture_v2/synchronizers.py` |90/90=100.00% | PASS |
|`capture_v2/epoch.py` |26/28=92.86% | PASS |
|`capture_v2/transport.py` |12/12=100.00% | PASS |
|`capture_v2/collector.py` |47/52=90.38% | PASS |
|`capture_v2/cli.py` |37/38=97.37% | PASS |

Evidence:

-`validation/m7-capture/coverage.json`, SHA-256
  `f10c7056c07202e9df80387fc5f98968e6d2db446a3d255e5dbc8e7aea93005b`.
-`validation/m7-capture/pytest-junit.xml`, SHA-256
  `12d46df3d93b2c577bfb23724f77cdb0ce74e182ea52a95f01c2c23fe4fe7a97`.

## Machine preflight and public TLS probe

At execution time, Windows reported F and H as different volume IDs. The explicit local
configuration used F for hot/restore and H for archive. The archive reserve was150GiB. Both safe
preflight and an explicitly confirmed`run`attempt returned`PAUSED_PREFLIGHT_FAILED`before network
startup because projected archive free bytes were74,011,439,104, below the current20% archive
floor199,710,735,564. All eight stream reports recorded:

-`final_state=PAUSED`;
-`websocket_messages=0`;
-`archive_preflight_receipt=null`;
-`long_running_capture_started=false`;
-`continuous_days=0`and`market_data_certified=false`.

The immutable run reports are retained outside the repository at:

-`F:\puresaber-m7-capture-hot\capture\run-reports\capture-61310df5526f4abb8828c9e08313a325.json`;
-`F:\puresaber-m7-capture-hot\capture\run-reports\capture-39b3ae579fa1400ab9a20863bffff15a.json`.

A transport-only public TLS handshake probe was attempted without subscribing or receiving data.
Both Binance and OKX were reset by the current host network with`ConnectionResetError (WinError64)`.
This is retained as a visible environment limitation, not reported as a successful network test.
No collector probe was started and the archive gate was not bypassed.

## Remaining certification work

- GitHub CI must run the locked matrix on Python3.10、3.11 and3.12 for the final pushed commit.
- A future operator-provisioned archive volume must pass the physical identity, capacity and
  restore-hash preflight before any bounded collector probe.
- Real Binance/OKX TLS access remains unverified on this host because both public handshakes were
  reset by the network.
- 30-day continuous retained data, cross-source data-quality review and real market-data
  certification remain explicitly incomplete.
