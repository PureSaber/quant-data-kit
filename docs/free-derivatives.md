# Public derivative research sources

`qdk-derivatives free-fetch` adds bounded official-source ingestion. No API key,
broker connection or live order is involved. A successful request publishes an
immutable contracts/quotes/manifest bundle with source URLs, response hashes,
normalization assumptions, excluded records and allowed research modes.

| Provider | Explicit selection | Supported data | Boundary |
| --- | --- | --- | --- |
| `cboe-vix` | Actual VX expiry dates, comma separated | Cboe per-contract daily OHLC, settlement, volume and OI | VX futures only; select actual expiry, including holiday adjustments |
| `nse-fo` | `FinInstrmNm`, e.g. `NIFTY25JAN23500PE` | NSE UDiFF daily index futures/options | Historical `NewBrdLotQty` and actual expiry come from each row; stock/physical delivery unsupported |
| `deribit` | BTC/ETH option names | `trades`: historical public trades aggregated daily; `snapshot`: current vendor marks | USD analytical projection of native coin premiums; **analysis only** |

Example, run from an installed locked environment:

```powershell
qdk-derivatives free-fetch --provider cboe-vix --kind future --selection 2025-02-19,2025-03-18 --start 2025-01-08 --end 2025-01-10 --output D:/research/vx-v1 --rights-note "Personal local research; no raw redistribution"
qdk-derivatives free-fetch --provider nse-fo --kind option --selection NIFTY25JAN23500PE --start 2025-01-08 --end 2025-01-10 --output D:/research/nse-v1 --rights-note "Personal local research; no raw redistribution"
qdk-derivatives free-fetch --provider deribit --kind option --selection BTC-31JAN25-100000-C --source-mode trades --start 2025-01-08 --end 2025-01-10 --output D:/research/btc-v1 --rights-note "Personal local research; no raw redistribution"
```

Each request covers 1–32 inclusive dates and explicit contracts (20 maximum;
Deribit 10). A snapshot requires today's UTC date and unexpired options. The
source's daily session is retained in the normalized quote; date requests are
calendar bounds, not a claim of complete exchange-calendar coverage.

Daily zero-volume records are disclosed and excluded, not replaced by fabricated
OHLC. Missing NSE HTTP 404 dates are disclosed without calling them holidays.
Access denials stop the request without bypass or automatic retry. No incomplete
download is published. Deribit pagination is capped at 20 pages per instrument;
sequence gaps, overlaps or an ambiguous boundary fail closed. Same-millisecond
trades are ordered by their official sequence number.

For official files downloaded by the user, add `--files-directory PATH`. Use the
original `VX_YYYY-MM-DD.csv` or
`BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip` names. Files are read locally;
untrusted ZIP paths and oversized decompression are rejected.

Research defaults are analysis only. Cboe/NSE may enable modeled replay with both
`--initial-margin` and `--maintenance-margin` (per contract, contract currency).
These and `--tick` are **explicit research assumptions**, not historical exchange
margin certification. Samples must end before expiry; no final settlement is
inferred. Publication times and first-observed listing times in daily files are
conservative retrospective assumptions, not point-in-time evidence.

Deribit trade premiums use each trade's USD index. Snapshot marks use one shared
index fixing within a bounded capture interval; the source timestamps are kept.
These are calculation snapshots, not synchronized executable quotes. Native coin
settlement, inverse payoff, collateral and variable tick rules are not modeled
by the USD ledger. Bundle validation and the research/replay layers must enforce
`allowed_modes`. Multi-leg historical analysis also requires synchronized last
observations; using different trades as if simultaneous is rejected.

Public access does not grant redistribution rights. Raw downloaded responses and
customer data stay outside Git. Unit tests use synthetic official-shaped records.

Official references:

- [Cboe historical files](https://www.cboe.com/markets/us/futures/market-statistics/historical-data/futures)
- [VX specifications](https://www.cboe.com/tradable-products/vix/vix-futures/specifications)
- [NSE derivative reports](https://www.nseindia.com/all-reports-derivatives)
- [NSE index contract specifications](https://www.nseindia.com/static/products-services/equity-derivatives-contract-specifications)
- [Deribit public API](https://docs.deribit.com/)
