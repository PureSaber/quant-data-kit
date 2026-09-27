# US research data v1

`quant_data_kit.us_research` is an explicit USD research namespace. It does not change the legacy six-digit domestic symbol adapters.

- `calendar.schedule`: XNYS sessions with UTC opens/closes, DST and half-days.
- `calendar.settlement_session`: standard T+2/T+1 date regimes from 2018.
- `prices.validate_prices/write_bundle/load_bundle`: OHLC/action validation and immutable hashed CSV snapshots.
- `prices.fetch_yahoo`: opt-in Yahoo retrospective split-normalized prices, not raw as-traded prices or historical constituent certification. Dividend payment dates remain unknown.
- `universe.members_asof`: source-backed membership events selected by effective session and knowledge time.
- `sec.download_sec/read_sec_snapshot`: explicit SEC contact identity, rate-limited snapshots, full submissions pagination and hash checks.
- `sec.filing_table/fact_table/facts_asof/annual_quality`: accession acceptance plus modeled processing lag, retained restatements, period-matched annual quality metrics. Facts with unknown acceptance are excluded and audited.

Install `.[us-research]` for optional calendar/Yahoo/SEC clients. Unit tests never require live data or contact credentials. `historical_pit` is source evidence supplied by the importer, not automatic certification by the package.

Settlement uses regular exchange holidays plus Columbus/Veterans bank holidays, independently of exchange-only ad-hoc closures. Saturday Veterans Day is not observed on Friday by Federal Reserve Banks. The daily research account models settled funds as available on the settlement date, without intraday clearing or broker-specific holds. Sources: [Federal Reserve bank holidays](https://www.federalreserve.gov/releases/k8/default.htm), [DTCC Veterans Day 2024](https://www.dtcc.com/-/media/Files/pdf/2024/10/11/a9503.pdf), [SIFMA January 2025 closure matrix](https://www.sifma.org/resources/guides-playbooks/unscheduled-close-market-matrix). Future or exceptional clearing changes require calendar review.
