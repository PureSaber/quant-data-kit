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
