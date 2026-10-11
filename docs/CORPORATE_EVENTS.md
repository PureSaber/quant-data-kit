# Corporate events v1

`quant_data_kit.us_research.corporate_events` provides `import_events(database, records)`
and `asof_events(database, timestamp)` for a local append-only SQLite store.

Required fields (unknown fields are rejected):

| Field | Meaning |
|---|---|
| event_id, revision | Stable deal ID; contiguous positive revisions starting at 1 |
| event_type | `merger` in version 1 |
| target, acquirer | Exact instrument IDs; acquirer may be empty only for cash consideration |
| currency | Three-letter settlement currency |
| announced_at, available_at | Timezone-aware announcement and revision availability; availability cannot precede announcement |
| expected_close_at | Expected completion time, an assumption known with the revision |
| status | announced, pending, completed, terminated |
| cash_per_share, exchange_ratio | Nonnegative decimal strings, with positive total consideration |
| source_ref | Reviewed source URL/document reference, or explicit synthetic reference |
| evidence_kind | synthetic or retrospective; the store does not certify historical PIT |

Reimporting the same canonical row is idempotent. A conflicting overwrite, noncontiguous
revision, nonincreasing availability, changed identity or reopened terminal event is
rejected transactionally. SHA-256 binds every payload to its database index; all rows
are validated on read. `asof_events` selects the last visible revision per deal, including
visible completed/terminated records. Consumers decide whether these remain eligible.

The immutable price bundle continues to use `puresaber.us-research/1`. Event terms and
raw prices must use the same share units. Historical accuracy, exhaustive deal coverage,
source licensing, publication timing verification and calibrated probabilities require
additional evidence. No automatic web scraping or financial-news interpretation is included.

Usage and synthetic-input generation: `quant-workspace/profiles/institutional-research/README.md`.
