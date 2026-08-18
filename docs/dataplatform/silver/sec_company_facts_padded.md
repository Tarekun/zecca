# SEC Company Facts Padded (silver)

Implementation: [etl/transformation/silver/sec_company_facts_padded.py](../../../etl/transformation/silver/sec_company_facts_padded.py)

One row per `(cik, reference_date)` calendar day, forward-filling each of the three [sec_company_facts](sec_company_facts.md) metrics (shares outstanding, public float, annual net income) independently so any date can be looked up directly instead of hunting for the most recent filing.

## Sources

- **`SecCompanyFactsSilver`** — the only source; this model is a pure reshape of it, no new raw data is read.

## Implementation

`_pad_series` expands one metric's sparse filing-event rows into a dense daily series for a single CIK:

- Rows are sorted by the metric's own `filed` date, and each entry is held valid from its `filed` date up to (but not including) the next entry's `filed` date — the most recent entry per CIK is held valid through today.
- Padding is anchored on the **filing date**, not the reported period end: a value only becomes public knowledge once filed, so padding forward from the period end would leak information before the market could have known it (look-ahead bias).
- Rows with a null period-end or filed date are dropped before padding.

`compute_from_source` reads `sec_company_facts`, drops `source_file`, and pads shares outstanding, public float, and annual net income separately (`_pad_series` three times) so that a gap in one metric's filings never affects another metric's forward-fill boundaries. The three padded series are then outer-joined on `(cik, reference_date)`, `last_filed` is computed as the max of the three metrics' filing dates active on that day, and `entity_name`/`ticker` are joined back in from the first row seen per CIK.

## Data quality checks

Declared on `SecCompanyFactsPaddedSilver` ([etl/transformation/silver/sec_company_facts_padded.py:197](../../../etl/transformation/silver/sec_company_facts_padded.py#L197)):

- **`not_empty()`** — fails if the model produces no rows at all.
- **`not_null(["cik", "reference_date", "last_filed"])`** — every row must have a CIK, a calendar date, and a most-recent-filing date; these three are never expected to be missing regardless of which metrics are actually populated that day.
- **`not_null(["ticker"])`** — every CIK should resolve to a ticker via `company_tickers`.
- **`unique(["cik", "reference_date"])`** — `(cik, reference_date)` must be a unique key; the padding/join logic should never produce two rows for the same company on the same day.
- **`column_comparison("reference_date", ">=", "last_filed")`** — a day's `reference_date` can never precede its own `last_filed` date, since a reference date's values are only ever forward-filled from a filing that has already happened.
- **`no_gaps("reference_date", group_by="cik")`** — for each CIK, every calendar day between its first and last `reference_date` must be present — the padding must produce a daily-continuous series with no missing dates.
- **`test_every_filing_filed_date_present_as_reference_date`** — every `(cik, filed_date)` pair from any of the three source metrics in `sec_company_facts` must appear as a `(cik, reference_date)` row here; padding must only fill gaps *between* filings, never drop a filing's own date. Violations are written to `sec_company_facts_padded_missing_filing_dates.csv`.

## Known issues

### `not_null(["ticker"])` fails

we have ~20k json files for the company_facts, but the `company_tickers` contain just about ~10k. We still don't know if this is caused by the fact that the `company_tickers` only tracks companies that are traded today (which would cause survivorship bias) or if the missing companies were never tradable to begin with (hence no ticker for the stock market).