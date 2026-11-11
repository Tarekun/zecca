# SEC Company Facts (silver)

Implementation: [etl/transformation/silver/sec_company_facts.py](../../../etl/transformation/silver/sec_company_facts.py)

One row per disclosed fact (shares outstanding, public float, or annual net income) across every SEC registrant, flattened out of the SEC's per-CIK XBRL "company facts" bulk data.

## Sources

- **`SecCompanyFacts`** ([etl/ingestion/sec.py](../../../etl/ingestion/sec.py)) — the SEC's bulk `companyfacts.zip` download, one JSON file per CIK (`dataplatform/raw/company_facts/CIK##########.json`), each holding every XBRL fact the company has ever disclosed under `facts.dei` (entity metadata) and `facts.us-gaap` (GAAP financials).
- **`CompanyTickersSilver`** — CIK → ticker mapping, used only to look up the opening price for `estimated_float_shares`.
- **`CandlesDailySilver`** — daily OHLC candles, used the same way.

## Implementation

For each CIK's JSON file, three fact series are extracted and unioned into one flat table (`_extract_rows_from_dict`):

- **Shares outstanding** — every `dei.EntityCommonStockSharesOutstanding` entry, one row each.
- **Public float** — every `dei.EntityPublicFloat` entry, one row each.
- **Annual net income** — `us-gaap.NetIncomeLoss` entries filtered down to genuine full fiscal years (duration 350–380 days), keeping the most recently filed entry per period end. This filters out the same figure re-tagged as a quarterly comparative under `fp="FY"` (10-Ks tag every fact — including prior-period comparatives — with the filing's own fiscal period).

Each metric keeps only its own columns populated; a row from one series has nulls in the other two series' columns. A CIK with none of the three facts still gets one row (common columns only, no metric) so no source file is silently dropped.

After the union, `_enrich_with_float_price` left-joins in a ticker (via CIK) and that ticker's opening price on `public_float_end`, computing `estimated_float_shares = non_affiliate_valuation / open`. If the ticker/candles dependencies aren't on disk yet, this column is filled with null instead of failing the build.

## Data quality checks

Declared on `SecCompanyFactsSilver` ([etl/transformation/silver/sec_company_facts.py:240](../../../etl/transformation/silver/sec_company_facts.py#L240)):

- **`not_empty()`** — fails if the model produces no rows at all. Every other check here is vacuously true on an empty frame, so this catches an upstream source silently returning nothing.
- **`is_finite(["estimated_float_shares"])`** — fails on any row where `estimated_float_shares` is infinite or NaN (e.g. a division by a zero opening price). Nulls pass — this only guards against non-finite results of the `non_affiliate_valuation / open` computation.
- **`not_null(["cik", "source_file"])`** — fails on any row missing `cik` or `source_file`; every row must be traceable back to the source JSON file it came from.
- **`accepted_values("shares_outstanding_fp", [...])`** — `shares_outstanding_fp` must be one of the SEC's known fiscal-period codes: `Q1`–`Q4`, `FY`, `T1`–`T3` (trimester filers, form-transition/foreign filers), `H1`/`H2` (semiannual filers), `CY` (calendar year). Nulls pass silently (`is_in` on null is null, not `True`, so `~is_in` drops out of the filter) — only non-null values outside this list fail.
- **`test_each_cik_has_at_least_one_metric`** — every CIK must have at least one row with a non-null `shares_outstanding` or `non_affiliate_valuation`; CIKs with neither are written to `sec_company_facts_missing_val.csv` for inspection.
- **`test_cik_count_matches_file_count`** — the number of distinct `cik` values in the model must equal the number of source JSON files under `dataplatform/raw/sec` (one row, possibly null, per file) — a sanity check that no source file was dropped or duplicated during extraction.

## Known issues

Company facts filed with the SEC span the full history of XBRL adoption (mandatory rollout 2009–2011) across every registrant, so the raw data is not uniformly well-tagged — occasional malformed or missing fields for individual filings are expected rather than exceptional. The checks above are what surface these; each failure should be triaged as a source-data quirk (documented here) or an actual transformation bug before deciding how to handle it.

### Empty `shares_outstanding_fp` for Microchip Technology (CIK 827054)

The `accepted_values` check on `shares_outstanding_fp` fails with 2 rows for Microchip Technology Incorporated, both with `shares_outstanding_fp = ""`.

**Root cause — one bad fact in the SEC source data.** Of Microchip's 69 `EntityCommonStockSharesOutstanding` entries, exactly one has `"fy": 0, "fp": ""` instead of a real fiscal year/period: accession `0000827054-10-000049`, a 10-Q filed 2010-02-09 for period end 2010-01-31. Every other entry (before and after) has a proper `fp` (`Q1`/`Q2`/`Q3`/`FY`). This filing lands right at the start of the SEC's mandatory XBRL rollout (2009–2010) and looks like the filer's DEI fiscal-period tag was left blank — yet EDGAR still assigned it a structured "frame" (`CY2009Q4I`). It's a gap in the source data, not a transformation bug, and not specific to Microchip as a business.

**Why it shows up as 2 rows instead of 1.** Microchip has two ticker symbols mapped to the same CIK in `company_tickers` (`MCHP` common, `MCHPP` preferred). `_enrich_with_float_price`'s `df.join(tickers, on="cik", how="left")` fans out *every* row for a CIK by its ticker count, so this single malformed fact is duplicated into 2 output rows — it's one anomaly, not two independent ones.

**Planned fix.** Normalize `""` to `None` when extracting `fp` in `_shares_outstanding_rows` (`e.get("fp") or None`), since an empty string isn't a real fiscal-period code — it's a missing tag. The `accepted_values` check already treats null as passing, so this stops the false failure without adding `""` as if it were a legitimate SEC fiscal-period designation.

### Thousands of CIKs with no metric at all

`test_each_cik_has_at_least_one_metric` currently fails with over 3000 violating rows.

**Root cause.** These rows correspond to registrants whose company-facts JSON contains none of the three facts this model reads (`EntityCommonStockSharesOutstanding`, `EntityPublicFloat`, `NetIncomeLoss`) — e.g. filers that never tagged those specific concepts, entities with only partial XBRL coverage, or registrants that only file forms where these facts aren't applicable. `_extract_rows_from_dict`'s fallback (`rows = [...] or [common]`) then emits a single row with just `cik`/`entity_name`/`source_file` set and every metric column null, which is exactly what this check flags as "no metric data."

This is expected at the current scale — most SEC registrants are not the kind of company these three metrics are meant to characterize — rather than a sign that extraction is broken for those files. It's listed here as a known, high-volume failure so it isn't mistaken for a regression; whether to keep flagging it, exclude these CIKs upstream, or relax the check is still open.
