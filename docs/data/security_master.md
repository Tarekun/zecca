# Security master (`silver.security_master`)

Every company that ever filed periodic reports with the SEC, including those that were delisted, acquired or went bankrupt. Companies are keyed by **CIK**, which never changes, and each company has the tickers it traded under over time. This is the survivorship-free universe for #111. Built in #122.

## Using it

```python
from etl.transformation.silver.security_master import listed_at, ticker_to_cik

listed_at(date(2008, 6, 2))                       # every company alive that day
listed_at(date(2008, 6, 2), require_ticker=True)  # only those with a known ticker
ticker_to_cik("FB", date(2020, 1, 2))             # 1326801 (Meta)
ticker_to_cik("GOOG", date(2012, 1, 3))           # 1288776 (Google Inc, not Alphabet)
```

Each row means: from `valid_from` (inclusive) to `valid_to` (exclusive, null = still valid), company `cik` was named `company_name` and traded as `ticker`.
- A company with several share classes has one row per class for the same period (GOOG and GOOGL).
- A ticker change closes one row and opens the next (FB until 2022-07-27, then META).
- Periods with no known ticker have a row with a null `ticker`. `has_known_ticker = false` flags companies with no ticker at all, such as funds, ABS trusts, shells and most companies that died before 2011.
- `source` says where the ticker comes from:
  - `xbrl`: declared on the cover page of a filing
  - `xbrl_filename`: read from the name of a filing's XBRL files (`cpwr-20131231.xml`), see below
  - `filing_text`: named in the text of an annual report, for companies gone before XBRL, see below
  - `company_tickers`: only in today's mapping
  - `backfill`: assumed, see below
  - `full-index`: no ticker, only proof the company existed

Pass `include_backfill=False` to get only observed tickers.

## Sources (raw layer)

| Source | What it gives | Size | Incremental |
|---|---|---|---|
| `raw.edgar_full_index` (EDGAR `full-index/{year}/QTR{n}/master.gz`) | Every filing since 1993: CIK, name at the time, form type, date | 27M rows, 660 MB | A quarter downloaded more than 7 days after it ends is final and never downloaded again. A rerun only fetches the current quarter. |
| `raw.xbrl_cover_pages` (DERA *Financial Statement and Notes* datasets) | `dei:TradingSymbol`, `SecurityExchangeName` and `Security12bTitle` of every XBRL filing (10-K, 10-Q, 8-K, 20-F, ...) since 2009 | 77 zips, ~28 GB downloaded, 46 MB kept | Published datasets don't change, so only new ones are downloaded. Each zip is streamed to a temporary file and deleted after parsing. |
| `raw.xbrl_filings` (EDGAR monthly XBRL feeds `edgar/monthly/xbrlrss-YYYY-MM.xml`) | Every XBRL filing since April 2005 with the names of its instance and schema files | 260 feeds, ~0.5 GB downloaded gzipped, 13 MB kept | Like the full-index, per month: a month downloaded more than 7 days after it ends is never downloaded again. |
| `raw.filing_text_tickers` (annual reports in `edgar/data/...`) | The symbol named in the last annual report (10-K, 20-F, ...) of each company with evidence of public shares that never filed XBRL; the report before it when the last names none | One download per company, up to 10 MB each (~2 reports a second) | A report already read is never read again, and a company whose symbol was found is done. |
| `raw.company_tickers` / `raw.company_tickers_exchange` | Today's CIK, ticker and exchange | <1 MB | Full download every run |

What was checked and left out:
- **`submissions.zip`** (1.5 GB) has no ticker for inactive companies (checked on Twitter and Lehman). For current companies it holds the same tickers as `company_tickers_exchange.json`, and the full-index already has the company names over time.
- **XBRL `companyfacts`** only has numeric facts, so no tickers.
- **The DERA datasets** also carry the instance file name (`sub.instance`), but getting it from there means downloading the notes datasets again (~40 GB) instead of the monthly feeds (~0.5 GB).

## How it's built

**When a company exists**
- **Start:** the earliest of its first periodic report (10-K, 10-Q, 20-F or 40-F, including the `10KSB`/`10QSB` and transition variants), its registration (8-A, Form 10, 20-FR), its successor-issuer filing (8-K12B, as for Google → Alphabet), or its first declared ticker.
- **End:** its delisting (Form 25 / 25-NSE) or deregistration (Form 15), whichever comes first. A Form 25 counts only if filed between 1 year before the last periodic report and the date the next report was due:
  - Earlier Form 25s, followed by years of reports, were a transfer to another exchange or a single class of notes.
  - Inside that window the *last* Form 25 is used. Issuers with listed notes file one each time a series matures: Lehman filed dozens, the first one two months before the stock was delisted. Taking the last one keeps a dying company in the universe a little longer and never drops it before its collapse.
- **No Form 25 or 15:** the company ends 6 months after its last report (15 months for 20-F/40-F filers).
- **Form 25 or 15 before the first report:** the company's life predates EDGAR and the report was a final one, filed after it had already deregistered. It keeps a single zero-length row, so it's in the master but never in the universe (about 7,900 companies, none with a ticker).
- Companies in today's `company_tickers.json` are always still alive, with null `delisting_date` and `deregistration_date`.

**Which ticker a company had**

Tickers come from three kinds of evidence, strongest first:
1. **Declared on the cover page** (`dei:TradingSymbol`), mandatory since 2019 and optional before.
2. **The XBRL file names.** The EDGAR Filer Manual asks for `<prefix>-<period date>.xml` with the prefix being "the registrant's ticker symbol or other mnemonic abbreviation". On filings that also declare a ticker, the prefix matches it in 98-99% of the 2009-2018 reports. This is what gives a ticker to the companies that left between 2009 and 2019 without ever declaring one (Compuware, Aetna, Staples, St. Jude, Dow Chemical). Only periodic reports are used, and a filing that declares its ticker never uses its file name. A prefix is ignored when:
   - it's a placeholder (`none`, `ck0001234567`, `fil`) or is used by three or more companies in the same year (a filing agent's template, such as GlobeNewswire's `gnw-20190101.xsd` on 8-Ks)
   - the company used it on filings declaring a different ticker. Then it's a mnemonic, not a ticker: `psx` for Phillips 66 Partners (PSXP). If those filings always declared one same ticker, the prefix is read as that ticker instead (`pseg` → PEG), back to the last change of company name.
   - the company had no publicly held shares anymore. "Publicly held" means Schedules 13D/13G (only filed about a registered class of equity) or proxy statements in the year, and the prefix is dropped when they stopped more than 2 years before. Puget Energy went private in 2009 and still files as `psd` for its debt. A report dropped this way ends any ticker still open.
   - two companies used it at the same time: over the common period it goes to the one with more equity evidence, which is how a parent beats a subsidiary filing under the parent's ticker (Great Plains Energy's `gxp` on Kansas City Power & Light's reports). The XBRL feeds list a combined report (a holding company and its subsidiaries filing together) under one co-registrant only, often the subsidiary, so the prefix of a combined report is attributed to all its co-registrants first (PSE&G's `peg` → PSEG).
   - it appears on a single report of a company with no equity evidence that year.
3. **The text of the last annual report**, only for companies with no ticker from the two sources above, which are the companies that never filed XBRL. Item 5 of a 10-K (Item 9 of a 20-F) almost always says "traded on the NYSE under the symbol XYZ", and press releases say "(NASDAQ: XYZ)". The most mentioned symbol wins; OTC markers are dropped (`XYZ.OB` → `XYZ`) and SPAC units and warrants ignored. Tested on 150 companies whose ticker XBRL gives, using their last 10-K before XBRL: 103 right, 7 another company's symbol (dropped by the conflict rule below), 10 wrong, 28 nothing found. So about 91% of the tickers the model keeps from this source are right; the typical errors are a debt or preferred symbol of the same group (Dominion: DRU instead of D) or a company named in the report.
4. **Today's mapping** (`company_tickers.json`), always open-ended even if some filings leave the ticker out.

A ticker is valid from the first filing showing it until the first later filing that no longer does, so the change date is only as precise as the filing frequency. Since 2019 every 8-K also declares the ticker, so that's usually within weeks.

A company whose only reports showing a ticker were filed within a year after its delisting (its final 10-Q or 10-K, often its first in XBRL) had that ticker when it left: the ticker is kept up to the delisting and backfilled from there (Cogo, MedQuist, Allis-Chalmers).

**Backfill.** Before its first observation, a company's earliest ticker is assumed to go back to when the company started, but never past:
- a change of company name (ignoring legal forms and state tags, so `COMPUWARE CORPORATION` and `Compuware Corp /MI/` are the same name). A rename usually comes with a new ticker, and a CIK can be a shell reused by an unrelated business: CIK 827099 was Creative Vending, then TetriDyn, then Ocean Thermal, which took CPWR in 2017. Without this limit CPWR was backfilled to 1996, over Compuware.
- a silence of more than two years in its periodic reports (a shell revived as a new business).

A backfill survives only if the ticker it extends survives the conflicts below.

Declared values are cleaned up:
- `BAX (NYSE)` → `BAX`
- `BRK.B` → `BRK-B`, matching today's spelling
- `GOOG, GOOGL` → two tickers
- placeholders (`N/A`, `None`, `false`) and preferreds written as three words (`JPM PR C`) are dropped. `NA` and `TRUE` are real tickers (Nano Labs, TrueCar), so they're kept only for the company holding them today.
- facts of co-registrants are ignored
- before 2019, a declaration without an exchange by a company that no longer had publicly held shares is ignored (Serena Software, private since 2006, declared SRNA until 2014)

**Conflicts:** a ticker belongs to one company at a time. When two companies claim it for the same period, priority goes in this order:
1. today's owner
2. the stronger evidence: declared or current, then file name, then annual report text, then backfill
3. a company that was exchange-listed (8-A12B, Form 25)
4. the company seen with it more often (for example, a subsidiary that repeats its parent's ticker loses)

The loser keeps only the period that doesn't overlap, with one exception: a file-name or text ticker that another company was already showing with stronger evidence within a year of its first use was never this company's ticker, and is dropped whole (ALCO Inc named its files `alco` while Alico traded as ALCO). A later overlap is just a former owner's stale prefix and is trimmed: Dow Chemical keeps DOW until Dow Inc. takes it in 2019.

## Quality checks

- `valid_from <= valid_to`
- unique `(cik, ticker, valid_from)`
- ticker format
- no overlapping intervals for the same ticker across different CIKs
- no overlapping intervals for the same `(cik, ticker)`
- null-ticker rows don't overlap other rows of the same company
- every entry of today's `company_tickers.json` has an open-ended row
- no row of a delisted or deregistered company is valid after that date
- tickers contain a letter and aren't placeholders
- every backfilled row is followed by an observation of the same ticker for the same company, under the same name
- at least 93% of the companies delisted (terminal Form 25) after mid-2011, when every filer had to file XBRL, have a ticker (`min_share` check). It's 96.2% today; the rest are mostly BDCs, which only had to file XBRL from 2022, and foreign issuers with a single report and no equity evidence.

The cases from the issue are tested twice: on synthetic data in `tests/transformations/test_security_master.py`, and on the real data in `tests/data_quality/silver/test_security_master_known_cases.py`. The file-name rules have their own synthetic cases, each modelled on a real company, in `tests/transformations/test_security_master_filenames.py`.

## Coverage (built on 2026-11-07)

51,375 CIKs in the master versus 8,004 in today's `company_tickers.json`. 18,671 of them have a known ticker at some point.

**Delisted companies with a ticker**, by year of their last periodic report (companies with a terminal Form 25, so certainly exchange-listed). "Before" is the master with cover page tickers only.

| Last report | Companies | Cover pages only | + file names | + annual report text |
|---|---|---|---|---|
| 2004-2008 | 1,476 | 0% | 0% | 0% |
| 2009 | 345 | 0% | 1% | 75% |
| 2010 | 340 | 2% | 9% | 80% |
| 2011 | 306 | 14% | 46% | 84% |
| 2012 | 317 | 29% | 81% | 86% |
| 2013 | 291 | 40% | 85% | 88% |
| 2014 | 262 | 40% | 86% | 90% |
| 2015 | 338 | 52% | 90% | 94% |
| 2016 | 325 | 49% | 89% | 95% |
| 2017 | 302 | 57% | 91% | 95% |
| 2018 | 281 | 62% | 94% | 96% |
| 2019 | 285 | 72% | 96% | 98% |
| 2020 | 242 | 87% | 96% | 98% |
| 2021 | 296 | 92% | 97% | 98% |
| 2022-2026 | 2,061 | 99% | 99% | 100% |

The annual report text has only been read for companies whose last report is from 2009 or later (2,620 companies); before that the source hasn't been run yet, hence the 0% up to 2008.

**Companies with publicly held shares that year** (a Schedule 13D/13G or a proxy statement) alive at year end, and how many have no ticker:

| Year | Public companies alive | Without ticker | Share |
|---|---|---|---|
| 1996 | 7,037 | 5,453 | 77% |
| 2000 | 8,879 | 6,075 | 68% |
| 2004 | 7,883 | 3,919 | 50% |
| 2008 | 7,435 | 1,559 | 21% |
| 2010 | 6,655 | 628 | 9% |
| 2012 | 6,143 | 320 | 5% |
| 2016 | 5,971 | 287 | 5% |
| 2020 | 5,879 | 146 | 2% |
| 2024 | 6,138 | 112 | 2% |

All companies alive at year end (2026: June 30). Columns:
- **with ticker:** has a ticker that day, observed or backfilled
- **observed ticker:** declared, read from a file name or named in an annual report, not backfilled
- **in / not in `company_tickers.json`:** whether the CIK is in today's mapping; "not in" is the companies a survivors-only universe misses

| Year | Alive | With ticker | Observed ticker | Distinct tickers | In `company_tickers.json` | Not in it |
|---|---|---|---|---|---|---|
| 1994 | 2,352 | 539 | 0 | 567 | 457 | 1,895 |
| 1996 | 12,294 | 2,092 | 0 | 2,124 | 1,346 | 10,948 |
| 1998 | 13,319 | 2,611 | 0 | 2,646 | 1,569 | 11,750 |
| 2000 | 14,596 | 3,273 | 1 | 3,319 | 1,822 | 12,774 |
| 2002 | 14,312 | 4,076 | 1 | 4,139 | 2,176 | 12,136 |
| 2004 | 13,482 | 4,805 | 1 | 4,913 | 2,413 | 11,069 |
| 2006 | 12,684 | 5,958 | 12 | 6,075 | 2,674 | 10,010 |
| 2008 | 12,072 | 7,414 | 116 | 7,561 | 2,995 | 9,077 |
| 2010 | 10,960 | 8,151 | 1,734 | 8,352 | 3,241 | 7,719 |
| 2012 | 10,243 | 8,350 | 7,336 | 8,621 | 3,533 | 6,710 |
| 2014 | 10,024 | 8,246 | 7,165 | 8,573 | 3,910 | 6,114 |
| 2016 | 9,372 | 7,757 | 6,656 | 8,128 | 4,239 | 5,133 |
| 2018 | 8,988 | 7,519 | 6,656 | 7,914 | 4,688 | 4,300 |
| 2019 | 8,787 | 7,377 | 6,540 | 8,756 | 4,927 | 3,860 |
| 2020 | 8,956 | 7,587 | 6,604 | 9,513 | 5,282 | 3,674 |
| 2021 | 9,813 | 8,486 | 7,411 | 11,827 | 5,889 | 3,924 |
| 2022 | 9,627 | 8,354 | 7,423 | 11,706 | 6,155 | 3,472 |
| 2023 | 9,142 | 7,909 | 7,012 | 10,791 | 6,404 | 2,738 |
| 2024 | 8,987 | 7,773 | 6,813 | 10,486 | 6,789 | 2,198 |
| 2025 | 9,138 | 7,912 | 6,763 | 10,858 | 7,402 | 1,736 |
| 2026 | 9,401 | 8,135 | 6,896 | 11,243 | 7,813 | 1,588 |

How to read it:
- **1993–1996:** EDGAR electronic filing was being phased in, so the early years undercount companies.
- **Before 2011:** XBRL started in mid-2009 for the largest filers and reached every filer in mid-2011, so companies that died before then have no ticker from XBRL. Those whose last report is from 2009 or later get one from their annual report text; earlier ones don't yet. The tickers in those years are the backfill of companies that survived into the XBRL era; the others are present with their CIK, name and dates but **no ticker**, so for those years the master tells you *who* was missing, not their symbol.
- **Fewer backfilled tickers before 2009 than in the first version** (2,882 instead of 3,294 companies in 2000): the backfill no longer crosses a change of company name. Many of the dropped ones were wrong, a later ticker pushed back over a rename that came with a ticker change (ICMB was CMFN until 2019, BBDC was Triangle Capital's TCAP until 2018). Some were right: companies, especially closed-end funds, renamed while keeping their ticker.
- **"Not in it":** these are what a universe built from `company_tickers.json` misses. Besides delisted companies they include issuers without listed stock (ABS trusts, companies with only public debt), which is why the count stays above zero today.

## Known limitations

- Ticker change dates are approximate: the old ticker stays valid until the first filing showing the new one.
- Backfilled tickers are assumptions. A conflict with another company's observed ticker cuts them, but before 2009 there is little to contradict them.
- Companies that died before 2009 have no ticker yet (68% of the public companies alive in 2000, 21% in 2008): `raw.filing_text_tickers` has only been run for companies whose last report is from 2009 or later. Running it on all of them is a matter of a few hours of downloads; it gives the ticker at the end of a company's life, backfilled to its last change of name.
- Tickers from annual report text are about 91% right on the companies that are kept (see above).
- File-name tickers of companies without listed shares (non-traded REITs, cooperatives, private issuers of debt) are mnemonics: they never beat a listed company's ticker, but can hold a ticker in periods when no listed company uses it (Tri-State Generation's `tris`).
- BDCs only file XBRL since 2022, so those that died before have no ticker.
- `delisting_reason` (acquisition, bankruptcy, ...) is always null for now; that's left to a follow-up issue.
