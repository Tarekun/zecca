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
- Periods with no known ticker have a row with a null `ticker`. `has_known_ticker = false` flags companies with no ticker at all, such as funds, ABS trusts, shells and companies that died before 2009.
- `source` says where the ticker comes from:
  - `xbrl`: declared in a filing
  - `company_tickers`: only in today's mapping
  - `backfill`: assumed, see below
  - `full-index`: no ticker, only proof the company existed

Pass `include_backfill=False` to get only observed tickers.

## Sources (raw layer)

| Source | What it gives | Size | Incremental |
|---|---|---|---|
| `raw.edgar_full_index` (EDGAR `full-index/{year}/QTR{n}/master.gz`) | Every filing since 1993: CIK, name at the time, form type, date | 27M rows, 660 MB | A quarter downloaded more than 7 days after it ends is final and never downloaded again. A rerun only fetches the current quarter. |
| `raw.xbrl_cover_pages` (DERA *Financial Statement and Notes* datasets) | `dei:TradingSymbol`, `SecurityExchangeName` and `Security12bTitle` of every XBRL filing (10-K, 10-Q, 8-K, 20-F, ...) since 2009 | 77 zips, ~28 GB downloaded, 46 MB kept | Published datasets don't change, so only new ones are downloaded. Each zip is streamed to a temporary file and deleted after parsing. |
| `raw.company_tickers` / `raw.company_tickers_exchange` | Today's CIK, ticker and exchange | <1 MB | Full download every run |

What was checked and left out:
- **`submissions.zip`** (1.5 GB) has no ticker for inactive companies (checked on Twitter and Lehman). For current companies it holds the same tickers as `company_tickers_exchange.json`, and the full-index already has the company names over time.
- **XBRL `companyfacts`** only has numeric facts, so no tickers.

## How it's built

**When a company exists**
- **Start:** the earliest of its first periodic report (10-K, 10-Q, 20-F or 40-F, including the `10KSB`/`10QSB` and transition variants), its registration (8-A, Form 10, 20-FR), its successor-issuer filing (8-K12B, as for Google → Alphabet), or its first declared ticker.
- **End:** its delisting (Form 25 / 25-NSE) or deregistration (Form 15), whichever comes first. A Form 25 counts only if filed between 1 year before the last periodic report and the date the next report was due:
  - Earlier Form 25s, followed by years of reports, were a transfer to another exchange or a single class of notes.
  - Inside that window the *last* Form 25 is used. Issuers with listed notes file one each time a series matures: Lehman filed dozens, the first one two months before the stock was delisted. Taking the last one keeps a dying company in the universe a little longer and never drops it before its collapse.
- **No Form 25 or 15:** the company ends 6 months after its last report (15 months for 20-F/40-F filers).
- Companies in today's `company_tickers.json` are always still alive, with null `delisting_date` and `deregistration_date`.

**Which ticker a company had**
- A ticker is valid from the first filing declaring it until the first later filing that no longer does, so the change date is only as precise as the filing frequency. Since 2019 every 8-K also declares the ticker, so that's usually within weeks.
- A ticker in today's mapping stays open-ended even if some filings leave it out.
- Before its first declaration, a company's earliest known ticker is assumed to go back to the company's start (`source = backfill`).
- Declared values are cleaned up:
  - `BAX (NYSE)` → `BAX`
  - `BRK.B` → `BRK-B`, matching today's spelling
  - `GOOG, GOOGL` → two tickers
  - placeholders (`N/A`, `None`) and preferreds written as three words (`JPM PR C`) are dropped, about 2% of declarations
  - facts of co-registrants are ignored

**Conflicts:** a ticker belongs to one company at a time. When two companies claim it for the same period, priority goes in this order:
1. today's owner
2. a declared ticker over a backfilled one
3. the company that declared it more often (for example, a subsidiary that repeats its parent's ticker loses)

The loser keeps only the period that doesn't overlap.

## Quality checks

- `valid_from <= valid_to`
- unique `(cik, ticker, valid_from)`
- ticker format
- no overlapping intervals for the same ticker across different CIKs
- no overlapping intervals for the same `(cik, ticker)`
- null-ticker rows don't overlap other rows of the same company
- every entry of today's `company_tickers.json` has an open-ended row

The cases from the issue are tested twice: on synthetic data in `tests/transformations/test_security_master.py`, and on the real data in `tests/data_quality/silver/test_security_master_known_cases.py`.

## Coverage (built on 2026-11-07)

51,375 CIKs in the master versus 8,004 in today's `company_tickers.json`. 14,095 of them have a known ticker at some point.

Companies alive at year end (2026: June 30). Columns:
- **with ticker:** has a ticker that day, observed or backfilled
- **observed ticker:** declared in a filing, not backfilled
- **in / not in `company_tickers.json`:** whether the CIK is in today's mapping; "not in" is the companies a survivors-only universe misses

| Year | Alive | With ticker | Observed ticker | Distinct tickers | In `company_tickers.json` | Not in it |
|---|---|---|---|---|---|---|
| 1994 | 2,352 | 643 | 0 | 837 | 457 | 1,895 |
| 1996 | 12,295 | 2,289 | 0 | 2,526 | 1,346 | 10,949 |
| 1998 | 13,325 | 2,711 | 0 | 2,974 | 1,569 | 11,756 |
| 2000 | 14,604 | 3,294 | 0 | 3,631 | 1,822 | 12,782 |
| 2002 | 14,320 | 3,883 | 0 | 4,399 | 2,176 | 12,144 |
| 2004 | 13,492 | 4,319 | 0 | 4,940 | 2,413 | 11,079 |
| 2006 | 12,841 | 4,943 | 0 | 5,585 | 2,674 | 10,167 |
| 2008 | 12,079 | 5,776 | 0 | 6,497 | 2,995 | 9,084 |
| 2010 | 10,960 | 6,314 | 405 | 7,076 | 3,241 | 7,719 |
| 2012 | 10,245 | 6,786 | 2,470 | 7,604 | 3,533 | 6,712 |
| 2014 | 10,025 | 7,206 | 3,090 | 8,074 | 3,910 | 6,115 |
| 2016 | 9,372 | 7,238 | 3,932 | 8,151 | 4,239 | 5,133 |
| 2018 | 8,988 | 7,272 | 4,237 | 8,230 | 4,688 | 4,300 |
| 2019 | 8,788 | 7,247 | 5,172 | 8,958 | 4,927 | 3,861 |
| 2020 | 8,956 | 7,505 | 5,528 | 9,809 | 5,282 | 3,674 |
| 2021 | 9,813 | 8,421 | 6,697 | 11,916 | 5,889 | 3,924 |
| 2022 | 9,627 | 8,267 | 6,882 | 11,653 | 6,155 | 3,472 |
| 2023 | 9,142 | 7,845 | 6,506 | 10,756 | 6,404 | 2,738 |
| 2024 | 8,987 | 7,720 | 6,344 | 10,456 | 6,789 | 2,198 |
| 2025 | 9,138 | 7,831 | 6,323 | 10,813 | 7,402 | 1,736 |
| 2026 | 9,401 | 8,034 | 6,456 | 11,176 | 7,813 | 1,588 |

How to read it:
- **1993–1996:** EDGAR electronic filing was being phased in, so the early years undercount companies.
- **Before 2009:** there are no observed tickers. The tickers in those years are the backfill of companies that survived into the XBRL era. Companies that died before 2009 are present with their CIK, name and dates but **no ticker**, so for those years the master tells you *who* was missing, not their symbol.
- **"Not in it":** these are what a universe built from `company_tickers.json` misses. Besides delisted companies they include issuers without listed stock (ABS trusts, companies with only public debt), which is why the count stays above zero today.

## Known limitations

- Ticker change dates are approximate: the old ticker stays valid until the first filing showing the new one.
- Backfilled tickers are assumptions. A conflict with another company's *declared* ticker cuts them, but before 2009 there is little to contradict them.
- 2009–2018 coverage is partial: tagging the cover page only became mandatory in 2019.
- `delisting_reason` (acquisition, bankruptcy, ...) is always null for now; that's left to a follow-up issue.
