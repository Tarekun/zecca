# Survivorship bias

## The problem

The price data contains only companies that still trade today:
- `raw.ticker_daily` is downloaded from yfinance for the tickers in SEC's `company_tickers.json`, which lists today's registrants.
- Companies that were delisted, acquired or went bankrupt during the backtest period are missing entirely.

Many of those companies lost value before they disappeared. Holding them would have hurt, so every backtest on this data is **optimistic**. Strategies that favor small, volatile or distressed stocks are hurt most, because those are the stocks most likely to die.

Measured on 2026-11-07:
- **`silver.candles_daily`:** 10,142 symbols from 2000 to 2026, and **none** stops trading before the last day. The data is built entirely from survivors.
- **SEC company facts:** 4,174 companies filed XBRL reports in 2021 or later but are not in today's `company_tickers.json`. Some were acquired (e.g. Hess, Adams Resources), others are funds or subsidiaries. Either way, all are absent from the prices.

## What the framework does about it

- **Measures it.** `analysis.framework.diagnose(data)` reports how many symbols stop appearing and the yearly rate, and returns warnings to show in reports.
  - Run it on **unfiltered** prices (`silver.candles_daily`).
  - In a filtered dataset like `gold.stocks_ml_ready` (top symbols by market cap), symbols also disappear when they drop out of the filter. That looks like delistings but isn't: it shows about 3% a year that has nothing to do with survivorship.
- **Supports a point-in-time universe.**
  - Market data may carry a boolean `in_universe` column, for example index membership as of each day.
  - `MarketView.universe()` then returns only that day's members, and the engine rejects target weights on anything else.
  - Universe members without a price can't be traded: the engine skips the buy and the diagnostics count those member-days.
  - This makes the universe point-in-time, but it can't bring back the prices of companies that are missing from the data.

## Fixing it: data sources

A real fix needs daily prices for delisted companies. Free options found in November 2026 (not yet evaluated in depth):

| Need | Source | Notes |
|---|---|---|
| Delisted prices | [Massive](https://massive.com/stocks) (formerly Polygon) | Delisted tickers kept with history; free tier is end-of-day and limited to 5 calls per minute |
| Delisted prices | [MarketParquet](https://marketparquet.com/guides/free-historical-stock-data), [FinzData](https://finzdata.com/survivorship-free-stock-data) | Survivorship-free, but the free tier covers only the last ~12 months; full history is paid |
| Delisted symbol list | [EODHD](https://eodhd.com/financial-apis/delisted-stock-companies-data-2) | Delisted US stocks list available with a free signup; price history is commercial |
| Delisting dates | SEC EDGAR Form 25 / 25-NSE | **Ingested** (#122): `silver.security_master` ([docs](../data/security_master.md)). Gives who died and when, not prices. |
| Index membership | [unliftedq/index-constitution](https://awesome.ecosyste.ms/projects/github.com%2Funliftedq%2Findex-constitution) | Historical S&P 500, Nasdaq 100 and Dow 30 membership with a change log, MIT licensed. Uses current tickers, so renamed companies aren't preserved. |

The point-in-time universe now exists: `silver.security_master` lists every company alive on any day since 1993, delisted ones included, keyed by CIK and with the ticker valid at the time (`listed_at(date)`, `ticker_to_cik(ticker, date)`; see [the docs](../data/security_master.md)). Prices for the delisted companies are still missing, so until a price source is ingested the bias stays and every evaluation report states it.
