"""Survivorship-bias diagnostics of a market dataset.

A dataset built from today's list of tickers only contains companies that
survived until today: those delisted, acquired or bankrupt during the period
are missing, and since many of them lost money before disappearing, every
backtest on such data is optimistic. Fixing it needs price history of dead
companies (not available from free sources at the time of writing, see
docs/strategies/survivorship.md); these diagnostics measure how exposed a
dataset is, so that every evaluation can state it."""

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import polars as pl

from analysis.framework.market import DATE_COL, SYMBOL_COL, UNIVERSE_COL, Panel

# a symbol whose last row is this many trading days before the end of the data
# left the dataset (delisted, acquired, ...) rather than missing a few days
_LEFT_AFTER_TRADING_DAYS = 21
_TRADING_DAYS_PER_YEAR = 252


@dataclass(frozen=True)
class SurvivorshipDiagnostics:
    """Measure it on the *unfiltered* price data (e.g. `silver.candles_daily`):
    in a filtered dataset (top N by market cap, ...) symbols also disappear
    when they leave the filter, which looks like delistings but isn't.

    - `symbols`: distinct symbols in the data.
    - `symbols_leaving`: symbols whose data ends more than a month of trading
      days before the end of the dataset, i.e. that stopped trading.
    - `leaving_per_year`: `symbols_leaving / symbols` per year of data.
    - `universe_member_days_without_price`: with an `in_universe` column,
      (day, symbol) rows flagged as members but without a close price:
      members the strategy could not have traded (None without the column)."""

    first_day: date
    last_day: date
    symbols: int
    symbols_leaving: int
    leaving_per_year: float
    universe_member_days_without_price: int | None

    @property
    def survivors_only(self) -> bool:
        return self.symbols_leaving == 0

    def warnings(self) -> list[str]:
        out = []
        if self.survivors_only:
            out.append(
                f"survivorship bias: none of the {self.symbols} symbols stops trading "
                f"between {self.first_day} and {self.last_day}, so the data was built "
                "from today's survivors and every company delisted, acquired or gone "
                "bankrupt in the period is missing: results are optimistic"
            )
        else:
            out.append(
                f"survivorship: {self.symbols_leaving} of {self.symbols} symbols stop "
                f"appearing in the period ({self.leaving_per_year:.1%} per year); if "
                "these are real delistings (not exits from a filter on the data), "
                "check the rate against the market's before trusting the data to be "
                "survivorship-free"
            )
        if self.universe_member_days_without_price:
            out.append(
                f"{self.universe_member_days_without_price} universe member-days have "
                "no price: those members could not be traded"
            )
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            **{
                k: v.isoformat() if isinstance(v, date) else v
                for k, v in asdict(self).items()
            },
            "survivors_only": self.survivors_only,
            "warnings": self.warnings(),
        }


def diagnose(data: pl.DataFrame | Panel) -> SurvivorshipDiagnostics:
    panel = data if isinstance(data, Panel) else Panel(data)
    df = panel.frame
    days = panel.dates
    if not days:
        raise ValueError("No data to diagnose")
    # a symbol "leaves" if its last row is before the cutoff trading day
    cutoff = days[max(0, len(days) - 1 - _LEFT_AFTER_TRADING_DAYS)]
    last_seen = df.group_by(SYMBOL_COL).agg(pl.col(DATE_COL).max().alias("last"))
    symbols = last_seen.height
    leaving = last_seen.filter(pl.col("last") < cutoff).height
    years = max(len(days) / _TRADING_DAYS_PER_YEAR, 1 / _TRADING_DAYS_PER_YEAR)

    missing_prices = None
    if UNIVERSE_COL in df.columns:
        price = pl.col("close") if "close" in df.columns else pl.lit(None)
        missing_prices = df.filter(
            pl.col(UNIVERSE_COL).fill_null(False) & price.is_null()
        ).height

    return SurvivorshipDiagnostics(
        first_day=days[0],
        last_day=days[-1],
        symbols=symbols,
        symbols_leaving=leaving,
        leaving_per_year=(leaving / symbols / years) if symbols else 0.0,
        universe_member_days_without_price=missing_prices,
    )
