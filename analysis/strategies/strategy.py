from abc import ABC, abstractmethod
from datetime import date
import polars as pl
from typing import Callable

from analysis.strategies.utils import prices_on


class Strategy(ABC):
    @abstractmethod
    def make_decision(
        self,
        df: pl.DataFrame,
        execution_date: date,
        liquidity: float,
        positions: dict[str, float],
    ) -> dict[str, float]:
        """Returns the full target `positions` (symbol -> shares) to hold
        starting `execution_date`, given the `liquidity` (cash) and
        `positions` held going into today.

        Returning `positions` unchanged means "do nothing today" -- deciding
        which days actually warrant a rebalance (e.g. only the first trading
        day of a month) is entirely up to the strategy. This is what lets
        `daily_backtest` call this every single day and still work correctly
        for daily, weekly, monthly, ... rebalancing strategies alike."""
        pass

    def daily_backtest(
        self, df: pl.DataFrame, starting_balance: float, log_mlflow: bool = False
    ) -> pl.DataFrame:
        """Calls `make_decision` for every trading day found in `df`, applying
        whatever position change it returns at that day's median (high+low)/2
        price with no transaction costs, and returns the resulting portfolio
        value time series (one row per day)."""

        def run_backtest():
            trading_days = (
                df.select("timeframe").unique().sort("timeframe")["timeframe"].to_list()
            )
            if not trading_days:
                return pl.DataFrame(
                    schema={"timeframe": pl.Date, "portfolio_value": pl.Float64}
                )

            liquidity = starting_balance
            positions: dict[str, float] = {}
            history: list[tuple[date, float]] = []

            for day in trading_days:
                prices = prices_on(df, day)
                new_positions = self.make_decision(df, day, liquidity, positions)

                old_value = sum(
                    shares * prices.get(symbol, 0.0)
                    for symbol, shares in positions.items()
                )
                new_value = sum(
                    shares * prices.get(symbol, 0.0)
                    for symbol, shares in new_positions.items()
                )
                # a rebalance is a zero-cost exchange at today's prices: whatever
                # cash it frees up/consumes is exactly old_value - new_value, so
                # this one line covers both rebalance days and no-op days alike
                liquidity = liquidity + old_value - new_value
                positions = new_positions

                history.append((day, liquidity + new_value))

            return pl.DataFrame(
                history, schema=["timeframe", "portfolio_value"], orient="row"
            )

        if log_mlflow:
            return mlflow_logged_backtest(run_backtest)
        else:
            return run_backtest()


def mlflow_logged_backtest(backtest: Callable) -> pl.DataFrame:
    return backtest()
