from abc import ABC, abstractmethod
from datetime import date
import polars as pl
from typing import Callable

from analysis.strategies.utils import prices_on
from analysis.strategies.reporting import compute_metrics
from analysis.mlflow_utils import ExperimentLogger, mlflow_experiment
from analysis.strategies.wallet import Order, Wallet


class Strategy(ABC):
    @abstractmethod
    def place_orders(
        self,
        df: pl.DataFrame,
        execution_date: date,
        liquidity: float,
        positions: dict[str, float],
    ) -> list[Order]:
        """Returns the `Order`s (e.g. `LiquidateStock`, `BuyStock`) to place
        starting `execution_date`, given the `liquidity` (cash) and
        `positions` (symbol -> shares) held going into today.

        Returning an empty list means "do nothing today" -- deciding which
        days actually warrant a rebalance (e.g. only the first trading day of
        a month) is entirely up to the strategy. This is what lets
        `daily_backtest` call this every single day and still work correctly
        for daily, weekly, monthly, ... rebalancing strategies alike."""
        pass

    def daily_backtest(
        self, df: pl.DataFrame, starting_balance: float, log_on_mlflow: bool = False
    ) -> pl.DataFrame:
        """Calls `place_orders` once per trading day in `df`, placing whatever
        orders it returns against a `Wallet` at that day's median (high+low)/2
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

            history: list[tuple[date, float]] = []
            wallet = Wallet(starting_balance)

            for day in trading_days:
                prices = prices_on(df, day)

                for order in self.place_orders(
                    df, day, wallet.liquidity, wallet.holdings
                ):
                    order.place(wallet)

                history.append((day, wallet.net_worth(prices)))

            return pl.DataFrame(
                history, schema=["timeframe", "portfolio_value"], orient="row"
            )

        @mlflow_experiment(
            name=self.__class__.__name__,
            tags={"strategy-backtest": True},
            log_config_params=("strategy_params",),
        )
        def logged_backtest(
            strategy_params: dict, logger: ExperimentLogger | None = None
        ) -> pl.DataFrame:
            history = run_backtest()
            if logger is not None:
                # step-indexed so mlflow renders it as a native line chart
                # (the day-by-day equity curve) in the run's Metrics tab
                for step, value in enumerate(history["portfolio_value"]):
                    logger.log_metric("portfolio_value", value, step=step)

                metrics = compute_metrics(history)
                logger.log_metrics({k: v for k, v in metrics.items() if v is not None})
            return history

        if log_on_mlflow:
            strategy_params = {
                key: value
                for key, value in vars(self).items()
                if not key.startswith("_")
            }
            return logged_backtest(strategy_params)
        else:
            return run_backtest()
