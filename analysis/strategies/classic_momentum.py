from datetime import date, timedelta
import polars as pl
from dateutil.relativedelta import relativedelta
from typing import cast, Literal

from analysis.strategies.strategy import Strategy
from analysis.strategies.utils import prices_on, period_key
from analysis.strategies.wallet import Order, BuyStock, LiquidateStock


def top_n_on_window(
    df: pl.DataFrame,
    months_lookback: int,
    weeks_ignore: int,
    top_n: int,
    execution_date: date,
) -> list[str]:
    window_start = execution_date - relativedelta(months=months_lookback)
    window_end = execution_date - timedelta(weeks=weeks_ignore)

    momentum = (
        df.filter(pl.col("timeframe").is_between(window_start, window_end))
        .sort(["symbol", "timeframe"])
        .group_by("symbol", maintain_order=True)
        .agg(
            pl.col("close").first().alias("start_price"),
            pl.col("close").last().alias("end_price"),
        )
        .filter(pl.col("start_price") > 0)
        .with_columns(
            (pl.col("end_price") / pl.col("start_price") - 1).alias("momentum")
        )
        .sort("momentum", descending=True)
    )

    return momentum.head(top_n)["symbol"].to_list()


def equal_weight_buy_orders(
    symbols: list[str], prices: dict[str, float], total_value: float
) -> list[Order]:
    """Splits `total_value` evenly into a `BuyStock` order per symbol among
    whichever of `symbols` have a price today."""

    buyable = [symbol for symbol in symbols if symbol in prices]
    if not buyable:
        return []
    allocation = total_value / len(buyable)
    return [BuyStock(symbol, prices[symbol], allocation) for symbol in buyable]


class TopNReturnsMomentum(Strategy):
    """Rebalances into the top `top_n` momentum symbols (`make_choice`) on the
    first trading day of every `rebalance` period, equal-weighting the
    portfolio's full value across them. Does nothing in between, and does
    nothing until a full months_lookback+weeks_ignore window of history is
    available."""

    def __init__(
        self,
        months_lookback: int,
        weeks_ignore: int,
        top_n: int,
        rebalance: Literal["weekly", "monthly", "quarterly"],
    ):
        self.months_lookback = months_lookback
        self.weeks_ignore = weeks_ignore
        self.top_n = top_n
        self.rebalance: Literal["weekly", "monthly", "quarterly"] = rebalance
        self._last_period_key = None

    def place_orders(
        self,
        df: pl.DataFrame,
        execution_date: date,
        liquidity: float,
        positions: dict[str, float],
    ) -> list[Order]:
        history_start: date = cast(date, df["timeframe"].min())
        warmup_end = (
            history_start
            + relativedelta(months=self.months_lookback)
            + timedelta(weeks=self.weeks_ignore)
        )
        # avoid trading if there isnt a full self.months_lookback+self.weeks_ignore of time from execution_date
        if execution_date < warmup_end:
            return []

        current_key = period_key(execution_date, self.rebalance)
        # only trade on the configured self.rebalance
        if current_key == self._last_period_key:
            return []
        self._last_period_key = current_key

        chosen = top_n_on_window(
            df, self.months_lookback, self.weeks_ignore, self.top_n, execution_date
        )
        prices = prices_on(df, execution_date)
        total_value = liquidity + sum(
            shares * prices.get(symbol, 0.0) for symbol, shares in positions.items()
        )

        liquidations: list[Order] = [
            LiquidateStock(symbol, prices.get(symbol, 0.0)) for symbol in positions
        ]
        purchases = equal_weight_buy_orders(chosen, prices, total_value)
        return liquidations + purchases
