"""Cross-sectional momentum: hold the symbols that went up the most.

`TopNMomentum` ranks every symbol by its return over the last
`months_lookback` months, skipping the most recent `weeks_ignore` weeks
(short-term reversal), and holds the `top_n` best equally weighted,
rebalancing at the start of every `rebalance` period.

The adaptive variant -- parameters re-optimized periodically on the trailing
year -- is not a separate strategy but a walk-forward run of `TopNMomentum`,
see `adaptive_momentum`."""

import polars as pl

from analysis.framework import (
    Param,
    ParamSpace,
    Strategy,
    TargetWeights,
    WalkForwardResult,
    walk_forward,
)

# lookbacks are expressed in calendar units but measured in trading days
TRADING_DAYS_PER_MONTH = 21
TRADING_DAYS_PER_WEEK = 5


class TopNMomentum(Strategy):
    """Holds the `top_n` symbols with the highest return over the last
    `months_lookback` months excluding the last `weeks_ignore` weeks, equally
    weighted, rebalanced on the first trading day of every `rebalance` period.

    Months and weeks are measured in trading days (21 and 5)."""

    param_space = ParamSpace(
        months_lookback=Param(default=12, values=[3, 6, 9, 12]),
        weeks_ignore=Param(default=4, values=[0, 2, 4]),
        top_n=Param(default=20, low=10, high=30, step=10),
        rebalance=Param(default="month", values=["week", "month", "quarter"]),
    )

    def prepare(self, data: pl.LazyFrame) -> pl.LazyFrame:
        lookback = self.months_lookback * TRADING_DAYS_PER_MONTH
        skip = self.weeks_ignore * TRADING_DAYS_PER_WEEK
        start = pl.col("close").shift(lookback).over("symbol")
        end = pl.col("close").shift(skip).over("symbol")
        return data.select(
            "timeframe",
            "symbol",
            pl.when((start > 0) & (end > 0)).then(end / start - 1).alias("momentum"),
        )

    def decide(self, market, portfolio):
        if not market.is_first_trading_day_of(self.rebalance):
            return None
        ranked = (
            market.signals(["momentum"])
            .filter(
                pl.col("momentum").is_finite()
                & pl.col("symbol").is_in(market.universe())
            )
            .top_k(self.top_n, by="momentum")
        )
        # before enough history exists nothing is ranked: stay in cash
        return TargetWeights.equal(ranked["symbol"])


# the grid the old AdaptiveTopNReturnsMomentum re-optimized over
ADAPTIVE_PARAM_SPACE = ParamSpace(
    months_lookback=Param(default=12, values=[3, 9, 12]),
    weeks_ignore=Param(default=4, values=[2, 4]),
    top_n=Param(default=20, low=10, high=30, step=10),
    rebalance=Param(default="month", values=["week", "month", "quarter"]),
)


def adaptive_momentum(
    data: pl.DataFrame | pl.LazyFrame, reoptimize: str = "3mo", **kwargs
) -> WalkForwardResult:
    """`TopNMomentum` with parameters re-optimized every `reoptimize`
    ("1w", "1mo", "3mo", ...) by Sharpe over the trailing year, on
    `ADAPTIVE_PARAM_SPACE`. The 13 months of warmup cover the longest
    lookback + skip, so every candidate has full history from the first
    training day. Any other `walk_forward` argument can be overridden."""
    kwargs.setdefault("param_space", ADAPTIVE_PARAM_SPACE)
    kwargs.setdefault("train", "1y")
    kwargs.setdefault("warmup", "13mo")
    return walk_forward(TopNMomentum, data, test=reoptimize, **kwargs)
