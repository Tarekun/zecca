"""Strategy framework: the contract between investment strategies and the
machinery that evaluates them (backtest engine, walk-forward, robustness
tests -- see the strategy-framework epic on GitHub).

A strategy subclasses `Strategy`, declares a `ParamSpace`, optionally
computes vectorized signals in `prepare()` and, every trading day at the
close, returns `TargetWeights` (or None) from `decide()` given a point-in-time
`MarketView` and the current `PortfolioState`:

    class TopNMomentum(Strategy):
        param_space = ParamSpace(
            lookback=Param(default=252, values=[63, 126, 252]),
            top_n=Param(default=20, low=10, high=30, step=10),
        )

        def prepare(self, data):
            return data.select(
                "timeframe", "symbol",
                pl.col("close").pct_change(self.lookback).over("symbol").alias("momentum"),
            )

        def decide(self, market, portfolio):
            if not market.is_first_trading_day_of("month"):
                return None
            top = (
                market.signals(["momentum"])
                .drop_nulls("momentum")
                .top_k(self.top_n, by="momentum")["symbol"]
            )
            return TargetWeights.equal(top)
"""

from analysis.framework.market import DATE_COL, SYMBOL_COL, MarketView, Panel
from analysis.framework.params import InvalidParams, Param, ParamSpace
from analysis.framework.periods import Period, period_key
from analysis.framework.portfolio import PortfolioState
from analysis.framework.strategy import Strategy, build_signals
from analysis.framework.weights import InvalidTargetWeights, TargetWeights

__all__ = [
    "DATE_COL",
    "SYMBOL_COL",
    "InvalidParams",
    "InvalidTargetWeights",
    "MarketView",
    "Panel",
    "Param",
    "ParamSpace",
    "Period",
    "PortfolioState",
    "Strategy",
    "TargetWeights",
    "build_signals",
    "period_key",
]
