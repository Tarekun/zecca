"""Strategies used by the CLI tests (referenced as module:Class)."""

import polars as pl

from analysis.framework import Param, ParamSpace, Strategy, TargetWeights


class Pick(Strategy):
    """All-in on one symbol, re-targeted monthly."""

    param_space = ParamSpace(
        symbol=Param(default="A", values=["A", "B", "C"]),
        exposure=Param(default=1.0, values=[0.5, 1.0]),
    )

    def decide(self, market, portfolio):
        if not market.is_first_trading_day_of("month"):
            return None
        return TargetWeights({self.symbol: self.exposure})


class Peeker(Strategy):
    """Looks one day ahead: buys whatever goes up tomorrow."""

    def prepare(self, data):
        tomorrow = pl.col("close").shift(-1).over("symbol") / pl.col("close") - 1
        return data.select("timeframe", "symbol", tomorrow.alias("tomorrow"))

    def decide(self, market, portfolio):
        best = market.signals(["tomorrow"]).drop_nulls().top_k(1, by="tomorrow")
        return TargetWeights.equal(best["symbol"])


class NeedsRsi(Pick):
    data_columns = ("rsi",)
