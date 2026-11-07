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

    result = run_backtest(
        TopNMomentum(top_n=30),
        data,                              # (timeframe, symbol, open, close, ...)
        BacktestConfig(max_leverage=1.0),  # costs default to 5 + 5 bps
        risk_free=load_risk_free(),
    )
    result.equity, result.trades, result.positions, result.events
"""

from analysis.framework.costs import BpsCostModel, CostModel, NoCosts
from analysis.framework.data import load_risk_free
from analysis.framework.engine import run_backtest
from analysis.framework.guardrails import LookaheadCheck, check_lookahead
from analysis.framework.market import (
    DATE_COL,
    SYMBOL_COL,
    UNIVERSE_COL,
    MarketView,
    Panel,
)
from analysis.framework.metrics import cagr, max_drawdown, sharpe, summary, volatility
from analysis.framework.optimization import (
    Fold,
    GridSearchResult,
    WalkForwardResult,
    grid_search,
    make_folds,
    walk_forward,
)
from analysis.framework.params import InvalidParams, Param, ParamSpace
from analysis.framework.periods import Period, period_key
from analysis.framework.portfolio import PortfolioState
from analysis.framework.result import BacktestConfig, BacktestResult
from analysis.framework.strategy import Strategy, build_signals
from analysis.framework.survivorship import SurvivorshipDiagnostics, diagnose
from analysis.framework.weights import InvalidTargetWeights, TargetWeights

__all__ = [
    "UNIVERSE_COL",
    "SurvivorshipDiagnostics",
    "diagnose",
    "LookaheadCheck",
    "check_lookahead",
    "BacktestConfig",
    "BacktestResult",
    "BpsCostModel",
    "CostModel",
    "DATE_COL",
    "Fold",
    "GridSearchResult",
    "InvalidParams",
    "InvalidTargetWeights",
    "MarketView",
    "NoCosts",
    "Panel",
    "Param",
    "ParamSpace",
    "Period",
    "PortfolioState",
    "SYMBOL_COL",
    "Strategy",
    "TargetWeights",
    "WalkForwardResult",
    "build_signals",
    "cagr",
    "grid_search",
    "load_risk_free",
    "make_folds",
    "max_drawdown",
    "period_key",
    "run_backtest",
    "sharpe",
    "summary",
    "volatility",
    "walk_forward",
]
