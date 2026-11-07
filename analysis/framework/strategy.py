from abc import ABC, abstractmethod
from types import MappingProxyType
from typing import Any, ClassVar, Mapping

import polars as pl

from analysis.framework.market import DATE_COL, SYMBOL_COL, MarketView, Panel
from analysis.framework.params import InvalidParams, ParamSpace
from analysis.framework.portfolio import PortfolioState
from analysis.framework.weights import TargetWeights


class Strategy(ABC):
    """Base class for every investment strategy.

    A strategy is defined by three things:

    1. `param_space` (class attribute): the parameters it accepts, their
       defaults and search domains. The framework builds instances from plain
       dicts (`MyStrategy(top_n=20)` or `MyStrategy.from_params({...})`), and
       each parameter is available as an attribute (`self.top_n`) and in
       `self.params`.
    2. `prepare(data)` (optional): vectorized computation of signals over the
       whole dataset, run once before the simulation.
    3. `decide(market, portfolio)`: called at the close of every trading day;
       returns the `TargetWeights` to rebalance to (executed at the next
       open), or `None` to leave the portfolio untouched.

    The framework creates a new instance for every run (backtest, every
    walk-forward fold, every grid search candidate), so attributes set inside
    `decide` live for one run only. Subclasses should not need to override
    `__init__`; if they do, they must accept `**params` and call
    `super().__init__(**params)` first.

    `data_columns` (class attribute) lists the dataset columns the strategy
    reads beyond `timeframe`, `symbol` and OHLCV (e.g. `("rsi", "earnings")`),
    so that tools loading the data only load what is needed.

    Rules (enforced by the framework's guardrail tests, not by convention):
    - the decision at day t may only depend on data with timeframe <= t;
    - the `prepare()` output row for day t may only depend on data <= t
      (no `shift(-n)`, no full-sample statistics such as global means/ranks);
    - decisions must be deterministic (seed any randomness);
    - only long positions; total weight may exceed 1 only up to the run's
      configured maximum leverage.
    """

    param_space: ClassVar[ParamSpace] = ParamSpace()
    data_columns: ClassVar[tuple[str, ...]] = ()

    def __init__(self, **params: Any) -> None:
        resolved = type(self).param_space.resolve(params)
        for name in resolved:
            if name == "params" or hasattr(type(self), name):
                raise InvalidParams(
                    f"Parameter {name!r} of {type(self).__name__} clashes with an existing attribute"
                )
            setattr(self, name, resolved[name])
        self.params: Mapping[str, Any] = MappingProxyType(resolved)

    @classmethod
    def from_params(cls, params: Mapping[str, Any]) -> "Strategy":
        return cls(**params)

    @property
    def name(self) -> str:
        return type(self).__name__

    def prepare(self, data: pl.LazyFrame) -> pl.LazyFrame | None:
        """Optional: compute signals for every (timeframe, symbol) at once,
        e.g. `pl.col("close").pct_change(252).over("symbol")`. The result
        must contain `timeframe` and `symbol` (unique together) and is exposed
        to `decide` through `market.signals()`. Return None (the default) when
        the strategy computes everything inside `decide`.

        `data` is sorted by (symbol, timeframe), so window expressions
        (`shift`, `pct_change`, `rolling_*`, ...) used with `.over("symbol")`
        run in chronological order within each symbol."""
        return None

    @abstractmethod
    def decide(
        self, market: MarketView, portfolio: PortfolioState
    ) -> TargetWeights | None:
        """Decision at the close of `market.date`. Return the target
        portfolio as `TargetWeights` (symbols left out are sold, the rest of
        net worth stays in cash), `TargetWeights.cash()` to sell everything,
        or None to do nothing today."""

    def __repr__(self) -> str:
        args = ", ".join(f"{k}={v!r}" for k, v in self.params.items())
        return f"{self.name}({args})"


def build_signals(
    strategy: Strategy, data: pl.DataFrame | pl.LazyFrame
) -> Panel | None:
    """Runs `strategy.prepare` on the market data and wraps the result in a
    `Panel` ready to be passed to `MarketView`; None if the strategy has no
    `prepare` step."""
    lf = data.lazy() if isinstance(data, pl.DataFrame) else data
    signals = strategy.prepare(lf.sort([SYMBOL_COL, DATE_COL]))
    if signals is None:
        return None
    if isinstance(signals, pl.DataFrame):
        signals = signals.lazy()
    if not isinstance(signals, pl.LazyFrame):
        raise TypeError(
            f"{strategy.name}.prepare() must return a LazyFrame or None, got {type(signals).__name__}"
        )
    return Panel(signals.collect())
