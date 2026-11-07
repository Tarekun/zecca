from dataclasses import dataclass, field
from typing import Any, Mapping

import polars as pl

from analysis.framework.costs import BpsCostModel, CostModel
from analysis.framework.portfolio import PortfolioState


@dataclass(frozen=True)
class BacktestConfig:
    """Everything about a backtest run that isn't the strategy or the data.

    - `initial_cash`: starting net worth, all in cash.
    - `costs`: commission and slippage model (default 5 + 5 bps).
    - `max_leverage`: maximum total weight a strategy may target (1 = no
      leverage). Borrowed cash pays the risk-free rate + `borrow_spread`.
    - `borrow_spread`: annual spread over the risk-free rate on negative cash.
    - `cash_earns_interest`: whether positive cash earns the risk-free rate.
    - `fractional_shares`: if False quantities are rounded down to whole shares.
    - `min_trade_value`: rebalancing trades smaller than this (in cash) are
      skipped, except full exits.
    - `delisting_days` / `delisting_haircut`: a held symbol without a price
      for this many consecutive trading days is force-liquidated at its last
      price reduced by the haircut (0.0 = last price, 1.0 = worthless).
    """

    initial_cash: float = 10_000.0
    costs: CostModel = field(default_factory=BpsCostModel)
    max_leverage: float = 1.0
    borrow_spread: float = 0.01
    cash_earns_interest: bool = True
    fractional_shares: bool = True
    min_trade_value: float = 0.0
    delisting_days: int = 5
    delisting_haircut: float = 0.0

    def __post_init__(self) -> None:
        if self.initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        if self.max_leverage < 0:
            raise ValueError("max_leverage must be non-negative")
        if self.delisting_days < 1:
            raise ValueError("delisting_days must be at least 1")
        if not 0.0 <= self.delisting_haircut <= 1.0:
            raise ValueError("delisting_haircut must be between 0 and 1")
        if self.min_trade_value < 0:
            raise ValueError("min_trade_value must be non-negative")


@dataclass(frozen=True)
class BacktestResult:
    """Output of `run_backtest`. Every table is a polars DataFrame:

    - `equity`: one row per trading day, valued at the close: `timeframe`,
      `portfolio_value`, `cash`, `invested`, `exposure` (invested / value),
      `interest` (earned (+) or paid (-) on cash that day), `risk_free`
      (the daily risk-free return applied that day).
    - `trades`: every execution: `timeframe`, `symbol`, `side`, `quantity`,
      `quote` (open price), `price` (after slippage), `notional`,
      `commission`, `slippage` (cost of slippage in cash), `reason`
      (`rebalance`, `exit_retry` or `delisting`).
    - `positions`: holdings at each close: `timeframe`, `symbol`,
      `quantity`, `price` (mark), `value`, `weight`, `stale_days` (trading
      days since the mark price was last updated).
    - `decisions`: the target weights returned by the strategy:
      `timeframe` (decision day), `symbol`, `weight`. A decision to hold only
      cash appears as a single row with a null symbol.
    - `events`: anything the engine had to work around: `timeframe`,
      `symbol`, `kind`, `detail`.

    `initial_value` is the net worth the simulation started from and
    `final_state` the portfolio at the last close (the starting point of the
    next walk-forward fold).
    """

    strategy: str
    params: Mapping[str, Any]
    config: BacktestConfig
    equity: pl.DataFrame
    trades: pl.DataFrame
    positions: pl.DataFrame
    decisions: pl.DataFrame
    events: pl.DataFrame
    initial_value: float
    final_state: PortfolioState

    @property
    def final_value(self) -> float:
        return float(self.equity["portfolio_value"][-1])

    @property
    def total_return(self) -> float:
        return self.final_value / self.initial_value - 1
