import sys

sys.path.append("../..")

from datetime import date, timedelta
import itertools
import polars as pl
from dateutil.relativedelta import relativedelta
from typing import cast, Literal

from etl.transformation.gold import StocksMlReadyGold
from analysis.strategies.strategy import Strategy
from analysis.strategies.classic_momentum import ClassicMomentum
from analysis.strategies.reporting import _compute_metrics
from analysis.strategies.utils import period_key

# same strategy as classic_momentum, but months_lookback/weeks_ignore/top_n are
# re-optimized at every rebalance instead of being fixed up front
_PARAM_GRID = {
    "months_lookback": [3, 9, 12],
    "weeks_ignore": [2, 4],
    "top_n": [10, 20, 30],
}


def optimize_parameters(
    df: pl.DataFrame,
    execution_date: date,
    param_grid: dict[str, list],
) -> dict:
    """Grid-searches `param_grid`, running a full `ClassicMomentum.daily_backtest`
    over the trailing year up to (but excluding) `execution_date` for every
    combination, and returns the combination that maximized Sharpe over that
    year. Each candidate gets its own months_lookback+weeks_ignore of extra
    history before the year window, so its own warmup doesn't eat into the
    evaluated trading year. Only ever looks at data strictly before
    `execution_date`, so this introduces no lookahead into the outer
    simulation."""
    starting_balance = 10_000
    keys = list(param_grid.keys())
    fallback = dict(zip(keys, (values[0] for values in param_grid.values())))

    trading_year_start = execution_date - relativedelta(years=1)

    best_params = None
    best_sharpe = float("-inf")
    for values in itertools.product(*param_grid.values()):
        params = dict(zip(keys, values))
        data_start = (
            trading_year_start
            - relativedelta(months=params["months_lookback"])
            - timedelta(weeks=params["weeks_ignore"])
        )
        window_df = df.filter(
            pl.col("timeframe").is_between(data_start, execution_date, closed="left")
        )
        if window_df.is_empty():
            continue

        candidate = ClassicMomentum(
            params["months_lookback"],
            params["weeks_ignore"],
            params["top_n"],
            params["rebalance"],
        )
        history = candidate.daily_backtest(window_df, starting_balance)
        trading_year_history = history.filter(pl.col("timeframe") >= trading_year_start)
        sharpe = _compute_metrics(trading_year_history)["sharpe"]
        if sharpe is not None and sharpe > best_sharpe:
            best_sharpe = sharpe
            best_params = params

    return best_params if best_params is not None else fallback


class AdaptiveMomentum(Strategy):
    """Same trading rules as `ClassicMomentum`, but strategy parameters
    are re-optimized (by trailing-year Sharpe, see `optimize_parameters`)
    at every rebalance instead of being fixed up front."""

    def __init__(
        self,
        reoptimize: Literal["weekly", "monthly", "quarterly"],
        param_grid: dict[str, list] = _PARAM_GRID,
    ):
        self.reoptimize: Literal["weekly", "monthly", "quarterly"] = reoptimize
        self.param_grid = param_grid
        self._last_period_key = None
        self._inner: ClassicMomentum | None = None

    def make_decision(
        self,
        df: pl.DataFrame,
        execution_date: date,
        liquidity: float,
        positions: dict[str, float],
    ) -> dict[str, float]:
        history_start = cast(date, df["timeframe"].min())
        # need a full year to optimize over, plus the grid's longest lookback
        # so the candidate being tested itself has data to rank on within that year
        warmup_end = (
            history_start
            + relativedelta(years=1)
            + relativedelta(months=max(self.param_grid["months_lookback"]))
            + timedelta(weeks=max(self.param_grid["weeks_ignore"]))
        )
        if execution_date < warmup_end:
            return positions

        current_key = period_key(execution_date, self.reoptimize)
        if current_key != self._last_period_key or self._inner is None:
            best_params = optimize_parameters(
                df,
                execution_date,
                self.param_grid,
            )
            print(f"{execution_date}: optimized params {best_params}")
            self._inner = ClassicMomentum(
                best_params["months_lookback"],
                best_params["weeks_ignore"],
                best_params["top_n"],
                best_params["rebalance"],
            )
            self._last_period_key = current_key

        return self._inner.make_decision(df, execution_date, liquidity, positions)
