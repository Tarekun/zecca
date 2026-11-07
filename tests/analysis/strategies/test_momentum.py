import math
import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))
from analysis.framework import (
    BacktestConfig,
    MarketView,
    NoCosts,
    Panel,
    Param,
    ParamSpace,
    PortfolioState,
    WalkForwardResult,
    build_signals,
    run_backtest,
)
from analysis.strategies.momentum import (
    ADAPTIVE_PARAM_SPACE,
    TopNMomentum,
    adaptive_momentum,
)


def business_days(start: date, n: int) -> list[date]:
    days, d = [], start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


DAYS = business_days(date(2020, 1, 1), 700)
# daily growth per symbol: S00 grows slowest, S11 fastest
GROWTH = {f"S{i:02d}": 0.0001 * (i + 1) for i in range(12)}


def market(days: list[date] = DAYS) -> pl.DataFrame:
    rows = []
    for symbol, g in GROWTH.items():
        for i, d in enumerate(days):
            price = 100 * (1 + g) ** i * (1 + 0.002 * math.sin(i + len(symbol)))
            rows.append(
                {"timeframe": d, "symbol": symbol, "open": price, "close": price}
            )
    return pl.DataFrame(rows)


DATA = market()


def signals_for(strategy: TopNMomentum, data: pl.DataFrame = DATA) -> pl.DataFrame:
    panel = build_signals(strategy, data)
    assert panel is not None
    return panel.frame


class TestPrepare:
    def test_momentum_skips_recent_weeks(self):
        s = TopNMomentum(months_lookback=3, weeks_ignore=2)
        lookback, skip = 63, 10
        closes = (
            DATA.filter(pl.col("symbol") == "S05").sort("timeframe")["close"].to_list()
        )
        row = signals_for(s).filter(
            (pl.col("symbol") == "S05") & (pl.col("timeframe") == DAYS[100])
        )
        expected = closes[100 - skip] / closes[100 - lookback] - 1
        assert row["momentum"].item() == pytest.approx(expected)

    def test_null_until_full_lookback(self):
        s = TopNMomentum(months_lookback=3, weeks_ignore=0)
        c = signals_for(s).filter(pl.col("symbol") == "S05").sort("timeframe")
        assert c["momentum"][:63].null_count() == 63
        assert c["momentum"][63:].null_count() == 0

    def test_non_positive_prices_give_no_signal(self):
        bad = DATA.with_columns(
            pl.when((pl.col("symbol") == "S00") & (pl.col("timeframe") == DAYS[0]))
            .then(-1.0)
            .otherwise(pl.col("close"))
            .alias("close")
        )
        s = TopNMomentum(months_lookback=3, weeks_ignore=0)
        a = signals_for(s, bad).filter(pl.col("symbol") == "S00").sort("timeframe")
        assert a["momentum"][63] is None
        assert a["momentum"][64] is not None


class TestDecide:
    def view(self, strategy: TopNMomentum, day: date) -> MarketView:
        return MarketView(Panel(DATA), day, build_signals(strategy, DATA))

    def test_holds_the_top_n_equally_weighted(self):
        s = TopNMomentum(months_lookback=3, weeks_ignore=0, top_n=10)
        panel = Panel(DATA)
        day = next(
            d
            for d in DAYS[200:]
            if MarketView(panel, d).is_first_trading_day_of("month")
        )
        weights = s.decide(self.view(s, day), PortfolioState.empty(day, 1.0))
        # the two slowest growers are left out
        assert weights == {f"S{i:02d}": pytest.approx(0.1) for i in range(2, 12)}

    def test_only_rebalances_at_period_start(self):
        s = TopNMomentum(rebalance="quarter")
        panel, signals = Panel(DATA), build_signals(s, DATA)
        decided = [
            d
            for d in DAYS
            if s.decide(MarketView(panel, d, signals), PortfolioState.empty(d, 1.0))
            is not None
        ]
        assert all(d.month in (1, 4, 7, 10) for d in decided)
        assert len(decided) == len({(d.year, (d.month - 1) // 3) for d in DAYS})

    def test_cash_before_enough_history(self):
        s = TopNMomentum(months_lookback=12)
        view = self.view(s, DAYS[0])
        assert s.decide(view, PortfolioState.empty(DAYS[0], 1.0)) == {}


def test_backtest_does_not_depend_on_future_data():
    cutoff = DAYS[500]
    s = dict(months_lookback=3, weeks_ignore=2)
    full = run_backtest(TopNMomentum(**s), DATA)
    truncated = run_backtest(
        TopNMomentum(**s), DATA.filter(pl.col("timeframe") <= cutoff)
    )
    assert truncated.equity.equals(full.equity.filter(pl.col("timeframe") <= cutoff))
    assert full.trades.height > 0


def test_adaptive_momentum_is_a_walk_forward():
    small = ParamSpace(
        months_lookback=Param(default=3, values=[3, 6]),
        weeks_ignore=Param(default=0, values=[0]),
        top_n=Param(default=10, values=[10]),
        rebalance=Param(default="month", values=["month"]),
    )
    result = adaptive_momentum(
        DATA,
        reoptimize="3mo",
        param_space=small,
        train="6mo",
        warmup="7mo",
        config=BacktestConfig(costs=NoCosts()),
    )
    assert isinstance(result, WalkForwardResult)
    assert result.folds.height > 1
    assert result.candidates.height == result.folds.height * 2
    assert ADAPTIVE_PARAM_SPACE.grid_size() == 3 * 2 * 3 * 3  # the old grid
