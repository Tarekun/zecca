import math
import random
import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))
from analysis.framework import (
    BacktestConfig,
    NoCosts,
    Strategy,
    TargetWeights,
    check_lookahead,
)
from analysis.strategies.momentum import TopNMomentum


def business_days(start: date, n: int) -> list[date]:
    days, d = [], start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


DAYS = business_days(date(2020, 1, 1), 400)


def market() -> pl.DataFrame:
    rows = []
    for k, symbol in enumerate("ABCDEF"):
        price = 100.0
        for i, d in enumerate(DAYS):
            open_ = price
            price *= 1 + 0.0003 * k + 0.015 * math.sin(i * (k + 1) / 3)
            rows.append(
                {"timeframe": d, "symbol": symbol, "open": open_, "close": price}
            )
    return pl.DataFrame(rows)


DATA = market()
FREE = BacktestConfig(costs=NoCosts())


class PeeksOneDay(Strategy):
    def prepare(self, data):
        tomorrow = pl.col("close").shift(-1).over("symbol") / pl.col("close") - 1
        return data.select("timeframe", "symbol", tomorrow.alias("signal"))

    def decide(self, market, portfolio):
        best = market.signals(["signal"]).drop_nulls().top_k(1, by="signal")
        return TargetWeights.equal(best["symbol"])


class FullSampleStatistic(Strategy):
    """Normalizes prices by their mean over the *whole* dataset."""

    def prepare(self, data):
        relative = pl.col("close") / pl.col("close").mean().over("symbol")
        return data.select("timeframe", "symbol", relative.alias("signal"))

    def decide(self, market, portfolio):
        if not market.is_first_trading_day_of("week"):
            return None
        best = market.signals(["signal"]).top_k(2, by="signal")
        return TargetWeights.equal(best["symbol"])


class Unseeded(Strategy):
    def decide(self, market, portfolio):
        return TargetWeights({random.choice(market.universe()): 1.0})


@pytest.mark.parametrize("strategy", [PeeksOneDay, FullSampleStatistic, Unseeded])
def test_leaks_are_detected(strategy):
    check = check_lookahead(strategy, {}, DATA, config=FREE)
    assert not check.passed
    assert check.failures and len(check.cutoffs) == 3


def test_honest_strategy_passes_and_returns_the_full_backtest():
    params = dict(months_lookback=3, weeks_ignore=2, top_n=10)
    check = check_lookahead(TopNMomentum, params, DATA, config=FREE)
    assert check.passed, check.failures
    assert check.result is not None and check.result.equity.height == len(DAYS)
    assert check.result.trades.height > 0


def test_cutoffs_fall_in_the_evaluated_period():
    start = DAYS[200]
    check = check_lookahead(TopNMomentum, {}, DATA, config=FREE, start=start)
    assert all(c > start for c in check.cutoffs)
    assert check.to_dict()["cutoffs"] == [c.isoformat() for c in check.cutoffs]
