import math
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))
from analysis.framework import (
    BacktestConfig,
    BacktestResult,
    NoCosts,
    Param,
    ParamSpace,
    PortfolioState,
    Strategy,
    TargetWeights,
    grid_search,
    make_folds,
    max_drawdown,
    cagr,
    run_backtest,
    sharpe,
    walk_forward,
)

FREE = BacktestConfig(costs=NoCosts())


def business_days(start: date, end: date) -> list[date]:
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


DAYS = business_days(date(2020, 1, 1), date(2022, 12, 31))
SWITCH = date(2021, 7, 1)


def regime_market() -> pl.DataFrame:
    """A trends up and B down until SWITCH, then the opposite. A small
    deterministic wiggle keeps volatility (and so Sharpe) defined."""
    rows, a, b = [], 100.0, 100.0
    for i, d in enumerate(DAYS):
        drift = 0.002 if d < SWITCH else -0.002
        wiggle = 0.001 * math.sin(i)
        a_open, b_open = a, b
        a *= 1 + drift + wiggle
        b *= 1 - drift + wiggle
        rows.append({"timeframe": d, "symbol": "A", "open": a_open, "close": a})
        rows.append({"timeframe": d, "symbol": "B", "open": b_open, "close": b})
    return pl.DataFrame(rows)


DATA = regime_market()


class Pick(Strategy):
    """All-in on one symbol, re-targeted every month."""

    param_space = ParamSpace(symbol=Param(default="A", values=["A", "B"]))

    def decide(self, market, portfolio):
        if not market.is_first_trading_day_of("month"):
            return None
        return TargetWeights({self.symbol: 1.0})


class Recorder(Strategy):
    """Records the last date of the data each run is given."""

    param_space = ParamSpace(x=Param(default=1, values=[1, 2, 3]))
    seen: list[date] = []

    def prepare(self, data):
        Recorder.seen.append(data.select(pl.col("timeframe").max()).collect().item())
        return None

    def decide(self, market, portfolio):
        return None


class TestFolds:
    def test_rolling(self):
        folds = make_folds(DAYS, train="1y", test="6mo", warmup="6mo")
        assert [(f.train_start, f.train_end, f.test_end) for f in folds] == [
            (date(2020, 7, 1), date(2021, 7, 1), date(2022, 1, 1)),
            (date(2021, 1, 1), date(2022, 1, 1), date(2022, 7, 1)),
            (date(2021, 7, 1), date(2022, 7, 1), date(2023, 1, 1)),
        ]
        assert all(f.test_start == f.train_end for f in folds)

    def test_anchored(self):
        folds = make_folds(DAYS, train="1y", test="6mo", warmup="6mo", anchored=True)
        assert {f.train_start for f in folds} == {date(2020, 7, 1)}
        assert folds[-1].train_end == date(2022, 7, 1)

    def test_too_short(self):
        with pytest.raises(ValueError, match="too short"):
            make_folds(DAYS, train="3y", test="3mo", warmup="1y")

    def test_invalid_duration(self):
        with pytest.raises(ValueError, match="Invalid duration"):
            make_folds(DAYS, train="one year", test="3mo", warmup="1y")


class TestWalkForward:
    def test_no_fold_sees_its_test_data_while_optimizing(self):
        Recorder.seen.clear()
        result = walk_forward(
            Recorder, DATA, train="6mo", test="3mo", warmup="1mo", config=FREE
        )

        def last_day_before(end: date) -> date:
            return max(d for d in DAYS if d < end)

        expected = Counter()
        for row in result.folds.iter_rows(named=True):
            expected[last_day_before(row["train_end"])] += 3  # one per candidate
            expected[last_day_before(row["test_end"])] += 1  # the out-of-sample run
        assert Counter(Recorder.seen) == expected
        assert all(
            seen < row["test_end"]
            for row in result.folds.iter_rows(named=True)
            for seen in Recorder.seen
            if seen < row["train_end"]
        )

    def test_follows_the_regime_switch(self):
        result = walk_forward(
            Pick, DATA, train="6mo", test="3mo", warmup="1mo", config=FREE
        )
        chosen = [p["symbol"] for p in result.folds["params"].to_list()]
        train_ends = result.folds["train_end"].to_list()
        # folds trained entirely before / after the switch pick the winner of that regime
        assert all(s == "A" for s, end in zip(chosen, train_ends) if end <= SWITCH)
        starts = result.folds["train_start"].to_list()
        assert all(s == "B" for s, start in zip(chosen, starts) if start >= SWITCH)
        assert set(chosen) == {"A", "B"}

    def test_outputs(self):
        result = walk_forward(
            Pick, DATA, train="6mo", test="3mo", warmup="1mo", config=FREE
        )
        n_folds = result.folds.height
        assert result.candidates.height == n_folds * 2
        assert set(result.candidates.columns) >= {"fold", "params", "score", "sharpe"}
        first_test_day = min(d for d in DAYS if d >= result.folds["test_start"].min())
        assert result.oos.equity["timeframe"].min() == first_test_day
        assert result.oos.equity["timeframe"].is_sorted()
        assert result.oos.equity["timeframe"].n_unique() == result.oos.equity.height
        m = result.metrics
        assert m["is_oos_gap"] == pytest.approx(
            m["mean_train_score"] - m["mean_test_score"]
        )
        assert m["oos_score"] == pytest.approx(sharpe(result.oos))
        assert result.elapsed_seconds > 0

    def test_chained_folds_equal_one_continuous_backtest(self):
        """With a single candidate there is nothing to optimize: chaining the
        folds must reproduce a single backtest over the whole test period
        (portfolio carried across folds, no trading gap at fold boundaries)."""
        only_a = ParamSpace(symbol=Param(default="A", values=["A"]))
        config = BacktestConfig()  # with costs, so extra trades would show
        result = walk_forward(
            Pick,
            DATA,
            train="6mo",
            test="3mo",
            warmup="1mo",
            param_space=only_a,
            config=config,
        )
        last_end = result.folds["test_end"].max()
        continuous = run_backtest(
            Pick(symbol="A"),
            DATA.filter(pl.col("timeframe") < last_end),
            config,
            start=result.folds["test_start"].min(),
        )
        assert (
            result.oos.equity["timeframe"].to_list()
            == continuous.equity["timeframe"].to_list()
        )
        assert result.oos.equity["portfolio_value"].to_list() == pytest.approx(
            continuous.equity["portfolio_value"].to_list()
        )
        assert result.oos.trades.height == continuous.trades.height

    def test_parallel_matches_sequential(self):
        kwargs = dict(train="6mo", test="3mo", warmup="1mo", config=FREE)
        sequential = walk_forward(Pick, DATA, n_jobs=1, **kwargs)
        parallel = walk_forward(Pick, DATA, n_jobs=2, **kwargs)
        assert parallel.candidates.equals(sequential.candidates)
        assert parallel.folds.equals(sequential.folds)


class TestGridSearch:
    def test_ranks_candidates(self):
        before = grid_search(Pick, DATA, end=SWITCH, config=FREE)
        after = grid_search(Pick, DATA, start=SWITCH, config=FREE)
        assert before.best_params == {"symbol": "A"}
        assert after.best_params == {"symbol": "B"}
        assert before.scores.height == 2
        assert before.best_score == before.scores["score"].max()

    def test_end_is_exclusive(self):
        Recorder.seen.clear()
        grid_search(Recorder, DATA, end=SWITCH, config=FREE)
        assert Recorder.seen == [date(2021, 6, 30)] * 3

    def test_nan_scores_never_win(self):
        def objective(result):
            return math.nan if result.params["symbol"] == "A" else -1.0

        assert grid_search(
            Pick, DATA, objective=objective, config=FREE
        ).best_params == {"symbol": "B"}

    def test_custom_param_space(self):
        only_b = ParamSpace(symbol=Param(default="B", values=["B"]))
        result = grid_search(Pick, DATA, param_space=only_b, config=FREE)
        assert result.scores.height == 1 and result.best_params == {"symbol": "B"}


def fake_result(
    values: list[float], initial: float = 100.0, rf: float = 0.0
) -> BacktestResult:
    days = DAYS[: len(values)]
    equity = pl.DataFrame(
        {
            "timeframe": days,
            "portfolio_value": values,
            "risk_free": [rf] * len(values),
        }
    )
    empty = pl.DataFrame()
    return BacktestResult(
        strategy="fake",
        params={},
        config=BacktestConfig(initial_cash=initial),
        equity=equity,
        trades=empty,
        positions=empty,
        decisions=empty,
        events=empty,
        initial_value=initial,
        final_state=PortfolioState.empty(days[-1], values[-1]),
    )


class TestMetrics:
    def test_sharpe(self):
        values = [101.0, 100.0, 102.0, 101.0]
        result = fake_result(values, rf=0.001)
        returns = [v / p - 1 for p, v in zip([100.0] + values, values)]
        excess = [r - 0.001 for r in returns]
        mean = sum(excess) / len(excess)
        std = (sum((x - mean) ** 2 for x in excess) / (len(excess) - 1)) ** 0.5
        assert sharpe(result) == pytest.approx(mean / std * math.sqrt(252))

    def test_sharpe_undefined_without_volatility(self):
        assert math.isnan(sharpe(fake_result([100.0, 100.0, 100.0])))

    def test_max_drawdown_includes_initial_value(self):
        assert max_drawdown(fake_result([120.0, 90.0, 130.0])) == pytest.approx(-0.25)
        assert max_drawdown(fake_result([80.0, 90.0])) == pytest.approx(-0.2)

    def test_cagr(self):
        assert cagr(fake_result([100.0] * 251 + [110.0])) == pytest.approx(0.1)
