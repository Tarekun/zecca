import math
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))
from analysis.framework import (
    BacktestConfig,
    GridSearchResult,
    NoCosts,
    Param,
    ParamSpace,
    Strategy,
    TargetWeights,
    run_backtest,
)
from analysis.framework.robustness import (
    RandomPortfolios,
    bootstrap,
    decision_schedule,
    deflated_sharpe,
    expected_max_sharpe,
    parameter_sensitivity,
    probabilistic_sharpe,
    random_baseline,
    random_walk_test,
    synthetic_market,
)
from analysis.strategies.momentum import TopNMomentum

FREE = BacktestConfig(costs=NoCosts())


def business_days(start: date, n: int) -> list[date]:
    days, d = [], start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


DAYS = business_days(date(2020, 1, 1), 500)


def market(n_symbols: int = 20, seed: int = 1) -> pl.DataFrame:
    """Symbols with persistent, different drifts (momentum works here) plus noise."""
    rng = np.random.default_rng(seed)
    rows = []
    for k in range(n_symbols):
        drift = 0.004 * (k / n_symbols - 0.5)
        price = 100.0
        for d, noise in zip(DAYS, rng.normal(0, 0.01, len(DAYS))):
            open_ = price * (1 + rng.normal(0, 0.002))
            price = open_ * (1 + drift + noise)
            rows.append(
                {"timeframe": d, "symbol": f"S{k:02d}", "open": open_, "close": price}
            )
    return pl.DataFrame(rows)


DATA = market()


class BestDrift(Strategy):
    """Knows nothing it shouldn't: holds the 3 best performers of the last
    60 days, monthly."""

    def prepare(self, data):
        c = pl.col("close")
        return data.select(
            "timeframe", "symbol", (c / c.shift(60) - 1).over("symbol").alias("m")
        )

    def decide(self, market, portfolio):
        if not market.is_first_trading_day_of("month"):
            return None
        best = market.signals(["m"]).drop_nulls().top_k(3, by="m")
        return TargetWeights.equal(best["symbol"])


class PeeksTomorrow(Strategy):
    def prepare(self, data):
        nxt = pl.col("close").shift(-1).over("symbol") / pl.col("close") - 1
        return data.select("timeframe", "symbol", nxt.alias("next"))

    def decide(self, market, portfolio):
        best = market.signals(["next"]).drop_nulls().top_k(3, by="next")
        return TargetWeights.equal(best["symbol"])


class TestDeflatedSharpe:
    def test_paper_example(self):
        """Bailey & López de Prado (2014): annual SR 2.5 over 5 years of
        daily data, skew -3, kurtosis 10, best of 100 trials whose annual
        Sharpe ratios have variance 0.5 -> DSR ~ 0.90."""
        days_per_year = 250  # the paper's convention
        threshold = expected_max_sharpe(100, 0.5 / days_per_year)
        assert threshold == pytest.approx(0.1132, abs=5e-4)
        dsr = probabilistic_sharpe(
            2.5 / math.sqrt(days_per_year), 1250, -3, 10, threshold
        )
        assert dsr == pytest.approx(0.9004, abs=2e-3)

    def test_more_trials_raise_the_bar(self):
        assert expected_max_sharpe(1, 0.01) == 0.0
        values = [expected_max_sharpe(n, 0.01) for n in (2, 10, 100, 1000)]
        assert values == sorted(values) and values[0] > 0

    def test_on_a_backtest(self):
        result = run_backtest(BestDrift(), DATA, FREE)
        single = deflated_sharpe(result, 1, None)
        assert single["deflated_sharpe"] == pytest.approx(
            single["probabilistic_sharpe"]
        )
        many = deflated_sharpe(result, 50, 0.5)
        assert many["deflated_sharpe"] < single["deflated_sharpe"]
        assert many["expected_max_sharpe_annual"] > 0
        unknown = deflated_sharpe(result, 50, None)
        assert math.isnan(unknown["deflated_sharpe"]) and "unknown" in unknown["reason"]


class TestBootstrap:
    def test_intervals(self):
        result = run_backtest(BestDrift(), DATA, FREE)
        out = bootstrap(result, n=300, seed=3)
        for metric in ("sharpe", "cagr", "max_drawdown"):
            assert out[metric]["low"] <= out[metric]["high"]
        assert (
            out["sharpe"]["low"] <= out["sharpe"]["estimate"] <= out["sharpe"]["high"]
        )
        assert out["max_drawdown"]["high"] <= 0
        assert 0 <= out["prob_sharpe_positive"] <= 1
        assert bootstrap(result, n=300, seed=3) == out  # deterministic


class TestRandomBaseline:
    def test_random_portfolios_replay_the_schedule(self):
        result = run_backtest(BestDrift(), DATA, FREE)
        schedule = decision_schedule(result)
        replay = run_backtest(RandomPortfolios(schedule, seed=7), DATA, FREE)
        assert decision_schedule(replay) == schedule  # same days, counts and weights

    def test_skill_ranks_high(self):
        result = run_backtest(BestDrift(), DATA, FREE)
        out = random_baseline(result, DATA, n=30, config=FREE)
        assert out["sharpe"]["percentile"] > 0.8
        assert out["total_return"]["p_value"] < 0.2


class TestRandomWalk:
    def test_synthetic_market_keeps_the_panel_shape(self):
        synthetic, bench = synthetic_market(DATA, seed=0)
        key = ["timeframe", "symbol"]
        assert synthetic.select(key).sort(key).equals(DATA.select(key).sort(key))
        assert bench.height == len(DAYS)
        assert (synthetic["high"] >= synthetic["low"]).all()

    def test_shuffling_removes_the_edge(self):
        out = random_walk_test(BestDrift, {}, DATA, n_paths=4, config=FREE)
        assert out["status"] == "ok" and not out["suspicious"]

    def test_look_ahead_is_suspicious(self):
        out = random_walk_test(PeeksTomorrow, {}, DATA, n_paths=3, config=FREE)
        assert out["suspicious"]
        assert out["significant_paths"] == 3

    def test_momentum_finds_nothing(self):
        params = dict(months_lookback=3, weeks_ignore=0, top_n=10)
        out = random_walk_test(TopNMomentum, params, DATA, n_paths=4, config=FREE)
        assert not out["suspicious"]

    def test_strategies_needing_extra_columns_are_skipped(self):
        class NeedsRsi(BestDrift):
            data_columns = ("rsi",)

        assert random_walk_test(NeedsRsi, {}, DATA)["status"] == "skipped"


SPACE = ParamSpace(
    a=Param(default=1, values=[1, 2, 3, 4, 5]),
    b=Param(default="x", values=["x", "y", "z"]),
)


def grid(score) -> GridSearchResult:
    rows = [{"params": p, "score": float(score(p))} for p in SPACE.grid()]
    best = max(rows, key=lambda r: r["score"])
    return GridSearchResult(
        scores=pl.DataFrame(rows),
        best_params=best["params"],
        best_score=best["score"],
        elapsed_seconds=0.0,
    )


class TestSensitivity:
    def test_smooth_optimum(self):
        out = parameter_sensitivity(grid(lambda p: -((p["a"] - 3) ** 2)), SPACE)
        assert out["best_params"]["a"] == 3 and not out["isolated"]
        assert out["marginals"]["a"]["3"] == max(out["marginals"]["a"].values())
        assert out["candidates"] == 15

    def test_isolated_peak(self):
        spike = lambda p: 5.0 if (p["a"], p["b"]) == (4, "y") else 0.0  # noqa: E731
        out = parameter_sensitivity(grid(spike), SPACE)
        assert out["best_params"] == {"a": 4, "b": "y"}
        assert out["isolated"]
        assert out["neighbor_mean_score"] == 0.0
