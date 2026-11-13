"""Robustness checks: is a backtest result significant, stable and real?

- `random_baseline`: how the strategy ranks among random portfolios with the
  same rebalancing days, number of positions and weights, on the same
  universe (is the stock selection better than chance?).
- `bootstrap`: block-bootstrap confidence intervals of Sharpe, CAGR and max
  drawdown (how much of the result could be noise?).
- `random_walk_test`: runs the strategy on synthetic markets where days are
  shuffled, so no strategy can predict anything; a significant excess return
  there means a bug or look-ahead, not skill.
- `parameter_sensitivity`: the objective over a parameter grid, flagging
  optima isolated among poor neighbors (overfit parameters).
- `deflated_sharpe` / `probabilistic_sharpe`: the probability that the true
  Sharpe ratio is positive, correcting for non-normal returns, track length
  and the number of variants tried (Bailey & López de Prado, 2014).

Every function returns a JSON-serializable dict."""

import math
import random
from statistics import NormalDist
from typing import Any

import numpy as np
import polars as pl

from analysis.framework.engine import run_backtest
from analysis.framework.market import DATE_COL, SYMBOL_COL, Panel
from analysis.framework.metrics import (
    TRADING_DAYS_PER_YEAR,
    cagr,
    daily_returns,
    max_drawdown,
    sharpe,
)
from analysis.framework.optimization import GridSearchResult
from analysis.framework.params import ParamSpace
from analysis.framework.result import BacktestConfig, BacktestResult
from analysis.framework.strategy import Strategy
from analysis.framework.weights import TargetWeights

_EULER_GAMMA = 0.5772156649015329
_NORMAL = NormalDist()
_SIGNIFICANT_T = 2.0


# ---- random baseline -------------------------------------------------------------


class RandomPortfolios(Strategy):
    """Replays a strategy's decisions with random symbols: on every day the
    original strategy rebalanced, picks as many symbols as it held, uniformly
    from that day's universe, and gives them the same weights."""

    def __init__(self, schedule: dict[Any, list[float]], seed: int) -> None:
        super().__init__()
        self._schedule = schedule
        self._rng = random.Random(seed)

    def decide(self, market, portfolio):
        weights = self._schedule.get(market.date)
        if weights is None:
            return None
        universe = market.universe()
        chosen = self._rng.sample(universe, min(len(weights), len(universe)))
        return TargetWeights(dict(zip(chosen, weights)))


def decision_schedule(result: BacktestResult) -> dict[Any, list[float]]:
    """Decision day -> the weights decided that day (empty list = all cash)."""
    schedule: dict[Any, list[float]] = {}
    for (day,), group in result.decisions.group_by(DATE_COL, maintain_order=True):
        weights = group.filter(pl.col(SYMBOL_COL).is_not_null())["weight"]
        schedule[day] = sorted(weights.to_list(), reverse=True)
    return schedule


def random_baseline(
    result: BacktestResult,
    data: pl.DataFrame | Panel,
    *,
    n: int = 100,
    seed: int = 0,
    config: BacktestConfig | None = None,
    risk_free: pl.DataFrame | None = None,
    start: Any = None,
) -> dict[str, Any]:
    """Percentile of the strategy's Sharpe and total return among `n` random
    portfolios (same rebalance days, position counts and weights, same
    universe, same costs). `p_value` is the share of random portfolios at
    least as good: below 0.05 the stock selection beats chance."""
    panel = data if isinstance(data, Panel) else Panel(data)
    schedule = decision_schedule(result)
    config = config or result.config
    start = start if start is not None else result.equity[DATE_COL][0]
    random_runs = [
        run_backtest(
            RandomPortfolios(schedule, seed * 1_000_003 + i),
            panel,
            config,
            risk_free,
            start=start,
        )
        for i in range(n)
    ]
    out: dict[str, Any] = {"n": n, "seed": seed}
    for name, value, values in [
        ("sharpe", sharpe(result), [sharpe(r) for r in random_runs]),
        ("total_return", result.total_return, [r.total_return for r in random_runs]),
    ]:
        finite = np.array([v for v in values if math.isfinite(v)])
        out[name] = {
            "strategy": value,
            "random_mean": float(finite.mean()) if finite.size else math.nan,
            "random_p05": float(np.quantile(finite, 0.05)) if finite.size else math.nan,
            "random_p95": float(np.quantile(finite, 0.95)) if finite.size else math.nan,
            "percentile": float((finite < value).mean()) if finite.size else math.nan,
            "p_value": float(((finite >= value).sum() + 1) / (finite.size + 1)),
        }
    return out


# ---- bootstrap -----------------------------------------------------------------------


def bootstrap(
    result: BacktestResult,
    *,
    n: int = 1000,
    block: int = 21,
    confidence: float = 0.95,
    seed: int = 0,
) -> dict[str, Any]:
    """Circular block bootstrap of daily returns (blocks keep short-term
    autocorrelation): `confidence` intervals of annualized Sharpe (over the
    risk-free rate), CAGR and max drawdown, plus the share of resamples with a
    positive Sharpe."""
    returns = daily_returns(result).to_numpy()
    rf = result.equity["risk_free"].to_numpy()
    days = returns.size
    if days < 2:
        raise ValueError("Not enough days to bootstrap")
    block = max(1, min(block, days // 4 or 1))
    rng = np.random.default_rng(seed)
    blocks = math.ceil(days / block)
    starts = rng.integers(0, days, size=(n, blocks))
    idx = ((starts[:, :, None] + np.arange(block)) % days).reshape(n, -1)[:, :days]
    sampled, sampled_rf = returns[idx], rf[idx]

    excess = sampled - sampled_rf
    std = excess.std(axis=1, ddof=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        sharpes = np.where(
            std > 0,
            excess.mean(axis=1) / std * math.sqrt(TRADING_DAYS_PER_YEAR),
            np.nan,
        )
    growth = np.prod(1 + sampled, axis=1)
    cagrs = np.where(growth > 0, growth ** (TRADING_DAYS_PER_YEAR / days) - 1, np.nan)
    paths = np.cumprod(1 + sampled, axis=1)
    peaks = np.maximum(np.maximum.accumulate(paths, axis=1), 1.0)
    drawdowns = np.minimum((paths / peaks - 1).min(axis=1), 0.0)

    tail = (1 - confidence) / 2

    def interval(values: np.ndarray, estimate: float) -> dict[str, float]:
        finite = values[np.isfinite(values)]
        return {
            "estimate": estimate,
            "low": float(np.quantile(finite, tail)) if finite.size else math.nan,
            "high": float(np.quantile(finite, 1 - tail)) if finite.size else math.nan,
        }

    return {
        "n": n,
        "block_days": block,
        "confidence": confidence,
        "sharpe": interval(sharpes, sharpe(result)),
        "cagr": interval(cagrs, cagr(result)),
        "max_drawdown": interval(drawdowns, max_drawdown(result)),
        "prob_sharpe_positive": float(np.nanmean(sharpes > 0)),
    }


# ---- synthetic random walk -----------------------------------------------------------


def synthetic_market(
    data: pl.DataFrame | Panel, seed: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """A market with the same symbols, trading days and pool of daily
    returns as `data`, where nothing is predictable: the order of days is
    shuffled (no serial dependence, no market timing) and, within each day,
    the returns are reassigned at random among the symbols trading that day
    (no stock keeps its own drift, so past returns say nothing about which
    stocks will do well). The market's daily average and cross-sectional
    dispersion are preserved. Returns the synthetic OHLCV panel and its
    equal-weight benchmark daily returns (`timeframe`, `benchmark`)."""
    df = (data.frame if isinstance(data, Panel) else Panel(data).frame).select(
        DATE_COL, SYMBOL_COL, "open", "close"
    )
    close = df.pivot(
        on=SYMBOL_COL, index=DATE_COL, values="close", sort_columns=True
    ).sort(DATE_COL)
    opens = df.pivot(
        on=SYMBOL_COL, index=DATE_COL, values="open", sort_columns=True
    ).sort(DATE_COL)
    days, symbols = close[DATE_COL], close.columns[1:]
    c = close.select(symbols).to_numpy().astype(float)
    o = opens.select(symbols).to_numpy().astype(float)

    present = np.isfinite(c) & np.isfinite(o) & (c > 0) & (o > 0)
    prev = np.vstack([np.full((1, c.shape[1]), np.nan), c[:-1]])
    with np.errstate(divide="ignore", invalid="ignore"):
        close_ret = c / prev - 1
        gap = o / prev - 1
    usable = present & np.isfinite(close_ret) & np.isfinite(gap)

    rng = np.random.default_rng(seed)
    # 1. shuffle the order of days
    perm = rng.permutation(len(days))
    day_r, day_g, day_ok = close_ret[perm], gap[perm], usable[perm]
    # 2. within each day, hand that day's returns to the symbols trading on
    # the synthetic day at random (presence pattern of the original dates)
    r = np.zeros_like(c)
    g = np.zeros_like(c)
    for t in range(len(days)):
        targets = np.flatnonzero(present[t])
        pool = np.flatnonzero(day_ok[t])
        if targets.size == 0 or pool.size == 0:
            continue
        picks = rng.choice(pool, size=targets.size, replace=targets.size > pool.size)
        r[t, targets] = day_r[t, picks]
        g[t, targets] = day_g[t, picks]
    r, g = np.clip(r, -0.99, None), np.clip(g, -0.99, None)
    close_path = 100 * np.cumprod(1 + r, axis=0)
    prev_close = np.vstack([np.full((1, c.shape[1]), 100.0), close_path[:-1]])
    open_path = prev_close * (1 + g)

    # equal-weight market: mean return of the symbols trading each day
    bench_ret = np.where(present, r, 0.0).sum(axis=1) / np.maximum(
        present.sum(axis=1), 1
    )

    t_idx, s_idx = np.nonzero(present)
    sym = np.array(symbols)
    o_v, c_v = open_path[t_idx, s_idx], close_path[t_idx, s_idx]
    synthetic = pl.DataFrame(
        {
            DATE_COL: days.gather(t_idx.tolist()),
            SYMBOL_COL: sym[s_idx],
            "open": o_v,
            "high": np.maximum(o_v, c_v),
            "low": np.minimum(o_v, c_v),
            "close": c_v,
            "volume": np.ones_like(c_v),
        }
    )
    bench = pl.DataFrame({DATE_COL: days, "benchmark": bench_ret})
    return synthetic, bench


def random_walk_test(
    strategy_cls: type[Strategy],
    params: dict[str, Any],
    data: pl.DataFrame | Panel,
    *,
    n_paths: int = 5,
    seed: int = 0,
    config: BacktestConfig = BacktestConfig(),
    start: Any = None,
) -> dict[str, Any]:
    """Runs the strategy on `n_paths` shuffled-days markets (see
    `synthetic_market`) and measures its daily excess return over the
    equal-weight market of each path (which pays no costs, so the expected
    excess is slightly negative). `suspicious` when most paths show a
    significantly positive excess (t-stat > 2): there should be nothing to
    find, so finding something means a bug or look-ahead."""
    if strategy_cls.data_columns:
        return {
            "status": "skipped",
            "reason": f"needs data columns {list(strategy_cls.data_columns)} that "
            "cannot be synthesized (only OHLCV is)",
        }
    paths = []
    for i in range(n_paths):
        synthetic, bench = synthetic_market(data, seed * 1_000_003 + i)
        result = run_backtest(strategy_cls(**params), synthetic, config, start=start)
        joined = pl.DataFrame(
            {DATE_COL: result.equity[DATE_COL], "strategy": daily_returns(result)}
        ).join(bench, on=DATE_COL, how="left")
        excess = (joined["strategy"] - joined["benchmark"].fill_null(0.0)).to_numpy()
        std = excess.std(ddof=1)
        t = float(excess.mean() / std * math.sqrt(excess.size)) if std > 0 else 0.0
        paths.append(
            {
                "seed": seed * 1_000_003 + i,
                "annual_excess_return": float(excess.mean() * TRADING_DAYS_PER_YEAR),
                "t_stat": t,
            }
        )
    significant = sum(p["t_stat"] > _SIGNIFICANT_T for p in paths)
    return {
        "status": "ok",
        "n_paths": n_paths,
        "mean_annual_excess_return": float(
            np.mean([p["annual_excess_return"] for p in paths])
        ),
        "significant_paths": significant,
        "suspicious": significant > n_paths / 2,
        "paths": paths,
    }


# ---- parameter sensitivity -----------------------------------------------------------


def parameter_sensitivity(
    grid: GridSearchResult, param_space: ParamSpace
) -> dict[str, Any]:
    """Shape of the objective around the best parameters. Neighbors of the
    best candidate differ in one parameter by one step of its grid; the
    optimum is `isolated` when it beats its neighbors' mean by more than the
    spread (std) of all candidates -- a lone peak that small changes destroy
    is likely overfit. `marginals` give the mean score per parameter value
    (the rows/columns of a heatmap)."""
    table = grid.scores
    sharpes = (
        table["sharpe"].to_list()
        if "sharpe" in table.columns
        else [None] * table.height
    )
    rows = [
        {"params": dict(p), "score": s, "sharpe": sr}
        for p, s, sr in zip(
            table["params"].to_list(), table["score"].to_list(), sharpes
        )
    ]
    scores = np.array([r["score"] for r in rows], dtype=float)
    finite = scores[np.isfinite(scores)]
    best = grid.best_params
    grids = {name: p.grid() for name, p in param_space.items()}

    neighbors = []
    for name, values in grids.items():
        if best[name] not in values:
            continue
        i = values.index(best[name])
        for j in (i - 1, i + 1):
            if 0 <= j < len(values):
                wanted = best | {name: values[j]}
                neighbors += [r["score"] for r in rows if r["params"] == wanted]
    neighbor_scores = [s for s in neighbors if math.isfinite(s)]
    neighbor_mean = float(np.mean(neighbor_scores)) if neighbor_scores else math.nan
    spread = float(finite.std()) if finite.size > 1 else math.nan
    drop = grid.best_score - neighbor_mean

    marginals = {
        name: {
            str(v): _nanmean([r["score"] for r in rows if r["params"][name] == v])
            for v in values
        }
        for name, values in grids.items()
    }
    return {
        "candidates": len(rows),
        "best_params": best,
        "best_score": grid.best_score,
        "neighbor_mean_score": neighbor_mean,
        "drop_to_neighbors": drop,
        "scores_std": spread,
        "isolated": bool(
            math.isfinite(drop) and math.isfinite(spread) and drop > spread
        ),
        "marginals": marginals,
        "scores": rows,
    }


def _nanmean(values: list[float]) -> float:
    finite = [v for v in values if math.isfinite(v)]
    return float(np.mean(finite)) if finite else math.nan


# ---- deflated Sharpe ratio -----------------------------------------------------------


def probabilistic_sharpe(
    sharpe_ratio: float,
    observations: int,
    skew: float,
    kurtosis: float,
    benchmark: float = 0.0,
) -> float:
    """P(true Sharpe > `benchmark`) given an observed per-period Sharpe ratio
    over `observations` periods with returns' `skew` and (non-excess)
    `kurtosis` (Bailey & López de Prado, 2012). Ratios are per period, not
    annualized."""
    variance = 1 - skew * sharpe_ratio + (kurtosis - 1) / 4 * sharpe_ratio**2
    if observations < 2 or variance <= 0:
        return math.nan
    z = (sharpe_ratio - benchmark) * math.sqrt(observations - 1) / math.sqrt(variance)
    return _NORMAL.cdf(z)


def expected_max_sharpe(n_trials: int, sharpe_variance: float) -> float:
    """Expected maximum per-period Sharpe among `n_trials` unskilled variants
    whose Sharpe ratios have variance `sharpe_variance` (per period)."""
    if n_trials <= 1:
        return 0.0
    return math.sqrt(sharpe_variance) * (
        (1 - _EULER_GAMMA) * _NORMAL.inv_cdf(1 - 1 / n_trials)
        + _EULER_GAMMA * _NORMAL.inv_cdf(1 - 1 / (n_trials * math.e))
    )


def deflated_sharpe(
    result: BacktestResult, n_trials: int, trials_sharpe_variance: float | None
) -> dict[str, Any]:
    """Deflated Sharpe ratio: probability that the strategy's true Sharpe is
    positive *given that it is the best of `n_trials` variants tried*, whose
    annualized Sharpe ratios have variance `trials_sharpe_variance` (Bailey &
    López de Prado, 2014). Above 0.95 the result survives the multiple-testing
    correction. With one trial it equals the probabilistic Sharpe ratio."""
    returns = (daily_returns(result) - result.equity["risk_free"]).to_numpy()
    observations = returns.size
    std = returns.std(ddof=1) if observations > 1 else 0.0
    if std == 0 or not math.isfinite(std):
        return {"deflated_sharpe": math.nan, "reason": "no return variability"}
    sr = returns.mean() / std
    centered = returns - returns.mean()
    m2 = (centered**2).mean()
    skew = float((centered**3).mean() / m2**1.5)
    kurtosis = float((centered**4).mean() / m2**2)

    if n_trials > 1 and (
        trials_sharpe_variance is None or not math.isfinite(trials_sharpe_variance)
    ):
        return {
            "deflated_sharpe": math.nan,
            "n_trials": n_trials,
            "reason": "variance of the trials' Sharpe ratios unknown",
        }
    per_period_variance = (trials_sharpe_variance or 0.0) / TRADING_DAYS_PER_YEAR
    threshold = expected_max_sharpe(n_trials, per_period_variance)
    return {
        "deflated_sharpe": probabilistic_sharpe(
            sr, observations, skew, kurtosis, threshold
        ),
        "probabilistic_sharpe": probabilistic_sharpe(sr, observations, skew, kurtosis),
        "n_trials": n_trials,
        "trials_sharpe_variance": trials_sharpe_variance,
        "expected_max_sharpe_annual": threshold * math.sqrt(TRADING_DAYS_PER_YEAR),
        "sharpe_annual": sr * math.sqrt(TRADING_DAYS_PER_YEAR),
        "skew": skew,
        "kurtosis": kurtosis,
        "observations": observations,
    }
