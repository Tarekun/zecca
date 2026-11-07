"""Parameter optimization protocols: grid search and walk-forward.

Both evaluate a strategy *class* over its `ParamSpace` (or a custom one):
every candidate is a fresh instance built from a params dict and run by the
backtest engine on data that ends before the evaluation window ends, so no
candidate ever sees data from the period it is later tested on.

Candidates run sequentially by default; `n_jobs > 1` runs them in worker
processes (spawned, so the strategy class and the objective must be
importable from a module -- not defined in a notebook or as a lambda)."""

import logging
import math
import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Callable

import polars as pl

from analysis.framework.engine import run_backtest
from analysis.framework.market import DATE_COL, Panel
from analysis.framework.metrics import sharpe, summary
from analysis.framework.params import ParamSpace
from analysis.framework.portfolio import PortfolioState
from analysis.framework.result import BacktestConfig, BacktestResult
from analysis.framework.strategy import Strategy

logger = logging.getLogger(__name__)

Objective = Callable[[BacktestResult], float]


# ---- results ---------------------------------------------------------------


@dataclass(frozen=True)
class GridSearchResult:
    """`scores` has one row per candidate: `params` (struct, `.unnest()` it
    for one column per parameter), `score` (the objective) and the summary
    metrics. `best_params` maximizes the objective (NaN scores never win)."""

    scores: pl.DataFrame
    best_params: dict[str, Any]
    best_score: float
    elapsed_seconds: float


@dataclass(frozen=True)
class Fold:
    """Windows are half-open: [train_start, train_end) and [test_start, test_end)."""

    index: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date


@dataclass(frozen=True)
class WalkForwardResult:
    """- `folds`: one row per fold: windows, chosen `params`, `train_score`
      (best in-sample objective) and `test_score` (same params, out of sample).
    - `candidates`: every (fold, candidate) evaluated in-sample, with its
      score and summary metrics -- the full count of variants tried, needed
      for multiple-testing corrections.
    - `oos`: the out-of-sample backtest, folds chained so the portfolio is
      carried from one fold to the next (`oos.params` is empty: parameters
      change per fold, see `folds`).
    - `metrics`: summary of `oos` plus `oos_score`, `mean_train_score`,
      `mean_test_score` and `is_oos_gap` (mean train - mean test score: a
      large gap means the in-sample optimum doesn't carry over, i.e. overfit).
    """

    folds: pl.DataFrame
    candidates: pl.DataFrame
    oos: BacktestResult
    metrics: dict[str, float]
    elapsed_seconds: float


# ---- public protocols --------------------------------------------------------


def grid_search(
    strategy_cls: type[Strategy],
    data: pl.DataFrame | pl.LazyFrame,
    *,
    start: date | None = None,
    end: date | None = None,
    param_space: ParamSpace | None = None,
    objective: Objective = sharpe,
    config: BacktestConfig = BacktestConfig(),
    risk_free: pl.DataFrame | None = None,
    n_jobs: int = 1,
) -> GridSearchResult:
    """Backtests every combination of `param_space` (default: the strategy's
    own) over [`start`, `end`) and ranks them by `objective`. Data before
    `start` is warmup history; data from `end` on is never seen."""
    began = time.perf_counter()
    df = _normalize(data)
    grid = list((param_space or strategy_cls.param_space).grid())
    end = end or df[DATE_COL].max() + timedelta(days=1)  # type: ignore[operator]
    tasks = [
        _Task(0, strategy_cls, params, start, end, config, objective) for params in grid
    ]
    rows = _run_tasks(tasks, df, risk_free, n_jobs)
    best = _best(rows)
    return GridSearchResult(
        scores=_rows_frame(rows),
        best_params=best["params"],
        best_score=best["score"],
        elapsed_seconds=time.perf_counter() - began,
    )


def walk_forward(
    strategy_cls: type[Strategy],
    data: pl.DataFrame | pl.LazyFrame,
    *,
    train: str = "1y",
    test: str = "3mo",
    warmup: str = "1y",
    anchored: bool = False,
    param_space: ParamSpace | None = None,
    objective: Objective = sharpe,
    config: BacktestConfig = BacktestConfig(),
    risk_free: pl.DataFrame | None = None,
    n_jobs: int = 1,
) -> WalkForwardResult:
    """Walk-forward optimization.

    The first `warmup` of data is history only (so lookbacks are filled from
    the first training day). Then, fold after fold: every candidate is
    backtested on the `train` window, the best one by `objective` is run on
    the following `test` window, and the window moves forward by `test`.
    With `anchored=True` the training window always starts at the end of the
    warmup and grows; otherwise it rolls with fixed length.

    Test windows are contiguous and run as one continuous simulation: each
    fold starts from the portfolio the previous one ended with, and the new
    parameters decide at the close before the fold's first day. Durations
    use polars syntax: "1y", "6mo", "2w", "30d"."""
    began = time.perf_counter()
    df = _normalize(data)
    folds = make_folds(
        df[DATE_COL].unique().sort().to_list(), train, test, warmup, anchored
    )
    grid = list((param_space or strategy_cls.param_space).grid())
    logger.info(
        "Walk-forward %s: %d folds x %d candidates",
        strategy_cls.__name__,
        len(folds),
        len(grid),
    )

    tasks = [
        _Task(
            f.index, strategy_cls, params, f.train_start, f.train_end, config, objective
        )
        for f in folds
        for params in grid
    ]
    rows = _run_tasks(tasks, df, risk_free, n_jobs)

    fold_rows, results = [], []
    state: PortfolioState | None = None
    for fold in folds:
        best = _best([r for r in rows if r["fold"] == fold.index])
        result = run_backtest(
            strategy_cls(**best["params"]),
            Panel(df.filter(pl.col(DATE_COL) < fold.test_end)),
            config,
            risk_free,
            start=fold.test_start,
            initial=state,
        )
        state = result.final_state
        results.append(result)
        fold_rows.append(
            {
                "fold": fold.index,
                "train_start": fold.train_start,
                "train_end": fold.train_end,
                "test_start": fold.test_start,
                "test_end": fold.test_end,
                "params": best["params"],
                "train_score": best["score"],
                "test_score": objective(result),
            }
        )
        logger.info("Fold %d %s: %s", fold.index, fold.test_start, fold_rows[-1])

    oos = _chain(strategy_cls.__name__, config, results)
    folds_df = pl.DataFrame(fold_rows)
    mean_train = _nanmean(folds_df["train_score"])
    mean_test = _nanmean(folds_df["test_score"])
    metrics = summary(oos) | {
        "oos_score": objective(oos),
        "mean_train_score": mean_train,
        "mean_test_score": mean_test,
        "is_oos_gap": mean_train - mean_test,
    }
    return WalkForwardResult(
        folds=folds_df,
        candidates=_rows_frame(rows),
        oos=oos,
        metrics=metrics,
        elapsed_seconds=time.perf_counter() - began,
    )


def make_folds(
    dates: list[date], train: str, test: str, warmup: str, anchored: bool = False
) -> list[Fold]:
    """Fold windows over the trading `dates` (see `walk_forward`)."""
    if not dates:
        raise ValueError("No dates to split into folds")
    first_train_start = _offset(dates[0], warmup)
    train_end = _offset(first_train_start, train)
    folds: list[Fold] = []
    while train_end <= dates[-1]:
        test_end = _offset(train_end, test)
        train_start = first_train_start if anchored else _offset(train_end, f"-{train}")
        folds.append(Fold(len(folds), train_start, train_end, train_end, test_end))
        train_end = test_end
    if not folds:
        raise ValueError(
            f"Data from {dates[0]} to {dates[-1]} is too short for warmup={warmup} + train={train}"
        )
    return folds


# ---- candidate evaluation ----------------------------------------------------


@dataclass(frozen=True)
class _Task:
    fold: int
    strategy_cls: type[Strategy]
    params: dict[str, Any]
    start: date | None
    end: date  # exclusive: the candidate never sees data from here on
    config: BacktestConfig
    objective: Objective


# per-process state: the market data and the panel of the window being evaluated
_WORKER: dict[str, Any] = {}


def _init_worker(data: pl.DataFrame, risk_free: pl.DataFrame | None) -> None:
    _WORKER.clear()
    _WORKER.update(data=data, risk_free=risk_free, panel_end=None, panel=None)


def _panel_until(end: date) -> Panel:
    # tasks arrive grouped by window, so caching the last panel avoids
    # re-validating the same data for every candidate of a fold
    if _WORKER["panel_end"] != end:
        _WORKER["panel"] = Panel(_WORKER["data"].filter(pl.col(DATE_COL) < end))
        _WORKER["panel_end"] = end
    return _WORKER["panel"]


def _evaluate(task: _Task) -> dict[str, Any]:
    result = run_backtest(
        task.strategy_cls(**task.params),
        _panel_until(task.end),
        task.config,
        _WORKER["risk_free"],
        start=task.start,
    )
    return {
        "fold": task.fold,
        "params": task.params,
        "score": float(task.objective(result)),
        **summary(result),
    }


def _run_tasks(
    tasks: list[_Task], data: pl.DataFrame, risk_free: pl.DataFrame | None, n_jobs: int
) -> list[dict[str, Any]]:
    workers = (os.cpu_count() or 1) if n_jobs == -1 else n_jobs
    if workers < 1:
        raise ValueError(f"n_jobs must be >= 1 or -1, got {n_jobs}")
    if workers == 1 or len(tasks) == 1:
        _init_worker(data, risk_free)
        try:
            return [_evaluate(t) for t in tasks]
        finally:
            _WORKER.clear()

    per_window = max(1, len(tasks) // len({t.fold for t in tasks}))
    chunksize = max(1, min(per_window, math.ceil(len(tasks) / (workers * 2))))
    # polars is multithreaded: split the cores among workers instead of
    # letting every worker start one thread per core (read at polars import,
    # so it must be in the environment the spawned workers inherit)
    previous = os.environ.get("POLARS_MAX_THREADS")
    os.environ["POLARS_MAX_THREADS"] = str(max(1, (os.cpu_count() or 1) // workers))
    try:
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_init_worker,
            initargs=(data, risk_free),
        ) as pool:
            return list(pool.map(_evaluate, tasks, chunksize=chunksize))
    finally:
        if previous is None:
            del os.environ["POLARS_MAX_THREADS"]
        else:
            os.environ["POLARS_MAX_THREADS"] = previous


# ---- helpers -------------------------------------------------------------------


def _normalize(data: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Validated, Date-typed, sorted market data."""
    return Panel(data.collect() if isinstance(data, pl.LazyFrame) else data).frame


def _offset(day: date, duration: str) -> date:
    try:
        return pl.Series([day], dtype=pl.Date).dt.offset_by(duration)[0]
    except Exception as exc:
        raise ValueError(f"Invalid duration {duration!r}: {exc}") from exc


def _best(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Highest score, first in grid order on ties; NaN never wins unless
    every candidate is NaN (then the first candidate is used)."""
    best = max(rows, key=lambda r: -math.inf if math.isnan(r["score"]) else r["score"])
    if math.isnan(best["score"]):
        logger.warning("Every candidate scored NaN; using %s", best["params"])
    return best


def _rows_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _nanmean(values: pl.Series) -> float:
    finite = values.filter(values.is_not_nan())
    return float(finite.mean()) if finite.len() else math.nan  # type: ignore[arg-type]


def _chain(
    name: str, config: BacktestConfig, results: list[BacktestResult]
) -> BacktestResult:
    """Joins consecutive fold backtests into a single result."""

    def concat(table: str) -> pl.DataFrame:
        return pl.concat([getattr(r, table) for r in results])

    return BacktestResult(
        strategy=name,
        params={},
        config=config,
        equity=concat("equity"),
        trades=concat("trades"),
        positions=concat("positions"),
        decisions=concat("decisions"),
        events=concat("events"),
        initial_value=results[0].initial_value,
        final_state=results[-1].final_state,
    )
