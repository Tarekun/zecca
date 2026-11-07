# Strategies: writing and evaluating them

This page is the contract for anyone — person or coding agent — who writes or
iterates on investment strategies in this repo.

- Strategies live in `analysis/strategies/<name>.py`, one or more per file.
- They are built on the framework in `analysis/framework/`.
- They are evaluated only through the CLI: `python -m analysis.evaluate`.

## Writing a strategy

A strategy is a subclass of `analysis.framework.Strategy` with three parts:

```python
import polars as pl
from analysis.framework import Param, ParamSpace, Strategy, TargetWeights


class TopNMomentum(Strategy):
    """One-paragraph description: shown by `python -m analysis.evaluate list`."""

    # every tunable parameter, with its default and its search domain
    param_space = ParamSpace(
        months_lookback=Param(default=12, values=[3, 6, 9, 12]),
        top_n=Param(default=20, low=10, high=30, step=10),
    )
    # dataset columns used beyond timeframe, symbol, open, high, low, close, volume
    data_columns = ()

    def prepare(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Optional. Vectorized signals for every (timeframe, symbol), computed
        once. `data` is sorted by (symbol, timeframe): use `.over("symbol")`."""
        c = pl.col("close")
        lookback = self.months_lookback * 21
        return data.select(
            "timeframe", "symbol",
            (c / c.shift(lookback) - 1).over("symbol").alias("momentum"),
        )

    def decide(self, market, portfolio):
        """Called at every close. Return the full target portfolio as weights
        of net worth, or None to do nothing today."""
        if not market.is_first_trading_day_of("month"):
            return None
        ranked = market.signals(["momentum"]).drop_nulls().top_k(self.top_n, by="momentum")
        return TargetWeights.equal(ranked["symbol"])
```

What `decide` receives:
- `market` is a point-in-time view that never returns rows after today:
  - `history(columns, lookback)`, `today()`, `signals(columns, lookback)`
  - `universe()`, `trading_days()`, `is_first_trading_day_of("week" | "month" | "quarter" | "year")`
- `portfolio` is read-only: `cash`, `positions`, `prices`, `weights`, `net_worth`, `exposure`.

How execution works:
- `TargetWeights` describe the whole portfolio. Symbols that are left out are sold, and anything not invested stays in cash.
- Orders execute at the **next open**, with 5 bps commission plus 5 bps slippage.
- Cash earns the risk-free rate.
- A held symbol with no price for 5 trading days is liquidated as delisted.

Parameters are available as attributes (`self.top_n`) and validated against
`param_space`. The framework creates a new instance for every run.

To see which columns the dataset has, run `list` or check `gold.stocks_ml_ready`. Available columns include:
- returns, volatility and Sharpe over several windows
- RSI
- shares outstanding, float and earnings
- market cap
- a 16-dimensional symbol `embedding`

## Rules

The framework enforces what it can. These rules hold either way:

1. **No look-ahead.** A decision at day t may only use data up to t's close.
   - Don't use `shift(-n)`.
   - Don't use statistics over the whole sample (global means, ranks or z-scores). Use rolling or expanding windows instead.
   - Make every decision deterministic: seed any randomness.
   - Every evaluation runs a truncation-invariance guardrail first. If it fails, the run reports `"status": "guardrail_failed"`, exits with code 3, and returns no metrics.
2. **Never use the holdout.** Data from `holdout_start` in `configs/evaluation.yml` onwards is reserved for final, manual checks. The CLI refuses windows that reach into it. Agents must never pass `--holdout`.
3. **Don't change the evaluation to improve a result.** Leave these alone:
   - `analysis/framework/` and `configs/evaluation.yml`
   - costs, the data cache and the experiment log
4. **Every attempt counts.** Each run is appended to the experiment log, and the report shows `variants_tested`. A variant is a distinct pair of strategy file content and parameters, and every walk-forward candidate counts as one. The more variants are tried, the more likely the best one is luck, so prefer a few well-motivated ideas over many blind tweaks.
5. **Don't hard-code dates, symbols or events** that you know about from the data or from hindsight.
6. **Be suspicious of great results.**
   - The data contains stale and unadjusted prices: some stocks jump 50–100× in a day. The report warns about portfolio days that moved more than 25%.
   - The universe has survivorship bias: the price data contains only today's survivors. See [survivorship.md](survivorship.md).
   - Before trusting a result, inspect what drove it.

## Evaluating

```bash
python -m analysis.evaluate list                       # strategies, parameters, descriptions
python -m analysis.evaluate run TopNMomentum           # backtest with default parameters
python -m analysis.evaluate run TopNMomentum --params '{"top_n": 10}'
python -m analysis.evaluate run TopNMomentum --protocol walk_forward \
    --params '{"rebalance": "month"}' --param-grid '{"top_n": [10, 30]}' --n-jobs 6
python -m analysis.evaluate run TopNMomentum --protocol robustness --n-jobs 6   # is it real?
python -m analysis.evaluate history TopNMomentum       # past evaluations, variants tested
```

`run` options:

| option | meaning |
|---|---|
| `--params JSON` | Parameter values. Missing ones use the defaults. With `walk_forward` they are fixed, not searched. |
| `--protocol` | `backtest` (default): fixed parameters. `walk_forward`: parameters re-optimized on a rolling training window and tested on the following period. `robustness`: the backtest plus the robustness checks below. |
| `--start`, `--end` | Evaluated period (`YYYY-MM-DD`, end exclusive). Earlier data is always available as history. `--end` defaults to the holdout start. |
| `--max-leverage` | Maximum total target weight (default 1.0). |
| `--train`, `--test`, `--warmup`, `--anchored` | Walk-forward windows (polars durations, e.g. `1y`, `3mo`). Defaults: `1y` / `3mo` / `1y`, rolling. |
| `--param-grid JSON` | Walk-forward candidate values per parameter (default: the strategy's `param_space`). |
| `--objective` | Walk-forward selection metric: `sharpe` (default), `cagr` or `total_return`. |
| `--n-jobs` | Parallel processes for walk-forward or sensitivity-grid candidates (`-1`: all cores). |
| `--checks` | Robustness checks to run, comma-separated (default: all): `bootstrap`, `random_baseline`, `random_walk`, `sensitivity`, `deflated_sharpe`. |
| `--n-random`, `--bootstrap-samples`, `--block`, `--n-paths` | Robustness sizes: random portfolios (100), bootstrap resamples (1000), bootstrap block length in days (21), synthetic markets (5). `--param-grid` and `--objective` set the sensitivity grid. |
| `--no-mlflow` | Don't register the run on MLflow. The experiment log still records it. |
| `--output PATH` | Also write the report to a file. |
| `--refresh-data` | Rebuild the market data snapshot from the data platform first. |

The look-ahead guardrail is checked at different parameters depending on the protocol:
- **`backtest`:** the given parameters.
- **`walk_forward`:** the defaults merged with `--params`.
- **`robustness`:** the given parameters, like `backtest`.

### Robustness checks

A good backtest number isn't evidence on its own. `--protocol robustness` asks five questions:

| check | question | read it as |
|---|---|---|
| `bootstrap` | How much of the result could be noise? Block-bootstrap confidence intervals of Sharpe, CAGR and max drawdown. | If the Sharpe interval includes 0, the result is not distinguishable from luck. |
| `random_baseline` | Is the stock selection better than chance? Random portfolios with the same rebalancing days, number of positions and weights. | `percentile` near 1 and `p_value` below 0.05 mean the picks beat random picks. |
| `random_walk` | Does the strategy find an edge where none exists? It runs on synthetic markets where days are shuffled and returns reassigned among stocks at random. | `suspicious: true` means a bug or look-ahead. Negative excess is normal: costs, cash during warmup, and the benchmark paying no costs. |
| `sensitivity` | Are the parameters a lucky spike? The objective over the parameter grid, with the best candidate's neighbors. | `isolated: true` means small changes destroy the result (overfit). `evaluated_params_rank` places your parameters in the grid. |
| `deflated_sharpe` | Does the Sharpe survive the number of variants tried? Probability that the true Sharpe is positive, given the best of `variants_tested` (Bailey & López de Prado). | Above 0.95 the result survives. Every variant you try raises the bar. |

The sensitivity grid counts as variants tried, so running `robustness` raises `variants_tested` by the grid size.

### Output

stdout carries exactly one JSON document, and logs go to stderr. Exit codes:

| code | meaning |
|---|---|
| 0 | Evaluation completed. |
| 1 | Unexpected runtime error: `{"status": "error", "error": ...}`. |
| 2 | Invalid input: unknown strategy or parameter, bad JSON, window in the holdout. |
| 3 | The look-ahead guardrail failed: `status` is `guardrail_failed` and `metrics` is null. |

### Report (`schema_version: 1`)

| field | content |
|---|---|
| `status` | `ok` or `guardrail_failed` |
| `run_id`, `timestamp` | Identity of the evaluation. |
| `strategy` | `name`, `reference` (`module:Class`), `code_hash` (of the strategy file), `params` (resolved, defaults included). |
| `protocol`, `protocol_args` | Protocol and its settings (leverage, walk-forward windows, grid, objective). |
| `window` | Requested `start`/`end`, the `holdout` flag and `holdout_start`, then `first_evaluated_day` and `last_day` actually simulated. |
| `data` | Snapshot info (`created_at`, rows, symbols, date range), `symbols_loaded`, and `survivorship`: diagnostics measured on the unfiltered prices (see [survivorship.md](survivorship.md)), or null if they couldn't be measured. |
| `git` | `commit` and `dirty` flag of the repo. |
| `guardrails.lookahead` | `passed`, `cutoffs`, `failures`. |
| `metrics` | `total_return`, `cagr`, `volatility`, `sharpe` (annualized, over the risk-free rate), `max_drawdown`. Walk-forward adds `oos_score`, `mean_train_score`, `mean_test_score` and `is_oos_gap`: in-sample minus out-of-sample score, where a large gap means overfitting. |
| `walk_forward` | Walk-forward only: `candidates_per_fold`, and `folds` with windows, chosen `params`, `train_score` and `test_score`. |
| `robustness` | Robustness only: one section per check (see above). Headline numbers are also in `metrics`: `bootstrap_sharpe_low` and `_high`, `random_baseline_percentile` and `_p_value`, `random_walk_excess_return` and `_suspicious`, `sensitivity_isolated`, `deflated_sharpe`. |
| `activity` | `trades`, `costs_paid`, `average_exposure`, and `events` counted by kind (`delisting`, `exit_deferred`, `buy_skipped`, `rebalance_skipped`, `buys_scaled`). |
| `warnings` | Data caveats to read before trusting the numbers: extreme portfolio days, and the measured survivorship bias. |
| `mlflow_run_id` | Or null with `--no-mlflow`. If tracking failed, `mlflow_error` explains why; the evaluation itself is unaffected. |
| `experiment_log` | `evaluations_of_strategy` and `variants_tested`, including this run. |
| `elapsed_seconds` | |

## Where things are

- **Configuration:** `configs/evaluation.yml` (holdout start, costs, paths, MLflow experiment).
- **Market data snapshot:** `dataplatform/cache/stocks_ml_ready.parquet`, built on first use.
- **Experiment log:** `dataplatform/experiments/evaluations.jsonl`, append-only, one JSON per line.
- **MLflow:** experiment `strategy-evaluation`, using the tracking URI from `.env` (`MLFLOW_TRACKING_URI`) or `mlflow.db` in the repo root. Each run records tags, parameters, metrics, the equity curve as a `portfolio_value` step metric, and `report.json` plus `equity.csv` as artifacts.
