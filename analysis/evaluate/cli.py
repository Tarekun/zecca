"""`python -m analysis.evaluate`: evaluate strategies, for people and agents.

Subcommands:
    list                      strategies available, with their parameters
    run STRATEGY [options]    evaluate one strategy, print a JSON report
    history [STRATEGY]        past evaluations from the experiment log

stdout carries only JSON (the report, or {"status": "error", ...}); logs go
to stderr. Exit codes: 0 ok, 1 runtime error, 2 invalid input, 3 guardrail
failed (look-ahead detected: no metrics are reported). See
docs/strategies/README.md for the report schema and the rules."""

import argparse
import json
import logging
import math
import subprocess
import sys
import time
import uuid
from datetime import date, datetime
from typing import Any, Sequence

import polars as pl

from analysis.evaluate import data as data_mod
from analysis.evaluate import experiment_log, registry, tracking
from analysis.evaluate.config import DEFAULT_CONFIG, REPO_ROOT, EvaluationConfig
from analysis.framework import (
    DATE_COL,
    BacktestResult,
    InvalidParams,
    InvalidTargetWeights,
    Panel,
    Param,
    ParamSpace,
    Strategy,
    check_lookahead,
    grid_search,
    summary,
    walk_forward,
)
from analysis.framework.metrics import cagr, sharpe, total_return
from analysis.framework.robustness import (
    bootstrap,
    deflated_sharpe,
    parameter_sensitivity,
    random_baseline,
    random_walk_test,
)

SCHEMA_VERSION = 1
OBJECTIVES = {"sharpe": sharpe, "cagr": cagr, "total_return": total_return}
EXTREME_DAILY_RETURN = 0.25
ROBUSTNESS_CHECKS = (
    "bootstrap",
    "random_baseline",
    "random_walk",
    "sensitivity",
    "deflated_sharpe",
)

EXIT_OK, EXIT_ERROR, EXIT_INVALID, EXIT_GUARDRAIL = 0, 1, 2, 3


class InvalidInput(ValueError):
    pass


def main(argv: Sequence[str] | None = None) -> int:
    _logs_to_stderr()
    args = _parser().parse_args(argv)
    try:
        config = EvaluationConfig.load(args.config)
        if args.command == "list":
            return _emit(_list())
        if args.command == "history":
            return _emit(_history(config, args.strategy, args.limit))
        return _run(config, args)
    except (
        InvalidInput,
        InvalidParams,
        InvalidTargetWeights,
        registry.UnknownStrategy,
        data_mod.DataError,
    ) as exc:
        return _emit({"status": "error", "error": str(exc)}, EXIT_INVALID)
    except Exception as exc:
        logging.getLogger(__name__).exception("evaluation failed")
        return _emit(
            {"status": "error", "error": f"{type(exc).__name__}: {exc}"}, EXIT_ERROR
        )


# ---- subcommands ---------------------------------------------------------------


def _list() -> dict[str, Any]:
    strategies, errors = registry.discover()
    return {
        "status": "ok",
        "strategies": [registry.describe(cls) for cls in strategies],
        "import_errors": errors,
    }


def _history(
    config: EvaluationConfig, strategy: str | None, limit: int
) -> dict[str, Any]:
    entries = experiment_log.read(config.experiment_log, strategy)
    result: dict[str, Any] = {"status": "ok", "evaluations": len(entries)}
    if strategy is not None:
        result["variants_tested"] = experiment_log.variants(entries, strategy)
    result["entries"] = entries[-limit:]
    return result


def _run(config: EvaluationConfig, args: argparse.Namespace) -> int:
    began = time.perf_counter()
    cls = registry.resolve(args.strategy)
    fixed = _json_arg(args.params, "--params")
    params = cls.param_space.resolve(fixed)  # validates names and domains
    start, end = _window(config, args)
    objective = OBJECTIVES[args.objective]
    backtest_config = config.backtest_config(args.max_leverage)

    meta = data_mod.cache_meta(config, refresh=args.refresh_data)
    market = data_mod.load_market(config, list(cls.data_columns), end)
    if args.protocol == "walk_forward" and start is not None:
        # history before the first training window: the walk-forward warmup
        market = market.filter(pl.col(DATE_COL) >= _offset(start, f"-{args.warmup}"))
    panel = Panel(market)
    risk_free = data_mod.load_rf(config)

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "run_id": uuid.uuid4().hex,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "strategy": {
            "name": cls.__name__,
            "reference": f"{cls.__module__}:{cls.__name__}",
            "code_hash": registry.code_hash(cls),
            "params": params,
        },
        "protocol": args.protocol,
        "protocol_args": _protocol_args(args, backtest_config.max_leverage),
        "window": {
            "start": _iso(start),
            "end": _iso(end),
            "holdout": args.holdout,
            "holdout_start": config.holdout_start.isoformat(),
        },
        "data": {**meta, "symbols_loaded": market["symbol"].n_unique()},
        "git": _git_state(),
    }

    # guardrails first: a strategy that looks ahead gets no metrics at all
    check = check_lookahead(
        cls,
        params,
        panel,
        config=backtest_config,
        risk_free=risk_free,
        start=None if args.protocol == "walk_forward" else start,
    )
    report["guardrails"] = {"lookahead": check.to_dict()}
    candidates: list[dict[str, Any]] = [params]
    if not check.passed:
        report["status"] = "guardrail_failed"
        report["metrics"] = None
        result: BacktestResult | None = None
    elif args.protocol == "backtest":
        result = check.result
        assert result is not None
        report["metrics"] = summary(result)
    elif args.protocol == "robustness":
        result = check.result
        assert result is not None
        report["metrics"] = summary(result)
        report["robustness"], grid = _robustness(
            cls, params, panel, result, args, start, backtest_config, risk_free
        )
        if grid is not None:
            candidates += [dict(p) for p in grid.scores["params"].to_list()]
    else:
        space = _param_space(cls, fixed, _json_arg(args.param_grid, "--param-grid"))
        wf = walk_forward(
            cls,
            panel.frame,
            train=args.train,
            test=args.test,
            warmup=args.warmup,
            anchored=args.anchored,
            param_space=space,
            objective=objective,
            config=backtest_config,
            risk_free=risk_free,
            n_jobs=args.n_jobs,
        )
        result = wf.oos
        candidates = [dict(p) for p in wf.candidates["params"].to_list()]
        report["metrics"] = wf.metrics
        report["walk_forward"] = {
            "candidates_per_fold": space.grid_size(),
            "folds": [
                {k: _iso(v) if isinstance(v, date) else v for k, v in row.items()}
                for row in wf.folds.iter_rows(named=True)
            ],
        }

    if result is not None:
        report["window"]["first_evaluated_day"] = _iso(result.equity[DATE_COL].min())
        report["window"]["last_day"] = _iso(result.equity[DATE_COL].max())
        report["activity"] = _activity(result)
        report["warnings"] = _warnings(result, meta.get("survivorship"))

    entry = {
        "run_id": report["run_id"],
        "timestamp": report["timestamp"],
        "strategy": cls.__name__,
        "code_hash": report["strategy"]["code_hash"],
        "protocol": args.protocol,
        "params": params,
        "candidates": candidates if args.protocol != "backtest" else None,
        "protocol_args": report["protocol_args"],
        "window": report["window"],
        "status": report["status"],
    }
    previous = experiment_log.read(config.experiment_log, cls.__name__)
    variants = experiment_log.variants(previous + [entry], cls.__name__)
    report["experiment_log"] = {
        "evaluations_of_strategy": len(previous) + 1,
        "variants_tested": variants,
    }
    if "robustness" in report and "deflated_sharpe" in report["robustness"]:
        assert result is not None
        dsr = deflated_sharpe(
            result, variants, _trials_sharpe_variance(report["robustness"], previous)
        )
        report["robustness"]["deflated_sharpe"] = dsr
    if "robustness" in report:
        report["metrics"] |= _robustness_metrics(report["robustness"])

    report["mlflow_run_id"] = None
    if result is not None and not args.no_mlflow:
        try:
            report["mlflow_run_id"] = tracking.log_run(
                config.mlflow_experiment, report, result
            )
        except Exception as exc:  # tracking problems must not lose the evaluation
            report["mlflow_error"] = f"{type(exc).__name__}: {exc}"

    report["elapsed_seconds"] = round(time.perf_counter() - began, 1)
    experiment_log.append(
        config.experiment_log,
        entry
        | {
            "metrics": report["metrics"],
            "git": report["git"],
            "mlflow_run_id": report["mlflow_run_id"],
        },
    )

    if args.output:
        with open(args.output, "w") as f:
            json.dump(_clean(report), f, indent=2)
    return _emit(report, EXIT_OK if check.passed else EXIT_GUARDRAIL)


# ---- helpers -------------------------------------------------------------------


def _window(
    config: EvaluationConfig, args: argparse.Namespace
) -> tuple[date | None, date | None]:
    """[start, end) of the evaluated period. Without --holdout the period
    always ends before the holdout; with it, a backtest covers the holdout."""
    start = _date_arg(args.start, "--start")
    end = _date_arg(args.end, "--end")
    if args.holdout:
        if args.protocol != "backtest":
            raise InvalidInput("--holdout is only supported with --protocol backtest")
        return start or config.holdout_start, end
    if end is not None and end > config.holdout_start:
        raise InvalidInput(
            f"--end {end} reaches into the holdout period (from {config.holdout_start}); "
            "the holdout is reserved for final, manual checks"
        )
    if start is not None and start >= config.holdout_start:
        raise InvalidInput(f"--start {start} is inside the holdout period")
    return start, end or config.holdout_start


def _param_space(
    cls: type[Strategy], fixed: dict[str, Any], grid: dict[str, Any]
) -> ParamSpace:
    """The strategy's own space, with `fixed` params pinned to one value and
    `grid` replacing the candidate values of the others."""
    unknown = (set(fixed) | set(grid)) - set(cls.param_space)
    if unknown:
        raise InvalidParams(f"Unknown parameter(s) {sorted(unknown)}")
    both = set(fixed) & set(grid)
    if both:
        raise InvalidInput(f"{sorted(both)} given both in --params and --param-grid")
    space = {}
    for name, param in cls.param_space.items():
        if name in fixed:
            space[name] = Param(default=fixed[name], values=[fixed[name]])
        elif name in grid:
            values = grid[name] if isinstance(grid[name], list) else [grid[name]]
            outside = [v for v in values if not param.accepts(v)]
            if not values or outside:
                raise InvalidParams(
                    f"--param-grid values {outside or values} invalid for {name!r}"
                )
            space[name] = Param(default=values[0], values=values)
        else:
            space[name] = param
    return ParamSpace(**space)


def _activity(result: BacktestResult) -> dict[str, Any]:
    trades = result.trades
    return {
        "trades": trades.height,
        "costs_paid": round(
            float(trades["commission"].sum() + trades["slippage"].sum()), 2
        ),
        "average_exposure": _finite(result.equity["exposure"].mean()),
        "events": dict(result.events.group_by("kind").len().sort("kind").iter_rows()),
    }


def _warnings(result: BacktestResult, survivorship: dict[str, Any] | None) -> list[str]:
    values = result.equity["portfolio_value"]
    returns = (values / values.shift(1) - 1).fill_null(0.0)
    extreme = result.equity.filter((returns.abs() > EXTREME_DAILY_RETURN).to_list())
    warnings = []
    if extreme.height:
        days = ", ".join(_iso(d) for d in extreme[DATE_COL].head(5).to_list())  # type: ignore[misc]
        warnings.append(
            f"{extreme.height} day(s) with a portfolio move above "
            f"{EXTREME_DAILY_RETURN:.0%} ({days}{', ...' if extreme.height > 5 else ''}): "
            "most likely bad price data (stale or unadjusted quotes), not skill; "
            "inspect the positions held on those days"
        )
    if survivorship is not None:
        warnings += survivorship["warnings"]
    else:
        warnings.append(
            "survivorship bias could not be measured (unfiltered prices unavailable); "
            "assume delisted companies are missing and results are optimistic"
        )
    return warnings


def _robustness(
    cls: type[Strategy],
    params: dict[str, Any],
    panel: Panel,
    result: BacktestResult,
    args: argparse.Namespace,
    start: date | None,
    backtest_config: Any,
    risk_free: pl.DataFrame | None,
) -> tuple[dict[str, Any], Any]:
    checks = _checks(args.checks)
    out: dict[str, Any] = {}
    grid = None
    if "bootstrap" in checks:
        out["bootstrap"] = bootstrap(result, n=args.bootstrap_samples, block=args.block)
    if "random_baseline" in checks:
        out["random_baseline"] = random_baseline(
            result,
            panel,
            n=args.n_random,
            config=backtest_config,
            risk_free=risk_free,
            start=start,
        )
    if "random_walk" in checks:
        out["random_walk"] = random_walk_test(
            cls,
            params,
            panel,
            n_paths=args.n_paths,
            config=backtest_config,
            start=start,
        )
    if "sensitivity" in checks:
        space = _param_space(cls, {}, _json_arg(args.param_grid, "--param-grid"))
        grid = grid_search(
            cls,
            panel.frame,
            start=start,
            param_space=space,
            objective=OBJECTIVES[args.objective],
            config=backtest_config,
            risk_free=risk_free,
            n_jobs=args.n_jobs,
        )
        sensitivity = parameter_sensitivity(grid, space)
        ranked = sorted(
            (r for r in sensitivity["scores"] if math.isfinite(r["score"])),
            key=lambda r: r["score"],
            reverse=True,
        )
        rank = next(
            (i + 1 for i, r in enumerate(ranked) if r["params"] == params), None
        )
        sensitivity["evaluated_params_rank"] = rank
        out["sensitivity"] = sensitivity
    if "deflated_sharpe" in checks:
        out["deflated_sharpe"] = None  # computed once the variant count is known
    return out, grid


def _checks(value: str) -> list[str]:
    checks = [c.strip() for c in value.split(",") if c.strip()]
    unknown = sorted(set(checks) - set(ROBUSTNESS_CHECKS))
    if unknown or not checks:
        raise InvalidInput(
            f"--checks must be a comma-separated subset of {', '.join(ROBUSTNESS_CHECKS)}"
        )
    return checks


def _trials_sharpe_variance(
    robustness: dict[str, Any], previous: list[dict[str, Any]]
) -> float | None:
    """Variance of the Sharpe ratios of the variants tried: from this run's
    sensitivity grid when there is one, else from earlier evaluations."""
    if "sensitivity" in robustness:
        values = [r.get("sharpe") for r in robustness["sensitivity"]["scores"]]
    else:
        values = [(e.get("metrics") or {}).get("sharpe") for e in previous]
    finite = [v for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
    if len(finite) < 2:
        return None
    mean = sum(finite) / len(finite)
    return sum((v - mean) ** 2 for v in finite) / (len(finite) - 1)


def _robustness_metrics(robustness: dict[str, Any]) -> dict[str, Any]:
    """Headline numbers of the robustness checks, flat (logged to MLflow)."""
    out: dict[str, Any] = {}
    if "bootstrap" in robustness:
        out["bootstrap_sharpe_low"] = robustness["bootstrap"]["sharpe"]["low"]
        out["bootstrap_sharpe_high"] = robustness["bootstrap"]["sharpe"]["high"]
    if "random_baseline" in robustness:
        rb = robustness["random_baseline"]["sharpe"]
        out["random_baseline_percentile"] = rb["percentile"]
        out["random_baseline_p_value"] = rb["p_value"]
    walk = robustness.get("random_walk") or {}
    if walk.get("status") == "ok":
        out["random_walk_excess_return"] = walk["mean_annual_excess_return"]
        out["random_walk_suspicious"] = float(walk["suspicious"])
    if "sensitivity" in robustness:
        out["sensitivity_isolated"] = float(robustness["sensitivity"]["isolated"])
    if robustness.get("deflated_sharpe"):
        out["deflated_sharpe"] = robustness["deflated_sharpe"]["deflated_sharpe"]
    return out


def _protocol_args(args: argparse.Namespace, max_leverage: float) -> dict[str, Any]:
    out: dict[str, Any] = {"max_leverage": max_leverage}
    if args.protocol == "robustness":
        out |= {
            "checks": _checks(args.checks),
            "n_random": args.n_random,
            "bootstrap_samples": args.bootstrap_samples,
            "block": args.block,
            "n_paths": args.n_paths,
            "objective": args.objective,
            "param_grid": _json_arg(args.param_grid, "--param-grid"),
        }
    if args.protocol == "walk_forward":
        out |= {
            "train": args.train,
            "test": args.test,
            "warmup": args.warmup,
            "anchored": args.anchored,
            "objective": args.objective,
            "param_grid": _json_arg(args.param_grid, "--param-grid"),
        }
    return out


def _git_state() -> dict[str, Any]:
    def git(*cmd: str) -> str:
        return subprocess.run(
            ["git", *cmd], cwd=REPO_ROOT, capture_output=True, text=True, check=False
        ).stdout.strip()

    return {
        "commit": git("rev-parse", "HEAD") or None,
        "dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
    }


def _json_arg(value: str | None, flag: str) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise InvalidInput(f"{flag} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise InvalidInput(f"{flag} must be a JSON object")
    return parsed


def _date_arg(value: str | None, flag: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise InvalidInput(f"{flag} must be a YYYY-MM-DD date") from exc


def _offset(day: date, duration: str) -> date:
    return pl.Series([day], dtype=pl.Date).dt.offset_by(duration)[0]


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, date) else value


def _finite(value: Any) -> float | None:
    return (
        float(value)
        if isinstance(value, (int, float)) and math.isfinite(value)
        else None
    )


def _clean(value: Any) -> Any:
    """JSON-safe copy: NaN/inf become null, dates ISO strings."""
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, date):
        return value.isoformat()
    return value


def _emit(payload: dict[str, Any], code: int = EXIT_OK) -> int:
    json.dump(_clean(payload), sys.stdout, indent=2)
    sys.stdout.write("\n")
    return code


def _logs_to_stderr() -> None:
    """stdout is reserved for JSON: move every log handler to stderr,
    including the etl package's (which writes to stdout)."""
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    import etl.logger  # noqa: F401  (installs its handlers)

    for handler in logging.getLogger("etl").handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream is sys.stdout:
            handler.setStream(sys.stderr)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m analysis.evaluate",
        description="Evaluate trading strategies. Prints JSON on stdout.",
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="strategies available and their parameters")

    history = sub.add_parser("history", help="past evaluations from the experiment log")
    history.add_argument("strategy", nargs="?", help="only this strategy (class name)")
    history.add_argument("--limit", type=int, default=20, help="most recent N entries")

    run = sub.add_parser("run", help="evaluate a strategy")
    run.add_argument("strategy", help="class name (e.g. TopNMomentum) or module:Class")
    run.add_argument(
        "--params", help="JSON object of parameters, e.g. '{\"top_n\": 10}'"
    )
    run.add_argument(
        "--protocol",
        choices=["backtest", "walk_forward", "robustness"],
        default="backtest",
    )
    run.add_argument(
        "--start", help="first evaluated day YYYY-MM-DD (earlier data is history)"
    )
    run.add_argument(
        "--end",
        help="end of the evaluation YYYY-MM-DD, exclusive (default: holdout start)",
    )
    run.add_argument(
        "--max-leverage", type=float, default=None, help="default from the config (1.0)"
    )
    run.add_argument(
        "--no-mlflow", action="store_true", help="do not register the run on MLflow"
    )
    run.add_argument("--output", help="also write the report to this file")
    run.add_argument(
        "--refresh-data",
        action="store_true",
        help="rebuild the market data snapshot first",
    )
    run.add_argument("--holdout", action="store_true", help=argparse.SUPPRESS)

    wf = run.add_argument_group("walk-forward options")
    wf.add_argument(
        "--train", default="1y", help="training window (polars duration, default 1y)"
    )
    wf.add_argument(
        "--test",
        default="3mo",
        help="test window / re-optimization period (default 3mo)",
    )
    wf.add_argument(
        "--warmup",
        default="1y",
        help="history before the first training window (default 1y)",
    )
    wf.add_argument(
        "--anchored",
        action="store_true",
        help="expanding instead of rolling training window",
    )
    wf.add_argument(
        "--param-grid",
        help="JSON object of candidate values, e.g. '{\"top_n\": [10, 20]}'",
    )
    wf.add_argument("--objective", choices=sorted(OBJECTIVES), default="sharpe")
    wf.add_argument(
        "--n-jobs",
        type=int,
        default=1,
        help="parallel worker processes (-1: all cores)",
    )

    rob = run.add_argument_group(
        "robustness options (--param-grid, --objective, --n-jobs apply to the sensitivity grid)"
    )
    rob.add_argument(
        "--checks",
        default=",".join(ROBUSTNESS_CHECKS),
        help=f"comma-separated subset of: {', '.join(ROBUSTNESS_CHECKS)} (default: all)",
    )
    rob.add_argument(
        "--n-random", type=int, default=100, help="random portfolios (default 100)"
    )
    rob.add_argument(
        "--bootstrap-samples",
        type=int,
        default=1000,
        help="bootstrap resamples (default 1000)",
    )
    rob.add_argument(
        "--block",
        type=int,
        default=21,
        help="bootstrap block length in days (default 21)",
    )
    rob.add_argument(
        "--n-paths",
        type=int,
        default=5,
        help="synthetic random-walk markets (default 5)",
    )
    return parser
