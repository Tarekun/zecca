"""MLflow logging of an evaluation (skipped with --no-mlflow)."""

import contextlib
import math
import os
import sys
from typing import Any

from analysis.framework import BacktestResult

_BATCH = 1000  # mlflow's limit of metrics per log_batch call


def log_run(experiment: str, report: dict[str, Any], result: BacktestResult) -> str:
    """Logs `report` (params, metrics, tags, the report itself as JSON) and
    the equity curve (as a step metric, rendered as a chart by mlflow, and as
    CSV). Returns the mlflow run id."""
    # an unreachable tracking server should fail in seconds, not after
    # mlflow's default retries with backoff (minutes); explicit settings win
    os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "1")
    os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "10")
    import mlflow
    from mlflow.entities import Metric

    from analysis.mlflow_utils import _get_or_create_experiment

    with contextlib.redirect_stdout(sys.stderr):
        mlflow.set_experiment(experiment_id=_get_or_create_experiment(experiment))
        strategy = report["strategy"]
        with mlflow.start_run(
            run_name=f"{strategy['name']}-{report['protocol']}"
        ) as run:
            mlflow.set_tags(
                {
                    "strategy": strategy["name"],
                    "protocol": report["protocol"],
                    "code_hash": strategy["code_hash"],
                    "git_commit": report["git"]["commit"],
                    "git_dirty": report["git"]["dirty"],
                    "holdout": report["window"]["holdout"],
                    "guardrails_passed": report["guardrails"]["lookahead"]["passed"],
                }
            )
            mlflow.log_params(
                _flatten(
                    {
                        "params": strategy["params"],
                        "protocol": report["protocol_args"],
                        "window": report["window"],
                    }
                )
            )
            mlflow.log_metrics(
                {
                    k: v
                    for k, v in report["metrics"].items()
                    if isinstance(v, (int, float)) and math.isfinite(v)
                }
            )
            client = mlflow.MlflowClient()
            values = result.equity["portfolio_value"].to_list()
            metrics = [
                Metric("portfolio_value", v, 0, step) for step, v in enumerate(values)
            ]
            for i in range(0, len(metrics), _BATCH):
                client.log_batch(run.info.run_id, metrics=metrics[i : i + _BATCH])
            mlflow.log_dict(report, "report.json")
            mlflow.log_text(result.equity.write_csv(), "equity.csv")
            return run.info.run_id


def _flatten(values: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in values.items():
        name = f"{prefix}{key}"
        if isinstance(value, dict):
            flat |= _flatten(value, f"{name}.")
        else:
            flat[name] = (
                value if isinstance(value, (int, float, str, bool)) else str(value)
            )
    return flat
