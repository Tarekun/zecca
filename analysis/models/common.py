import dataclasses
from dataclasses import dataclass
from typing import Any, Callable

import numpy as np

from analysis.mlflow_utils import ExperimentLogger

MetricValue = float | dict[Any, float]
Metrics = dict[str, Callable[[np.ndarray, np.ndarray], MetricValue]]


@dataclass
class TrainingResult:
    train_metrics: dict[str, MetricValue]
    val_metrics: dict[str, MetricValue] | None


def per_class(scorer: Callable[..., Any]) -> Callable[[np.ndarray, np.ndarray], dict]:
    """Wraps a sklearn scorer that accepts `average=None` into a metric function
    that returns one value per class"""

    def compute(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
        classes = sorted(np.unique(y_true))
        values = scorer(y_true, y_pred, labels=classes, average=None, zero_division=0)
        return dict(zip(classes, values))

    return compute


def _log_metrics(logger: ExperimentLogger, split: str, metrics: dict[str, MetricValue]):
    for name, value in metrics.items():
        if isinstance(value, dict):
            logger.log_metrics(
                {f"{split}_{name}.class_{c}": v for c, v in value.items()}
            )
        else:
            logger.log_metric(f"{split}_{name}", value)


def train_sklearn_model(
    model,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray | None,
    y_test: np.ndarray | None,
    logger: ExperimentLogger | None,
    metrics: Metrics,
) -> tuple[TrainingResult, Any]:
    """Fits `model` and reports each of `metrics` on the train split, plus the
    test split when given -- shared by every per-model `train` wrapper
    (decision tree, random forest, ...) so they only differ in how `model`
    itself gets built from its config.

    Returns `(result, model)` rather than bundling `model` into `TrainingResult`,
    so callers that don't need the fitted model (e.g. `run_search` sweeping many
    configs) can simply discard it and let it be freed instead of holding every
    fitted model in memory for the whole sweep."""
    model.fit(X_train, y_train)
    train_pred = model.predict(X_train)
    train_metrics = {name: fn(y_train, train_pred) for name, fn in metrics.items()}

    val_metrics = None
    if X_test is not None and y_test is not None:
        val_pred = model.predict(X_test)
        val_metrics = {name: fn(y_test, val_pred) for name, fn in metrics.items()}

    print(
        f"train_metrics={train_metrics}"
        + (f" val_metrics={val_metrics}" if val_metrics is not None else "")
    )

    if logger is not None:
        _log_metrics(logger, "train", train_metrics)
        if val_metrics is not None:
            _log_metrics(logger, "val", val_metrics)
        logger.log_model(model, flavor="sklearn")

    return TrainingResult(train_metrics=train_metrics, val_metrics=val_metrics), model


def run_search(
    train_fn,
    overrides: list[dict[str, Any]],
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
    base_config,
    extra_params: dict[str, Any] | None = None,
) -> list[dict]:
    """For each dict in `overrides`, builds a config by overriding `base_config`'s
    defaults with the dict's values and runs `train_fn` with it, logging every
    combination as its own mlflow run."""

    results = []
    for override in overrides:
        config = dataclasses.replace(base_config, **override)
        result, _model = train_fn(
            X_train, y_train, X_test, y_test, config, extra_params=extra_params
        )
        results.append({"overrides": override, "config": config, "result": result})

    return results
