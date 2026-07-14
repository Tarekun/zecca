import dataclasses
from dataclasses import dataclass
from typing import Any

import numpy as np
from sklearn.metrics import accuracy_score, f1_score, precision_recall_fscore_support

from analysis.mlflow_utils import ExperimentLogger


@dataclass
class TrainingResult:
    train_accuracy: float
    val_accuracy: float | None
    val_f1: float | None
    model: Any


def train_and_log(
    model,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray | None,
    y_val: np.ndarray | None,
    logger: ExperimentLogger | None,
) -> TrainingResult:
    """Fits `model` and reports train/val accuracy plus macro-F1 -- shared by
    every per-model `train` wrapper (decision tree, random forest, ...) so
    they only differ in how `model` itself gets built from its config."""
    model.fit(X_train, y_train)
    train_accuracy = accuracy_score(y_train, model.predict(X_train))

    val_accuracy, val_f1 = None, None
    val_precision_per_class, val_recall_per_class = None, None
    if X_val is not None and y_val is not None:
        val_pred = model.predict(X_val)
        val_accuracy = accuracy_score(y_val, val_pred)
        val_f1 = f1_score(y_val, val_pred, average="macro")

        val_classes = sorted(np.unique(y_val))
        precision, recall, _, _ = precision_recall_fscore_support(
            y_val, val_pred, labels=val_classes, average=None, zero_division=0
        )
        val_precision_per_class = {c: p for c, p in zip(val_classes, precision)}
        val_recall_per_class = {c: r for c, r in zip(val_classes, recall)}

    print(
        f"train_accuracy={train_accuracy}"
        + (
            f" val_accuracy={val_accuracy} val_f1={val_f1}"
            if val_accuracy is not None
            else ""
        )
    )

    if logger is not None:
        logger.log_metric("train_accuracy", train_accuracy)
        if val_precision_per_class is not None and val_recall_per_class is not None:
            logger.log_metrics({"val_accuracy": val_accuracy, "val_f1": val_f1})
            logger.log_metrics(
                {f"val_precision.class_{c}": p for c, p in val_precision_per_class.items()}
            )
            logger.log_metrics(
                {f"val_recall.class_{c}": r for c, r in val_recall_per_class.items()}
            )
        logger.log_model(model, flavor="sklearn")

    return TrainingResult(
        train_accuracy=train_accuracy,
        val_accuracy=val_accuracy,
        val_f1=val_f1,
        model=model,
    )


def run_search(
    train_fn,
    overrides: list[dict[str, Any]],
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    base_config,
    extra_params: dict[str, Any] | None = None,
) -> list[dict]:
    """For each dict in `overrides`, builds a config by overriding `base_config`'s
    defaults with the dict's values and runs `train_fn` with it, logging every
    combination as its own mlflow run. Returns all results sorted best-first by
    validation accuracy."""
    results = []
    for override in overrides:
        config = dataclasses.replace(base_config, **override)
        result = train_fn(X_train, y_train, X_val, y_val, config, extra_params=extra_params)
        results.append({"overrides": override, "config": config, "result": result})

    results.sort(key=lambda r: r["result"].val_accuracy, reverse=True)
    return results
