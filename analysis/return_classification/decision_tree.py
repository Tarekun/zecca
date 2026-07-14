import dataclasses
from dataclasses import dataclass
from typing import Any

import numpy as np
from sklearn.metrics import accuracy_score, f1_score
from sklearn.tree import DecisionTreeClassifier

from analysis.mlflow_utils import ExperimentLogger, mlflow_experiment


@dataclass
class DecisionTreeConfig:
    criterion: str = "gini"
    splitter: str = "best"
    max_depth: int | None = None
    min_samples_split: int | float = 2
    min_samples_leaf: int | float = 1
    max_features: int | float | str | None = None
    class_weight: str | dict | None = None
    ccp_alpha: float = 0.0
    random_state: int | None = 42


@dataclass
class TrainingResult:
    train_accuracy: float
    val_accuracy: float | None
    val_f1: float | None
    model: DecisionTreeClassifier


@mlflow_experiment(
    name="return-window-classifier-tree",
    tags={"model_class": "DecisionTreeClassifier"},
    log_config_params=("config", "extra_params"),
)
def train(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray | None,
    y_val: np.ndarray | None,
    config: DecisionTreeConfig,
    extra_params: dict[str, Any] | None = None,
    logger: ExperimentLogger | None = None,
) -> TrainingResult:
    model = DecisionTreeClassifier(
        criterion=config.criterion,
        splitter=config.splitter,
        max_depth=config.max_depth,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        max_features=config.max_features,
        class_weight=config.class_weight,
        ccp_alpha=config.ccp_alpha,
        random_state=config.random_state,
    )
    model.fit(X_train, y_train)

    train_accuracy = accuracy_score(y_train, model.predict(X_train))

    val_accuracy, val_f1 = None, None
    if X_val is not None and y_val is not None:
        val_pred = model.predict(X_val)
        val_accuracy = accuracy_score(y_val, val_pred)
        val_f1 = f1_score(y_val, val_pred, average="macro")

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
        if val_accuracy is not None:
            logger.log_metrics({"val_accuracy": val_accuracy, "val_f1": val_f1})
        logger.log_model(model, flavor="sklearn")

    return TrainingResult(
        train_accuracy=train_accuracy,
        val_accuracy=val_accuracy,
        val_f1=val_f1,
        model=model,
    )


def search_hyperparameters(
    overrides: list[dict[str, Any]],
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    base_config: DecisionTreeConfig = DecisionTreeConfig(),
    extra_params: dict[str, Any] | None = None,
) -> list[dict]:
    """For each dict in `overrides`, builds a `DecisionTreeConfig` by overriding
    `base_config`'s defaults with the dict's values and runs `train` with it,
    logging every combination as its own mlflow run. `extra_params` is passed
    through to every `train` call unchanged (e.g. the feature list used to
    build `X_train`/`X_val`, so it ends up logged alongside each run). Returns
    all results sorted best-first by validation accuracy."""
    results = []
    for override in overrides:
        config = dataclasses.replace(base_config, **override)
        result = train(X_train, y_train, X_val, y_val, config, extra_params=extra_params)
        results.append({"overrides": override, "config": config, "result": result})

    results.sort(key=lambda r: r["result"].val_accuracy, reverse=True)
    return results
