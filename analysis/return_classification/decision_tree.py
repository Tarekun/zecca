import dataclasses
from dataclasses import dataclass
from typing import Any

import numpy as np
from sklearn.tree import DecisionTreeClassifier

from analysis.mlflow_utils import ExperimentLogger, mlflow_experiment
from analysis.return_classification.common import TrainingResult, run_search, train_and_log


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
) -> tuple[TrainingResult, DecisionTreeClassifier]:
    model = DecisionTreeClassifier(**dataclasses.asdict(config))
    return train_and_log(model, X_train, y_train, X_val, y_val, logger)


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
    return run_search(train, overrides, X_train, y_train, X_val, y_val, base_config, extra_params)
