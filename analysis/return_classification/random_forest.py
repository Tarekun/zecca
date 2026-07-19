import dataclasses
from dataclasses import dataclass
from typing import Any

import numpy as np
from sklearn.ensemble import RandomForestClassifier

from analysis.mlflow_utils import ExperimentLogger, mlflow_experiment
from analysis.return_classification.common import TrainingResult, run_search, train_and_log


@dataclass
class RandomForestConfig:
    n_estimators: int = 100
    criterion: str = "gini"
    max_depth: int | None = None
    min_samples_split: int | float = 2
    min_samples_leaf: int | float = 1
    max_features: int | float | str | None = "sqrt"
    bootstrap: bool = True
    class_weight: str | dict | None = None
    ccp_alpha: float = 0.0
    n_jobs: int | None = -1
    random_state: int | None = 42


@mlflow_experiment(
    name="return-window-classifier-forest",
    tags={"model_class": "RandomForestClassifier"},
    log_config_params=("config", "extra_params"),
)
def train(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray | None,
    y_val: np.ndarray | None,
    config: RandomForestConfig,
    extra_params: dict[str, Any] | None = None,
    logger: ExperimentLogger | None = None,
) -> tuple[TrainingResult, RandomForestClassifier]:
    model = RandomForestClassifier(**dataclasses.asdict(config))
    return train_and_log(model, X_train, y_train, X_val, y_val, logger)


def search_hyperparameters(
    overrides: list[dict[str, Any]],
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    base_config: RandomForestConfig = RandomForestConfig(),
    extra_params: dict[str, Any] | None = None,
) -> list[dict]:
    """Same as `analysis.return_classification.decision_tree.search_hyperparameters`,
    but for `RandomForestConfig`/`RandomForestClassifier`."""
    return run_search(train, overrides, X_train, y_train, X_val, y_val, base_config, extra_params)
