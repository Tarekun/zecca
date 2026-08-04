import dataclasses
from dataclasses import dataclass
import numpy as np
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.tree import DecisionTreeClassifier
from typing import Any

from analysis.mlflow_utils import ExperimentLogger, mlflow_experiment
from analysis.models.common import (
    TrainingResult,
    per_class,
    run_search,
    train_sklearn_model,
)


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
def train_decision_tree(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray | None,
    y_val: np.ndarray | None,
    config: DecisionTreeConfig,
    extra_params: dict[str, Any] | None = None,
    logger: ExperimentLogger | None = None,
) -> tuple[TrainingResult, DecisionTreeClassifier]:
    model = DecisionTreeClassifier(**dataclasses.asdict(config))
    metrics = {
        "accuracy": accuracy_score,
        "precision": precision_score,
        "recall": recall_score,
        "f1": lambda y_true, y_pred: f1_score(y_true, y_pred, average="macro"),
        "precision_per_class": per_class(precision_score),
        "recall_per_class": per_class(recall_score),
    }
    return train_sklearn_model(model, X_train, y_train, X_val, y_val, logger, metrics)
