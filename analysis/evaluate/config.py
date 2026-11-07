from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

from analysis.framework import BacktestConfig, BpsCostModel

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "configs" / "evaluation.yml"


@dataclass(frozen=True)
class EvaluationConfig:
    """`configs/evaluation.yml`, with paths resolved against the repo root."""

    holdout_start: date
    data_cache: Path
    dataplatform_root: Path
    risk_free_region: str | None
    initial_cash: float
    commission_bps: float
    slippage_bps: float
    max_leverage: float
    experiment_log: Path
    mlflow_experiment: str

    @classmethod
    def load(cls, path: Path | str = DEFAULT_CONFIG) -> "EvaluationConfig":
        with open(path) as f:
            raw = yaml.safe_load(f)
        for key in ("data_cache", "dataplatform_root", "experiment_log"):
            raw[key] = REPO_ROOT / raw[key]  # absolute paths are kept as they are
        if not isinstance(raw["holdout_start"], date):
            raw["holdout_start"] = date.fromisoformat(str(raw["holdout_start"]))
        return cls(**raw)

    def backtest_config(self, max_leverage: float | None = None) -> BacktestConfig:
        return BacktestConfig(
            initial_cash=self.initial_cash,
            costs=BpsCostModel(self.commission_bps, self.slippage_bps),
            max_leverage=self.max_leverage if max_leverage is None else max_leverage,
        )
