import copy
import dataclasses
import time
from dataclasses import dataclass, field
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

from analysis.mlflow_utils import mlflow_experiment, ExperimentLogger

Metric = Callable[[torch.Tensor, torch.Tensor], float]


@dataclass
class TrainingConfig:
    learning_rate: float = 1e-3
    batch_size: int = 32
    num_epochs: int = 100
    # epochs without validation-loss improvement before stopping
    patience: int = 10
    optimizer: Callable[..., torch.optim.Optimizer] = torch.optim.Adam
    # extra kwargs forwarded to the optimizer constructor (e.g. weight_decay, momentum)
    optimizer_kwargs: dict = field(default_factory=dict)
    # None means no scheduler
    scheduler: Callable[..., torch.optim.lr_scheduler.LRScheduler] | None = None
    # extra kwargs forwarded to the scheduler constructor (e.g. step_size, T_max)
    scheduler_kwargs: dict = field(default_factory=dict)
    criterion: nn.Module = field(default_factory=nn.CrossEntropyLoss)


@dataclass
class TrainingResult:
    train_losses: list[float]
    val_losses: list[float]
    best_val_loss: float | None


def _run_epoch(
    model: nn.Module,
    loader: DataLoader,
    criterion: nn.Module,
    device: torch.device,
    optimizer: torch.optim.Optimizer | None,
    metrics: list[Metric] | None = None,
) -> tuple[float, float, dict[str, float]]:
    """One pass over `loader`. Trains (and steps `optimizer`) if it's given,
    otherwise just evaluates. Batches are `(*inputs, target)` tuples -- `model`
    is called as `model(*inputs)`, so this works for any supervised model
    whose forward signature matches its dataset's item shape.

    `metrics` are extra functions called per batch as `metric(output, target)`
    alongside `criterion`, sample-weighted-averaged over the epoch the same
    way the loss is. Returns `(avg_loss, elapsed_seconds, avg_metrics)`, where
    `avg_metrics` maps each metric function's `__name__` to its average."""

    metrics = metrics or []
    start = time.perf_counter()

    model.train(optimizer is not None)
    running_loss = 0.0
    running_metrics = {metric.__name__: 0.0 for metric in metrics}
    n_samples = 0
    with torch.set_grad_enabled(optimizer is not None):
        for batch in loader:
            *inputs, target = (t.to(device) for t in batch)
            output = model(*inputs)
            loss = criterion(output, target)

            if optimizer is not None:
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            batch_size = target.shape[0]
            running_loss += loss.item() * batch_size
            for metric in metrics:
                running_metrics[metric.__name__] += metric(output, target) * batch_size
            n_samples += batch_size

    elapsed = time.perf_counter() - start
    avg_loss = running_loss / n_samples
    avg_metrics = {name: total / n_samples for name, total in running_metrics.items()}
    return avg_loss, elapsed, avg_metrics


@mlflow_experiment(
    name="return-window-classifier",
    tags=lambda args: {"model_class": type(args["model"]).__name__},
    log_config_params=("config", "network_config"),
)
def train(
    model: nn.Module,
    train_dataset: Dataset,
    val_dataset: Dataset | None,
    config: TrainingConfig,
    network_config: "DataclassInstance | None" = None,  # logged only, not used
    logger: ExperimentLogger | None = None,
) -> TrainingResult:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Running on device {device}")
    model.to(device)

    train_loader = DataLoader(train_dataset, batch_size=config.batch_size, shuffle=True)
    val_loader = (
        DataLoader(val_dataset, batch_size=config.batch_size, shuffle=False)
        if val_dataset is not None
        else None
    )

    optimizer = config.optimizer(
        model.parameters(), lr=config.learning_rate, **config.optimizer_kwargs
    )
    scheduler = (
        config.scheduler(optimizer, **config.scheduler_kwargs)
        if config.scheduler is not None
        else None
    )
    early_stopping = val_loader is not None and config.patience > 0

    train_losses, val_losses = [], []
    best_val_loss = float("inf")
    best_state = None
    epochs_without_improvement = 0

    for i in range(config.num_epochs):
        train_loss, train_s, _ = _run_epoch(
            model, train_loader, config.criterion, device, optimizer
        )
        train_losses.append(train_loss)

        val_loss, val_s = None, 0
        if val_loader is not None:
            val_loss, val_s, _ = _run_epoch(
                model, val_loader, config.criterion, device, None
            )
            val_losses.append(val_loss)

        if scheduler is not None:
            if isinstance(scheduler, torch.optim.lr_scheduler.ReduceLROnPlateau):
                scheduler.step(val_loss if val_loss is not None else train_loss)
            else:
                scheduler.step()

        print(
            f"Epoch {i+1}/{config.num_epochs} ({train_s+val_s}s): train_loss={train_loss}"
            + (f" val_loss={val_loss}" if val_loss is not None else "")
        )
        if logger is not None:
            metrics = {"train_loss": train_loss}
            if val_loss is not None:
                metrics["val_loss"] = val_loss
            logger.log_metrics(metrics, step=i)

        if early_stopping and val_loss is not None:
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state = copy.deepcopy(model.state_dict())
                epochs_without_improvement = 0
            else:
                epochs_without_improvement += 1
                if epochs_without_improvement >= config.patience:
                    break

    if best_state is not None:
        model.load_state_dict(best_state)

    if logger is not None:
        logger.log_model(model, flavor="pytorch")
        if early_stopping:
            logger.log_metric("best_val_loss", best_val_loss)

    return TrainingResult(
        train_losses=train_losses,
        val_losses=val_losses,
        best_val_loss=best_val_loss if early_stopping else None,
    )


def _split_overrides(overrides: dict, *configs: "DataclassInstance") -> list[dict]:
    """Splits a flat dict of overrides across several dataclass instances, by
    matching each key against each config's field names. Raises if a key
    matches none of them, so a typo'd sweep entry fails loudly rather than
    being silently ignored."""
    field_names = [{f.name for f in dataclasses.fields(cfg)} for cfg in configs]
    per_config: list[dict] = [{} for _ in configs]
    for key, value in overrides.items():
        matched = False
        for names, bucket in zip(field_names, per_config):
            if key in names:
                bucket[key] = value
                matched = True
        if not matched:
            raise ValueError(
                f"override key {key!r} matches no field in the given configs"
            )
    return per_config


def sweep(
    overrides: list[dict],
    model_cls: type[nn.Module],
    network_config: "DataclassInstance",
    train_config: TrainingConfig,
    train_dataset: Dataset,
    val_dataset: Dataset | None = None,
) -> list[dict]:
    """For each dict in `overrides`, builds a network config and a
    TrainingConfig by overriding `network_config`/`train_config`'s defaults
    with the dict's values, constructs `model_cls(network_config)`, and runs
    `train` with it."""
    results = []
    for override in overrides:
        net_override, train_override = _split_overrides(
            override, network_config, train_config
        )
        net_cfg = dataclasses.replace(network_config, **net_override)
        train_cfg = dataclasses.replace(train_config, **train_override)

        model = model_cls(net_cfg)
        result = train(
            model,
            train_dataset,
            val_dataset,
            train_cfg,
            network_config=net_cfg,
        )

        results.append(
            {
                "overrides": override,
                "network_config": net_cfg,
                "train_config": train_cfg,
                "model": model,
                "result": result,
            }
        )
    return results


def evaluate(
    model: nn.Module,
    dataset: Dataset,
    config: TrainingConfig,
    metrics: list[Metric] | None = None,
) -> tuple[float, dict[str, float]]:
    """Runs `dataset` through `model` with no gradient updates and returns the
    average `config.criterion` loss -- e.g. for a final held-out test-set score
    after picking the best config from a `sweep`."""
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    loader = DataLoader(dataset, batch_size=config.batch_size, shuffle=False)
    loss, _, evaluations = _run_epoch(
        model, loader, config.criterion, device, None, metrics=metrics
    )
    return loss, evaluations
