import dataclasses
from dotenv import load_dotenv
import functools
import inspect
import mlflow
import os
from pathlib import Path
import traceback
from typing import Union, Optional, Callable, Any

load_dotenv()

mlflow.set_tracking_uri(
    os.getenv(
        "MLFLOW_TRACKING_URI",
        # if URI unspecified default to local db in the project root
        f"sqlite:///{Path(__file__).resolve().parent.parent / 'mlflow.db'}",
    )
)

# to bypass mlflow frontend a push models directly to blob store
_ARTIFACT_LOCATION = os.getenv("MLFLOW_ARTIFACT_LOCATION")


def _get_or_create_experiment(name: str) -> str:
    # an experiment's artifact_location is fixed at creation and mlflow has no
    # API to change it, so if the active experiment under `name` was created
    # under a different MLFLOW_ARTIFACT_LOCATION than the one configured now,
    # reusing it would keep silently writing artifacts to the old location.
    # `get_experiment_by_name` also returns soft-deleted experiments, and
    # mlflow refuses to create a new one under a name a deleted one still
    # occupies -- in both cases, walk forward to a numbered variant instead of
    # failing or silently reusing a stale location. Once created, a variant is
    # stable: later calls find it active with a matching location and stop.
    candidate = name
    suffix = 1
    while True:
        experiment = mlflow.get_experiment_by_name(candidate)
        if experiment is None:
            return mlflow.create_experiment(
                candidate, artifact_location=_ARTIFACT_LOCATION
            )
        if experiment.lifecycle_stage == "active" and (
            _ARTIFACT_LOCATION is None
            or experiment.artifact_location == _ARTIFACT_LOCATION
        ):
            return experiment.experiment_id
        suffix += 1
        candidate = f"{name}-{suffix}"


class ExperimentLogger:
    def log_metric(self, key, value, step=None):
        mlflow.log_metric(key, value, step=step)

    def log_metrics(self, metrics: dict, step=None):
        mlflow.log_metrics(metrics, step=step)

    def log_model(self, model, flavor="pytorch"):
        getattr(mlflow, flavor).log_model(model, "model")


def _log_param_value(key, val):
    if val is None or isinstance(val, (int, float, str, bool)):
        mlflow.log_param(key, val)
    elif isinstance(val, dict):
        for k, v in val.items():
            _log_param_value(f"{key}.{k}", v)
    elif isinstance(val, type) or callable(val):
        mlflow.log_param(key, getattr(val, "__name__", str(val)))
    else:
        mlflow.log_param(key, repr(val)[:250])


def _log_dataclass_params(prefix, obj):
    for f in dataclasses.fields(obj):
        _log_param_value(f"{prefix}.{f.name}", getattr(obj, f.name))


def mlflow_experiment(
    name,
    tags: Optional[Union[dict, Callable[[Any], dict]]] = None,
    log_config_params=(),
):
    """
    tags: dict, or callable(bound_arguments) -> dict, for dynamic tagging
          (e.g. tagging with the model class or sweep override).
    log_config_params: names of dataclass- or dict-valued params to flatten
          into mlflow params automatically.
    """

    def decorator(fn):
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            mlflow.set_experiment(experiment_id=_get_or_create_experiment(name))
            with mlflow.start_run():
                resolved_tags = tags(bound.arguments) if callable(tags) else tags
                if resolved_tags:
                    mlflow.set_tags(resolved_tags)

                for pname in log_config_params:
                    val = bound.arguments.get(pname)
                    if val is None:
                        continue
                    if dataclasses.is_dataclass(val):
                        _log_dataclass_params(pname, val)
                    else:
                        _log_param_value(pname, val)

                call_kwargs = dict(bound.arguments)
                if "logger" in sig.parameters:
                    call_kwargs["logger"] = ExperimentLogger()

                try:
                    return fn(**call_kwargs)
                except Exception:
                    mlflow.set_tag("status", "failed")
                    mlflow.log_text(traceback.format_exc(), "traceback.txt")
                    raise

        return wrapper

    return decorator
