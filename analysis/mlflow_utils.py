import dataclasses
import functools
import inspect
import os
import traceback
from pathlib import Path

from dotenv import load_dotenv
import mlflow

load_dotenv()

# defaults to an absolute, repo-rooted local store so every experiment lands in
# the same place regardless of the cwd a script/notebook happens to be launched
# from. Set MLFLOW_TRACKING_URI (e.g. in a .env file) to a server URL -- such
# as the one for the locally-hosted mlflow instance -- to log there instead.
_LOCAL_TRACKING_URI = f"sqlite:///{Path(__file__).resolve().parent.parent / 'mlflow.db'}"
mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", _LOCAL_TRACKING_URI))

# When logging to a remote tracking server, artifacts routed through its
# `mlflow-artifacts:/...` proxy get buffered in the server's own memory before
# reaching the backing store -- OOM-prone for large models on small hosts.
# Setting MLFLOW_ARTIFACT_LOCATION to a store URI the client can reach directly
# (e.g. `s3://bucket/prefix` for an S3-compatible store, with
# MLFLOW_S3_ENDPOINT_URL/AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY also set) makes
# newly-created experiments upload straight to the store instead, bypassing the
# server for artifact bytes entirely. Only takes effect for experiments that
# don't already exist -- an experiment's artifact_location is fixed at creation.
_ARTIFACT_LOCATION = os.getenv("MLFLOW_ARTIFACT_LOCATION")


def _get_or_create_experiment(name: str) -> str:
    experiment = mlflow.get_experiment_by_name(name)
    if experiment is not None:
        return experiment.experiment_id
    return mlflow.create_experiment(name, artifact_location=_ARTIFACT_LOCATION)


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


def mlflow_experiment(name, tags=None, log_config_params=()):
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
