"""Strategy discovery: every concrete `Strategy` subclass defined in a module
of the `analysis.strategies` package is available by class name. Strategies
elsewhere can be referenced as `package.module:ClassName`."""

import hashlib
import importlib
import inspect
import pkgutil
from typing import Any

from analysis.framework import Strategy

STRATEGIES_PACKAGE = "analysis.strategies"


class UnknownStrategy(ValueError):
    pass


def discover() -> tuple[list[type[Strategy]], dict[str, str]]:
    """Strategies of the package, plus modules that failed to import
    (module name -> error), so one broken file doesn't hide the others."""
    package = importlib.import_module(STRATEGIES_PACKAGE)
    found: list[type[Strategy]] = []
    errors: dict[str, str] = {}
    for info in sorted(pkgutil.iter_modules(package.__path__), key=lambda i: i.name):
        name = f"{STRATEGIES_PACKAGE}.{info.name}"
        try:
            module = importlib.import_module(name)
        except Exception as exc:  # a broken strategy file must not break the CLI
            errors[name] = f"{type(exc).__name__}: {exc}"
            continue
        found += _strategies_in(module)
    return found, errors


def resolve(reference: str) -> type[Strategy]:
    """`ClassName` (searched in the strategies package) or `module:ClassName`
    (`module` relative to the package when it has no dots)."""
    if ":" in reference:
        module_name, class_name = reference.split(":", 1)
        if "." not in module_name:
            module_name = f"{STRATEGIES_PACKAGE}.{module_name}"
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            raise UnknownStrategy(f"Cannot import {module_name}: {exc}") from exc
        cls = getattr(module, class_name, None)
        if cls not in _strategies_in(module):
            raise UnknownStrategy(f"{reference} is not a concrete Strategy subclass")
        return cls  # type: ignore[return-value]

    strategies, errors = discover()
    matches = [cls for cls in strategies if cls.__name__ == reference]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        options = ", ".join(f"{c.__module__}:{c.__name__}" for c in matches)
        raise UnknownStrategy(f"{reference} is ambiguous, use one of: {options}")
    available = ", ".join(sorted(c.__name__ for c in strategies)) or "none"
    hint = f" (modules failing to import: {', '.join(errors)})" if errors else ""
    raise UnknownStrategy(
        f"Unknown strategy {reference!r}; available: {available}{hint}"
    )


def describe(cls: type[Strategy]) -> dict[str, Any]:
    params = {}
    for name, p in cls.param_space.items():
        domain = (
            {"values": list(p.values)}  # type: ignore[arg-type]
            if not p.is_range
            else {"low": p.low, "high": p.high, "step": p.step}
        )
        params[name] = {"default": p.default, **domain}
    return {
        "name": cls.__name__,
        "reference": f"{cls.__module__}:{cls.__name__}",
        "description": inspect.cleandoc(cls.__doc__ or "").split("\n\n")[0],
        "params": params,
        "data_columns": list(cls.data_columns),
        "code_hash": code_hash(cls),
    }


def code_hash(cls: type[Strategy]) -> str:
    """Hash of the source file defining the strategy: any edit to it makes
    a new variant in the experiment log."""
    source = inspect.getsource(inspect.getmodule(cls))  # type: ignore[arg-type]
    return hashlib.sha256(source.encode()).hexdigest()[:12]


def _strategies_in(module) -> list[type[Strategy]]:
    return [
        obj
        for _, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, Strategy)
        and obj is not Strategy
        and obj.__module__ == module.__name__
        and not inspect.isabstract(obj)
    ]
