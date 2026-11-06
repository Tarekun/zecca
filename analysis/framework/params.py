import itertools
import math
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Sequence


class InvalidParams(ValueError):
    """Raised when strategy parameters don't match the declared `ParamSpace`."""


@dataclass(frozen=True)
class Param:
    """A single strategy parameter: its `default` value plus the domain the
    framework may search over (grid search, walk-forward, sensitivity).

    The domain is declared either as an explicit list of `values` (any type,
    e.g. `["weekly", "monthly"]`) or as a numeric range `low..high` (inclusive)
    with a `step`. A range is searchable on the grid `low, low+step, ...,
    high`; any value inside the range is accepted when passed explicitly.

    Examples:
        Param(default=12, low=3, high=12, step=3)        # ints 3, 6, 9, 12
        Param(default="monthly", values=["weekly", "monthly", "quarterly"])
        Param(default=0.5, low=0.0, high=1.0, step=0.25) # floats
    """

    default: Any
    values: Sequence[Any] | None = None
    low: float | None = None
    high: float | None = None
    step: float | None = None

    def __post_init__(self) -> None:
        has_values = self.values is not None
        has_range = any(x is not None for x in (self.low, self.high, self.step))
        if has_values == has_range:
            raise InvalidParams(
                "Param needs exactly one domain: either `values` or `low`/`high`/`step`"
            )
        if has_values:
            if len(self.values) == 0:  # type: ignore[arg-type]
                raise InvalidParams("Param `values` must not be empty")
            object.__setattr__(self, "values", tuple(self.values))  # type: ignore[arg-type]
        else:
            if self.low is None or self.high is None or self.step is None:
                raise InvalidParams(
                    "A range Param needs all of `low`, `high` and `step`"
                )
            if self.low > self.high:
                raise InvalidParams(
                    f"Param low={self.low} is greater than high={self.high}"
                )
            if self.step <= 0:
                raise InvalidParams(f"Param step must be positive, got {self.step}")
        if not self.accepts(self.default):
            raise InvalidParams(f"Param default {self.default!r} is outside its domain")

    @property
    def is_range(self) -> bool:
        return self.values is None

    def accepts(self, value: Any) -> bool:
        if not self.is_range:
            return value in self.values  # type: ignore[operator]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        if isinstance(value, float) and not math.isfinite(value):
            return False
        return self.low <= value <= self.high  # type: ignore[operator]

    def grid(self) -> list[Any]:
        """Discrete candidate values for search."""
        if not self.is_range:
            return list(self.values)  # type: ignore[arg-type]

        low, high, step = self.low, self.high, self.step
        integral = all(float(x).is_integer() for x in (low, high, step)) and isinstance(  # type: ignore[arg-type]
            self.default, int
        )
        n = int(math.floor((high - low) / step + 1e-9)) + 1  # type: ignore[operator]
        points = [low + i * step for i in range(n)]  # type: ignore[operator]
        return [int(p) for p in points] if integral else [float(p) for p in points]


class ParamSpace(Mapping[str, Param]):
    """The full set of parameters a `Strategy` accepts, keyed by name.

    Declared once on the strategy class (`param_space = ParamSpace(...)`) so
    that the framework can build strategy instances from plain dicts, fill in
    defaults, validate user/agent supplied values and enumerate the search
    grid without knowing anything about the specific strategy."""

    def __init__(self, **params: Param) -> None:
        for name, param in params.items():
            if not isinstance(param, Param):
                raise InvalidParams(
                    f"Parameter {name!r} must be a Param, got {param!r}"
                )
            if name.startswith("_"):
                raise InvalidParams(
                    f"Parameter names must not start with '_': {name!r}"
                )
        self._params = dict(params)

    def __getitem__(self, name: str) -> Param:
        return self._params[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._params)

    def __len__(self) -> int:
        return len(self._params)

    def __repr__(self) -> str:
        return f"ParamSpace({self._params!r})"

    def defaults(self) -> dict[str, Any]:
        return {name: p.default for name, p in self._params.items()}

    def resolve(self, values: Mapping[str, Any]) -> dict[str, Any]:
        """Returns a complete parameter dict: `values` validated against the
        declared domains, with defaults for anything missing. Raises
        `InvalidParams` on unknown names or out-of-domain values."""
        unknown = set(values) - set(self._params)
        if unknown:
            raise InvalidParams(
                f"Unknown parameter(s) {sorted(unknown)}; declared: {sorted(self._params)}"
            )
        resolved = self.defaults() | dict(values)
        for name, value in resolved.items():
            if not self._params[name].accepts(value):
                raise InvalidParams(
                    f"Value {value!r} for parameter {name!r} is outside its domain"
                )
        return resolved

    def grid(self) -> Iterator[dict[str, Any]]:
        """Cartesian product of every parameter's candidate values."""
        names = list(self._params)
        for combo in itertools.product(*(self._params[n].grid() for n in names)):
            yield dict(zip(names, combo))

    def grid_size(self) -> int:
        return math.prod(len(p.grid()) for p in self._params.values())
