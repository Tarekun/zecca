import math
from typing import Iterable, Iterator, Mapping

# tolerance on the sum of weights, so that e.g. ten weights of 0.1 don't fail
# a max_leverage=1.0 check because of floating point error
_TOLERANCE = 1e-9


class InvalidTargetWeights(ValueError):
    """Raised when the weights returned by a strategy are not acceptable."""


class TargetWeights(Mapping[str, float]):
    """The portfolio a strategy wants to hold from the next execution onwards,
    as fractions of current net worth: `{"AAPL": 0.5, "MSFT": 0.5}`.

    The mapping describes the *whole* target portfolio: any symbol currently
    held but absent from it has an implicit weight of 0 and is sold. Whatever
    is not invested stays in cash (`cash_weight = 1 - exposure`), which is
    negative when the portfolio is leveraged (exposure > 1).

    Constraints checked at construction (independent of any configuration):
    symbols are non-empty strings, weights are finite and >= 0 (long only).
    Constraints that depend on the run configuration (maximum leverage,
    investable universe) are checked by `validate()`, which the engine calls
    on every decision."""

    def __init__(self, weights: Mapping[str, float] | None = None) -> None:
        weights = dict(weights or {})
        for symbol, weight in weights.items():
            if not isinstance(symbol, str) or not symbol:
                raise InvalidTargetWeights(f"Invalid symbol {symbol!r}")
            # apparently in python bool<:int, so isinstance(True, int)->True
            if not isinstance(weight, (int, float)) or isinstance(weight, bool):
                raise InvalidTargetWeights(
                    f"Weight for {symbol} is not a number: {weight!r}"
                )
            if not math.isfinite(weight):
                raise InvalidTargetWeights(
                    f"Weight for {symbol} is not finite: {weight}"
                )
            if weight < 0:
                raise InvalidTargetWeights(
                    f"Weight for {symbol} is negative ({weight}): only long positions are supported"
                )
        self._weights = {s: float(w) for s, w in weights.items()}

    @classmethod
    def equal(cls, symbols: Iterable[str], exposure: float = 1.0) -> "TargetWeights":
        """Splits `exposure` evenly across `symbols` (e.g. exposure=1.5 for a
        1.5x leveraged equal-weight portfolio). No symbols → all cash."""
        symbols = list(dict.fromkeys(symbols))
        if not symbols:
            return cls()
        return cls({s: exposure / len(symbols) for s in symbols})

    @classmethod
    def cash(cls) -> "TargetWeights":
        """Sell everything and hold only cash."""
        return cls()

    def __getitem__(self, symbol: str) -> float:
        return self._weights[symbol]

    def __iter__(self) -> Iterator[str]:
        return iter(self._weights)

    def __len__(self) -> int:
        return len(self._weights)

    def __repr__(self) -> str:
        return f"TargetWeights({self._weights!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Mapping):
            return dict(self.items()) == dict(other.items())
        return NotImplemented

    def __hash__(self) -> int:
        return hash(frozenset(self._weights.items()))

    @property
    def exposure(self) -> float:
        """Sum of weights: 1.0 fully invested, < 1 partly in cash, > 1 leveraged."""
        return math.fsum(self._weights.values())

    @property
    def cash_weight(self) -> float:
        return 1.0 - self.exposure

    def validate(
        self, max_leverage: float = 1.0, universe: Iterable[str] | None = None
    ) -> None:
        """Raises `InvalidTargetWeights` if exposure exceeds `max_leverage` or,
        when `universe` is given, if a symbol with a non-zero weight is not
        investable."""
        if self.exposure > max_leverage + _TOLERANCE:
            raise InvalidTargetWeights(
                f"Total exposure {self.exposure:.6f} exceeds max leverage {max_leverage}"
            )
        if universe is not None:
            allowed = set(universe)
            outside = sorted(
                s for s, w in self._weights.items() if w > 0 and s not in allowed
            )
            if outside:
                shown = ", ".join(outside[:10]) + ("..." if len(outside) > 10 else "")
                raise InvalidTargetWeights(
                    f"{len(outside)} symbol(s) not in the investable universe: {shown}"
                )
