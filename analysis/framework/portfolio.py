import math
from dataclasses import dataclass, field
from datetime import date
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class PortfolioState:
    """Read-only snapshot of the portfolio at the close of `date`, handed to
    `Strategy.decide` by the engine.

    `positions` are share quantities (fractional allowed), `prices` the prices
    each position is marked at (the engine guarantees one for every held
    symbol, using the last known price when today's is missing). `cash` is
    negative when the portfolio is borrowing (leverage)."""

    date: date
    cash: float
    positions: Mapping[str, float] = field(default_factory=dict)
    prices: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        missing = sorted(s for s in self.positions if s not in self.prices)
        if missing:
            raise ValueError(f"No mark price for held symbol(s): {missing}")
        for symbol, shares in self.positions.items():
            if shares < 0:
                raise ValueError(
                    f"Negative position in {symbol} ({shares}): only long positions are supported"
                )
        # freeze the mappings, so a strategy can't mutate engine state through them
        object.__setattr__(self, "positions", MappingProxyType(dict(self.positions)))
        object.__setattr__(self, "prices", MappingProxyType(dict(self.prices)))

    @classmethod
    def empty(cls, day: date, cash: float) -> "PortfolioState":
        return cls(date=day, cash=cash)

    def market_value(self, symbol: str) -> float:
        """Current value of the position in `symbol` (0 if not held)."""
        return self.positions.get(symbol, 0.0) * self.prices.get(symbol, 0.0)

    @property
    def invested_value(self) -> float:
        return math.fsum(self.market_value(s) for s in self.positions)

    @property
    def net_worth(self) -> float:
        return self.cash + self.invested_value

    @property
    def weights(self) -> dict[str, float]:
        """Current weight of every held symbol as a fraction of net worth --
        the same unit as `TargetWeights`."""
        nw = self._positive_net_worth()
        return {s: self.market_value(s) / nw for s in self.positions}

    @property
    def exposure(self) -> float:
        """Invested value / net worth: > 1 means the portfolio is leveraged."""
        return self.invested_value / self._positive_net_worth()

    def _positive_net_worth(self) -> float:
        nw = self.net_worth
        if nw <= 0:
            raise ValueError(
                f"Weights are undefined with non-positive net worth ({nw})"
            )
        return nw
