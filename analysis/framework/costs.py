from abc import ABC, abstractmethod
from typing import Literal

Side = Literal["buy", "sell"]


class CostModel(ABC):
    """How much a trade costs on top of the quoted price.

    Two components: slippage, applied to the execution price (buys fill
    above the quote, sells below), and commission, an explicit fee paid in
    cash on the traded notional."""

    @abstractmethod
    def execution_price(self, quote: float, side: Side) -> float:
        """Price actually obtained when trading at `quote`."""

    @abstractmethod
    def commission(self, notional: float) -> float:
        """Fee charged on a trade of absolute value `notional`."""


class BpsCostModel(CostModel):
    """Fixed costs in basis points (1 bp = 0.01%) of the traded notional:
    `slippage_bps` worsens the fill price, `commission_bps` is charged as a
    fee. The defaults (5 + 5 bps, ~0.2% per round trip) are a reasonable
    baseline for liquid US stocks."""

    def __init__(self, commission_bps: float = 5.0, slippage_bps: float = 5.0) -> None:
        if commission_bps < 0 or slippage_bps < 0:
            raise ValueError("Costs must be non-negative")
        self.commission_bps = commission_bps
        self.slippage_bps = slippage_bps

    def execution_price(self, quote: float, side: Side) -> float:
        slip = self.slippage_bps / 10_000
        return quote * (1 + slip) if side == "buy" else quote * (1 - slip)

    def commission(self, notional: float) -> float:
        return abs(notional) * self.commission_bps / 10_000

    def __repr__(self) -> str:
        return f"BpsCostModel(commission_bps={self.commission_bps}, slippage_bps={self.slippage_bps})"


class NoCosts(BpsCostModel):
    """Frictionless trading, for tests and comparisons."""

    def __init__(self) -> None:
        super().__init__(0.0, 0.0)

    def __repr__(self) -> str:
        return "NoCosts()"
