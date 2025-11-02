from wallet.interface import Wallet


class Simulated(Wallet):
    """Fake wallet to run simulations and benchmarking of strategies"""

    def __init__(self, initial_balance: float = 10000.0):
        self._balance = initial_balance

    def get_balance(self) -> float:
        return self._balance
