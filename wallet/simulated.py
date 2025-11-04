from wallet.interface import Wallet


class Simulated(Wallet):
    """Fake wallet to run simulations and benchmarking of strategies"""

    def __init__(self, initial_balance: float = 10000.0):
        self.balance = initial_balance
        self.owned = {}

    def invest_fixed(self, ticker: str, amount: int, at: float):
        investment_value = amount * at
        if investment_value > self.balance:
            return
            raise ValueError("Broke nigga detected")

        self.balance -= investment_value
        owned_stocks = self.owned.get(ticker, 0)
        self.owned[ticker] = owned_stocks + amount

    def sell_fixed(self, ticker: str, amount: int, at: float):
        owned_stocks = self.owned.get(ticker, 0)
        if owned_stocks < amount:
            return
            raise ValueError(f"Tryna sell too much")

        self.balance += amount * at
        self.owned[ticker] = owned_stocks - amount
