from analysis.wallet.interface import Wallet


class Simulated(Wallet):
    """Fake wallet to run simulations and benchmarking of strategies"""

    def __init__(self, initial_balance: float = 10000.0):
        self.balance = initial_balance
        self.owned = {}

    def invest_fixed(self, ticker: str, amount: int, at: float):
        # if self.balance < 1000:
        #     print("\n\n")
        #     print(amount)
        #     print(at)
        #     print(amount * at)
        #     print(self.balance)

        investment_value = amount * at
        if investment_value > self.balance:
            # if self.balance < 1000:
            #     print("broke nigga leaving")
            return

        # if self.balance < 1000:
        #     print("finna spend this money")
        self.balance -= investment_value
        owned_stocks = self.owned.get(ticker, 0)
        self.owned[ticker] = owned_stocks + amount

    def sell_fixed(self, ticker: str, amount: int, at: float):
        owned_stocks = self.owned.get(ticker, 0)
        if owned_stocks < amount:
            return

        if amount < 0 or at < 0:
            print("\n\nselling and losing money?")
            print(amount)
            print(at)
            print(amount * at)
            print(self.balance)
        self.balance += amount * at
        self.owned[ticker] = owned_stocks - amount
