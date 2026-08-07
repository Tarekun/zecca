from abc import ABC, abstractmethod


class Wallet:
    def __init__(self, starting_balance: float):
        self.liquidity = starting_balance
        self.holdings: dict[str, float] = {}

    def get_symbol_position(self, symbol: str) -> float | None:
        return self.holdings.get(symbol)

    def move_liquidity(self, amount: float):
        self.liquidity += amount

    def update_position(
        self,
        symbol: str,
        new_holdings: float,
    ):
        if new_holdings != 0:
            self.holdings[symbol] = new_holdings
        else:
            del self.holdings[symbol]

    def net_worth(self, prices: dict[str, float]) -> float:
        return self.liquidity + sum(
            shares * prices.get(symbol, 0.0) for symbol, shares in self.holdings.items()
        )


class Order(ABC):
    @abstractmethod
    def place(self, wallet: Wallet):
        pass


class BuyStock(Order):
    def __init__(self, symbol: str, unit_price: float, amount: float):
        self.symbol = symbol
        self.amount = amount
        self.unit_price = unit_price
        self.qt = amount / unit_price

    def place(self, wallet: Wallet):
        current_position = wallet.get_symbol_position(self.symbol) or 0

        wallet.move_liquidity(-self.amount)
        wallet.update_position(self.symbol, current_position + self.qt)


class LiquidateStock(Order):
    def __init__(self, symbol: str, unit_price: float):
        self.symbol = symbol
        self.unit_price = unit_price

    def place(self, wallet: Wallet):
        current_position = wallet.get_symbol_position(self.symbol)
        if current_position is not None:
            amount = self.unit_price * current_position
            wallet.move_liquidity(amount)
            wallet.update_position(self.symbol, 0)
