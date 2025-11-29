from abc import ABC, abstractmethod


class Wallet(ABC):
    # @abstractmethod
    # def invest_ratio(self, ticker: str, ratio: float, at: float):
    #     """Invest `ratio` percentage of the wallet's funds
    #     on stocks valued at `at`"""
    #     pass

    # @abstractmethod
    # def sell_ratio(self, ticker: str, ratio: float, at: float):
    #     """Sell `ratio` percentage of the owned stocks valued
    #     at `at`"""
    #     pass

    @abstractmethod
    def invest_fixed(self, ticker: str, amount: int, at: float):
        """Buy `amount` amount of stokcs valued at `at`"""
        pass

    @abstractmethod
    def sell_fixed(self, ticker: str, amount: int, at: float):
        """Sell `amount` amount of stokcs valued at `at`"""
        pass
