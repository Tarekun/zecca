from abc import ABC, abstractmethod

class Wallet(ABC):
    @abstractmethod
    def get_balance(self) -> float:
        """Return the current wallet balance."""
        pass
