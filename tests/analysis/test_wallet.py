import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parents[2]))
from analysis.strategies.wallet import Wallet, Order, BuyStock, LiquidateStock


class TestWallet:
    def test_starts_with_balance_and_no_holdings(self):
        wallet = Wallet(1000.0)
        assert wallet.liquidity == 1000.0
        assert wallet.holdings == {}

    def test_get_symbol_position_returns_none_when_absent(self):
        wallet = Wallet(1000.0)
        assert wallet.get_symbol_position("AAPL") is None

    def test_get_symbol_position_returns_current_shares(self):
        wallet = Wallet(1000.0)
        wallet.holdings["AAPL"] = 5.0
        assert wallet.get_symbol_position("AAPL") == 5.0

    def test_move_liquidity_can_increase_and_decrease(self):
        wallet = Wallet(1000.0)
        wallet.move_liquidity(500.0)
        assert wallet.liquidity == 1500.0
        wallet.move_liquidity(-2000.0)
        assert wallet.liquidity == -500.0

    def test_update_position_sets_nonzero_holdings(self):
        wallet = Wallet(1000.0)
        wallet.update_position("AAPL", 3.0)
        assert wallet.holdings["AAPL"] == 3.0

    def test_update_position_overwrites_existing_holdings(self):
        wallet = Wallet(1000.0)
        wallet.update_position("AAPL", 3.0)
        wallet.update_position("AAPL", 7.0)
        assert wallet.holdings["AAPL"] == 7.0

    def test_update_position_removes_symbol_when_zeroed(self):
        wallet = Wallet(1000.0)
        wallet.update_position("AAPL", 3.0)
        wallet.update_position("AAPL", 0)
        assert "AAPL" not in wallet.holdings

    def test_net_worth_is_liquidity_when_no_holdings(self):
        wallet = Wallet(1000.0)
        assert wallet.net_worth({"AAPL": 150.0}) == 1000.0

    def test_net_worth_adds_value_of_all_holdings(self):
        wallet = Wallet(1000.0)
        wallet.holdings = {"AAPL": 2.0, "MSFT": 1.0}
        net_worth = wallet.net_worth({"AAPL": 150.0, "MSFT": 300.0})
        assert net_worth == 1000.0 + 2.0 * 150.0 + 1.0 * 300.0

    def test_net_worth_treats_missing_price_as_zero(self):
        wallet = Wallet(1000.0)
        wallet.holdings = {"AAPL": 2.0}
        assert wallet.net_worth({}) == 1000.0


class TestOrder:
    def test_cannot_be_instantiated_directly(self):
        with pytest.raises(TypeError):
            Order()  # type: ignore


class TestBuyStock:
    def test_computes_quantity_from_amount_and_price(self):
        order = BuyStock("AAPL", unit_price=50.0, amount=500.0)
        assert order.qt == 10.0

    def test_place_debits_liquidity_by_amount(self):
        wallet = Wallet(1000.0)
        BuyStock("AAPL", unit_price=50.0, amount=500.0).place(wallet)
        assert wallet.liquidity == 500.0

    def test_place_credits_shares_for_new_position(self):
        wallet = Wallet(1000.0)
        BuyStock("AAPL", unit_price=50.0, amount=500.0).place(wallet)
        assert wallet.holdings["AAPL"] == 10.0

    def test_place_adds_to_existing_position(self):
        wallet = Wallet(1000.0)
        wallet.holdings["AAPL"] = 4.0
        BuyStock("AAPL", unit_price=50.0, amount=500.0).place(wallet)
        assert wallet.holdings["AAPL"] == 14.0

    def test_place_does_not_affect_other_symbols(self):
        wallet = Wallet(1000.0)
        wallet.holdings["MSFT"] = 2.0
        BuyStock("AAPL", unit_price=50.0, amount=500.0).place(wallet)
        assert wallet.holdings["MSFT"] == 2.0

    def test_two_orders_accumulate_position_and_debit_liquidity(self):
        wallet = Wallet(10_000.0)
        BuyStock("AAPL", unit_price=50.0, amount=500.0).place(wallet)
        BuyStock("AAPL", unit_price=100.0, amount=1000.0).place(wallet)
        assert wallet.holdings["AAPL"] == 20.0
        assert wallet.liquidity == 10_000.0 - 500.0 - 1000.0


class TestLiquidateStock:
    def test_credits_liquidity_at_current_price(self):
        wallet = Wallet(0.0)
        wallet.holdings["AAPL"] = 10.0
        LiquidateStock("AAPL", unit_price=55.0).place(wallet)
        assert wallet.liquidity == 550.0

    def test_removes_position(self):
        wallet = Wallet(0.0)
        wallet.holdings["AAPL"] = 10.0
        LiquidateStock("AAPL", unit_price=55.0).place(wallet)
        assert "AAPL" not in wallet.holdings

    def test_is_noop_when_symbol_not_held(self):
        wallet = Wallet(1000.0)
        LiquidateStock("AAPL", unit_price=55.0).place(wallet)
        assert wallet.liquidity == 1000.0
        assert wallet.holdings == {}

    def test_only_affects_targeted_symbol(self):
        wallet = Wallet(0.0)
        wallet.holdings = {"AAPL": 10.0, "MSFT": 5.0}
        LiquidateStock("AAPL", unit_price=55.0).place(wallet)
        assert wallet.holdings == {"MSFT": 5.0}


class TestBuyAndLiquidateRoundTrip:
    def test_round_trips_liquidity_at_same_price(self):
        wallet = Wallet(1000.0)
        BuyStock("AAPL", unit_price=50.0, amount=500.0).place(wallet)
        LiquidateStock("AAPL", unit_price=50.0).place(wallet)
        assert wallet.liquidity == 1000.0
        assert wallet.holdings == {}

    def test_at_higher_price_is_profitable(self):
        wallet = Wallet(1000.0)
        BuyStock("AAPL", unit_price=50.0, amount=500.0).place(wallet)
        LiquidateStock("AAPL", unit_price=60.0).place(wallet)
        assert wallet.liquidity == 500.0 + 10.0 * 60.0

    def test_at_lower_price_is_a_loss(self):
        wallet = Wallet(1000.0)
        BuyStock("AAPL", unit_price=50.0, amount=500.0).place(wallet)
        LiquidateStock("AAPL", unit_price=40.0).place(wallet)
        assert wallet.liquidity == 500.0 + 10.0 * 40.0
