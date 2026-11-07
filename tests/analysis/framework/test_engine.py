import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))
from analysis.framework import (
    BacktestConfig,
    BpsCostModel,
    InvalidTargetWeights,
    NoCosts,
    Param,
    ParamSpace,
    Strategy,
    TargetWeights,
    run_backtest,
)


def trading_days(start: date, n: int) -> list[date]:
    days, d = [], start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


DAYS = trading_days(date(2024, 1, 1), 12)
FREE = BacktestConfig(costs=NoCosts(), borrow_spread=0.0)


def market(prices: dict[str, list[tuple[float, float] | None]]) -> pl.DataFrame:
    """`prices[symbol][i]` is the (open, close) of DAYS[i]; None = no row."""
    rows = [
        {"timeframe": DAYS[i], "symbol": s, "open": oc[0], "close": oc[1]}
        for s, series in prices.items()
        for i, oc in enumerate(series)
        if oc is not None
    ]
    return pl.DataFrame(rows)


def flat(price: float, n: int = len(DAYS)) -> list[tuple[float, float] | None]:
    return [(price, price)] * n


def scripted(schedule: dict[int, dict[str, float] | None]) -> Strategy:
    """A strategy returning `schedule[i]` at the close of DAYS[i] and
    recording the portfolio it was shown every day."""

    class Scripted(Strategy):
        def __init__(self, **params):
            super().__init__(**params)
            self.seen = []

        def decide(self, market, portfolio):
            self.seen.append(portfolio)
            target = schedule.get(DAYS.index(market.date))
            return None if target is None else TargetWeights(target)

    return Scripted()


def equity(result) -> list[float]:
    return result.equity["portfolio_value"].to_list()


class TestExecution:
    def test_buy_and_hold_executes_at_next_open(self):
        prices = [(10.0, 11.0), (12.0, 13.0), (13.0, 15.0), (15.0, 14.0)]
        result = run_backtest(scripted({0: {"A": 1.0}}), market({"A": prices}), FREE)

        trade = result.trades.row(0, named=True)
        assert (trade["timeframe"], trade["side"], trade["quote"]) == (
            DAYS[1],
            "buy",
            12.0,
        )
        qty = 10_000 / 12.0
        assert trade["quantity"] == pytest.approx(qty)
        assert equity(result) == pytest.approx([10_000, qty * 13, qty * 15, qty * 14])
        assert result.equity["cash"][-1] == pytest.approx(0.0)
        assert result.total_return == pytest.approx(qty * 14 / 10_000 - 1)

    def test_costs(self):
        config = BacktestConfig(costs=BpsCostModel(commission_bps=10, slippage_bps=20))
        result = run_backtest(
            scripted({0: {"A": 1.0}}), market({"A": flat(100.0, 3)}), config
        )

        trade = result.trades.row(0, named=True)
        assert trade["price"] == pytest.approx(100.0 * 1.002)
        assert trade["commission"] == pytest.approx(trade["notional"] * 0.001)
        assert trade["slippage"] == pytest.approx(trade["quantity"] * 0.2)
        # buys are scaled so that price + slippage + commission fit the cash
        assert trade["quantity"] == pytest.approx(10_000 / (100.0 * 1.002 * 1.001))
        assert result.equity["cash"][-1] == pytest.approx(0.0, abs=1e-6)
        assert result.final_value == pytest.approx(trade["quantity"] * 100.0)
        assert result.events["kind"].to_list() == ["buys_scaled"]

    def test_default_costs_are_5_plus_5_bps(self):
        result = run_backtest(scripted({0: {"A": 1.0}}), market({"A": flat(100.0, 3)}))
        trade = result.trades.row(0, named=True)
        assert trade["price"] == pytest.approx(100.05)
        assert trade["commission"] == pytest.approx(trade["notional"] * 0.0005)

    def test_rebalance_trades_only_the_difference(self):
        a = [(100.0, 100.0)] * 3 + [(200.0, 200.0)] * 3
        data = market({"A": a, "B": flat(100.0, 6)})
        result = run_backtest(
            scripted({0: {"A": 0.5, "B": 0.5}, 3: {"A": 0.5, "B": 0.5}}), data, FREE
        )

        second = result.trades.filter(pl.col("timeframe") == DAYS[4]).sort("symbol")
        assert second["side"].to_list() == ["sell", "buy"]
        # A doubled: 50 shares worth 10k out of 15k -> sell 12.5 shares, buy 25 B
        assert second["quantity"].to_list() == pytest.approx([12.5, 25.0])
        last = result.positions.filter(pl.col("timeframe") == DAYS[5])
        assert last["weight"].to_list() == pytest.approx([0.5, 0.5])

    def test_targeting_cash_sells_everything(self):
        result = run_backtest(
            scripted({0: {"A": 1.0}, 2: {}}), market({"A": flat(50.0, 5)}), FREE
        )
        assert result.positions.filter(pl.col("timeframe") == DAYS[4]).is_empty()
        assert result.equity["cash"][-1] == pytest.approx(10_000)
        assert result.decisions.filter(pl.col("timeframe") == DAYS[2])[
            "symbol"
        ].to_list() == [None]

    def test_whole_shares(self):
        config = BacktestConfig(costs=NoCosts(), fractional_shares=False)
        result = run_backtest(
            scripted({0: {"A": 1.0}}), market({"A": flat(300.0, 3)}), config
        )
        assert result.trades["quantity"].to_list() == [33.0]
        assert result.equity["cash"][-1] == pytest.approx(100.0)

    def test_min_trade_value_skips_small_adjustments(self):
        a = [(100.0, 100.0)] * 3 + [(101.0, 101.0)] * 3
        data = market({"A": a, "B": flat(100.0, 6)})
        config = BacktestConfig(costs=NoCosts(), min_trade_value=100.0)
        result = run_backtest(
            scripted({0: {"A": 0.5, "B": 0.5}, 3: {"A": 0.5, "B": 0.5}}), data, config
        )
        assert result.trades.filter(pl.col("timeframe") == DAYS[4]).is_empty()

    def test_decide_sees_current_portfolio_and_not_last_day(self):
        strategy = scripted({0: {"A": 1.0}})
        run_backtest(strategy, market({"A": flat(100.0, 4)}), FREE)
        assert len(strategy.seen) == 3  # never asked on the last day
        assert strategy.seen[0].weights == {}
        assert strategy.seen[1].weights == pytest.approx({"A": 1.0})
        assert strategy.seen[1].positions == pytest.approx({"A": 100.0})


class TestMissingPrices:
    def test_target_without_open_price_stays_in_cash(self):
        data = market(
            {"A": flat(100.0, 3), "B": [(100.0, 100.0), None, (100.0, 100.0)]}
        )
        result = run_backtest(scripted({0: {"A": 0.5, "B": 0.5}}), data, FREE)
        assert result.trades["symbol"].to_list() == ["A"]
        assert result.equity["cash"][-1] == pytest.approx(5_000)
        event = result.events.row(0, named=True)
        assert (event["symbol"], event["kind"]) == ("B", "buy_skipped")

    def test_gap_marks_at_last_known_price(self):
        a = [(100.0, 100.0), (100.0, 110.0), None, None, (120.0, 120.0)]
        data = market({"A": a, "B": flat(1.0, 5)})
        result = run_backtest(scripted({0: {"A": 1.0}}), data, FREE)
        assert equity(result) == pytest.approx([10_000, 11_000, 11_000, 11_000, 12_000])
        assert result.positions["stale_days"].to_list() == [0, 1, 2, 0]
        assert result.events.is_empty()

    def test_delisting_forces_liquidation_with_haircut(self):
        a = [(100.0, 100.0), (100.0, 100.0), (100.0, 80.0)] + [None] * 5
        data = market({"A": a, "B": flat(1.0, 8)})
        config = BacktestConfig(
            costs=NoCosts(), delisting_days=3, delisting_haircut=0.5
        )
        result = run_backtest(scripted({0: {"A": 1.0}}), data, config)

        liquidation = result.trades.filter(pl.col("reason") == "delisting").row(
            0, named=True
        )
        assert liquidation["timeframe"] == DAYS[5]  # 3rd day without a price
        assert liquidation["price"] == pytest.approx(40.0)
        assert result.equity["cash"][-1] == pytest.approx(100 * 40.0)
        assert result.events["kind"].to_list() == ["delisting"]
        assert result.positions.filter(pl.col("timeframe") >= DAYS[5]).is_empty()

    def test_exit_without_price_is_retried(self):
        a = [
            (100.0, 100.0),
            (100.0, 100.0),
            (100.0, 100.0),
            None,
            (90.0, 90.0),
            (90.0, 90.0),
        ]
        data = market({"A": a, "B": flat(1.0, 6)})
        result = run_backtest(scripted({0: {"A": 1.0}, 2: {}}), data, FREE)

        assert result.events.select("timeframe", "symbol", "kind").rows() == [
            (DAYS[3], "A", "exit_deferred")
        ]
        exit_trade = result.trades.filter(pl.col("side") == "sell").row(0, named=True)
        assert (exit_trade["timeframe"], exit_trade["reason"], exit_trade["quote"]) == (
            DAYS[4],
            "exit_retry",
            90.0,
        )
        assert result.equity["cash"][-1] == pytest.approx(9_000)

    def test_new_decision_supersedes_pending_exit(self):
        # day 3 has a row (so A is in the universe) but no open price
        a = [(100.0, 100.0)] * 3 + [(None, 100.0)] + [(100.0, 100.0)] * 3
        data = market({"A": a, "B": flat(1.0, 7)})
        # exit decided on day 2 can't execute on day 3, then A is wanted again
        result = run_backtest(
            scripted({0: {"A": 1.0}, 2: {}, 3: {"A": 1.0}}), data, FREE
        )
        assert result.events["kind"].to_list() == ["exit_deferred"]
        assert result.trades.filter(pl.col("side") == "sell").is_empty()
        assert result.positions.filter(pl.col("timeframe") == DAYS[6])[
            "weight"
        ].to_list() == [pytest.approx(1.0)]


class TestCashAndLeverage:
    def test_leverage_borrows_and_pays_spread(self):
        config = BacktestConfig(costs=NoCosts(), max_leverage=1.5, borrow_spread=0.0252)
        result = run_backtest(
            scripted({0: {"A": 1.5}}), market({"A": flat(100.0, 3)}), config
        )
        day1 = result.equity.row(1, named=True)
        assert day1["interest"] == pytest.approx(-5_000 * 0.0001)
        assert day1["cash"] == pytest.approx(-5_000.5)
        assert day1["exposure"] == pytest.approx(15_000 / 9_999.5)

    def test_leverage_above_limit_is_rejected(self):
        with pytest.raises(InvalidTargetWeights, match="max leverage"):
            run_backtest(scripted({0: {"A": 1.2}}), market({"A": flat(100.0, 3)}), FREE)

    def test_cash_earns_risk_free(self, caplog):
        rf = pl.DataFrame({"timeframe": [DAYS[0] - timedelta(days=60)], "rf": [0.001]})
        with caplog.at_level(logging.WARNING):
            result = run_backtest(
                scripted({}), market({"A": flat(1.0, 4)}), FREE, risk_free=rf
            )
        assert equity(result) == pytest.approx([10_000 * 1.001**n for n in range(1, 5)])
        assert "is reused" in caplog.text

    def test_cash_interest_can_be_disabled(self):
        rf = pl.DataFrame({"timeframe": DAYS[:4], "rf": [0.001] * 4})
        config = BacktestConfig(costs=NoCosts(), cash_earns_interest=False)
        result = run_backtest(
            scripted({}), market({"A": flat(1.0, 4)}), config, risk_free=rf
        )
        assert equity(result) == pytest.approx([10_000] * 4)


class TestValidation:
    def test_requires_open_and_close(self):
        with pytest.raises(ValueError, match="open"):
            run_backtest(scripted({}), market({"A": flat(1.0, 3)}).drop("open"))

    def test_symbol_outside_universe_is_rejected(self):
        with pytest.raises(InvalidTargetWeights, match="universe"):
            run_backtest(scripted({0: {"ZZZ": 1.0}}), market({"A": flat(1.0, 3)}), FREE)

    def test_decide_must_return_weights(self):
        class Bad(Strategy):
            def decide(self, market, portfolio):
                return [("A", 1.0)]

        with pytest.raises(TypeError, match="TargetWeights"):
            run_backtest(Bad(), market({"A": flat(1.0, 3)}))

    def test_plain_dicts_are_accepted(self):
        class Dict(Strategy):
            def decide(self, market, portfolio):
                return {"A": 1.0}

        result = run_backtest(Dict(), market({"A": flat(1.0, 3)}), FREE)
        assert result.trades.height == 1


class Momentum(Strategy):
    param_space = ParamSpace(lookback=Param(default=3, values=[3]))

    def prepare(self, data):
        return data.select(
            "timeframe",
            "symbol",
            pl.col("close").pct_change(self.lookback).over("symbol").alias("momentum"),
        )

    def decide(self, market, portfolio):
        top = (
            market.signals(["momentum"]).drop_nulls().top_k(1, by="momentum")["symbol"]
        )
        return TargetWeights.equal(top)


def test_results_up_to_t_do_not_depend_on_future_data():
    import random

    rng = random.Random(0)
    days = trading_days(date(2024, 1, 1), 60)
    rows = []
    for s in "ABCD":
        price = 100.0
        for d in days:
            open_ = price * (1 + rng.uniform(-0.02, 0.02))
            price = open_ * (1 + rng.uniform(-0.03, 0.03))
            rows.append({"timeframe": d, "symbol": s, "open": open_, "close": price})
    data = pl.DataFrame(rows)

    cutoff = days[40]
    full = run_backtest(Momentum(), data)
    truncated = run_backtest(Momentum(), data.filter(pl.col("timeframe") <= cutoff))
    expected = full.equity.filter(pl.col("timeframe") <= cutoff)
    assert truncated.equity.equals(expected)
    assert full.trades.height > 10  # the strategy actually traded


class TestStartAndInitialPortfolio:
    class HistoryReader(Strategy):
        def __init__(self, **params):
            super().__init__(**params)
            self.history_lengths = []

        def decide(self, market, portfolio):
            self.history_lengths.append(len(market.trading_days()))
            return {"A": 1.0} if not portfolio.positions else None

    def test_warmup_is_visible_but_not_traded(self):
        prices = [(float(10 + i), float(10 + i)) for i in range(6)]
        strategy = self.HistoryReader()
        result = run_backtest(strategy, market({"A": prices}), FREE, start=DAYS[3])

        # decided at the close before start, executed at the start's open
        assert result.trades.select("timeframe", "quote").rows() == [(DAYS[3], 13.0)]
        assert result.equity["timeframe"].to_list() == DAYS[3:6]
        assert result.initial_value == 10_000
        # the first decision already sees the 3 warmup days
        assert strategy.history_lengths[0] == 3

    def test_start_on_a_non_trading_day_uses_the_next_one(self):
        result = run_backtest(
            scripted({}), market({"A": flat(1.0, 10)}), FREE, start=date(2024, 1, 6)
        )
        assert result.equity["timeframe"][0] == date(2024, 1, 8)

    def test_start_after_the_data(self):
        with pytest.raises(ValueError, match="No trading days"):
            run_backtest(scripted({}), market({"A": flat(1.0, 3)}), FREE, start=DAYS[5])

    def test_initial_portfolio(self):
        from analysis.framework import PortfolioState

        initial = PortfolioState(
            DAYS[0], cash=500.0, positions={"A": 10.0}, prices={"A": 50.0}
        )
        result = run_backtest(
            scripted({}), market({"A": [(60.0, 60.0)] * 3}), FREE, initial=initial
        )
        assert result.initial_value == 1_000.0
        assert equity(result) == pytest.approx([1_100.0] * 3)
        assert result.final_state.positions == {"A": 10.0}
        assert result.total_return == pytest.approx(0.1)

    def test_equity_records_risk_free(self):
        rf = pl.DataFrame({"timeframe": DAYS[:3], "rf": [0.001, 0.002, 0.003]})
        result = run_backtest(
            scripted({}), market({"A": flat(1.0, 3)}), FREE, risk_free=rf
        )
        assert result.equity["risk_free"].to_list() == [0.001, 0.002, 0.003]
