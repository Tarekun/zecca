import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))
from analysis.framework import (
    BacktestConfig,
    InvalidTargetWeights,
    MarketView,
    NoCosts,
    Panel,
    PortfolioState,
    Strategy,
    TargetWeights,
    build_signals,
    diagnose,
    run_backtest,
)
from analysis.strategies.momentum import TopNMomentum


def business_days(start: date, n: int) -> list[date]:
    days, d = [], start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


DAYS = business_days(date(2020, 1, 1), 300)


def market(
    last_day: dict[str, int] | None = None, growth: dict[str, float] | None = None
):
    """Symbols A..D with constant growth; `last_day[s]` ends a symbol early."""
    growth = growth or {"A": 0.001, "B": 0.002, "C": 0.003, "D": 0.004}
    last_day = last_day or {}
    rows = []
    for s, g in growth.items():
        for i, d in enumerate(DAYS[: last_day.get(s, len(DAYS) - 1) + 1]):
            price = 100 * (1 + g) ** i
            rows.append({"timeframe": d, "symbol": s, "open": price, "close": price})
    return pl.DataFrame(rows)


class TestUniverse:
    def test_without_column_every_symbol_trading_today(self):
        view = MarketView(Panel(market()), DAYS[10])
        assert view.universe() == ["A", "B", "C", "D"]

    def test_in_universe_column(self):
        data = market().with_columns(
            pl.when(pl.col("symbol") == "B")
            .then(None)  # unknown membership counts as out
            .otherwise(pl.col("symbol") != "D")
            .alias("in_universe")
        )
        view = MarketView(Panel(data), DAYS[10])
        assert view.universe() == ["A", "C"]

    def test_membership_changes_over_time(self):
        # D joins the universe on day 100
        data = market().with_columns(
            ((pl.col("symbol") != "D") | (pl.col("timeframe") >= DAYS[100])).alias(
                "in_universe"
            )
        )
        panel = Panel(data)
        assert "D" not in MarketView(panel, DAYS[99]).universe()
        assert "D" in MarketView(panel, DAYS[100]).universe()

    def test_engine_rejects_targets_outside_the_universe(self):
        class BuyD(Strategy):
            def decide(self, market, portfolio):
                return TargetWeights({"D": 1.0})

        data = market().with_columns((pl.col("symbol") != "D").alias("in_universe"))
        with pytest.raises(InvalidTargetWeights, match="universe"):
            run_backtest(BuyD(), data, BacktestConfig(costs=NoCosts()))

    def test_momentum_ranks_only_universe_members(self):
        # D has the strongest momentum but is not investable
        data = market().with_columns((pl.col("symbol") != "D").alias("in_universe"))
        s = TopNMomentum(months_lookback=3, weeks_ignore=0, top_n=10)
        panel = Panel(data)
        day = next(
            d
            for d in DAYS[100:]
            if MarketView(panel, d).is_first_trading_day_of("month")
        )
        view = MarketView(panel, day, build_signals(s, data))
        weights = s.decide(view, PortfolioState.empty(day, 1.0))
        assert set(weights) == {"A", "B", "C"}
        # and a full backtest runs without tripping the universe validation
        result = run_backtest(s, data, BacktestConfig(costs=NoCosts()))
        assert "D" not in result.trades["symbol"].to_list()


class TestDiagnose:
    def test_survivors_only_dataset_is_flagged(self):
        d = diagnose(market())
        assert d.symbols == 4 and d.symbols_leaving == 0
        assert d.survivors_only
        assert "survivorship bias" in d.warnings()[0]
        assert d.universe_member_days_without_price is None

    def test_symbols_leaving_are_counted(self):
        d = diagnose(market(last_day={"A": 150, "B": 290}))
        # A stops 149 trading days before the end, B only 9: B is a gap, not a delisting
        assert d.symbols_leaving == 1
        assert not d.survivors_only
        assert d.leaving_per_year == pytest.approx(1 / 4 / (300 / 252))
        assert "1 of 4 symbols stop appearing" in d.warnings()[0]

    def test_universe_members_without_price(self):
        data = market().with_columns(
            pl.lit(True).alias("in_universe"),
            pl.when((pl.col("symbol") == "C") & (pl.col("timeframe") >= DAYS[290]))
            .then(None)
            .otherwise(pl.col("close"))
            .alias("close"),
        )
        d = diagnose(data)
        assert d.universe_member_days_without_price == 10
        assert any("could not be traded" in w for w in d.warnings())

    def test_to_dict_is_json_ready(self):
        out = diagnose(market()).to_dict()
        assert out["first_day"] == DAYS[0].isoformat()
        assert out["survivors_only"] is True and out["warnings"]
