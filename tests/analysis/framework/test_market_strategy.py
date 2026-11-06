import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))
from analysis.framework import (
    InvalidParams,
    MarketView,
    Panel,
    Param,
    ParamSpace,
    PortfolioState,
    Strategy,
    TargetWeights,
    build_signals,
)


def trading_days(start: date, n: int) -> list[date]:
    days, d = [], start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


DAYS = trading_days(date(2024, 1, 1), 60)  # jan 1st 2024 is a monday


def make_market(days: list[date] = DAYS) -> pl.DataFrame:
    """Three symbols with distinct deterministic trends; "C" stops trading
    halfway (a delisting) to exercise symbols disappearing from the data."""
    rows = []
    for i, d in enumerate(days):
        rows.append({"timeframe": d, "symbol": "A", "close": 100.0 + i})
        rows.append({"timeframe": d, "symbol": "B", "close": 100.0 + 2 * i})
        if i < len(days) // 2:
            rows.append({"timeframe": d, "symbol": "C", "close": 100.0 + 3 * i})
    # shuffle row order: Panel must not rely on input ordering
    return pl.DataFrame(rows).sample(fraction=1.0, shuffle=True, seed=0)


@pytest.fixture
def panel() -> Panel:
    return Panel(make_market())


class TestPanel:
    def test_requires_key_columns(self):
        with pytest.raises(ValueError, match="missing required column"):
            Panel(pl.DataFrame({"timeframe": [DAYS[0]], "close": [1.0]}))

    def test_rejects_duplicates(self):
        df = pl.DataFrame(
            {"timeframe": [DAYS[0]] * 2, "symbol": ["A", "A"], "close": [1.0, 2.0]}
        )
        with pytest.raises(ValueError, match="duplicate"):
            Panel(df)

    def test_rejects_null_keys(self):
        df = pl.DataFrame({"timeframe": [DAYS[0], None], "symbol": ["A", "B"]})
        with pytest.raises(ValueError, match="nulls"):
            Panel(df)

    def test_datetime_is_truncated_to_date(self):
        df = pl.DataFrame({"timeframe": [datetime(2024, 1, 2, 16)], "symbol": ["A"]})
        assert Panel(df).dates == [date(2024, 1, 2)]

    def test_between(self, panel):
        rows = panel.between(DAYS[2], DAYS[4])
        assert rows["timeframe"].unique().sort().to_list() == DAYS[2:5]
        assert rows.height == 9
        assert panel.between(None, DAYS[0]).height == 3
        assert panel.between(DAYS[5], DAYS[4]).height == 0
        # dates outside the calendar snap to the trading days in range
        assert panel.between(date(2024, 1, 6), date(2024, 1, 7)).height == 0


class TestMarketView:
    def test_never_returns_future_rows(self, panel):
        for day in DAYS:
            view = MarketView(panel, day, signals=panel)
            assert view.history()["timeframe"].max() == day
            assert view.signals(lookback=None)["timeframe"].max() == day
            assert view.trading_days()[-1] == day

    def test_rejects_non_trading_days(self, panel):
        with pytest.raises(ValueError, match="not a trading day"):
            MarketView(panel, date(2024, 1, 6), None)  # saturday
        with pytest.raises(ValueError, match="not a trading day"):
            MarketView(panel, DAYS[-1] + timedelta(days=1), None)

    def test_lookback_counts_trading_days(self, panel):
        view = MarketView(panel, DAYS[10])
        assert (
            view.history(lookback=5)["timeframe"].unique().sort().to_list()
            == DAYS[6:11]
        )
        assert view.trading_days(lookback=5) == DAYS[6:11]
        assert view.trading_days(lookback=100) == DAYS[:11]
        assert view.today()["timeframe"].unique().to_list() == [DAYS[10]]
        for bad in (0, -1, 1.5, True):
            with pytest.raises(ValueError):
                view.history(lookback=bad)

    def test_column_selection_keeps_keys(self, panel):
        view = MarketView(panel, DAYS[3])
        assert view.today(["close"]).columns == ["timeframe", "symbol", "close"]
        assert view.today("close").columns == ["timeframe", "symbol", "close"]
        with pytest.raises(KeyError):
            view.today(["volume"])

    def test_signals_require_prepare_output(self, panel):
        with pytest.raises(RuntimeError, match="prepare"):
            MarketView(panel, DAYS[3]).signals()

    def test_universe_follows_listed_symbols(self, panel):
        assert MarketView(panel, DAYS[0]).universe() == ["A", "B", "C"]
        assert MarketView(panel, DAYS[-1]).universe() == ["A", "B"]

    def test_is_first_trading_day_of(self, panel):
        first_feb = next(d for d in DAYS if d.month == 2)
        assert MarketView(panel, DAYS[0]).is_first_trading_day_of("month")
        assert MarketView(panel, first_feb).is_first_trading_day_of("month")
        assert not MarketView(panel, DAYS[1]).is_first_trading_day_of("month")
        mondays = [d for d in DAYS if d.weekday() == 0]
        assert all(
            MarketView(panel, d).is_first_trading_day_of("week") for d in mondays
        )


class TopNMomentum(Strategy):
    """The example strategy from the package docstring."""

    param_space = ParamSpace(
        lookback=Param(default=5, values=[5, 10]),
        top_n=Param(default=1, low=1, high=2, step=1),
    )

    def prepare(self, data):
        return data.select(
            "timeframe",
            "symbol",
            pl.col("close").pct_change(self.lookback).over("symbol").alias("momentum"),
        )

    def decide(self, market, portfolio):
        if not market.is_first_trading_day_of("week"):
            return None
        top = (
            market.signals(["momentum"])
            .drop_nulls("momentum")
            .top_k(self.top_n, by="momentum")["symbol"]
        )
        return TargetWeights.equal(top)


def run_decisions(
    strategy: Strategy, data: pl.DataFrame
) -> dict[date, TargetWeights | None]:
    """Minimal daily loop (no execution): what the engine of the backtest
    issue will do, minus fills and costs."""
    market = Panel(data)
    signals = build_signals(strategy, data)
    portfolio = PortfolioState.empty(market.dates[0], 1000.0)
    return {
        d: strategy.decide(MarketView(market, d, signals), portfolio)
        for d in market.dates
    }


class TestStrategy:
    def test_params_resolve_to_attributes(self):
        s = TopNMomentum(top_n=2)
        assert s.top_n == 2 and s.lookback == 5
        assert dict(s.params) == {"lookback": 5, "top_n": 2}
        assert repr(s) == "TopNMomentum(lookback=5, top_n=2)"
        assert TopNMomentum.from_params({"lookback": 10}).lookback == 10

    def test_params_are_validated(self):
        with pytest.raises(InvalidParams):
            TopNMomentum(top_n=3)
        with pytest.raises(InvalidParams):
            TopNMomentum(unknown=1)

    def test_params_cannot_shadow_attributes(self):
        class Bad(Strategy):
            param_space = ParamSpace(decide=Param(default=1, values=[1]))

            def decide(self, market, portfolio):  # type: ignore[override]
                return None

        with pytest.raises(InvalidParams, match="clashes"):
            Bad()

        class AlsoBad(Bad):
            param_space = ParamSpace(params=Param(default=1, values=[1]))

        with pytest.raises(InvalidParams, match="clashes"):
            AlsoBad()

    def test_decide_is_abstract(self):
        with pytest.raises(TypeError):
            Strategy()  # type: ignore[abstract]

    def test_default_prepare_has_no_signals(self):
        class Hold(Strategy):
            def decide(self, market, portfolio):
                return TargetWeights.equal(market.universe())

        assert build_signals(Hold(), make_market()) is None

    def test_prepare_must_return_lazyframe(self):
        class Broken(TopNMomentum):
            def prepare(self, data):
                return 42

        with pytest.raises(TypeError, match="LazyFrame"):
            build_signals(Broken(), make_market())

    def test_example_strategy_end_to_end(self):
        decisions = run_decisions(TopNMomentum(top_n=1), make_market())
        mondays = [d for d in DAYS if d.weekday() == 0]
        assert {d for d, w in decisions.items() if w is not None} == set(mondays) | {
            DAYS[0]
        }
        # first day: not enough history for a 5 day return -> all cash
        assert decisions[DAYS[0]] == {}
        # afterwards the fastest growing symbol in relative terms wins until delisted
        assert decisions[mondays[1]] == {"C": 1.0}
        assert decisions[mondays[-1]] == {"B": 1.0}

    def test_decisions_do_not_depend_on_future_data(self):
        """Preview of the truncation-invariance guardrail: decisions up to day
        t are identical whether or not data after t exists."""
        data = make_market()
        full = run_decisions(TopNMomentum(), data)
        cutoff = DAYS[30]
        truncated = run_decisions(
            TopNMomentum(), data.filter(pl.col("timeframe") <= cutoff)
        )
        assert truncated == {d: w for d, w in full.items() if d <= cutoff}
