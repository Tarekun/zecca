import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parents[3]))
from analysis.framework import (
    InvalidTargetWeights,
    PortfolioState,
    TargetWeights,
    period_key,
)

DAY = date(2024, 1, 2)


class TestTargetWeights:
    def test_is_a_read_only_mapping(self):
        w = TargetWeights({"AAPL": 0.6, "MSFT": 0.4})
        assert dict(w) == {"AAPL": 0.6, "MSFT": 0.4}
        assert w == {"AAPL": 0.6, "MSFT": 0.4}
        with pytest.raises(TypeError):
            w["AAPL"] = 1.0  # type: ignore[index]

    def test_exposure_and_cash_weight(self):
        w = TargetWeights({"AAPL": 0.3, "MSFT": 0.4})
        assert w.exposure == pytest.approx(0.7)
        assert w.cash_weight == pytest.approx(0.3)

    def test_equal(self):
        assert TargetWeights.equal(["A", "B", "C", "D"]) == {s: 0.25 for s in "ABCD"}
        assert TargetWeights.equal(["A", "B"], exposure=1.5) == {"A": 0.75, "B": 0.75}
        assert TargetWeights.equal(["A", "A", "B"]) == {"A": 0.5, "B": 0.5}
        assert TargetWeights.equal([]) == TargetWeights.cash() == {}

    @pytest.mark.parametrize(
        "weights",
        [
            {"AAPL": -0.1},
            {"AAPL": float("nan")},
            {"AAPL": float("inf")},
            {"AAPL": "0.5"},
            {"AAPL": True},
            {"": 0.5},
            {3: 0.5},
        ],
    )
    def test_rejects_invalid_weights(self, weights):
        with pytest.raises(InvalidTargetWeights):
            TargetWeights(weights)

    def test_validate_leverage(self):
        TargetWeights({"A": 0.5, "B": 0.5}).validate(max_leverage=1.0)
        TargetWeights({"A": 1.0, "B": 0.5}).validate(max_leverage=1.5)
        with pytest.raises(InvalidTargetWeights, match="exceeds max leverage"):
            TargetWeights({"A": 0.6, "B": 0.5}).validate(max_leverage=1.0)

    def test_validate_tolerates_floating_point_error(self):
        TargetWeights.equal([str(i) for i in range(10)]).validate(max_leverage=1.0)
        TargetWeights.equal([str(i) for i in range(3)]).validate(max_leverage=1.0)

    def test_validate_universe(self):
        w = TargetWeights({"A": 0.5, "B": 0.5, "C": 0.0})
        w.validate(universe=["A", "B"])  # zero weight outside universe is fine
        with pytest.raises(
            InvalidTargetWeights, match="not in the investable universe"
        ):
            w.validate(universe=["A"])


class TestPortfolioState:
    def test_empty(self):
        p = PortfolioState.empty(DAY, 1000.0)
        assert p.net_worth == 1000.0
        assert p.weights == {}
        assert p.exposure == 0.0

    def test_weights_and_net_worth(self):
        p = PortfolioState(
            DAY, cash=200.0, positions={"A": 2, "B": 4}, prices={"A": 100.0, "B": 150.0}
        )
        assert p.net_worth == pytest.approx(1000.0)
        assert p.weights == pytest.approx({"A": 0.2, "B": 0.6})
        assert p.exposure == pytest.approx(0.8)
        assert p.market_value("C") == 0.0

    def test_leveraged_portfolio_has_negative_cash(self):
        p = PortfolioState(DAY, cash=-500.0, positions={"A": 15}, prices={"A": 100.0})
        assert p.net_worth == pytest.approx(1000.0)
        assert p.exposure == pytest.approx(1.5)

    def test_requires_price_for_every_position(self):
        with pytest.raises(ValueError, match="No mark price"):
            PortfolioState(DAY, cash=0.0, positions={"A": 1}, prices={})

    def test_rejects_short_positions(self):
        with pytest.raises(ValueError, match="Negative position"):
            PortfolioState(DAY, cash=0.0, positions={"A": -1}, prices={"A": 1.0})

    def test_weights_undefined_when_wiped_out(self):
        p = PortfolioState(DAY, cash=-1000.0, positions={"A": 5}, prices={"A": 100.0})
        with pytest.raises(ValueError, match="non-positive net worth"):
            p.weights

    def test_is_immutable_and_detached_from_inputs(self):
        positions = {"A": 1.0}
        p = PortfolioState(DAY, cash=0.0, positions=positions, prices={"A": 1.0})
        positions["A"] = 99.0
        assert p.positions["A"] == 1.0
        with pytest.raises(TypeError):
            p.positions["A"] = 2.0  # type: ignore[index]
        with pytest.raises(AttributeError):
            p.cash = 5.0  # type: ignore[misc]


@pytest.mark.parametrize(
    "a, b, period, same",
    [
        (date(2024, 1, 31), date(2024, 2, 1), "month", False),
        (date(2024, 2, 1), date(2024, 2, 29), "month", True),
        (date(2024, 3, 29), date(2024, 4, 1), "quarter", False),
        (date(2024, 1, 2), date(2024, 3, 29), "quarter", True),
        (date(2024, 1, 5), date(2024, 1, 8), "week", False),  # fri -> mon
        (date(2024, 12, 31), date(2025, 1, 1), "week", True),  # same ISO week
        (date(2024, 12, 31), date(2025, 1, 1), "year", False),
    ],
)
def test_period_key(a, b, period, same):
    assert (period_key(a, period) == period_key(b, period)) is same
