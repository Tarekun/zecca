import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parents[3]))
from analysis.framework import InvalidParams, Param, ParamSpace


class TestParam:
    def test_values_domain(self):
        p = Param(default="monthly", values=["weekly", "monthly"])
        assert p.grid() == ["weekly", "monthly"]
        assert p.accepts("weekly")
        assert not p.accepts("daily")

    def test_int_range_grid_is_inclusive_and_integral(self):
        p = Param(default=12, low=3, high=12, step=3)
        assert p.grid() == [3, 6, 9, 12]
        assert all(isinstance(v, int) for v in p.grid())

    def test_float_range_grid(self):
        p = Param(default=0.5, low=0.0, high=1.0, step=0.25)
        assert p.grid() == [0.0, 0.25, 0.5, 0.75, 1.0]

    def test_range_accepts_off_grid_values_inside_bounds(self):
        p = Param(default=12, low=3, high=12, step=3)
        assert p.accepts(7)
        assert not p.accepts(13)
        assert not p.accepts("7")
        assert not p.accepts(True)
        assert not p.accepts(float("nan"))

    @pytest.mark.parametrize(
        "kwargs",
        [
            dict(default=1),  # no domain
            dict(default=1, values=[1], low=0, high=2, step=1),  # both domains
            dict(default=1, values=[]),
            dict(default=1, low=0, high=2),  # missing step
            dict(default=1, low=2, high=0, step=1),
            dict(default=1, low=0, high=2, step=0),
            dict(default=5, low=0, high=2, step=1),  # default outside domain
            dict(default="x", values=["a", "b"]),
        ],
    )
    def test_invalid_declarations(self, kwargs):
        with pytest.raises(InvalidParams):
            Param(**kwargs)


class TestParamSpace:
    space = ParamSpace(
        lookback=Param(default=12, low=3, high=12, step=3),
        rebalance=Param(default="monthly", values=["weekly", "monthly"]),
    )

    def test_defaults(self):
        assert self.space.defaults() == {"lookback": 12, "rebalance": "monthly"}

    def test_resolve_fills_defaults(self):
        assert self.space.resolve({"lookback": 6}) == {
            "lookback": 6,
            "rebalance": "monthly",
        }

    def test_resolve_rejects_unknown_names(self):
        with pytest.raises(InvalidParams, match="Unknown parameter"):
            self.space.resolve({"top_n": 10})

    def test_resolve_rejects_out_of_domain_values(self):
        with pytest.raises(InvalidParams, match="outside its domain"):
            self.space.resolve({"rebalance": "daily"})

    def test_grid_is_cartesian_product(self):
        grid = list(self.space.grid())
        assert len(grid) == self.space.grid_size() == 8
        assert {"lookback": 3, "rebalance": "weekly"} in grid
        assert len({tuple(sorted(g.items())) for g in grid}) == 8

    def test_empty_space_has_a_single_empty_combination(self):
        assert list(ParamSpace().grid()) == [{}]

    def test_rejects_non_param_values_and_private_names(self):
        with pytest.raises(InvalidParams):
            ParamSpace(x=3)  # type: ignore[arg-type]
        with pytest.raises(InvalidParams):
            ParamSpace(_x=Param(default=1, values=[1]))
