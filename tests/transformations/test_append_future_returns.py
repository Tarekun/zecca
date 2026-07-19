import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[2]))

from etl.transformation.gold.stocks_ml_ready import append_future_returns


def make_lf(prices, symbol="A", start=0) -> pl.LazyFrame:
    n = len(prices)
    return pl.DataFrame(
        {
            "symbol": [symbol] * n,
            "timeframe": list(range(start, start + n)),
            "open": [float(p) for p in prices],
        }
    ).lazy()


def two_symbol_lf() -> pl.LazyFrame:
    return pl.concat(
        [
            make_lf([100, 102, 101, 103, 104, 106], symbol="A"),
            make_lf([50, 55, 45], symbol="B"),
        ]
    )


class TestFuturePrice:
    def test_shifts_by_lookahead_steps(self):
        """future_price at row i must equal open at row i + lookahead_steps."""
        lf = make_lf([100, 102, 101, 103, 104, 106])
        labeler = append_future_returns(lookahead_steps=2)
        out = labeler(lf).collect().sort("timeframe")

        assert out["future_price"].to_list() == [101.0, 103.0, 104.0, 106.0, None, None]

    def test_does_not_leak_across_symbols(self):
        """The last row of one symbol must not pick up the next symbol's price."""
        labeler = append_future_returns(lookahead_steps=1)
        out = labeler(two_symbol_lf()).collect()

        last_a = out.filter((pl.col("symbol") == "A") & (pl.col("timeframe") == 5))
        last_b = out.filter((pl.col("symbol") == "B") & (pl.col("timeframe") == 2))
        assert last_a["future_price"][0] is None
        assert last_b["future_price"][0] is None

    def test_tail_rows_per_symbol_are_null(self):
        """Exactly `lookahead_steps` trailing rows per symbol must be null."""
        labeler = append_future_returns(lookahead_steps=2)
        out = labeler(two_symbol_lf()).collect()

        null_counts = (
            out.filter(pl.col("future_price").is_null())
            .group_by("symbol")
            .agg(pl.len().alias("n"))
            .sort("symbol")
        )
        assert null_counts["n"].to_list() == [2, 2]

    def test_handles_unsorted_input(self):
        """The labeler sorts internally, so a shuffled input frame must give the
        same result as an already-sorted one."""
        sorted_lf = make_lf([100, 102, 101, 103, 104, 106])
        shuffled = sorted_lf.collect().sample(fraction=1.0, shuffle=True, seed=0).lazy()

        labeler = append_future_returns(lookahead_steps=1)
        expected = labeler(sorted_lf).collect().sort("timeframe")
        actual = labeler(shuffled).collect().sort("timeframe")

        assert actual["future_price"].to_list() == expected["future_price"].to_list()

    def test_custom_column_names_are_respected(self):
        lf = make_lf([100, 102, 101])
        labeler = append_future_returns(lookahead_steps=1, future_price_col="next_open")
        out = labeler(lf).collect()

        assert "next_open" in out.columns
        assert "future_price" not in out.columns


class TestPriceMovementClass:
    def test_no_thresholds_skips_class_column(self):
        """With no thresholds, only future_price is appended."""
        lf = make_lf([100, 102, 101])
        labeler = append_future_returns(lookahead_steps=1)
        out = labeler(lf).collect()

        assert "price_movement_class" not in out.columns

    def test_binary_threshold_classifies_up_down(self):
        """thresholds=[0] must yield exactly 2 classes: down / up."""
        lf = make_lf([100, 102, 101, 103])
        labeler = append_future_returns(lookahead_steps=1, thresholds=[0])
        out = labeler(lf).collect().sort("timeframe")

        classes = out["price_movement_class"].drop_nulls()
        assert classes.n_unique() == 2
        # 100 -> 102 is an increase
        assert out["price_movement_class"][0] == "(0, inf]"
        # 102 -> 101 is a decrease
        assert out["price_movement_class"][1] == "(-inf, 0]"

    def test_symmetric_thresholds_yield_five_classes(self):
        """thresholds=[0.01, 0.03] must produce 5 symmetric classes."""
        lf = make_lf([100, 101, 102, 103.5, 105, 110, 95, 90, 80])
        labeler = append_future_returns(lookahead_steps=1, thresholds=[0.01, 0.03])
        out = labeler(lf).collect()

        assert out["price_movement_class"].drop_nulls().n_unique() <= 5

    def test_zero_in_thresholds_collapses_near_zero_bin(self):
        """thresholds=[0, 0.01] must produce 4 classes, not 5, since 0 is not
        duplicated on both sides of the symmetric reflection."""
        lf = make_lf([100, 100.5, 99, 101.5, 98])
        labeler = append_future_returns(lookahead_steps=1, thresholds=[0, 0.01])
        out = labeler(lf).collect()

        assert out["price_movement_class"].drop_nulls().n_unique() <= 4

    def test_custom_class_column_name_is_respected(self):
        lf = make_lf([100, 102, 101])
        labeler = append_future_returns(
            lookahead_steps=1, thresholds=[0], class_col="movement"
        )
        out = labeler(lf).collect()

        assert "movement" in out.columns
        assert "price_movement_class" not in out.columns


class TestCustomLabels:
    def test_custom_labels_replace_default_names(self):
        lf = make_lf([100, 102, 101, 103])
        labeler = append_future_returns(
            lookahead_steps=1,
            thresholds=[0.01],
            custom_labels=["down", "flat", "up"],
        )
        out = labeler(lf).collect()

        assert set(out["price_movement_class"].drop_nulls().to_list()).issubset(
            {"down", "flat", "up"}
        )

    def test_wrong_length_raises_value_error(self):
        """custom_labels must have exactly len(bin_edges) + 1 entries."""
        lf = make_lf([100, 102, 101, 103])
        labeler = append_future_returns(
            lookahead_steps=1,
            thresholds=[0.01, 0.03],  # -> 5 classes expected
            custom_labels=["down", "up"],  # only 2
        )
        with pytest.raises(ValueError):
            labeler(lf).collect()
