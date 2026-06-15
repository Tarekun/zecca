import math
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[2]))
from etl.transformation.indicators import (
    relative_strength_index,
    rolling_avg,
    log_return,
    arithmatic_return,
    sharpe_ratio,
    volatility,
)

_DATA_ROOT = Path(__file__).parents[2] / "dataplatform" / "raw" / "ticker_daily"
_TICKERS = ["AAPL", "MSFT"]


@pytest.fixture(scope="module")
def sample_df() -> pl.DataFrame:
    """Two tickers from 2025 and 2026, sorted by (symbol, timeframe).

    Covers ~378 trading days per ticker — enough for every lookback period up to 252.
    Uses scan_parquet with extra_columns='ignore' to tolerate the legacy
    ``__index_level_0__`` column present in some monthly files.
    """
    df = pl.concat(
        [
            pl.scan_parquet(
                str(_DATA_ROOT / "year=2025" / "month=*" / "*.parquet"),
                extra_columns="ignore",
            )
            .filter(pl.col("ticker").is_in(_TICKERS))
            .select(["date", "ticker", "open", "close", "high", "low", "volume"])
            .collect(),
            pl.scan_parquet(
                str(_DATA_ROOT / "year=2026" / "month=*" / "*.parquet"),
                extra_columns="ignore",
            )
            .filter(pl.col("ticker").is_in(_TICKERS))
            .select(["date", "ticker", "open", "close", "high", "low", "volume"])
            .collect(),
        ]
    )
    return (
        df.rename({"ticker": "symbol"})
        .with_columns(pl.col("date").cast(pl.Date).alias("timeframe"))
        .drop("date")
        .sort(["symbol", "timeframe"])
        .select(["timeframe", "symbol", "open", "close", "high", "low", "volume"])
    )


class TestSafeLogReturn:
    def test_first_row_null(self, sample_df):
        """First row per symbol must be null: no prior price exists to compute a return."""
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        result = aapl.with_columns(
            log_return(pl.col("close"), pl.col("close").shift(1)).alias("lr")
        )
        assert result["lr"][0] is None, (
            "log_return at the first row must be null because shift(1) produces null "
            "and the formula returns null when either input is non-positive or missing"
        )

    def test_matches_formula(self, sample_df):
        """log return at row 1 must equal ln(close[1] / close[0])."""
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        result = aapl.with_columns(
            log_return(pl.col("close"), pl.col("close").shift(1)).alias("lr")
        )
        c0, c1 = aapl["close"][0], aapl["close"][1]
        expected = math.log(c1 / c0)
        actual = result["lr"][1]
        assert actual == pytest.approx(expected, rel=1e-9), (
            f"log_return at row 1: expected ln({c1:.4f}/{c0:.4f}) = {expected:.8f}, "
            f"got {actual:.8f}"
        )

    def test_null_for_nonpositive(self):
        """Result must be null when entry_value or current_value is zero or negative."""
        df = pl.DataFrame({"a": [10.0, 0.0, -5.0, 10.0], "b": [10.0, 10.0, 10.0, 0.0]})
        result = df.with_columns(log_return(pl.col("a"), pl.col("b")).alias("lr"))
        assert result["lr"][0] == pytest.approx(0.0), "ln(10/10) should be 0.0"
        assert result["lr"][1] is None, "entry_value = 0 should yield null"
        assert result["lr"][2] is None, "entry_value < 0 should yield null"
        assert result["lr"][3] is None, "current_value = 0 should yield null"


class TestSafeReturn:
    def test_first_row_null(self, sample_df):
        """First row per symbol must be null: shift(1) yields null, so initial = null != 0 check fails."""
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        result = aapl.with_columns(
            arithmatic_return(pl.col("close"), pl.col("close").shift(1)).alias("ret")
        )
        assert result["ret"][0] is None, (
            "arithmatic_return at the first row must be null because the lagged close is null "
            "and the formula propagates null through the arithmetic"
        )

    def test_matches_formula(self, sample_df):
        """Percentage return at row 1 must equal (close[1] - close[0]) / close[0]."""
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        result = aapl.with_columns(
            arithmatic_return(pl.col("close"), pl.col("close").shift(1)).alias("ret")
        )
        c0, c1 = aapl["close"][0], aapl["close"][1]
        expected = (c1 - c0) / c0
        actual = result["ret"][1]
        assert actual == pytest.approx(expected, rel=1e-9), (
            f"arithmatic_return at row 1: expected ({c1:.4f} - {c0:.4f}) / {c0:.4f} = {expected:.8f}, "
            f"got {actual:.8f}"
        )

    def test_null_for_zero_initial(self):
        """Result must be null when the initial price is zero (avoids division by zero)."""
        df = pl.DataFrame({"curr": [105.0, 100.0], "init": [100.0, 0.0]})
        result = df.with_columns(
            arithmatic_return(pl.col("curr"), pl.col("init")).alias("ret")
        )
        assert result["ret"][0] == pytest.approx(
            0.05
        ), "(105 - 100) / 100 should be 0.05"
        assert (
            result["ret"][1] is None
        ), "arithmatic_return with initial = 0 must yield null to guard against division by zero"


class TestSharpeRatio:
    def test_matches_formula(self):
        """sharpe_ratio returns ret / vol for normal non-zero inputs."""
        df = pl.DataFrame({"ret": [0.1, -0.05, 0.0], "vol": [0.2, 0.25, 0.1]})
        result = df.with_columns(sharpe_ratio(pl.col("ret"), pl.col("vol")).alias("sr"))
        assert result["sr"][0] == pytest.approx(0.5), "0.1 / 0.2 should be 0.5"
        assert result["sr"][1] == pytest.approx(-0.2), "-0.05 / 0.25 should be -0.2"
        assert result["sr"][2] == pytest.approx(0.0), "0.0 / 0.1 should be 0.0"

    def test_null_for_zero_vol(self):
        """sharpe_ratio must return null when volatility is zero."""
        df = pl.DataFrame({"ret": [0.1, 0.05], "vol": [0.2, 0.0]})
        result = df.with_columns(sharpe_ratio(pl.col("ret"), pl.col("vol")).alias("sr"))
        assert result["sr"][0] == pytest.approx(0.5), "0.1 / 0.2 should be 0.5"
        assert result["sr"][1] is None, "sharpe_ratio with vol = 0 must yield null"

    def test_null_for_null_vol(self):
        """sharpe_ratio must return null when volatility is null (e.g. 1-step lookback)."""
        df = pl.DataFrame({"ret": [0.1, 0.05], "vol": [0.2, None]})
        result = df.with_columns(sharpe_ratio(pl.col("ret"), pl.col("vol")).alias("sr"))
        assert result["sr"][0] == pytest.approx(0.5), "0.1 / 0.2 should be 0.5"
        assert result["sr"][1] is None, "sharpe_ratio with vol = null must yield null"


class TestRollingAvg:
    def test_first_row_equals_value(self, sample_df):
        """With min_periods=1 the first row must equal the value itself."""
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        result = aapl.with_columns(rolling_avg(pl.col("open"), 5).alias("ra"))
        assert result["ra"][0] == pytest.approx(aapl["open"][0]), (
            "rolling_avg at the first row (min_periods=1) should equal open[0] because "
            "there are no preceding rows to average over"
        )

    def test_no_nulls(self, sample_df):
        """Rolling average must never produce nulls since min_periods=1."""
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        result = aapl.with_columns(rolling_avg(pl.col("open"), 5).alias("ra"))
        null_count = result["ra"].null_count()
        assert null_count == 0, (
            f"rolling_avg with min_periods=1 should never produce nulls, "
            f"but found {null_count} null value(s)"
        )

    def test_correct_value(self, sample_df):
        """At the first full-window row (index = window - 1), the average must match the manual mean."""
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        window = 4
        result = aapl.with_columns(rolling_avg(pl.col("open"), window).alias("ra"))
        expected = aapl["open"][:4].mean()
        actual = result["ra"][3]
        assert actual == pytest.approx(expected, rel=1e-9), (
            f"rolling_avg at row 3 with window={window}: "
            f"expected mean(open[0:4]) = {expected:.6f}, got {actual:.6f}"
        )


class TestVolatility:
    def test_first_two_rows_null(self, sample_df):
        """Rows 0 and 1 of volatility must be null.

        Row 0: window=[ret[0]] has only 1 row which is null (no prior price).
        Row 1: window=[ret[0], ret[1]] = [null, val] has only 1 non-null value, below min_samples=2.
        """
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        ret = aapl.with_columns(
            log_return(pl.col("close"), pl.col("close").shift(1)).alias("ret")
        )
        result = ret.with_columns(volatility(pl.col("ret"), 2).alias("vol"))
        assert result["vol"][0] is None, (
            "volatility at row 0 must be null: window has only 1 row which is itself null "
            "(no prior price for log return), so min_samples=2 is not satisfied"
        )
        assert result["vol"][1] is None, (
            "volatility at row 1 must be null: window is [null, ret[1]], which provides "
            "only 1 non-null value — below the min_samples=2 threshold"
        )

    def test_second_full_window(self, sample_df):
        """Volatility at row 2 (first window with 2 non-null returns) equals the sample std dev."""
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        ret = aapl.with_columns(
            log_return(pl.col("close"), pl.col("close").shift(1)).alias("ret")
        )
        result = ret.with_columns(volatility(pl.col("ret"), 2).alias("vol"))
        r1, r2 = ret["ret"][1], ret["ret"][2]
        expected = abs(r2 - r1) / math.sqrt(2)
        actual = result["vol"][2]
        assert actual == pytest.approx(expected, rel=1e-9), (
            f"volatility at row 2 with window=1: expected |{r2:.6f} - {r1:.6f}| / sqrt(2) "
            f"= {expected:.8f}, got {actual:.8f}"
        )

    def test_non_negative(self, sample_df):
        """All non-null volatility values must be non-negative (it is a std dev)."""
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        ret = aapl.with_columns(
            log_return(pl.col("close"), pl.col("close").shift(1)).alias("ret")
        )
        result = ret.with_columns(volatility(pl.col("ret"), 20).alias("vol"))
        non_null = result.filter(pl.col("vol").is_not_null())["vol"]
        min_vol = non_null.min()
        assert min_vol >= 0, (
            f"volatility is a standard deviation and must be non-negative; "
            f"found minimum value {min_vol}"
        )


class TestRelativeStrengthIndex:
    def test_first_row_is_nan(self, sample_df):
        """RSI at the first row is NaN.

        The first price_diff is null (no prior close), so the RSI formula evaluates
        avg_gain = avg_loss = 0.0, yielding 0.0 / 0.0 = NaN.
        """
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        result = aapl.with_columns(
            (pl.col("close") - pl.col("close").shift(1)).alias("price_diff")
        ).with_columns(relative_strength_index(pl.col("price_diff"), 14).alias("rsi"))
        first_rsi = result["rsi"][0]
        assert first_rsi is not None and math.isnan(first_rsi), (
            f"RSI at the first row should be NaN (avg_gain = avg_loss = 0 → 0/0), "
            f"got {first_rsi!r}"
        )

    def test_range(self, sample_df):
        """All finite RSI values must lie within [0, 100]."""
        aapl = sample_df.filter(pl.col("symbol") == "AAPL").sort("timeframe")
        result = aapl.with_columns(
            (pl.col("close") - pl.col("close").shift(1)).alias("price_diff")
        ).with_columns(relative_strength_index(pl.col("price_diff"), 14).alias("rsi"))
        finite_rsi = result.filter(~pl.col("rsi").is_nan())["rsi"]
        assert (
            finite_rsi >= 0
        ).all(), (
            f"RSI must be >= 0 for all finite values; min found: {finite_rsi.min():.4f}"
        )
        assert (
            finite_rsi <= 100
        ).all(), f"RSI must be <= 100 for all finite values; max found: {finite_rsi.max():.4f}"

    def test_all_gains_is_100(self):
        """RSI must be exactly 100 when every price change in the window is positive."""
        closes = [float(100 + i) for i in range(20)]
        df = (
            pl.DataFrame({"close": closes})
            .with_columns(
                (pl.col("close") - pl.col("close").shift(1)).alias("price_diff")
            )
            .with_columns(
                relative_strength_index(pl.col("price_diff"), 14).alias("rsi")
            )
        )
        for i, val in enumerate(df["rsi"].tail(5).to_list()):
            assert val == pytest.approx(100.0), (
                f"RSI should be 100 when all price changes are gains "
                f"(avg_loss = 0 → RS = inf → RSI = 100); tail row {i} got {val}"
            )

    def test_all_losses_is_zero(self):
        """RSI must be exactly 0 when every price change in the window is negative."""
        closes = [float(200 - i) for i in range(20)]
        df = (
            pl.DataFrame({"close": closes})
            .with_columns(
                (pl.col("close") - pl.col("close").shift(1)).alias("price_diff")
            )
            .with_columns(
                relative_strength_index(pl.col("price_diff"), 14).alias("rsi")
            )
        )
        for i, val in enumerate(df["rsi"].tail(5).to_list()):
            assert val == pytest.approx(0.0), (
                f"RSI should be 0 when all price changes are losses "
                f"(avg_gain = 0 → RS = 0 → RSI = 0); tail row {i} got {val}"
            )
