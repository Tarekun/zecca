import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))
from etl.transformation.utils import load_ticker_daily

_RAW_ROOT = Path(__file__).parents[3] / "dataplatform" / "raw"
_TEST_OUTPUTS = Path(__file__).parents[3] / "dataplatform" / "test_outputs"

_PRICE_COLS = ["open", "close", "high", "low"]


def test_symbol_appears_on_all_dates_after_first():
    """Once a symbol appears it must be present on every subsequent date in the dataset.

    Builds the full expected (ticker, date) universe — all dates >= each symbol's first
    appearance — then asserts no pair is missing from the actual data.
    """
    df = load_ticker_daily(_RAW_ROOT)

    all_dates = df.select("date").unique()

    first_dates = df.group_by("ticker").agg(pl.col("date").min().alias("first_date"))

    expected = (
        first_dates.join(all_dates, how="cross")
        .filter(pl.col("date") >= pl.col("first_date"))
        .select(["ticker", "date"])
    )

    actual = df.select(["ticker", "date"]).unique()

    missing = expected.join(actual, on=["ticker", "date"], how="anti")

    if missing.height > 0:
        counts = (
            missing.group_by("ticker")
            .agg(pl.len().alias("missing_dates"))
            .sort("missing_dates", descending=True)
        )

        _TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        counts.write_csv(_TEST_OUTPUTS / "symbol_continuity_gaps.csv")

        affected = counts["ticker"].to_list()
        pytest.fail(
            f"{missing.height} (ticker, date) pairs are missing after the symbol's first appearance.\n"
            f"Affected symbols ({len(affected)}): {affected}\n"
            f"Gap counts written to dataplatform/test_outputs/symbol_continuity_gaps.csv\n"
            f"Sample (up to 20):\n{missing.sort(['ticker', 'date']).head(20)}"
        )


def test_no_negative_prices():
    """No row in ticker_daily should have a negative value for any price column.

    Writes a CSV of every offending row (ticker, date, open, close, high, low)
    sorted by ticker then date so gaps are easy to inspect.
    """
    df = load_ticker_daily(_RAW_ROOT)

    negative_mask = pl.lit(False)
    for col in _PRICE_COLS:
        negative_mask = negative_mask | (pl.col(col) < 0)

    offending = (
        df.filter(negative_mask)
        .select(["ticker", "date"] + _PRICE_COLS)
        .sort(["ticker", "date"])
    )

    if offending.height > 0:
        _TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        offending.write_csv(_TEST_OUTPUTS / "negative_prices.csv")

        affected = offending.select("ticker").unique().sort("ticker")["ticker"].to_list()
        pytest.fail(
            f"Found {offending.height} row(s) with negative prices across {len(affected)} symbol(s): {affected}\n"
            f"Full details written to dataplatform/test_outputs/negative_prices.csv\n"
            f"Sample (up to 20):\n{offending.head(20)}"
        )
