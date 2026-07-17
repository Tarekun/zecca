import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.stocks_daily import StocksDailySilver

TEST_OUTPUTS = Path(__file__).parents[3] / "dataplatform" / "test_outputs"

lf = StocksDailySilver().read_from_disk()
candles_lf = CandlesDailySilver("").read_from_disk()


def test_no_null_symbol():
    """No row in stocks_daily should have a null symbol."""

    null_rows = lf.select("symbol").filter(pl.col("symbol").is_null()).collect()

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null symbol.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_all_candles_pairs_present():
    """Every (symbol, timeframe) pair from candles_daily must appear in stocks_daily.

    Missing pairs are written to
    dataplatform/test_outputs/stocks_daily_missing_pairs.csv for inspection.
    """
    candles_pairs = candles_lf.select(["symbol", "timeframe"]).unique()
    stocks_pairs = lf.select(["symbol", "timeframe"]).unique()

    missing = (
        candles_pairs.join(stocks_pairs, on=["symbol", "timeframe"], how="anti")
        .sort(["symbol", "timeframe"])
        .collect()
    )

    if missing.height > 0:
        TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        missing.write_csv(TEST_OUTPUTS / "stocks_daily_missing_pairs.csv")

        affected_symbols = missing["symbol"].drop_nulls().unique().sort().to_list()
        pytest.fail(
            f"{missing.height} (symbol, timeframe) pair(s) from candles_daily are missing in stocks_daily.\n"
            f"Affected symbols ({len(affected_symbols)}): {affected_symbols[:20]}"
            f"{'...' if len(affected_symbols) > 20 else ''}\n"
            f"Full list written to dataplatform/test_outputs/stocks_daily_missing_pairs.csv"
        )
