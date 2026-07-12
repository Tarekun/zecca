import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[2]))
from etl.transformation.utils import load_ticker_daily

# Root of the yfinance data store — parent of ticker_daily/
_RAW_ROOT = Path(__file__).parents[2] / "dataplatform" / "raw"


def test_compute_candles_daily_nonempty():
    result = load_ticker_daily(_RAW_ROOT).collect()

    assert result.height > 0, (
        "compute_candles_daily returned an empty DataFrame — "
        "verify that parquet files exist under dataplatform/raw/ticker_daily/"
    )
    assert (
        result.width > 0
    ), "compute_candles_daily returned a DataFrame with no columns"
