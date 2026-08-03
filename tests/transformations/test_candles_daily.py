import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[2]))
from etl.ingestion.yfinance import YFinanceTicker

# Platform root — parent of raw/ticker_daily/
_DATAPLATFORM_ROOT = Path(__file__).parents[2] / "dataplatform"


def test_compute_candles_daily_nonempty():
    result = (
        YFinanceTicker("1d", dataplatform_root=str(_DATAPLATFORM_ROOT))
        .read_from_disk()
        .collect()
    )

    assert result.height > 0, (
        "compute_candles_daily returned an empty DataFrame — "
        "verify that parquet files exist under dataplatform/raw/ticker_daily/"
    )
    assert (
        result.width > 0
    ), "compute_candles_daily returned a DataFrame with no columns"
