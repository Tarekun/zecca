import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[2]))
from etl.transformation.candles_daily import compute_candles_daily

# Root of the yfinance data store — parent of ticker_daily/
_RAW_ROOT = Path(__file__).parents[2] / "dataplatform" / "raw"


def test_compute_candles_daily_nonempty():
    """Integration smoke test: compute_candles_daily must return a non-empty DataFrame.

    Loads the full ticker_daily dataset from disk.  This is intentionally an
    in-memory load of the complete dataset; it is expected to be slow.

    Known file-level issues handled transparently by read_ticker_daily:

    - Some monthly parquet files (e.g. year=2025/month=10/) were written by pandas
      and carry a spurious ``__index_level_0__`` column absent from the rest of the
      dataset.  ``scan_parquet(..., extra_columns='ignore')`` silently drops it.

    Known schema-level limitations surfaced by this test:

    - ``volatility_1_steps_1d`` and ``sharpe_1_steps_1d`` are always null.
      The 1-day lookback uses ``window_size=1``, which can never satisfy
      ``min_samples=2``, so the sample standard deviation (and the Sharpe ratio
      derived from it) is undefined for a single observation.
    """
    result = compute_candles_daily(_RAW_ROOT)

    assert result.height > 0, (
        "compute_candles_daily returned an empty DataFrame — "
        "verify that parquet files exist under dataplatform/raw/ticker_daily/"
    )
    assert result.width > 0, (
        "compute_candles_daily returned a DataFrame with no columns"
    )
