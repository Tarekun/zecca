from pathlib import Path

import polars as pl

from etl.logger import get_logger

logger = get_logger(__name__)

_RAW_COLS = ["date", "ticker", "open", "close", "high", "low", "volume"]


def load_ticker_daily(yfinance_data_path: str | Path) -> pl.LazyFrame:
    """Return a LazyFrame over the raw ticker_daily hive-partitioned parquet files.

    Uses ``extra_columns='ignore'`` to tolerate the legacy ``__index_level_0__``
    column written by pandas into some monthly files.

    Each monthly file is scanned and its ``date`` column normalized to a single
    time unit independently, then concatenated. Polars enforces a single
    ``date`` dtype when scanning multiple parquet files as one glob, but
    monthly files written by different pandas/pyarrow versions don't
    necessarily agree on datetime precision (e.g. ns vs ms); casting per-file
    before concatenating sidesteps that check instead of requiring every
    on-disk file to share the same precision.

    Args:
        yfinance_data_path: Root directory of the yfinance data store.

    Returns:
        LazyFrame with columns: ``date``, ``ticker``, ``open``, ``close``,
        ``high``, ``low``, ``volume``.
    """
    data_path = Path(yfinance_data_path)
    source_dir = data_path / "ticker_daily"
    files = sorted(str(p) for p in source_dir.glob("year=*/month=*/*.parquet"))

    logger.info("Reading %d ticker_daily parquet file(s) from %s", len(files), source_dir)
    return pl.concat(
        [
            pl.scan_parquet(f, extra_columns="ignore")
            .select(_RAW_COLS)
            .with_columns(pl.col("date").cast(pl.Datetime("us")))
            for f in files
        ],
        how="vertical",
    )
