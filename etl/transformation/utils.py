from pathlib import Path

import polars as pl

from etl.logger import get_logger

logger = get_logger(__name__)

_RAW_COLS = ["date", "ticker", "open", "close", "high", "low", "volume"]


def load_ticker_daily(yfinance_data_path: str | Path) -> pl.LazyFrame:
    """Return a LazyFrame over the raw ticker_daily hive-partitioned parquet files.

    Uses ``extra_columns='ignore'`` to tolerate the legacy ``__index_level_0__``
    column written by pandas into some monthly files.

    Args:
        yfinance_data_path: Root directory of the yfinance data store.

    Returns:
        LazyFrame with columns: ``date``, ``ticker``, ``open``, ``close``,
        ``high``, ``low``, ``volume``.
    """
    data_path = Path(yfinance_data_path)
    source_glob = str(data_path / "ticker_daily" / "year=*" / "month=*" / "*.parquet")

    logger.info("Reading ticker_daily parquet from %s", source_glob)
    return (
        pl.scan_parquet(source_glob, hive_partitioning=True, extra_columns="ignore")
        .select(_RAW_COLS)
    )
