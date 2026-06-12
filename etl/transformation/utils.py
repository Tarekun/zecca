from pathlib import Path

import polars as pl

from etl.logger import get_logger

logger = get_logger(__name__)

_RAW_COLS = ["date", "ticker", "open", "close", "high", "low", "volume"]


def load_ticker_daily(yfinance_data_path: str | Path) -> pl.DataFrame:
    """Read raw ticker_daily hive-partitioned parquet into an eager DataFrame.

    Uses ``scan_parquet`` with ``extra_columns='ignore'`` to tolerate the legacy
    ``__index_level_0__`` column written by pandas into some monthly files.  Without
    this flag Polars infers the schema from the first file it encounters and then
    raises a ``SchemaError`` when a subsequent file contains additional columns.

    Args:
        yfinance_data_path: Root directory of the yfinance data store — the value
            that maps to the ``yfinance_data`` dbt variable in ``profiles.yml``.

    Returns:
        Eager DataFrame with columns: ``date``, ``ticker``, ``open``, ``close``,
        ``high``, ``low``, ``volume``.
    """
    data_path = Path(yfinance_data_path)
    source_glob = str(data_path / "ticker_daily" / "year=*" / "month=*" / "*.parquet")

    logger.info("Reading ticker_daily parquet from %s", source_glob)
    df = (
        pl.scan_parquet(source_glob, hive_partitioning=True, extra_columns="ignore")
        .select(_RAW_COLS)
        .collect()
    )
    logger.info(
        "Loaded raw data: %d rows × %d cols — %.1f MB",
        df.height,
        df.width,
        df.estimated_size("mb"),
    )
    return df
