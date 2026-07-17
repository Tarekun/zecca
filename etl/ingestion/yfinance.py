from datetime import datetime, timezone
import math
import pandas as pd
from pandas import DataFrame
from pathlib import Path
import polars as pl
import re
from time import sleep
import yfinance as yf
from analysis.db.queries import read_tickers, run_custom_query
from etl.logger import get_logger
from etl.utils import upsert_df

logger = get_logger(__name__)


def ingest_ticker_daily(base_dir: str, incremental: bool = True):
    yfincance_ticker_ingestion("1d", base_dir, incremental)


def ingest_ticker_hourly(base_dir: str, incremental: bool = True):
    yfincance_ticker_ingestion("1h", base_dir, incremental)


def yfincance_ticker_ingestion(interval: str, base_dir: str, incremental: bool = True):
    tickers = read_tickers(base_dir)
    ticker_names = tickers["ticker"].dropna().astype(str).unique().tolist()
    total = len(ticker_names)
    batch_size = 100 if incremental else 50
    num_batches = math.ceil(total / batch_size)
    table_name = "ticker_daily" if interval == "1d" else "ticker_hourly"

    # request parameters defaults
    start = "1970-01-01"
    period = None
    if incremental:
        # tbh i think reprocessing the last day every day is best
        start_date: datetime = _get_latest_partition_date(base_dir, table_name)
        # start_date = get_latest_partition_date(base_dir, table_name) + timedelta(days=1)
        if start_date is None:
            logger.warning("No existing %s data, run a full refresh first.", table_name)
            return
        # TODO review timezone handling
        if start_date.replace(tzinfo=timezone.utc) >= datetime.now(timezone.utc):
            logger.info(
                "%s: data is already up to date (won't pull %s)", table_name, start_date
            )
            return
        start = start_date.strftime("%Y-%m-%d")
        logger.info("Incremental ingestion start date: %s", start)
    if not incremental and interval == "1h":
        period = "2y"
        start = None

    full_data = pl.DataFrame()
    for i in range(0, total, batch_size):
        try:
            batch = ticker_names[i : i + batch_size]
            logger.info(
                "Processing batch %d/%d: %d tickers",
                i // batch_size + 1,
                num_batches,
                len(batch),
            )

            df = yf.download(batch, interval=interval, period=period, start=start)
            if df is not None and not df.empty:
                df = _flatten_yf(df)
                full_data = pl.concat(
                    [full_data, pl.from_pandas(df)], how="vertical_relaxed"
                )
            sleep(30)
        except Exception as e:
            logger.warning("Batch %d failed: %s", i // batch_size + 1, e)

    full_data = full_data.with_columns(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
    )
    upsert_df(full_data, table_name, base_dir, ["date", "ticker"], ["year", "month"])


def _flatten_yf(df: DataFrame) -> DataFrame:
    """
    Flatten a yfinance DataFrame with multi-level columns into a long-form table.
    Columns: date, ticker, open, high, low, close, volume
    """
    # handle either multi-indexed or single-ticker df
    if isinstance(df.columns, pd.MultiIndex):
        df = (
            df.stack(level=-1, future_stack=True)
            .rename_axis(["date", "ticker"])
            .reset_index()
        )
    else:
        df = df.reset_index()
        df["ticker"] = "UNKNOWN"  # or pass it in manually if single ticker
        df = df.rename(columns=str.lower)

    df.columns = [c.lower() for c in df.columns]
    return df


def _get_latest_partition_date(base_dir: str, name: str) -> datetime:
    """Find the latest year/month partition in a dataset directory"""

    path = Path(base_dir) / name
    pattern = re.compile(r"year=(\d{4})[/\\]month=(\d{1,2})[/\\]?$")
    latest_year_month = None

    # walk all subdirectories and look for year=YYYY/month=MM pattern
    for p in path.rglob("*"):
        if p.is_dir():
            match = pattern.search(str(p))
            if match:
                y, m = int(match.group(1)), int(match.group(2))
                if latest_year_month is None or (y, m) > latest_year_month:
                    latest_year_month = (y, m)
    if latest_year_month is None:
        raise FileNotFoundError(f"No year/month partitions found under {path}")

    year, month = latest_year_month
    res = run_custom_query(f"""SELECT MAX(date)
        FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true)
        WHERE year={year}
        AND month={month}""")
    return res["max(date)"][0].to_pydatetime()
