import math
from datetime import datetime, timezone
import pandas as pd
from pandas import DataFrame
import polars as pl
from time import sleep
from typing import Literal
import yfinance as yf

from etl.ingestion.sec import SecTickers
from etl.ingestion.source import TableLike, DEFAULT_DATAPLATFORM_ROOT
from etl.logger import get_logger

logger = get_logger(__name__)

_RAW_COLS = ["date", "ticker", "open", "close", "high", "low", "volume"]


class YFinanceTicker(TableLike):
    """OHLCV candles for a batch of tickers, pulled from yfinance at a fixed
    `interval` ("1d" or "1h") and upserted into an on-disk, year/month
    hive-partitioned parquet store keyed by (date, ticker)."""

    def __init__(
        self,
        interval: Literal["1d", "1h"],
        **kwargs,
    ) -> None:
        super().__init__(
            name="ticker_daily" if interval == "1d" else "ticker_hourly",
            key_columns=["date", "ticker"],
            partitioning_columns=["year", "month"],
            **kwargs,
        )
        self.interval = interval

    def load(self, **kwargs) -> pl.DataFrame:
        tickers = SecTickers(dataplatform_root=self.dataplatform_root).read_from_disk()[
            0
        ]
        ticker_names = sorted(
            {entry["ticker"] for entry in tickers.values() if entry.get("ticker")}
        )
        total = len(ticker_names)
        batch_size = 100 if self.incremental else 50
        num_batches = math.ceil(total / batch_size)

        start = "1970-01-01"
        period = None
        if self.incremental:
            # tbh i think reprocessing the last day every day is best
            start_date = (
                self.read_from_disk().select(pl.col("date").max()).collect().item()
            )
            if start_date is None:
                logger.warning(
                    "No existing %s data, run a full refresh first.", self.name
                )
                return pl.DataFrame()
            # TODO review timezone handling
            if start_date.replace(tzinfo=timezone.utc) >= datetime.now(timezone.utc):
                logger.info(
                    "%s: data is already up to date (won't pull %s)",
                    self.name,
                    start_date,
                )
                return pl.DataFrame()
            start = start_date.strftime("%Y-%m-%d")
            logger.info("Incremental ingestion start date: %s", start)
        if not self.incremental and self.interval == "1h":
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

                df = yf.download(
                    batch,
                    interval=self.interval,
                    period=period,  # type: ignore
                    start=start,
                )
                if df is not None and not df.empty:
                    df = _flatten_yf(df)
                    full_data = pl.concat(
                        [full_data, pl.from_pandas(df)], how="vertical_relaxed"
                    )
                sleep(30)
            except Exception as e:
                logger.warning("Batch %d failed: %s", i // batch_size + 1, e)

        if full_data.is_empty():
            return full_data

        return full_data.with_columns(
            pl.col("date").dt.year().alias("year"),
            pl.col("date").dt.month().alias("month"),
        )

    def _normalize(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        # TODO: investigate the issue with source parquets that caused this
        return lf.select(_RAW_COLS).with_columns(pl.col("date").cast(pl.Datetime("us")))


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
