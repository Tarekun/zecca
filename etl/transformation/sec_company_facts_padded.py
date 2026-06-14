from datetime import date
from pathlib import Path

import polars as pl

from etl.logger import get_logger

logger = get_logger(__name__)


def compute_sec_company_facts_padded(dataplatform_root: str | Path) -> pl.DataFrame:
    """Read sec_company_facts from the silver layer and expand to one row per
    (cik, reference_date).

    Each report's values are duplicated for every calendar date starting from
    the report's `end` date up to (but not including) the next report's `end`
    date for the same CIK.  The most recent report per CIK is padded forward
    to today.  Rows without an `end` date are dropped since they cannot be
    placed on the time axis.

    Args:
        dataplatform_root: Root of the dataplatform directory (e.g. "./dataplatform").

    Returns:
        Eager DataFrame with all columns from sec_company_facts (``val`` renamed
        to ``number_of_shares``) plus a ``reference_date`` (Date) column.
    """
    parquet_path = (
        Path(dataplatform_root)
        / "silver"
        / "sec_company_facts"
        / "sec_company_facts.parquet"
    )

    tickers_path = (
        Path(dataplatform_root) / "silver" / "company_tickers" / "company_tickers.parquet"
    )

    logger.info("Reading sec_company_facts from %s", parquet_path)

    tickers = pl.read_parquet(tickers_path).select(
        pl.col("cik_str").alias("cik"), pl.col("ticker")
    )

    df = (
        pl.read_parquet(parquet_path)
        .rename({"val": "number_of_shares"})
        .filter(pl.col("cik").is_not_null() & pl.col("end").is_not_null())
        .drop("source_file")
        .join(tickers, on="cik", how="left")
        .sort(["cik", "end"])
    )

    today = date.today()

    # For each row, valid_until = (next report's end - 1 day) for that CIK,
    # or today for the most recent report.
    df = df.with_columns(
        pl.col("end").shift(-1).over("cik").alias("_next_end")
    ).with_columns(
        pl.when(pl.col("_next_end").is_null())
        .then(pl.lit(today))
        .otherwise(pl.col("_next_end") - pl.duration(days=1))
        .cast(pl.Date)
        .alias("valid_until")
    ).drop("_next_end")

    df = (
        df.with_columns(
            pl.date_ranges(
                pl.col("end"), pl.col("valid_until"), interval="1d"
            ).alias("reference_date")
        )
        .explode("reference_date")
        .drop("valid_until")
    )

    logger.info(
        "Returning sec_company_facts_padded: %d rows × %d cols — %.1f MB",
        df.height,
        df.width,
        df.estimated_size("mb"),
    )
    return df
