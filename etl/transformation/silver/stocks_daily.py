import duckdb
import polars as pl
from pathlib import Path

from etl.logger import get_logger
from etl.transformation.model import Model, DATAPLATFORM_ROOT
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.sec_company_facts_padded import (
    SecCompanyFactsPaddedSilver,
)

logger = get_logger(__name__)


def compute_with_polars() -> pl.DataFrame:
    logger.debug("Using source: CandlesDailySilver, SecCompanyFactsPaddedSilver")
    candles = CandlesDailySilver("").load_from_disk()
    sec = (
        SecCompanyFactsPaddedSilver()
        .load_from_disk()
        .select(["ticker", "reference_date", "shares_outstanding", "estimated_float_shares"])
    )

    df = candles.join(
        sec,
        left_on=["symbol", "timeframe"],
        right_on=["ticker", "reference_date"],
        how="left",
    ).with_columns((pl.col("shares_outstanding") * pl.col("open")).alias("evaluation"))

    return df


# TODO handle this better
_CANDLES_GLOB = str(
    Path(DATAPLATFORM_ROOT) / "silver" / "candles_daily" / "**" / "*.parquet"
)
_SEC_PATH = str(
    Path(DATAPLATFORM_ROOT)
    / "silver"
    / "sec_company_facts_padded"
    / "sec_company_facts_padded.parquet"
)


def compute_with_duckdb() -> pl.DataFrame:
    logger.debug("Using source: CandlesDailySilver, SecCompanyFactsPaddedSilver")
    with duckdb.connect() as conn:
        return conn.execute(f"""
            SELECT
                c.*,
                s.number_of_shares,
                s.number_of_shares * c.open AS evaluation
            FROM read_parquet('{_CANDLES_GLOB}', hive_partitioning = true) c
            LEFT JOIN (
                SELECT ticker, reference_date, number_of_shares
                FROM read_parquet('{_SEC_PATH}')
            ) s ON c.symbol = s.ticker AND c.timeframe = s.reference_date
        """).pl()


class StocksDailySilver(Model):
    def __init__(self) -> None:
        super().__init__(
            name="stocks_daily",
            layer="silver",
            partitioning_columns=["year", "month"],
        )

    def _build(self) -> pl.DataFrame:
        return compute_with_duckdb()
