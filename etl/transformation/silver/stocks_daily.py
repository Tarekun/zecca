import polars as pl

from etl.logger import get_logger
from etl.transformation.model import Model
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.sec_company_facts_padded import (
    SecCompanyFactsPaddedSilver,
)

logger = get_logger(__name__)


def compute_from_source() -> pl.DataFrame:
    logger.debug("Using source: CandlesDailySilver, SecCompanyFactsPaddedSilver")
    candles = CandlesDailySilver("").load_from_disk()
    sec = (
        SecCompanyFactsPaddedSilver()
        .load_from_disk()
        .select(["ticker", "reference_date", "number_of_shares"])
    )

    df = candles.join(
        sec,
        left_on=["symbol", "timeframe"],
        right_on=["ticker", "reference_date"],
        how="left",
    ).with_columns((pl.col("number_of_shares") * pl.col("open")).alias("evaluation"))

    return df


class StocksDailySilver(Model):
    def __init__(self) -> None:
        super().__init__(
            name="stocks_daily",
            layer="silver",
            partitioning_columns=["year", "month"],
        )

    def _build(self) -> pl.DataFrame:
        return compute_from_source()
