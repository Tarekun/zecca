import polars as pl

from etl.logger import get_logger
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.sec_company_facts_padded import (
    SecCompanyFactsPaddedSilver,
)

logger = get_logger(__name__)


class StocksDailySilver(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT) -> None:
        super().__init__(
            name="stocks_daily",
            layer="silver",
            partitioning_columns=["year", "month"],
            dataplatform_root=dataplatform_root,
        )
        self.configure_dependencies([CandlesDailySilver, SecCompanyFactsPaddedSilver])

    def _build(self) -> pl.LazyFrame:
        candles = CandlesDailySilver("").lazy_load()
        sec = (
            SecCompanyFactsPaddedSilver()
            .lazy_load()
            .select(
                [
                    "ticker",
                    "reference_date",
                    "shares_outstanding",
                    "estimated_float_shares",
                ]
            )
        )
        return candles.join(
            sec,
            left_on=["symbol", "timeframe"],
            right_on=["ticker", "reference_date"],
            how="left",
        ).with_columns(
            (pl.col("shares_outstanding") * pl.col("open")).alias("evaluation")
        )
