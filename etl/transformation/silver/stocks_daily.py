import polars as pl
from pathlib import Path

from etl.logger import get_logger
from etl.transformation.model import Model, DATAPLATFORM_ROOT
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.sec_company_facts_padded import (
    SecCompanyFactsPaddedSilver,
)

logger = get_logger(__name__)

_CANDLES_GLOB = str(
    Path(DATAPLATFORM_ROOT) / "silver" / "candles_daily" / "**" / "*.parquet"
)
_SEC_PATH = str(
    Path(DATAPLATFORM_ROOT)
    / "silver"
    / "sec_company_facts_padded"
    / "sec_company_facts_padded.parquet"
)


class StocksDailySilver(Model):
    def __init__(self) -> None:
        super().__init__(
            name="stocks_daily",
            layer="silver",
            partitioning_columns=["year", "month"],
        )
        self.configure_dependencies([CandlesDailySilver, SecCompanyFactsPaddedSilver])

    def _build(self) -> pl.LazyFrame:
        candles = pl.scan_parquet(_CANDLES_GLOB, hive_partitioning=True)
        sec = pl.scan_parquet(_SEC_PATH).select(
            ["ticker", "reference_date", "shares_outstanding", "estimated_float_shares"]
        )
        return candles.join(
            sec,
            left_on=["symbol", "timeframe"],
            right_on=["ticker", "reference_date"],
            how="left",
        ).with_columns(
            (pl.col("shares_outstanding") * pl.col("open")).alias("evaluation")
        )
