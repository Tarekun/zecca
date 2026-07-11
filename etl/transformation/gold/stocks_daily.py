import polars as pl

from etl.logger import get_logger
from etl.transformation.silver.stocks_daily import StocksDailySilver
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT

logger = get_logger(__name__)


class StocksDailyGold(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT):
        super().__init__(
            name="stocks_daily",
            layer="gold",
            partitioning_columns=["year", "month"],
            dataplatform_root=dataplatform_root,
        )
        self.configure_dependencies([StocksDailySilver])

    def _build(self) -> pl.LazyFrame:
        logger.debug("Using source: StocksDailySilver")
        return StocksDailySilver().read_from_disk()
