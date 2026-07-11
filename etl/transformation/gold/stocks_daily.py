import polars as pl

from etl.logger import get_logger
from etl.transformation.silver.stocks_daily import StocksDailySilver
from etl.transformation.model import Model

logger = get_logger(__name__)


class StocksDailyGold(Model):
    def __init__(self):
        super().__init__(
            name="stocks_daily",
            layer="gold",
            partitioning_columns=["year", "month"],
        )
        self.configure_dependencies([StocksDailySilver])

    def _build(self) -> pl.LazyFrame:
        logger.debug("Using source: StocksDailySilver")
        return StocksDailySilver().lazy_load()
