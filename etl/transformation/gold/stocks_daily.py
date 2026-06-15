from pathlib import Path
import polars as pl

from etl.transformation.silver.stocks_daily import StocksDailySilver
from etl.transformation.model import Model


class StocksDailyGold(Model):
    def __init__(self):
        super().__init__(
            name="stocks_daily",
            layer="gold",
            partitioning_columns=["year", "month"],
        )

    def _build(self) -> pl.DataFrame:
        return StocksDailySilver().load_from_disk()
