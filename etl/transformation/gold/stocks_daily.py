from pathlib import Path
import polars as pl

from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.model import Model


class StocksDailySilver(Model):
    def __init__(self):
        super().__init__(
            name="stocks_daily",
            layer="gold",
            partitioning_columns=["year", "month"],
        )

    def _build(self) -> pl.DataFrame:
        return CandlesDailySilver("").load_from_disk()
