from pathlib import Path
import polars as pl

from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.stocks_rankings import StocksRankingsSilver

RANK_THRESHOLD = 2500


class GoodSymbolsSilver(Model):
    def __init__(
        self, dataplatform_root: str | Path = DEFAULT_DATAPLATFORM_ROOT
    ) -> None:
        super().__init__(
            name="good_symbols", layer="silver", dataplatform_root=dataplatform_root
        )

    def _build(self) -> pl.LazyFrame:
        return (
            StocksRankingsSilver()
            .read_from_disk()
            .filter(pl.col("float_adjusted_market_cap_rank") < RANK_THRESHOLD)
            .select(["timeframe", "symbol"])
        )
