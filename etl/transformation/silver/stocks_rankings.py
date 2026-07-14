from pathlib import Path

import polars as pl

from etl.logger import get_logger
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.stocks_daily import StocksDailySilver

logger = get_logger(__name__)


def compute_from_source() -> pl.LazyFrame:
    """Read stocks_daily and produce a per-date ranking of stocks by
    float-adjusted market cap.

    Only rows where ``float_adjusted_market_cap`` is non-null and positive
    are eligible for ranking. Stocks are ranked within each ``timeframe``
    (rank 1 = largest float-adjusted market cap).

    Returns:
        LazyFrame with columns:

        - ``timeframe``                      – trading date
        - ``symbol``                         – ticker symbol
        - ``float_adjusted_market_cap``       – estimated_float_shares × open
        - ``float_adjusted_market_cap_rank``  – 1-based rank within the date (1 = largest)
    """
    return (
        StocksDailySilver()
        .read_from_disk()
        .select(["timeframe", "symbol", "float_adjusted_market_cap"])
        .filter(
            pl.col("float_adjusted_market_cap").is_not_null()
            & (pl.col("float_adjusted_market_cap") > 0)
        )
        .with_columns(
            pl.col("float_adjusted_market_cap")
            .rank(method="ordinal", descending=True)
            .over("timeframe")
            .alias("float_adjusted_market_cap_rank")
        )
        .sort(["timeframe", "float_adjusted_market_cap_rank"])
    )


class StocksRankingsSilver(Model):
    def __init__(
        self, dataplatform_root: str | Path = DEFAULT_DATAPLATFORM_ROOT
    ) -> None:
        super().__init__(
            name="stocks_rankings", layer="silver", dataplatform_root=dataplatform_root
        )

    def _build(self) -> pl.LazyFrame:
        return compute_from_source()
