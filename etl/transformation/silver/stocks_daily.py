from datetime import timedelta
import polars as pl

from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.quality_checks import with_root
from etl.transformation.quality_checks import (
    foreign_key,
    freshness,
    is_finite,
    not_empty,
    not_null,
    unique,
)
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.sec_company_facts_padded import (
    SecCompanyFactsPaddedSilver,
)


class StocksDailySilver(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT) -> None:
        super().__init__(
            name="stocks_daily",
            layer="silver",
            partitioning_columns=["year", "month"],
            strategy="fullrefresh",
            quality_checks=[
                not_empty(),
                not_null(["timeframe", "symbol"]),
                unique(["timeframe", "symbol"]),
                freshness("timeframe", timedelta(days=3)),
                foreign_key(
                    ["timeframe", "symbol"],
                    target_model=CandlesDailySilver(
                        dataplatform_root=dataplatform_root
                    ),
                ),
                is_finite(["price_to_earnings", "earnings_per_share"]),
                with_root(test_all_candles_pairs_present, dataplatform_root),
            ],
            dataplatform_root=dataplatform_root,
        )

    def _build(self, incremental: bool = False) -> pl.LazyFrame:
        after = None
        if incremental:
            # only compute what comes after what the table already holds
            try:
                after = (
                    self.read_from_disk()
                    .select(pl.col("timeframe").max())
                    .collect()
                    .item()
                )
            except pl.exceptions.ComputeError:
                pass  # nothing stored yet: build everything

        candles = CandlesDailySilver(
            dataplatform_root=self.dataplatform_root
        ).read_from_disk()
        sec = (
            SecCompanyFactsPaddedSilver(dataplatform_root=self.dataplatform_root)
            .read_from_disk()
            .select(
                [
                    "ticker",
                    "reference_date",
                    "shares_outstanding",
                    "estimated_float_shares",
                    "earnings",
                ]
            )
        )
        if after is not None:
            candles = candles.filter(pl.col("timeframe") > after)
            sec = sec.filter(pl.col("reference_date") > after)

        return (
            candles.join(
                sec,
                left_on=["symbol", "timeframe"],
                right_on=["ticker", "reference_date"],
                how="left",
            )
            .with_columns(
                (pl.col("shares_outstanding") * pl.col("open")).alias("evaluation")
            )
            .with_columns(
                (pl.col("evaluation") / pl.col("earnings")).alias("price_to_earnings")
            )
            .with_columns(
                (pl.col("estimated_float_shares") * pl.col("open")).alias(
                    "float_adjusted_market_cap"
                )
            )
            .with_columns(
                (pl.col("earnings") / pl.col("shares_outstanding")).alias(
                    "earnings_per_share"
                )
            )
        )


def test_all_candles_pairs_present(
    lf: pl.LazyFrame, dataplatform_root: str
) -> pl.LazyFrame:
    """Every (symbol, timeframe) pair from candles_daily must appear in stocks_daily"""

    candles_pairs = (
        CandlesDailySilver(dataplatform_root=dataplatform_root)
        .read_from_disk()
        .select(["symbol", "timeframe"])
        .unique()
    )
    stocks_pairs = lf.select(["symbol", "timeframe"]).unique()

    missing = candles_pairs.join(
        stocks_pairs, on=["symbol", "timeframe"], how="anti"
    ).sort(["symbol", "timeframe"])

    return missing
