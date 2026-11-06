import polars as pl

from analysis.framework.market import DATE_COL

DEFAULT_DATAPLATFORM_ROOT = "./dataplatform"


def load_risk_free(
    region: str = "north_america",
    dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
) -> pl.DataFrame:
    """Daily risk-free rate from the Fama-French library (`silver.fama_french_5`)
    as a `(timeframe, rf)` frame, with `rf` converted from the library's
    percent-per-day to a decimal daily return (0.02 -> 0.0002), ready to be
    passed as `risk_free` to `run_backtest`."""
    # imported here so the framework itself doesn't depend on the etl package
    from etl.transformation.silver import FamaFrench5Silver

    return (
        FamaFrench5Silver(dataplatform_root=dataplatform_root)
        .read_from_disk()
        .filter(pl.col("region") == region)
        .select(
            pl.col("date").alias(DATE_COL),
            (pl.col("RF") / 100).alias("rf"),
        )
        .sort(DATE_COL)
        .collect()
    )
