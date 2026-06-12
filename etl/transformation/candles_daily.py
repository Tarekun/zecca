from pathlib import Path

from etl.logger import get_logger

logger = get_logger(__name__)

import polars as pl

from etl.transformation.indicators import (
    relative_strength_index,
    rolling_avg,
    safe_div,
    safe_log_return,
    safe_return,
    volatility,
)
from etl.transformation.utils import load_ticker_daily

# Lookback periods in trading days, matching candles_enhanced([1, 5, 14, 20, 30, 62, 126, 252])
_LOOKBACKS = [1, 5, 14, 20, 30, 62, 126, 252]


def compute_candles_daily(yfinance_data_path: str | Path) -> pl.DataFrame:
    """Read ticker_daily parquet data and compute the full candles_daily indicator set.

    Replicates the logic of the dbt model ``silver/candles_daily.sql`` and its
    underlying ``candles_enhanced("1d", ...)`` macro. The returned DataFrame matches
    the schema of that model exactly, including column names and aliases.

    Args:
        yfinance_data_path: Root directory of the yfinance data store — the value
            that maps to the ``yfinance_data`` dbt variable in ``profiles.yml``.

    Returns:
        Eager DataFrame with one row per (symbol, date) containing:

        - Identity: ``timeframe``, ``year``, ``month``, ``symbol``
        - OHLCV: ``open``, ``close``, ``high``, ``low``, ``volume``
        - Log returns: ``log_return_1d/1w/1m/30_steps/1q/6m/1y``
        - Pct returns: ``return_1d/1w/1m/30_steps/1q/6m/1y``
        - Rolling open avg: ``open_rolling_1_steps_1d/1w/1m/30_steps/1q/6m/1y``
        - Volatility (rolling σ of 1-day log return): ``volatility_1_steps_1d/1w/1m/30_steps/1q/6m/1y``
        - Sharpe ratio: ``sharpe_1_steps_1d/1w/1m/30_steps/1q/6m/1y``
        - RSI (14-step): ``rsi``, ``overbought``, ``oversold``
        - RSI (other periods): ``rsi_1d/1w/1m/30_steps/1q/6m/1y``

    Known limitations:

    - ``volatility_1_steps_1d`` and ``sharpe_1_steps_1d`` are always null.
      A window of 1 row can never satisfy ``min_samples=2``, so the sample
      standard deviation is undefined for the 1-day lookback.
    """
    df = load_ticker_daily(yfinance_data_path)

    df = (
        df.rename({"ticker": "symbol"})
        .with_columns(pl.col("date").cast(pl.Date).alias("timeframe"))
        .drop("date")
        .sort(["symbol", "timeframe"])
    )

    # 1-day price diff (RSI input) and 1-day log return (volatility input)
    df = df.with_columns(
        (pl.col("close") - pl.col("close").shift(1).over("symbol")).alias(
            "_price_diff"
        ),
        safe_log_return(
            pl.col("close"),
            pl.col("close").shift(1).over("symbol"),
        ).alias("_return"),
    )

    # Per-lookback log returns and percentage returns
    df = df.with_columns(
        [
            expr
            for steps in _LOOKBACKS
            for expr in (
                safe_log_return(
                    pl.col("close"),
                    pl.col("close").shift(steps).over("symbol"),
                ).alias(f"log_return_{steps}_steps"),
                safe_return(
                    pl.col("close"),
                    pl.col("close").shift(steps).over("symbol"),
                ).alias(f"return_{steps}_steps"),
            )
        ]
    )

    # volatility requires window >= min_samples (2); exclude s=1 to avoid an
    # InvalidOperationError from Polars and emit that column as explicit null instead.
    _vol_lookbacks = [s for s in _LOOKBACKS if s >= 2]

    # Rolling averages, volatility, RSI — all partitioned by symbol
    df = df.with_columns(
        [
            rolling_avg(pl.col("open"), s)
            .over("symbol")
            .alias(f"open_rolling_{s}_steps")
            for s in _LOOKBACKS
        ]
        + [pl.lit(None, dtype=pl.Float64).alias("volatility_1_steps")]
        + [
            volatility(pl.col("_return"), s)
            .over("symbol")
            .alias(f"volatility_{s}_steps")
            for s in _vol_lookbacks
        ]
        + [
            relative_strength_index(pl.col("_price_diff"), s)
            .over("symbol")
            .alias(f"rsi_{s}_steps")
            for s in _LOOKBACKS
        ]
    )

    # Sharpe ratio = return / volatility; 1-step is null because volatility_1_steps is null.
    df = df.with_columns(
        [pl.lit(None, dtype=pl.Float64).alias("sharpe_1_steps")]
        + [
            safe_div(
                pl.col(f"return_{s}_steps"), pl.col(f"volatility_{s}_steps")
            ).alias(f"sharpe_{s}_steps")
            for s in _vol_lookbacks
        ]
    )

    # Final selection and renaming to match the candles_daily schema
    result = df.select(
        [
            "timeframe",
            pl.col("timeframe").dt.year().cast(pl.Int32).alias("year"),
            pl.col("timeframe").dt.month().cast(pl.Int32).alias("month"),
            "symbol",
            "open",
            "close",
            "high",
            "low",
            "volume",
            # Log returns
            pl.col("log_return_1_steps").alias("log_return_1d"),
            pl.col("log_return_5_steps").alias("log_return_1w"),
            pl.col("log_return_20_steps").alias("log_return_1m"),
            pl.col("log_return_30_steps"),
            pl.col("log_return_62_steps").alias("log_return_1q"),
            pl.col("log_return_126_steps").alias("log_return_6m"),
            pl.col("log_return_252_steps").alias("log_return_1y"),
            # Percentage returns
            pl.col("return_1_steps").alias("return_1d"),
            pl.col("return_5_steps").alias("return_1w"),
            pl.col("return_20_steps").alias("return_1m"),
            pl.col("return_30_steps"),
            pl.col("return_62_steps").alias("return_1q"),
            pl.col("return_126_steps").alias("return_6m"),
            pl.col("return_252_steps").alias("return_1y"),
            # Rolling open averages
            pl.col("open_rolling_1_steps").alias("open_rolling_1_steps_1d"),
            pl.col("open_rolling_5_steps").alias("open_rolling_1w"),
            pl.col("open_rolling_20_steps").alias("open_rolling_1m"),
            pl.col("open_rolling_30_steps"),
            pl.col("open_rolling_62_steps").alias("open_rolling_1q"),
            pl.col("open_rolling_126_steps").alias("open_rolling_6m"),
            pl.col("open_rolling_252_steps").alias("open_rolling_1y"),
            # Volatility (rolling σ of 1-day log return)
            # NOTE: volatility_1_steps_1d is always null — window_size=1 cannot
            # satisfy min_samples=2, so the 1-day sample std dev is undefined.
            pl.col("volatility_1_steps").alias("volatility_1_steps_1d"),
            pl.col("volatility_5_steps").alias("volatility_1w"),
            pl.col("volatility_20_steps").alias("volatility_1m"),
            pl.col("volatility_30_steps"),
            pl.col("volatility_62_steps").alias("volatility_1q"),
            pl.col("volatility_126_steps").alias("volatility_6m"),
            pl.col("volatility_252_steps").alias("volatility_1y"),
            # Sharpe ratios
            # NOTE: sharpe_1_steps_1d is always null for the same reason as volatility above.
            pl.col("sharpe_1_steps").alias("sharpe_1_steps_1d"),
            pl.col("sharpe_5_steps").alias("sharpe_1w"),
            pl.col("sharpe_20_steps").alias("sharpe_1m"),
            pl.col("sharpe_30_steps"),
            pl.col("sharpe_62_steps").alias("sharpe_1q"),
            pl.col("sharpe_126_steps").alias("sharpe_6m"),
            pl.col("sharpe_252_steps").alias("sharpe_1y"),
            # 14-step RSI with overbought/oversold flags
            pl.col("rsi_14_steps").alias("rsi"),
            (pl.col("rsi_14_steps") > 70).alias("overbought"),
            (pl.col("rsi_14_steps") < 30).alias("oversold"),
            # RSI for other periods
            pl.col("rsi_1_steps").alias("rsi_1d"),
            pl.col("rsi_5_steps").alias("rsi_1w"),
            pl.col("rsi_20_steps").alias("rsi_1m"),
            pl.col("rsi_30_steps"),
            pl.col("rsi_62_steps").alias("rsi_1q"),
            pl.col("rsi_126_steps").alias("rsi_6m"),
            pl.col("rsi_252_steps").alias("rsi_1y"),
        ]
    )
    logger.info(
        "Returning candles_daily: %d rows × %d cols — %.1f MB",
        result.height,
        result.width,
        result.estimated_size("mb"),
    )
    return result
