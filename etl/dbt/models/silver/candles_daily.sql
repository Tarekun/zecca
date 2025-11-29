{{ config(
    materialized='incremental',
    format='parquet',
    post_hook="{{ materialized_partitioned_parquet(['year','month']) }}"
) }}


WITH candles AS (
    {{candles_enhanced('1d', [1, 5, 20, 30, 50, 100, 200])}}
)

SELECT
    timeframe,
    EXTRACT(YEAR FROM timeframe) AS year,
    EXTRACT(MONTH FROM timeframe) AS month,
    symbol,
    open,
    close,
    high,
    low,
    volume,
    log_return_1_steps AS log_return,
    log_return_5_steps AS log_return_1w,
    log_return_20_steps AS log_return_1m,
    log_return_30_steps,
    log_return_50_steps,
    log_return_100_steps AS log_return_6m,
    log_return_200_steps AS log_return_1y,
    open_rolling_1_steps,
    open_rolling_5_steps AS open_rolling_1w,
    open_rolling_20_steps AS open_rolling_1m,
    open_rolling_30_steps,
    open_rolling_50_steps,
    open_rolling_100_steps AS open_rolling_6m,
    open_rolling_200_steps AS open_rolling_1y,
    volatility_1_steps,
    volatility_5_steps AS volatility_1w,
    volatility_20_steps AS volatility_1m,
    volatility_30_steps,
    volatility_50_steps,
    volatility_100_steps AS volatility_6m,
    volatility_200_steps AS volatility_1y,
FROM candles
