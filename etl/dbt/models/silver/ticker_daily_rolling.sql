{{ config(
    materialized='table',
    format='parquet',
    post_hook="{{ materialized_partitioned_parquet(['year','month']) }}"
) }}


WITH src AS (
    SELECT 
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(DAY FROM date) AS day,
        date,
        ticker,
        open,
        close,
        high,
        low,
        volume,
    FROM {{source_yfinance('ticker_daily')}}
)
, with_returns AS (
    SELECT
        *,
        -- daily log return for volatility
        {{safe_return(
            'close',
            'LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date)'
        )}} AS log_return,
        -- fixed-horizon total cumulative returns
        {{safe_return(
            'close',
            'LAG(close, 7) OVER (PARTITION BY ticker ORDER BY date)'
        )}} AS return_1w,
        {{safe_return(
            'close',
            'LAG(close, 30) OVER (PARTITION BY ticker ORDER BY date)'
        )}} AS return_1m,
        {{safe_return(
            'close',
            'LAG(close, 365) OVER (PARTITION BY ticker ORDER BY date)'
        )}} AS return_1y,
        {{safe_return(
            'close',
            'LAG(close, 730) OVER (PARTITION BY ticker ORDER BY date)'
        )}} AS return_2y,
    FROM src
)
, with_rolling AS (
    SELECT 
        *,
        -- rolling opening prices
        AVG(open) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS open_rolling_1w,
        AVG(open) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS open_rolling_2w,
        AVG(open) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS open_rolling_1m,
        AVG(open) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
        ) AS open_rolling_100d,
        AVG(open) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS open_rolling_200d,

        -- volatility: rolling std dev of 1-day log returns
        STDDEV_SAMP(log_return) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS volatility_1w,
        STDDEV_SAMP(log_return) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS volatility_2w,
        STDDEV_SAMP(log_return) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volatility_1m,
        STDDEV_SAMP(log_return) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
        ) AS volatility_100d,
        STDDEV_SAMP(log_return) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS volatility_200d
    FROM with_returns
)

SELECT * FROM with_rolling
