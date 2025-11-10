{{ config(
    materialized='table',
    format='parquet',
    post_hook="""
        COPY (
        SELECT * FROM {{ this }}
        ) TO '../dataplatform/transformed/ticker_daily_rolling/'
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE);
    """
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
    FROM read_parquet(
        '../dataplatform/raw/ticker_daily/year=*/month=*/*.parquet',
        hive_partitioning = true
    )
)
, with_rolling AS (
    SELECT 
        *,
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
        ) AS open_rolling_200d
    FROM src
)

SELECT * FROM with_rolling
