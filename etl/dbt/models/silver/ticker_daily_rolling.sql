{{ config(
    materialized='incremental',
    format='parquet',
    post_hook="{{ materialized_partitioned_parquet(['year','month']) }}"
) }}


-- determine the max date already processed
WITH latest_partition AS (
    {% if is_incremental() %}
        WITH max_year AS (
            SELECT MAX(year) AS year FROM {{ this }}
        ),
        max_month AS (
            SELECT my.year, MAX(t.month) AS month
            FROM {{ this }} t
            JOIN max_year my ON t.year = my.year
            GROUP BY my.year
        )

        SELECT
            MAX(t.date) AS max_date,
            MAX(t.year) AS year,
            MAX(t.month) AS month
        FROM {{ this }} t
        JOIN max_month mm ON t.year = mm.year AND t.month = mm.month
    {% else %}
        SELECT NULL AS max_date WHERE FALSE
    {% endif %}
)
, src AS (
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
    FROM {{source_yfinance('ticker_daily')}} td
    {% if is_incremental() %}
        JOIN latest_partition l
            ON l.year = td.year AND l.month = td.month
        WHERE td.date >= l.max_date
    {% endif %}
)

-- fixed-horizon total cumulative returns over different timeframes
, with_returns AS (
    SELECT
        *,
        {{safe_return(
            'close',
            'LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date)'
        )}} AS log_return,
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
-- rolling averages over different timeframes
, with_rolling AS (
    SELECT 
        *,
        -- rolling opening prices
        {{rolling_avg('open', 'ticker', 'date', 6)}} AS open_rolling_1w,
        {{rolling_avg('open', 'ticker', 'date', 13)}} AS open_rolling_2w,
        {{rolling_avg('open', 'ticker', 'date', 29)}} AS open_rolling_1m,
        {{rolling_avg('open', 'ticker', 'date', 99)}} AS open_rolling_100d,
        {{rolling_avg('open', 'ticker', 'date', 199)}} AS open_rolling_200d,

        -- volatility: rolling std dev of 1-day log returns
        {{volatility('log_return', 'ticker', 'date', 6)}} AS volatility_1w,
        {{volatility('log_return', 'ticker', 'date', 13)}} AS volatility_2w,
        {{volatility('log_return', 'ticker', 'date', 29)}} AS volatility_1m,
        {{volatility('log_return', 'ticker', 'date', 99)}} AS volatility_100d,
        {{volatility('log_return', 'ticker', 'date', 199)}} AS volatility_200d
    FROM with_returns
)

SELECT * FROM with_rolling
