{% macro candles_enhanced(time_unit, lookback_periods) %}
{% set lookback_list = lookback_periods | map('int') | list %}
WITH src AS (
    SELECT 
        -- TODO this probably wont be the same name always
        date AS timeframe,
        ticker AS symbol,
        open,
        close,
        high,
        low,
        volume,
    FROM {{source_yfinance('ticker_daily')}}
)

-- fixed-horizon total cumulative returns over different timeframes
, with_returns AS (
    SELECT
        *,
        {{safe_return(
            'close',
            'LAG(close, 1) OVER (PARTITION BY symbol ORDER BY timeframe)'
        )}} AS log_return
        {% for steps in lookback_list %}
            , {{safe_return(
                'close',
                'LAG(close, ' ~ steps ~ ') OVER (PARTITION BY symbol ORDER BY timeframe)'
            )}} AS log_return_{{ steps }}_steps
        {% endfor %}
    FROM src
)

-- rolling averages over different timeframes
, with_rolling AS (
    SELECT 
        *
        -- rolling opening prices
        {% for steps in lookback_list %}
            , {{rolling_avg(
                'open', 'symbol', 'timeframe', steps
            )}} AS open_rolling_{{ steps }}_steps
        {% endfor %}

        -- volatility: rolling std dev of 1-day log returns
        {% for steps in lookback_list %}
            , {{volatility(
                'log_return', 'symbol', 'timeframe', 6
            )}} AS volatility_{{ steps }}_steps
        {% endfor %}
    FROM with_returns
)

SELECT * FROM with_rolling
{% endmacro %}
