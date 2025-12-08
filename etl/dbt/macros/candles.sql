-- time_unit supported values: 1d
{% macro candles_enhanced(time_unit, lookback_periods) %}
    {% set lookback_list = lookback_periods | map("int") | list %}
    with
        src as (
            select
                -- TODO support different time units with proper source and aggregation
                case when '{{time_unit}}' = '1d' then date(date) else date end as timeframe,
                ticker as symbol,
                open,
                close,
                high,
                low,
                volume,
            from {{ source_yfinance("ticker_daily") }}
        ),
        -- fixed-horizon total cumulative returns over different timeframes
        with_returns as (
            select
                *,
                -- column used internally to compute volatility
                {{
                    safe_return(
                        "close", "LAG(close, 1) OVER (PARTITION BY symbol ORDER BY timeframe)"
                    )
                }} as log_return
                {% for steps in lookback_list %}
                    ,
                    {{
                        safe_return(
                            "close",
                            "LAG(close, "
                            ~ steps
                            ~ ") OVER (PARTITION BY symbol ORDER BY timeframe)",
                        )
                    }} as log_return_{{ steps }}_steps
                {% endfor %}
            from src
        ),
        -- rolling averages over different timeframes
        with_rolling as (
            select
                *
                -- rolling opening prices
                {% for steps in lookback_list %}
                    ,
                    {{ rolling_avg("open", "symbol", "timeframe", steps) }}
                    as open_rolling_{{ steps }}_steps
                {% endfor %}

                -- volatility: rolling std dev of 1-day log returns
                {% for steps in lookback_list %}
                    ,
                    {{ volatility("log_return", "symbol", "timeframe", 6) }}
                    as volatility_{{ steps }}_steps
                {% endfor %}
            from with_returns
        )

    select *
    from with_rolling
{% endmacro %}
