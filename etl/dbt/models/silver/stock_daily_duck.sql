{{ config(
    materialized = "view"
) }}


SELECT *
FROM {{ref("candles_daily")}}
