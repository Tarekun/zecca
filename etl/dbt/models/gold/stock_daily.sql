{{ config(
    materialized='table',
    format='parquet',
    post_hook="{{ materialized_partitioned_parquet(['year']) }}"
) }}


SELECT *
FROM {{ref("stock_daily_duck")}}
