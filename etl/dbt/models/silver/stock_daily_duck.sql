{{
    config(
        materialized="view",
    )
}}


select *
from {{ ref("candles_daily") }}
