{{
    config(
        materialized="table",
        format="parquet",
        post_hook="{{ materialized_partitioned_parquet(['year']) }}",
    )
}}


select *
from {{ ref("stock_daily_duck") }}
