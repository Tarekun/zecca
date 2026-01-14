{{ config(materialized="table") }}

with
    raw_json as (
        select data
        from
            read_json(
                '{{ var("company_tickers") }}/*.json',
                format = 'auto',
                columns = {data:'JSON'}
            )
    ),

    keys as (select unnest(json_keys(data)) as key, data from raw_json),

    parsed as (
        select
            json_extract_string(data, '$.' || key || '.cik_str') as cik_str,
            json_extract_string(data, '$.' || key || '.ticker') as ticker,
            json_extract_string(data, '$.' || key || '.title') as title
        from keys
    )

select *
from parsed
