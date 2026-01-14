{{ config(materialized="table") }}


with
    raw_data as (
        select
            cik, entityname as entity_name, json_extract(facts, '$') as facts,
        from
            read_json(
                '{{ var("sec_facts") }}/*.json',
                columns = {facts:'JSON', cik:'STRING', entityname:'STRING'}
            )
    ),
