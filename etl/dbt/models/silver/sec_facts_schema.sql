{{ config(materialized="table") }}


with
    raw as (
        select cik, json_extract(facts, '$') as facts,
        from
            read_json(
                '{{ var("sec_facts") }}/*.json',
                columns = {facts:'JSON', cik:'STRING'}
            )
    ),

    taxonomies as (
        select cik, t.key as top_level_field, t.value as taxonomy_object
        from raw
        cross join lateral json_each(facts) t
    ),

    attributes as (
        select
            cik,
            top_level_field,
            a.key as attribute_name,
            a.value as attribute_object
        from taxonomies
        cross join lateral json_each(taxonomy_object) a
    ),

    output_schema as (
        select
            top_level_field,
            attribute_name,
            attribute_object ->> 'label' as label,
            attribute_object ->> 'description' as description,
            count(distinct cik) as appearance_count,
        from attributes
        group by all
    )

select *
from output_schema
where label is not null
