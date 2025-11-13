{# macro needed because dbt and duckdb, like literally every single data
related tool, is a fucking convoluted piece of trash and i have to handle
partitioning of parquet by fucking hand.
Takes the list of names of columns to use in hive partitioning #}
{% macro materialized_partitioned_parquet(partition_cols) %}
    COPY (SELECT * FROM {{ this }})
    TO "{{ var('derived_data') }}/{{ this.name }}/" (
        FORMAT PARQUET,
        PARTITION_BY ({{ partition_cols | join(', ') }}),
        OVERWRITE_OR_IGNORE TRUE
    )
{% endmacro %}
