-- macro needed because dbt and duckdb, like literally every single data
-- related tool, is a fucking convoluted piece of trash and i have to handle
-- partitioning of parquet by fucking hand.
-- Takes the list of names of columns to use in hive partitioning
{% macro materialized_partitioned_parquet(partition_cols) %}
    -- reordering the result by partition column forces duckdb to create 
    -- one single parquet file per partition
    COPY (SELECT * FROM {{ this }} ORDER BY {{ partition_cols | join(', ') }})
    TO "{{ var('derived_data') }}/{{ this.name }}/" (
        FORMAT PARQUET,
        PARTITION_BY ({{ partition_cols | join(', ') }}),
        OVERWRITE_OR_IGNORE TRUE
    )
{% endmacro %}
