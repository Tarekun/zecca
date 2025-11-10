{% macro source_yfinance(table_name) %}
    read_parquet(
        "{{ var('yfinance_data') }}/{{ table_name }}/year=*/month=*/*.parquet",
        hive_partitioning = true
    )
{% endmacro %}