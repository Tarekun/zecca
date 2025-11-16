-- computes the "return" of a price point given a `entry_value` and a 
-- `current_value`, iff both values are positive, evaluates to NULL otherwise
{% macro safe_return(entry_value, current_value) %}
    CASE 
        WHEN {{entry_value}} > 0 AND {{current_value}} > 0
            THEN LN({{entry_value}} / {{current_value}})
        ELSE NULL
    END
{% endmacro %}

-- computes the rolling avg of `column`, over `num_rows` rows, partitioned by
-- `partition_by`, and ordered by `order_by`
{% macro rolling_avg(column, partition_by, order_by, num_rows) %}
    AVG({{ column }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ num_rows }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- computes the volatility of `column`, over `num_rows` rows, partitioned by
-- `partition_by`, and ordered by `order_by`
{% macro volatility(column, partition_by, order_by, num_rows) %}
    STDDEV_SAMP({{ column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ num_rows }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}
