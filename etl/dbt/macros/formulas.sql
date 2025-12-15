-- computes the "return" of a price point given a `entry_value` and a 
-- `current_value`, iff both values are positive, evaluates to NULL otherwise
{% macro safe_log_return(entry_value, current_value) %}
    case
        when {{ entry_value }} > 0 and {{ current_value }} > 0
        then ln({{ entry_value }} / {{ current_value }})
        else null
    end
{% endmacro %}

-- computes the return ratio: (current_value - initial_value) / initial_value
{% macro safe_return(current_column, initial_value, steps) %}
    case
        when {{ initial_value }} = 0
        then null
        else ({{ current_column }} - {{ initial_value }}) / {{ initial_value }}
    end
{% endmacro %}

-- computes the rolling avg of `column`, over `num_rows` rows, partitioned by
-- `partition_by`, and ordered by `order_by`
{% macro rolling_avg(column, partition_by, order_by, num_rows) %}
    avg({{ column }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }} rows between {{ num_rows }} preceding and current row
    )
{% endmacro %}

-- computes the volatility of `column`, over `num_rows` rows, partitioned by
-- `partition_by`, and ordered by `order_by`
{% macro volatility(column, partition_by, order_by, num_rows) %}
    stddev_samp({{ column }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }} rows between {{ num_rows }} preceding and current row
    )
{% endmacro %}


{% macro relative_strength_index(price_diff, partition_by, order_by, num_rows) %}
    (
        100
        - 100
        / (
            1 + avg(case when {{ price_diff }} > 0 then {{ price_diff }} else 0 end) over (
                partition by {{ partition_by }}
                order by {{ order_by }} rows between {{ num_rows }} preceding and current row
            )
            / avg(case when {{ price_diff }} < 0 then abs({{ price_diff }}) else 0 end) over (
                partition by {{ partition_by }}
                order by {{ order_by }} rows between {{ num_rows }} preceding and current row
            )
        )
    )
{% endmacro %}
