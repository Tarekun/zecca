{% macro safe_return(entry_value, current_value) %}
    CASE 
        WHEN {{entry_value}} > 0 AND {{current_value}} > 0
            THEN LN({{entry_value}} / {{current_value}})
        ELSE NULL
    END
{% endmacro %}
