-- SQL-safe division, evaluating to NULL in case of division by 0
{% macro safe_div(numerator, denominator) %}
    CASE
        WHEN {{denominator}} = 0 THEN NULL
        ELSE {{numerator}} / {{denominator}}
    END
{% endmacro %}
