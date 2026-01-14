{% macro custom_macro(column_name) %}
    upper({{ column_name }})
{% endmacro %}