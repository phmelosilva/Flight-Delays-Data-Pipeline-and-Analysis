{% macro parse_int(column_name) %}
    case
        when {{ column_name }} ~ '^[0-9]+$'
            then {{ column_name }}::int
        else null
    end
{% endmacro %}
