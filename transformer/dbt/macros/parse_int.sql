-- Macro: parse_int
-- Descrição: Converte uma coluna string contendo apenas dígitos para inteiro. 
-- Retorna NULL caso o valor não seja numérico.

{% macro parse_int(column_name) -%}

    CASE
        WHEN {{ column_name }} ~ '^[0-9]+$'
        THEN {{ column_name }}::int
        ELSE NULL
    END

{%- endmacro %}
