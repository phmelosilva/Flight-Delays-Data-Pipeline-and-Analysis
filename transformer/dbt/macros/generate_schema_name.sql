-- Macro: generate_schema_name
-- Descrição: Retorna o schema informado no config ou, caso ausente, utiliza o schema padrão do target.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}

{%- endmacro %}
