{% macro normalize_hhmm(col_name) %}
    case
        when {{ col_name }} is null or {{ col_name }} = ''
            then null
        else lpad(
            regexp_replace({{ col_name }}::text, '\.0$', ''),
            4,
            '0'
        )
    end
{% endmacro %}

{% macro build_ts(date_col, hhmm_col) %}
    case
        when {{ hhmm_col }} is null
            then null

        -- Trata 2400 como mudança de dia
        when {{ hhmm_col }} = '2400'
            then (
                {{ date_col }}::date + interval '1 day'
            )::timestamp

        -- Trata valores inválidos
        when {{ hhmm_col }} !~ '^[0-2][0-9][0-5][0-9]$'
            then null

        else to_timestamp(
            {{ date_col }}::text || ' ' || {{ hhmm_col }},
            'YYYY-MM-DD HH24MI'
        )
    end
{% endmacro %}
